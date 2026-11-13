"""签收更正批的 SQLite 存储与按批号连续入仓处理。

业务规则：

- 每个承运商的批号从 1 连续编号；同批运单号不得重复（由 API 层校验）。
- 越过缺口的批次先存为 ``pending``（待处理），不生效；缺口补齐后，
  从「入仓进度 + 1」起按批号连续核对、入仓。
- 每批入仓时逐票核对「更正前签收时刻」与仓内**入仓当时**的当前值；
  对不上则退回该批（``rejected``，记录冲突运单），已入仓的前序批不撤销，
  后续批继续等待。已退回批次允许更正后重交（覆盖原内容并重新处理）。
- 待处理 / 已入仓的批次同内容重传直接返回已存结果（不重算）；
  换内容则拒绝（``BatchContentConflict``）。并发提交靠进程锁 +
  SQLite ``BEGIN IMMEDIATE`` 与主键约束串行化，语义一致。
- 每批的签收事实与入仓进度在同一事务里一起生效。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    carrier      TEXT    NOT NULL,
    batch_no     INTEGER NOT NULL,
    payload      TEXT    NOT NULL,              -- 规范化后的批次内容（幂等比对用）
    status       TEXT    NOT NULL
                 CHECK (status IN ('pending', 'warehoused', 'rejected')),
    conflicts    TEXT,                          -- 退回时的逐票冲突（JSON）
    received_at  TEXT    NOT NULL,
    processed_at TEXT,
    PRIMARY KEY (carrier, batch_no)
);
CREATE TABLE IF NOT EXISTS batch_items (
    carrier        TEXT    NOT NULL,
    batch_no       INTEGER NOT NULL,
    seq            INTEGER NOT NULL,            -- 票在批内的到达顺序
    waybill_no     TEXT    NOT NULL,
    prev_signed_at INTEGER,                     -- 更正前签收时刻（UTC 秒），NULL = 未签收
    new_signed_at  INTEGER,                     -- 更正后签收时刻（UTC 秒），NULL = 撤销签收
    PRIMARY KEY (carrier, batch_no, waybill_no),
    FOREIGN KEY (carrier, batch_no) REFERENCES batches (carrier, batch_no)
);
CREATE TABLE IF NOT EXISTS signing_facts (
    carrier         TEXT    NOT NULL,
    waybill_no      TEXT    NOT NULL,
    signed_at       INTEGER,                    -- 当前签收时刻（UTC 秒），NULL = 未签收
    source_batch_no INTEGER NOT NULL,           -- 产生该事实的批号
    updated_at      TEXT    NOT NULL,
    PRIMARY KEY (carrier, waybill_no)
);
CREATE TABLE IF NOT EXISTS carrier_progress (
    carrier            TEXT PRIMARY KEY,
    warehoused_through INTEGER NOT NULL         -- 入仓进度：已连续入仓到的批号
);
"""

STATUS_PENDING = "pending"
STATUS_WAREHOUSED = "warehoused"
STATUS_REJECTED = "rejected"


@dataclass(frozen=True)
class CorrectionItemInput:
    """一票签收更正：更正前/后签收时刻（UTC 秒，None 表示未签收）。"""

    waybill_no: str
    prev_signed_at: Optional[int]
    new_signed_at: Optional[int]


class BatchContentConflict(Exception):
    """同一 (carrier, batch_no) 已存在且内容不同，而原批次不允许重写。"""

    def __init__(self, carrier: str, batch_no: int, status: str):
        self.carrier = carrier
        self.batch_no = batch_no
        self.status = status
        super().__init__(
            f"批次 {carrier} 第 {batch_no} 批已存在（状态 {status}），"
            "内容不一致，拒绝覆盖")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z")


def _canonical_payload(items: list[CorrectionItemInput]) -> str:
    """批次内容的规范化表示：与票的书写顺序无关，用于幂等比对。"""
    norm = [
        {"w": it.waybill_no, "p": it.prev_signed_at, "n": it.new_signed_at}
        for it in items
    ]
    norm.sort(key=lambda d: d["w"])
    return json.dumps(norm, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class Warehouse:
    """批次与签收事实的 SQLite 存储（数据卷持久化）。"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        parent = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(parent, exist_ok=True)
        self._lock = threading.Lock()  # 进程内写串行化；跨进程由 SQLite 锁保证
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA)
            conn.commit()
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    # ------------------------------------------------------------ 提交批次
    def submit_batch(self, carrier: str, batch_no: int,
                     items: list[CorrectionItemInput]) -> dict:
        if batch_no < 1:
            raise ValueError("batch_no 必须 >= 1")
        payload = _canonical_payload(items)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                now = _utcnow()
                row = conn.execute(
                    "SELECT payload, status FROM batches"
                    " WHERE carrier=? AND batch_no=?",
                    (carrier, batch_no)).fetchone()
                replay = False
                if row is not None and row["payload"] == payload:
                    # 原样重传：不重算，直接返回已存结果
                    replay = True
                elif row is not None and row["status"] in (
                        STATUS_PENDING, STATUS_WAREHOUSED):
                    # 待处理 / 已入仓的批次换内容：拒绝
                    raise BatchContentConflict(carrier, batch_no, row["status"])
                else:
                    if row is not None:
                        # 已退回批次更正后重交：覆盖原内容，重新排队
                        conn.execute(
                            "DELETE FROM batch_items"
                            " WHERE carrier=? AND batch_no=?",
                            (carrier, batch_no))
                        conn.execute(
                            "UPDATE batches SET payload=?, status=?, conflicts=NULL,"
                            " received_at=?, processed_at=NULL"
                            " WHERE carrier=? AND batch_no=?",
                            (payload, STATUS_PENDING, now, carrier, batch_no))
                    else:
                        conn.execute(
                            "INSERT INTO batches"
                            " (carrier, batch_no, payload, status, received_at)"
                            " VALUES (?,?,?,?,?)",
                            (carrier, batch_no, payload, STATUS_PENDING, now))
                    self._insert_items(conn, carrier, batch_no, items)

                applied: list[int] = []
                chain_rejection: Optional[dict] = None
                if not replay:
                    through, applied, chain_rejection = self._process_chain(
                        conn, carrier, now)
                else:
                    through = self._current_through(conn, carrier)

                final = conn.execute(
                    "SELECT status, conflicts FROM batches"
                    " WHERE carrier=? AND batch_no=?",
                    (carrier, batch_no)).fetchone()
                # 事实、批次状态与入仓进度在同一事务里一起生效
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

        conflicts = json.loads(final["conflicts"]) if final["conflicts"] else []
        return {
            "carrier": carrier,
            "batch_no": batch_no,
            "status": final["status"],
            "idempotent_replay": replay,
            "conflicts": conflicts,
            "applied_batches": applied,
            "chain_rejection": chain_rejection,
            "warehoused_through": through,
            "next_batch_no": through + 1,
        }

    def _insert_items(self, conn: sqlite3.Connection, carrier: str, batch_no: int,
                      items: list[CorrectionItemInput]) -> None:
        conn.executemany(
            "INSERT INTO batch_items"
            " (carrier, batch_no, seq, waybill_no, prev_signed_at, new_signed_at)"
            " VALUES (?,?,?,?,?,?)",
            [(carrier, batch_no, seq, it.waybill_no, it.prev_signed_at,
              it.new_signed_at) for seq, it in enumerate(items)])

    def _current_through(self, conn: sqlite3.Connection, carrier: str) -> int:
        row = conn.execute(
            "SELECT warehoused_through FROM carrier_progress WHERE carrier=?",
            (carrier,)).fetchone()
        return row["warehoused_through"] if row else 0

    def _process_chain(self, conn: sqlite3.Connection, carrier: str,
                       now: str) -> tuple[int, list[int], Optional[dict]]:
        """从入仓进度 +1 起连续处理待处理批：核对通过则入仓，否则退回并停止。"""
        through = self._current_through(conn, carrier)
        applied: list[int] = []
        rejection: Optional[dict] = None
        while True:
            nxt = through + 1
            row = conn.execute(
                "SELECT status FROM batches WHERE carrier=? AND batch_no=?",
                (carrier, nxt)).fetchone()
            if row is None or row["status"] != STATUS_PENDING:
                break  # 缺口或已退回的批：后续批继续等待
            conflicts = self._verify(conn, carrier, nxt)
            if conflicts:
                conn.execute(
                    "UPDATE batches SET status=?, conflicts=?, processed_at=?"
                    " WHERE carrier=? AND batch_no=?",
                    (STATUS_REJECTED, json.dumps(conflicts, ensure_ascii=False),
                     now, carrier, nxt))
                rejection = {"batch_no": nxt, "conflicts": conflicts}
                break
            self._apply(conn, carrier, nxt, now)
            through = nxt
            applied.append(nxt)
        conn.execute(
            "INSERT INTO carrier_progress (carrier, warehoused_through)"
            " VALUES (?,?)"
            " ON CONFLICT(carrier) DO UPDATE SET"
            " warehoused_through=excluded.warehoused_through",
            (carrier, through))
        return through, applied, rejection

    def _verify(self, conn: sqlite3.Connection, carrier: str,
                batch_no: int) -> list[dict]:
        """逐票核对批中声明的更正前值与仓内入仓当时的当前值。"""
        rows = conn.execute(
            "SELECT waybill_no, prev_signed_at FROM batch_items"
            " WHERE carrier=? AND batch_no=? ORDER BY seq",
            (carrier, batch_no)).fetchall()
        conflicts: list[dict] = []
        for r in rows:
            cur = conn.execute(
                "SELECT signed_at FROM signing_facts"
                " WHERE carrier=? AND waybill_no=?",
                (carrier, r["waybill_no"])).fetchone()
            actual = cur["signed_at"] if cur is not None else None
            if actual != r["prev_signed_at"]:
                conflicts.append({
                    "waybill_no": r["waybill_no"],
                    "expected_prev_signed_at": r["prev_signed_at"],
                    "actual_signed_at": actual,
                })
        return conflicts

    def _apply(self, conn: sqlite3.Connection, carrier: str, batch_no: int,
               now: str) -> None:
        rows = conn.execute(
            "SELECT waybill_no, new_signed_at FROM batch_items"
            " WHERE carrier=? AND batch_no=? ORDER BY seq",
            (carrier, batch_no)).fetchall()
        for r in rows:
            conn.execute(
                "INSERT INTO signing_facts"
                " (carrier, waybill_no, signed_at, source_batch_no, updated_at)"
                " VALUES (?,?,?,?,?)"
                " ON CONFLICT(carrier, waybill_no) DO UPDATE SET"
                " signed_at=excluded.signed_at,"
                " source_batch_no=excluded.source_batch_no,"
                " updated_at=excluded.updated_at",
                (carrier, r["waybill_no"], r["new_signed_at"], batch_no, now))
        conn.execute(
            "UPDATE batches SET status=?, conflicts=NULL, processed_at=?"
            " WHERE carrier=? AND batch_no=?",
            (STATUS_WAREHOUSED, now, carrier, batch_no))

    # ------------------------------------------------------------ 状态查询
    def carrier_state(self, carrier: str) -> dict:
        """同一时点（单事务快照）的入仓进度、下一批号与当前已签收明细。"""
        conn = self._connect()
        try:
            conn.execute("BEGIN")
            through = self._current_through(conn, carrier)
            pending = [r["batch_no"] for r in conn.execute(
                "SELECT batch_no FROM batches"
                " WHERE carrier=? AND status=? ORDER BY batch_no",
                (carrier, STATUS_PENDING)).fetchall()]
            rejected = [
                {"batch_no": r["batch_no"],
                 "conflicts": json.loads(r["conflicts"])}
                for r in conn.execute(
                    "SELECT batch_no, conflicts FROM batches"
                    " WHERE carrier=? AND status=? ORDER BY batch_no",
                    (carrier, STATUS_REJECTED)).fetchall()]
            signed = [
                {"waybill_no": r["waybill_no"],
                 "signed_at": r["signed_at"],
                 "source_batch_no": r["source_batch_no"]}
                for r in conn.execute(
                    "SELECT waybill_no, signed_at, source_batch_no"
                    " FROM signing_facts"
                    " WHERE carrier=? AND signed_at IS NOT NULL"
                    " ORDER BY waybill_no",
                    (carrier,)).fetchall()]
            conn.commit()
        finally:
            conn.close()
        return {
            "carrier": carrier,
            "warehoused_through": through,
            "next_batch_no": through + 1,
            "pending_batches": pending,
            "rejected_batches": rejected,
            "signed": signed,
        }
