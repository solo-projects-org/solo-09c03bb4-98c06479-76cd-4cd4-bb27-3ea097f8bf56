"""签收更正批 API 与仓储层测试。"""
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from app.main import app, get_warehouse
from app.warehouse import (
    BatchContentConflict,
    CorrectionItemInput,
    Warehouse,
)

CARRIER = "顺路快运"
T1 = 1794470400  # 2026-11-12T08:00:00Z
T2 = 1794475800  # 2026-11-12T09:30:00Z
T3 = 1794557700  # 2026-11-13T08:15:00Z

BATCH1 = {
    "carrier": CARRIER,
    "batch_no": 1,
    "items": [
        {"waybill_no": "WB1001", "prev_signed_at": None, "new_signed_at": T1},
        {"waybill_no": "WB1002", "prev_signed_at": None, "new_signed_at": T2},
        {"waybill_no": "WB1003", "prev_signed_at": None, "new_signed_at": None},
    ],
}
BATCH2 = {
    "carrier": CARRIER,
    "batch_no": 2,
    "items": [
        {"waybill_no": "WB1001", "prev_signed_at": T1, "new_signed_at": T3},
        {"waybill_no": "WB1002", "prev_signed_at": T2, "new_signed_at": None},
    ],
}


@pytest.fixture()
def client(tmp_path):
    warehouse = Warehouse(str(tmp_path / "warehouse.db"))
    app.dependency_overrides[get_warehouse] = lambda: warehouse
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def _items(*tuples):
    return [CorrectionItemInput(w, p, n) for w, p, n in tuples]


# ------------------------------------------------------------------ API 流程

def test_swagger_flow_batch2_first_then_batch1(client):
    # 先交第 2 批：越过缺口，先存不生效
    resp = client.post("/corrections/batches", json=BATCH2)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "pending"
    assert body["applied_batches"] == []
    assert body["warehoused_through"] == 0
    assert body["next_batch_no"] == 1

    state = client.get(f"/corrections/{CARRIER}").json()
    assert state["warehoused_through"] == 0
    assert state["next_batch_no"] == 1
    assert state["pending_batches"] == [2]
    assert state["signed"] == []

    # 再补第 1 批：两批按批号连续入仓
    resp = client.post("/corrections/batches", json=BATCH1)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "warehoused"
    assert body["applied_batches"] == [1, 2]
    assert body["warehoused_through"] == 2
    assert body["next_batch_no"] == 3

    # 查询结果：撤销与补签按批号顺序入账，而非到达顺序
    state = client.get(f"/corrections/{CARRIER}").json()
    assert state["warehoused_through"] == 2
    assert state["next_batch_no"] == 3
    assert state["pending_batches"] == []
    assert state["signed"] == [
        {"waybill_no": "WB1001", "signed_at": T3, "source_batch_no": 2},
    ]


def test_revocation_and_resign_follow_batch_order_not_arrival_order(client):
    # 第 2 批先到达（含撤销 WB1002、补签 WB1001），第 1 批后补
    assert client.post("/corrections/batches", json=BATCH2).json()["status"] == "pending"
    client.post("/corrections/batches", json=BATCH1)
    state = client.get(f"/corrections/{CARRIER}").json()
    signed = {row["waybill_no"]: row["signed_at"] for row in state["signed"]}
    # WB1002 被第 2 批撤销 -> 不在已签收明细；WB1001 以补签时刻为准
    assert signed == {"WB1001": T3}


def test_prev_value_checked_at_warehousing_time(client):
    # 第 2 批声明的更正前值只有在第 1 批入仓后才成立：入仓当时核对，不是到达时核对
    resp = client.post("/corrections/batches", json=BATCH2)
    assert resp.json()["status"] == "pending"  # 到达时不核对
    resp = client.post("/corrections/batches", json=BATCH1)
    assert resp.json()["applied_batches"] == [1, 2]  # 入仓时核对通过


def test_mismatch_rejects_batch_and_reports_conflicts(client):
    client.post("/corrections/batches", json=BATCH1)
    bad = {
        "carrier": CARRIER,
        "batch_no": 2,
        "items": [
            {"waybill_no": "WB1001", "prev_signed_at": 999, "new_signed_at": T3},
            {"waybill_no": "WB9999", "prev_signed_at": T1, "new_signed_at": T3},
        ],
    }
    resp = client.post("/corrections/batches", json=bad)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "rejected"
    assert body["warehoused_through"] == 1  # 已入仓的前序批不撤销
    conflicts = {c["waybill_no"]: c for c in body["conflicts"]}
    assert conflicts["WB1001"] == {
        "waybill_no": "WB1001",
        "expected_prev_signed_at": 999,
        "actual_signed_at": T1,
    }
    assert conflicts["WB9999"]["actual_signed_at"] is None

    # 后续批继续等待
    client.post("/corrections/batches", json={
        "carrier": CARRIER, "batch_no": 3,
        "items": [{"waybill_no": "WB1003", "prev_signed_at": None,
                   "new_signed_at": T3}],
    })
    state = client.get(f"/corrections/{CARRIER}").json()
    assert state["warehoused_through"] == 1
    assert state["next_batch_no"] == 2
    assert state["pending_batches"] == [3]
    assert [b["batch_no"] for b in state["rejected_batches"]] == [2]


def test_rejected_batch_can_be_corrected_and_resubmitted(client):
    client.post("/corrections/batches", json=BATCH1)
    bad = {
        "carrier": CARRIER, "batch_no": 2,
        "items": [{"waybill_no": "WB1001", "prev_signed_at": 999,
                   "new_signed_at": T3}],
    }
    assert client.post("/corrections/batches", json=bad).json()["status"] == "rejected"
    client.post("/corrections/batches", json={
        "carrier": CARRIER, "batch_no": 3,
        "items": [{"waybill_no": "WB1003", "prev_signed_at": None,
                   "new_signed_at": T3}],
    })
    # 更正后重交第 2 批：链恢复，第 3 批跟着入仓
    resp = client.post("/corrections/batches", json={
        "carrier": CARRIER, "batch_no": 2,
        "items": [{"waybill_no": "WB1001", "prev_signed_at": T1,
                   "new_signed_at": T3}],
    })
    body = resp.json()
    assert body["status"] == "warehoused"
    assert body["applied_batches"] == [2, 3]
    state = client.get(f"/corrections/{CARRIER}").json()
    assert state["warehoused_through"] == 3
    assert state["next_batch_no"] == 4


def test_identical_replay_is_not_recomputed(client):
    # 待处理批次原样重传
    first = client.post("/corrections/batches", json=BATCH2).json()
    replay = client.post("/corrections/batches", json=BATCH2).json()
    assert replay["status"] == "pending"
    assert replay["idempotent_replay"] is True
    assert first["idempotent_replay"] is False

    client.post("/corrections/batches", json=BATCH1)
    # 已入仓批次原样重传：不重算（此时仓内值已变，若重算必失败）
    replay = client.post("/corrections/batches", json=BATCH1).json()
    assert replay["status"] == "warehoused"
    assert replay["idempotent_replay"] is True
    assert replay["applied_batches"] == []
    state = client.get(f"/corrections/{CARRIER}").json()
    assert state["warehoused_through"] == 2


def test_changed_content_on_existing_batch_rejected_409(client):
    client.post("/corrections/batches", json=BATCH2)  # pending
    changed = dict(BATCH2, items=[dict(BATCH2["items"][0])])
    resp = client.post("/corrections/batches", json=changed)
    assert resp.status_code == 409
    assert resp.json()["detail"]["existing_status"] == "pending"

    client.post("/corrections/batches", json=BATCH1)  # 1、2 均入仓
    resp = client.post("/corrections/batches", json=changed)
    assert resp.status_code == 409
    assert resp.json()["detail"]["existing_status"] == "warehoused"


def test_duplicate_waybill_in_batch_returns_400(client):
    resp = client.post("/corrections/batches", json={
        "carrier": CARRIER,
        "batch_no": 1,
        "items": [
            {"waybill_no": "WB1", "prev_signed_at": None, "new_signed_at": T1},
            {"waybill_no": "WB1", "prev_signed_at": None, "new_signed_at": T2},
        ],
    })
    assert resp.status_code == 400
    assert "重复" in resp.json()["detail"][0]["message"]


def test_invalid_batch_no_returns_422(client):
    resp = client.post("/corrections/batches", json={
        "carrier": CARRIER, "batch_no": 0,
        "items": [{"waybill_no": "WB1", "prev_signed_at": None,
                   "new_signed_at": T1}],
    })
    assert resp.status_code == 422


def test_unknown_carrier_state_is_zeroed(client):
    state = client.get("/corrections/从未见过的承运商").json()
    assert state["warehoused_through"] == 0
    assert state["next_batch_no"] == 1
    assert state["pending_batches"] == []
    assert state["rejected_batches"] == []
    assert state["signed"] == []


def test_carriers_are_isolated(client):
    client.post("/corrections/batches", json=BATCH1)
    other = client.get("/corrections/另一家").json()
    assert other["warehoused_through"] == 0
    assert other["signed"] == []


def test_swagger_examples_and_analyze_still_present(client):
    schema = client.get("/openapi.json").json()
    assert "/corrections/batches" in schema["paths"]
    assert "/corrections/{carrier}" in schema["paths"]
    assert "/analyze" in schema["paths"]
    examples = (schema["paths"]["/corrections/batches"]["post"]
                ["requestBody"]["content"]["application/json"]["examples"])
    values = [e["value"] for e in examples.values()]
    assert any(v["batch_no"] == 2 for v in values)
    assert any(v["batch_no"] == 1 for v in values)


# ------------------------------------------------------------------ 仓储层

def test_persistence_across_reopen(tmp_path):
    db = str(tmp_path / "warehouse.db")
    wh = Warehouse(db)
    wh.submit_batch(CARRIER, 1, _items(("WB1", None, T1)))
    reopened = Warehouse(db)
    state = reopened.carrier_state(CARRIER)
    assert state["warehoused_through"] == 1
    assert state["signed"] == [
        {"waybill_no": "WB1", "signed_at": T1, "source_batch_no": 1}]
    # 重开后幂等仍然有效
    result = reopened.submit_batch(CARRIER, 1, _items(("WB1", None, T1)))
    assert result["idempotent_replay"] is True
    assert result["status"] == "warehoused"


def test_item_order_does_not_affect_idempotency(tmp_path):
    wh = Warehouse(str(tmp_path / "w.db"))
    first = wh.submit_batch(CARRIER, 1, _items(("WB1", None, T1), ("WB2", None, T2)))
    replay = wh.submit_batch(CARRIER, 1, _items(("WB2", None, T2), ("WB1", None, T1)))
    assert first["idempotent_replay"] is False
    assert replay["idempotent_replay"] is True


def test_concurrent_identical_submissions(tmp_path):
    wh = Warehouse(str(tmp_path / "w.db"))
    items = _items(("WB1", None, T1), ("WB2", None, T2))
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(
            lambda _: wh.submit_batch(CARRIER, 1, items), range(16)))
    assert all(r["status"] == "warehoused" for r in results)
    assert sum(1 for r in results if not r["idempotent_replay"]) == 1
    state = wh.carrier_state(CARRIER)
    assert state["warehoused_through"] == 1
    assert len(state["signed"]) == 2


def test_concurrent_conflicting_submissions(tmp_path):
    wh = Warehouse(str(tmp_path / "w.db"))
    variants = [
        _items(("WB1", None, T1)),
        _items(("WB1", None, T2)),
        _items(("WB1", None, T3)),
    ]

    def submit(items):
        try:
            return ("ok", wh.submit_batch(CARRIER, 1, items))
        except BatchContentConflict:
            return ("conflict", None)

    with ThreadPoolExecutor(max_workers=6) as pool:
        outcomes = list(pool.map(submit, variants * 4))
    oks = [o for o in outcomes if o[0] == "ok"]
    conflicts = [o for o in outcomes if o[0] == "conflict"]
    # 只有一个内容胜出；同内容的并发重传算幂等回放，其余一律拒绝
    winning = [r for _, r in oks if not r["idempotent_replay"]]
    assert len(winning) == 1
    assert len(conflicts) > 0
    assert wh.carrier_state(CARRIER)["warehoused_through"] == 1


def test_concurrent_gap_fill_and_submit(tmp_path):
    wh = Warehouse(str(tmp_path / "w.db"))
    wh.submit_batch(CARRIER, 2, _items(("WB1", T1, T3)))
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(
            lambda _: wh.submit_batch(CARRIER, 1, _items(("WB1", None, T1))),
            range(8)))
    assert all(r["status"] == "warehoused" for r in results)
    state = wh.carrier_state(CARRIER)
    assert state["warehoused_through"] == 2
    assert state["signed"] == [
        {"waybill_no": "WB1", "signed_at": T3, "source_batch_no": 2}]
