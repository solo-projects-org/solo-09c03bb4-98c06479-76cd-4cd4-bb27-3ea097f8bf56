# 配送数仓列依赖分析服务

运单源表即将停供旧邮编列。本服务接收源表定义、各模型的 DuckDB SELECT 与待下线源列，
**只用 SQLGlot 解析、不执行任何 SQL**，沿别名、普通星号与非递归 CTE 跨模型追踪列引用，
按模型输出：

- 依赖该源列的**输出列**，每项附完整引用链；
- 依赖该源列的 **WHERE / JOIN ON（含 USING）/ QUALIFY 条件**，每项附引用链。
  条件依赖**不计入**输出列。

另提供承运商**签收更正批**接口（见下文「签收更正批 API」）：更正批晚到时先存不生效，
补齐缺口后按批号连续入仓，撤销与补签不按到达顺序入账。

## 签收更正批 API

承运商的签收更正批按承运商从 **1 连续编号**。每批提交若干票更正：
`waybill_no`（运单号，同批不得重复）、`prev_signed_at` / `new_signed_at`
（更正前/后签收时刻，UTC 秒，`null` 表示未签收）。

- `POST /corrections/batches` —— 提交一个更正批；
- `GET /corrections/{carrier}` —— 查询该承运商**同一时点**的入仓进度、下一批号、
  待处理/已退回批次与当前已签收明细。

处理规则：

- **越过缺口先存不生效**：批号跳过缺口的批次存为 `pending`；缺口补齐后，
  从「入仓进度 + 1」起按批号**连续入仓**。
- **入仓当时逐票核对**：每批入仓时，逐票核对批中声明的 `prev_signed_at`
  与仓内当前签收值。对不上就**退回该批**（`rejected`，响应含批号与冲突运单），
  已入仓的前序批不撤销，后续批继续等待；更正后可按原批号重交。
- **幂等**：待处理/已入仓的批次原样重传直接返回已存结果（`idempotent_replay=true`，
  不重算）；换内容则 `409` 拒绝。并发提交语义相同。已退回批次允许更正后重交。
- **持久化与原子性**：批次与签收事实保存在数据卷中的 SQLite
  （`WAREHOUSE_DB_PATH`，Compose 下为 `/data/warehouse.db`，卷 `dw-data`）；
  每批事实与入仓进度在同一事务里一起生效。

提交响应的 `status`：`pending`（待处理）/ `warehoused`（已入仓）/ `rejected`（已退回）。

### 样例流程（Swagger 可直接 Try it out）

`POST /corrections/batches` 的请求体自带两个样例，先交第 2 批、再补第 1 批：

```bash
# 1) 第 2 批先到：越过缺口，先存不生效（status=pending，入仓进度仍为 0）
curl -X POST http://localhost:8000/corrections/batches \
  -H 'Content-Type: application/json' \
  --data @examples/corrections_batch2_first.json

# 2) 补交第 1 批：触发 1、2 两批按批号连续入仓（applied_batches=[1,2]）
curl -X POST http://localhost:8000/corrections/batches \
  -H 'Content-Type: application/json' \
  --data @examples/corrections_batch1_fill.json

# 3) 查询：入仓进度 2、下一批号 3；WB1001 以第 2 批补签时刻为准，
#    WB1002 已被第 2 批撤销（不在已签收明细中）
curl http://localhost:8000/corrections/顺路快运
```

查询响应示例：

```json
{
  "carrier": "顺路快运",
  "warehoused_through": 2,
  "next_batch_no": 3,
  "pending_batches": [],
  "rejected_batches": [],
  "signed": [
    {"waybill_no": "WB1001", "signed_at": 1794557700, "source_batch_no": 2}
  ]
}
```

值班员可据 `next_batch_no` 与 `pending_batches` / `rejected_batches`
找出该催补或待更正的批次。

## 追踪规则

- 别名：`receiver_zip_old AS zip` 会把依赖从 `zip` 继续追到源列。
- 普通星号：`SELECT *`、`SELECT t.*` 按关系实际输出列展开；支持
  `* EXCLUDE (...)`（DuckDB 里 `EXCEPT` 同义）与 `* REPLACE (...)`。
- 非递归 CTE：CTE 可引用同模型内更早的 CTE，作用域链以 `模型.CTE.列` 呈现。
- 跨模型：模型可以引用其他模型的输出列，分析按依赖拓扑顺序进行，链上保留模型跳。
- 名称匹配大小写不敏感，源表可带 catalog/schema 前缀（按表名匹配）。
- 引用链自当前模型的输出列/条件列起，到 `源表.源列` 止。

## 明确不支持（提交即 400 并指出所在模型）

- 聚合函数、`GROUP BY`、`HAVING`；
- 集合运算 `UNION / INTERSECT / EXCEPT`；
- CTE 之外的子查询（FROM 子查询、标量子查询、`EXISTS` 等）；
- 递归 CTE（`WITH RECURSIVE`）、`LATERAL`、`UNNEST`/表函数、`NATURAL JOIN`。

## 找不到或有歧义的引用（400，指出模型）

- 列在当前查询所有关系中都不存在；
- 未限定的列名可由多个关系提供（歧义，要求加表别名）；
- 表别名不存在、表/CTE 未定义、输出列重名等。

## 启动（Docker Compose）

```bash
docker compose up --build
# 服务: http://localhost:8000
# Swagger: http://localhost:8000/docs
# OpenAPI: http://localhost:8000/openapi.json
```

Swagger 的 `POST /analyze` 请求体自带"邮编改名后仍用于筛选"的样例，可直接 Try it out。

## 请求示例

`examples/request_zip_rename.json` 是完整样例（也可用 Swagger 内置样例）：

```bash
curl -X POST http://localhost:8000/analyze \
  -H 'Content-Type: application/json' \
  --data @examples/request_zip_rename.json
```

请求结构：

```json
{
  "sources": [
    {"name": "waybills", "columns": ["waybill_id", "receiver_zip_old", "city_id"]}
  ],
  "models": [
    {"name": "stg_waybill", "sql": "WITH cleaned AS (SELECT waybill_id, receiver_zip_old AS zip FROM waybills WHERE receiver_zip_old IS NOT NULL) SELECT * FROM cleaned"}
  ],
  "targets": [
    {"table": "waybills", "column": "receiver_zip_old"}
  ]
}
```

响应结构（按模型分组）：

```json
{
  "targets": [{"table": "waybills", "column": "receiver_zip_old"}],
  "models": {
    "stg_waybill": {
      "dependent_output_columns": [
        {
          "output_column": "zip",
          "reference_chains": [
            ["stg_waybill.zip", "stg_waybill.cleaned.zip", "waybills.receiver_zip_old"]
          ]
        }
      ],
      "filter_conditions": [
        {
          "type": "WHERE",
          "scope": "stg_waybill.cleaned",
          "expression": "receiver_zip_old IS NOT NULL",
          "column": "receiver_zip_old",
          "reference_chain": ["stg_waybill.cleaned.zip", "waybills.receiver_zip_old"]
        }
      ]
    }
  }
}
```

错误响应统一为 `400`，`detail` 是数组，每项含 `model`、`location`、`message`，
一次返回收集到的全部问题。

## 本地开发（不用 Docker）

```bash
python -m venv --without-pip .venv   # 若系统带 ensurepip 可直接 python -m venv .venv
.venv/bin/pip install -r requirements.txt
PYTHONPATH=. .venv/bin/uvicorn app.main:app --reload
.venv/bin/pytest                       # 54 个测试
```

## 目录

```
app/
  main.py        FastAPI 路由、请求/响应模型、Swagger 样例与入参校验（含签收更正批 API）
  lineage.py     SQLGlot 解析与列级血缘分析（不执行 SQL）
  warehouse.py   签收更正批的 SQLite 存储与按批号连续入仓处理
  tests/         分析器、API 与更正批测试
examples/        邮编改名场景的完整请求样例 + 签收更正批样例（先交第 2 批、再补第 1 批）
Dockerfile, docker-compose.yml（含 dw-data 数据卷）, requirements.txt
```
