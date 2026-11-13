"""FastAPI service: column-level dependency analysis for decommissioned source columns.

 additionally exposes the carrier signed-receipt correction batch APIs
 (签收更正批) backed by SQLite in the data volume.
"""
from __future__ import annotations

import os
from typing import Optional

from fastapi import Body, Depends, FastAPI, HTTPException
from pydantic import BaseModel, Field

from .lineage import AggregatedAnalysisError, AnalysisError, Analyzer
from .warehouse import (
    STATUS_PENDING,
    STATUS_REJECTED,
    STATUS_WAREHOUSED,
    BatchContentConflict,
    CorrectionItemInput,
    Warehouse,
)

app = FastAPI(
    title="配送数仓列依赖分析服务",
    description=(
        "基于 SQLGlot 解析（不执行 SQL），沿别名、普通星号与非递归 CTE 跨模型追踪"
        "待下线源列的引用，按模型输出依赖该列的输出列以及 WHERE / JOIN ON 条件，"
        "每项附引用链。不支持聚合、集合运算与 CTE 之外的子查询。\n\n"
        "另提供承运商**签收更正批**接口：批号按承运商从 1 连续编号，越过缺口的批次"
        "先存不生效，补齐后按批号连续入仓；每批入仓时逐票核对更正前签收值，"
        "对不上则退回该批并允许更正后重交；批次与签收事实存于数据卷中的 SQLite。"
    ),
    version="1.1.0",
)


class SourceTable(BaseModel):
    name: str = Field(..., description="源表名（大小写不敏感）")
    columns: list[str] = Field(..., description="源表当前全部列名")


class ModelSpec(BaseModel):
    name: str = Field(..., description="模型名称")
    sql: str = Field(..., description="该模型的 DuckDB SELECT 语句（可含非递归 CTE）")


class TargetColumn(BaseModel):
    table: str = Field(..., description="待下线列所在源表")
    column: str = Field(..., description="待下线的源列名（旧邮编列）")


class AnalyzeRequest(BaseModel):
    sources: list[SourceTable] = Field(..., description="源表及其列名")
    models: list[ModelSpec] = Field(..., description="各模型的名称和 DuckDB SELECT")
    target_columns: list[TargetColumn] = Field(
        ..., alias="targets", description="待下线的源列列表")

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {
            "examples": [
                {
                    "sources": [
                        {
                            "name": "waybills",
                            "columns": [
                                "waybill_id", "receiver_zip_old", "city_id", "weight_kg",
                            ],
                        },
                        {
                            "name": "dim_delivery_zone",
                            "columns": ["zone_id", "zip_code", "zone_name"],
                        },
                    ],
                    "models": [
                        {
                            "name": "stg_waybill",
                            "sql": (
                                "WITH cleaned AS (\n"
                                "  SELECT waybill_id, receiver_zip_old AS zip, city_id\n"
                                "  FROM waybills\n"
                                "  WHERE receiver_zip_old IS NOT NULL\n"
                                ")\n"
                                "SELECT waybill_id, zip, city_id\n"
                                "FROM cleaned"
                            ),
                        },
                        {
                            "name": "dwd_delivery_fact",
                            "sql": (
                                "SELECT s.waybill_id,\n"
                                "       s.zip AS receiver_zip,\n"
                                "       z.zone_name,\n"
                                "       s.weight_kg * 1000 AS weight_g\n"
                                "FROM stg_waybill s\n"
                                "JOIN dim_delivery_zone z ON s.zip = z.zip_code\n"
                                "WHERE s.zip LIKE '10%'"
                            ),
                        },
                    ],
                    "targets": [
                        {"table": "waybills", "column": "receiver_zip_old"}
                    ],
                }
            ]
        },
    }


class OutputDependency(BaseModel):
    output_column: str
    reference_chains: list[list[str]] = Field(
        ..., description="每条链自当前模型输出列起，到源表列止")


class ConditionDependency(BaseModel):
    type: str = Field(..., description="WHERE / JOIN ON / JOIN USING / QUALIFY")
    scope: str = Field(..., description="条件所在的模型或 CTE 作用域")
    expression: str
    column: str
    reference_chain: list[str]


class ModelResult(BaseModel):
    dependent_output_columns: list[OutputDependency]
    filter_conditions: list[ConditionDependency]


class AnalyzeResponse(BaseModel):
    targets: list[TargetColumn]
    models: dict[str, ModelResult]


class ErrorItem(BaseModel):
    model: Optional[str] = None
    location: Optional[str] = None
    message: str


class ErrorResponse(BaseModel):
    detail: list[ErrorItem]


@app.post(
    "/analyze",
    response_model=AnalyzeResponse,
    responses={400: {"model": ErrorResponse, "description": "解析、引用或不支持构造错误"}},
    summary="分析待下线源列在各模型中的依赖",
)
def analyze(request: AnalyzeRequest) -> AnalyzeResponse:
    source_names = [s.name for s in request.sources]
    dup_sources = {n for n in source_names if source_names.count(n) > 1}
    if dup_sources:
        raise HTTPException(400, [{"message": f"源表名重复: {sorted(dup_sources)}"}])
    model_names = [m.name for m in request.models]
    dup_models = {n for n in model_names if model_names.count(n) > 1}
    if dup_models:
        raise HTTPException(400, [{"message": f"模型名重复: {sorted(dup_models)}"}])
    if not request.target_columns:
        raise HTTPException(400, [{"message": "targets 不能为空"}])

    sources = {s.name: s.columns for s in request.sources}
    source_lookup = {
        name.lower(): {col.lower(): col for col in columns}
        for name, columns in sources.items()
    }
    target_tuples = [(t.table, t.column) for t in request.target_columns]
    unknown = [
        {"message": f"待下线列 {t.table}.{t.column} 在源表定义中不存在"}
        for t in request.target_columns
        if t.column.lower() not in source_lookup.get(t.table.lower(), {})
    ]
    if unknown:
        raise HTTPException(400, unknown)

    analyzer = Analyzer(
        sources=sources,
        models={m.name: m.sql for m in request.models},
        targets=target_tuples,
    )
    try:
        per_model = analyzer.run()
    except AggregatedAnalysisError as exc:
        raise HTTPException(400, [
            {"model": e.model, "location": e.location, "message": e.message}
            for e in exc.errors
        ])
    except AnalysisError as exc:
        raise HTTPException(400, [{
            "model": exc.model, "location": exc.location, "message": exc.message}])

    return AnalyzeResponse(
        targets=request.target_columns,
        models=per_model,
    )


@app.get("/healthz", include_in_schema=False)
def healthz() -> dict:
    return {"status": "ok"}


# --------------------------------------------------------------------------
# 签收更正批（carrier signed-receipt correction batches）
# --------------------------------------------------------------------------

def get_warehouse() -> Warehouse:
    """按环境变量 WAREHOUSE_DB_PATH 懒加载数据卷中的 SQLite 仓库。"""
    warehouse = getattr(app.state, "warehouse", None)
    if warehouse is None:
        warehouse = Warehouse(os.environ.get("WAREHOUSE_DB_PATH", "warehouse.db"))
        app.state.warehouse = warehouse
    return warehouse


class CorrectionItem(BaseModel):
    waybill_no: str = Field(..., min_length=1, description="运单号（同批不得重复）")
    prev_signed_at: Optional[int] = Field(
        ..., description="更正前签收时刻（UTC 秒），null 表示此前未签收；"
        "入仓时须与仓内当前值逐票核对一致")
    new_signed_at: Optional[int] = Field(
        ..., description="更正后签收时刻（UTC 秒），null 表示更正为未签收（撤销签收）")


class CorrectionBatchRequest(BaseModel):
    carrier: str = Field(..., min_length=1, description="承运商")
    batch_no: int = Field(..., ge=1, description="批号（该承运商从 1 连续编号）")
    items: list[CorrectionItem] = Field(
        ..., min_length=1, description="本批更正明细（每票一条）")


class ConflictItem(BaseModel):
    waybill_no: str = Field(..., description="核对不通过的运单号")
    expected_prev_signed_at: Optional[int] = Field(
        ..., description="批中声明的更正前签收时刻（UTC 秒，null 表示未签收）")
    actual_signed_at: Optional[int] = Field(
        ..., description="入仓当时仓内实际签收时刻（UTC 秒，null 表示未签收）")


class ChainRejection(BaseModel):
    batch_no: int = Field(..., description="被退回的批号")
    conflicts: list[ConflictItem] = Field(..., description="该批的逐票冲突")


class CorrectionBatchResponse(BaseModel):
    carrier: str
    batch_no: int
    status: str = Field(
        ..., description=f"{STATUS_PENDING}=待处理（越过缺口先存，未生效）/ "
        f"{STATUS_WAREHOUSED}=已入仓 / {STATUS_REJECTED}=已退回（可更正后重交）")
    idempotent_replay: bool = Field(
        ..., description="是否为同内容原样重传（直接返回已存结果，未重算）")
    conflicts: list[ConflictItem] = Field(
        default_factory=list, description="本批被退回时的逐票冲突")
    applied_batches: list[int] = Field(
        default_factory=list, description="本次调用触发连续入仓的批号（按入仓顺序）")
    chain_rejection: Optional[ChainRejection] = Field(
        None, description="连续入仓过程中被退回的批（处理到该批停止，后续批继续等待）")
    warehoused_through: int = Field(..., description="入仓进度：已连续入仓到的批号")
    next_batch_no: int = Field(..., description="下一批号（入仓进度 + 1）")


class RejectedBatchInfo(BaseModel):
    batch_no: int
    conflicts: list[ConflictItem]


class SignedDetail(BaseModel):
    waybill_no: str
    signed_at: int = Field(..., description="当前签收时刻（UTC 秒）")
    source_batch_no: int = Field(..., description="产生该事实的批号")


class CarrierStateResponse(BaseModel):
    carrier: str
    warehoused_through: int = Field(..., description="入仓进度：已连续入仓到的批号")
    next_batch_no: int = Field(
        ..., description="下一批号（入仓进度 + 1）；缺口或退回批即停在该号，需催补/更正")
    pending_batches: list[int] = Field(
        default_factory=list, description="已接收但尚未生效（待处理）的批号")
    rejected_batches: list[RejectedBatchInfo] = Field(
        default_factory=list, description="已退回待更正重交的批及其冲突")
    signed: list[SignedDetail] = Field(
        default_factory=list, description="当前已签收明细（同一时点快照）")


class BatchConflictResponse(BaseModel):
    detail: dict


BATCH2_FIRST_EXAMPLE = {
    "carrier": "顺路快运",
    "batch_no": 2,
    "items": [
        {"waybill_no": "WB1001", "prev_signed_at": 1794470400,
         "new_signed_at": 1794557700},
        {"waybill_no": "WB1002", "prev_signed_at": 1794475800,
         "new_signed_at": None},
    ],
}

BATCH1_FILL_EXAMPLE = {
    "carrier": "顺路快运",
    "batch_no": 1,
    "items": [
        {"waybill_no": "WB1001", "prev_signed_at": None,
         "new_signed_at": 1794470400},
        {"waybill_no": "WB1002", "prev_signed_at": None,
         "new_signed_at": 1794475800},
        {"waybill_no": "WB1003", "prev_signed_at": None,
         "new_signed_at": None},
    ],
}


@app.post(
    "/corrections/batches",
    response_model=CorrectionBatchResponse,
    responses={
        400: {"model": ErrorResponse, "description": "入参校验错误（如同批运单号重复）"},
        409: {"model": BatchConflictResponse,
              "description": "同批号已存在且内容不一致（待处理/已入仓批次拒绝覆盖）"},
    },
    summary="提交承运商签收更正批",
    description=(
        "批号按承运商从 1 连续编号。越过缺口的批次先存为待处理、不生效；"
        "缺口补齐后从入仓进度 +1 起按批号连续入仓。每批入仓时逐票核对"
        "更正前签收时刻与仓内当前值，对不上则退回该批（返回批号与冲突运单），"
        "已入仓的前序批不撤销，后续批继续等待；已退回批次可更正后重交。"
        "待处理/已入仓批次同内容重传不重算，换内容则 409 拒绝。"
        "典型用法：先交第 2 批（待处理），再补第 1 批（触发两批连续入仓）。"),
)
def submit_correction_batch(
    request: CorrectionBatchRequest = Body(
        ...,
        openapi_examples={
            "先交第2批（越过缺口，先存不生效）": {
                "summary": "第 2 批先到：补签 WB1001、撤销 WB1002，因缺第 1 批先存不生效",
                "value": BATCH2_FIRST_EXAMPLE,
            },
            "再补第1批（补齐缺口，两批按批号连续入仓）": {
                "summary": "补交第 1 批：初始签收事实入仓，随后第 2 批自动核对入仓",
                "value": BATCH1_FILL_EXAMPLE,
            },
        },
    ),
    warehouse: Warehouse = Depends(get_warehouse),
) -> CorrectionBatchResponse:
    waybills = [item.waybill_no for item in request.items]
    dups = sorted({w for w in waybills if waybills.count(w) > 1})
    if dups:
        raise HTTPException(400, [{"message": f"同批运单号重复: {dups}"}])
    items = [
        CorrectionItemInput(
            waybill_no=item.waybill_no,
            prev_signed_at=item.prev_signed_at,
            new_signed_at=item.new_signed_at,
        )
        for item in request.items
    ]
    try:
        result = warehouse.submit_batch(request.carrier, request.batch_no, items)
    except BatchContentConflict as exc:
        raise HTTPException(409, {
            "message": str(exc),
            "carrier": exc.carrier,
            "batch_no": exc.batch_no,
            "existing_status": exc.status,
        })
    return CorrectionBatchResponse(**result)


@app.get(
    "/corrections/{carrier}",
    response_model=CarrierStateResponse,
    summary="查询承运商当前入仓状态",
    description=(
        "按承运商返回同一时点（单事务快照）的入仓进度、下一批号、"
        "待处理/已退回批次与当前已签收明细。例如先交第 2 批再补第 1 批后，"
        "查询「顺路快运」可见入仓进度为 2、下一批号为 3。"),
)
def get_carrier_state(
    carrier: str,
    warehouse: Warehouse = Depends(get_warehouse),
) -> CarrierStateResponse:
    return CarrierStateResponse(**warehouse.carrier_state(carrier))
