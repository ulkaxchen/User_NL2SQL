from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

from .agent import agent_llm_public_config, iter_sql_agent_events, list_tools, run_sql_agent, run_tool
from .db import (
    DATA_DIR,
    bootstrap_database,
    count_users,
    drop_business_table,
    export_sql_as_bytes,
    get_mysql_database_label,
    import_tabular_to_mysql,
    list_columns,
    list_tables,
    set_query_feedback,
)
from .schemas import (
    DropTableResponse,
    ExportRequest,
    HealthResponse,
    ImportResponse,
    QueryFeedbackRequest,
    QueryFeedbackResponse,
    QueryRequest,
    QueryResponse,
    SchemaResponse,
    TableColumn,
    TableListEntry,
    TablesListResponse,
    ToolRunRequest,
    ToolRunResponse,
)

logger = logging.getLogger(__name__)


def _log_business_tables_at_startup(where: str) -> None:
    try:
        rows = list_tables()
        if not rows:
            logger.info("[%s] 业务表: （无）", where)
        else:
            logger.info(
                "[%s] 业务表: %s",
                where,
                ", ".join(f"{r['name']}({r['row_count']}行)" for r in rows),
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[%s] 无法枚举业务表: %s", where, exc)


bootstrap_database()


@asynccontextmanager
async def lifespan(_: FastAPI):
    bootstrap_database()
    _log_business_tables_at_startup("startup")
    yield


app = FastAPI(title="User RAG SQL Agent", version="3.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/system/health", response_model=HealthResponse)
def health() -> HealthResponse:
    llm = agent_llm_public_config()
    return HealthResponse(
        status="ok",
        imported=count_users() > 0,
        row_count=count_users(),
        tools=list_tools(),
        llm_backend=str(llm["backend"]),
        llm_model=str(llm["model"]),
        llm_base_url=str(llm["base_url"]),
        llm_config_ok=bool(llm["config_ok"]),
        llm_config_message=llm["config_message"],
    )


@app.get("/api/system/schema", response_model=SchemaResponse)
def schema() -> SchemaResponse:
    return SchemaResponse(table="users", columns=[TableColumn(**column) for column in list_columns()])


@app.get("/api/system/tables", response_model=TablesListResponse)
def system_tables() -> TablesListResponse:
    raw = list_tables()
    return TablesListResponse(
        tables=[
            TableListEntry(
                name=str(x["name"]),
                row_count=int(x["row_count"]),
                column_count=int(x["column_count"]),
                is_default_table=bool(x["is_default_table"]),
            )
            for x in raw
        ]
    )


@app.delete("/api/system/tables/{table_name}", response_model=DropTableResponse)
def delete_table(table_name: str) -> DropTableResponse:
    try:
        dropped = drop_business_table(table_name)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return DropTableResponse(ok=True, dropped=dropped)


@app.post("/api/system/import", response_model=ImportResponse)
async def import_xlsx(
    file: UploadFile = File(...),
    table: str = Form("users"),
) -> ImportResponse:
    content = await file.read()
    temp_path = DATA_DIR / f"upload_{file.filename}"
    temp_path.write_bytes(content)
    try:
        inserted, columns, target = import_tabular_to_mysql(temp_path, table.strip() or None)
    except ValueError as exc:
        temp_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    temp_path.unlink(missing_ok=True)
    return ImportResponse(
        imported_rows=inserted,
        detected_columns=columns,
        database_path=get_mysql_database_label(),
        target_table=target,
    )


@app.get("/api/tools")
def get_tools():
    return list_tools()


@app.post("/api/tools/run", response_model=ToolRunResponse)
def execute_tool(request: ToolRunRequest) -> ToolRunResponse:
    result = run_tool(request.name, request.arguments)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return ToolRunResponse(name=request.name, result=result)


@app.post("/api/chat/query", response_model=QueryResponse)
def query_data(request: QueryRequest) -> QueryResponse:
    try:
        ctx = request.context_tables if request.context_tables else None
        return run_sql_agent(request.question, ctx)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/chat/query/stream")
def query_data_stream(request: QueryRequest) -> StreamingResponse:
    """SSE：逐步推送 bootstrap / step / tool / sql，最后一条 type=done 含完整 QueryResponse。"""

    def event_gen():
        ctx = request.context_tables if request.context_tables else None
        for event in iter_sql_agent_events(request.question, ctx):
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_gen(), media_type="text/event-stream", headers=headers)


@app.post("/api/chat/feedback", response_model=QueryFeedbackResponse)
def submit_query_feedback(request: QueryFeedbackRequest) -> QueryFeedbackResponse:
    result = set_query_feedback(request.log_id, request.helpful)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return QueryFeedbackResponse(ok=True, id=result["id"], user_feedback=result["user_feedback"])


@app.post("/api/chat/export")
def export_query_result(request: ExportRequest) -> StreamingResponse:
    try:
        body, media_type, filename = export_sql_as_bytes(request.sql, request.format)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(iter([body]), media_type=media_type, headers=headers)
