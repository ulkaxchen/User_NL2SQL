from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


class ToolDefinition(BaseModel):
    name: str
    description: str
    input_schema: dict[str, Any] = Field(default_factory=dict)


class ToolTrace(BaseModel):
    step: int
    tool: str
    arguments: dict[str, Any]
    result: Any


class QueryRequest(BaseModel):
    question: str = Field(min_length=1)
    context_tables: list[str] = Field(
        default_factory=list,
        max_length=24,
        description="本轮纳入 schema 与默认工具 `table` 的业务表，可多选以支持 JOIN；空列表表示默认 users。",
    )

    @field_validator("context_tables", mode="before")
    @classmethod
    def _strip_table_names(cls, v: object) -> object:
        if not isinstance(v, list):
            return v
        return [str(x).strip() for x in v if str(x).strip()]


class ExportRequest(BaseModel):
    sql: str = Field(min_length=1)
    format: Literal["csv", "xlsx"] = "csv"


class QueryResponse(BaseModel):
    """query_log_id：可提交反馈。agent_transcript：ReAct 全文（Thought/Action/Observation），便于排查。"""

    question: str
    understanding: str
    sql: str | None = None
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    called_tools: list[str] = Field(default_factory=list)
    tool_trace: list[ToolTrace] = Field(default_factory=list)
    clarification: str | None = None
    summary: str | None = None
    query_log_id: int | None = None
    agent_transcript: str | None = None


class QueryFeedbackRequest(BaseModel):
    log_id: int = Field(ge=1)
    helpful: bool


class QueryFeedbackResponse(BaseModel):
    ok: bool
    id: int
    user_feedback: int


class ImportResponse(BaseModel):
    imported_rows: int
    detected_columns: list[str]
    database_path: str
    target_table: str


class TableListEntry(BaseModel):
    name: str
    row_count: int
    column_count: int
    is_default_table: bool


class TablesListResponse(BaseModel):
    tables: list[TableListEntry]


class DropTableResponse(BaseModel):
    ok: bool
    dropped: str


class HealthResponse(BaseModel):
    status: str
    imported: bool
    row_count: int
    tools: list[ToolDefinition]
    llm_backend: str = Field(default="ollama", description="主 Agent LLM：ollama 或 openai_compat")
    llm_model: str = Field(default="", description="当前使用的模型名")
    llm_base_url: str = Field(default="", description="Ollama 根地址或 OpenAI 兼容 Base URL（/v1）")
    llm_config_ok: bool = Field(default=True, description="强制 vLLM 但未配置 URL 时为 False")
    llm_config_message: str | None = Field(default=None, description="配置异常时的说明")


class TableColumn(BaseModel):
    name: str
    label: str | None = None
    sql_type: str
    nullable: bool | None = None
    primary_key: bool | None = None
    comment: str | None = None
    is_text_like: bool | None = None
    is_numeric: bool | None = None
    is_time_like: bool | None = None
    enum_candidate: str | None = None


class SchemaResponse(BaseModel):
    table: str
    columns: list[TableColumn]


class ToolRunRequest(BaseModel):
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class ToolRunResponse(BaseModel):
    name: str
    result: Any
