from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Iterator


def _env_float(name: str, default: float, lo: float = 0.0, hi: float = 2.0) -> float:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return max(lo, min(hi, float(raw)))
    except ValueError:
        return default


def _env_int(name: str, default: int, lo: int = 1, hi: int = 64) -> int:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return max(lo, min(hi, int(raw)))
    except ValueError:
        return default

import httpx

from .db import (
    USERS_TABLE,
    execute_sql,
    log_query,
    queryable_table_names,
)
from .schemas import QueryResponse, ToolDefinition, ToolTrace
from .tools import (
    REACT_PROBE_TOOL_NAMES,
    REACT_PROBE_TOOLS_DISPLAY,
    ToolExecutor,
    build_knowledge_context,
    search_relevant_schema,
)

logger = logging.getLogger("backend.agent")


OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://127.0.0.1:11434").strip() or "http://127.0.0.1:11434"
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b").strip() or "qwen2.5:3b"


def _llm_backend_mode() -> str:
    """auto：有 OpenAI 兼容 Base URL 则用 vLLM，否则 Ollama；ollama/local：强制 Ollama；openai/vllm/remote：强制 OpenAI 兼容。"""
    raw = (os.environ.get("USER_RAG_LLM_BACKEND") or "").strip().lower()
    if raw in ("ollama", "local"):
        return "ollama"
    if raw in ("openai", "vllm", "remote"):
        return "openai"
    return "auto"


# 若设置 USER_RAG_OPENAI_BASE_URL（或 OPENAI_BASE_URL），且在 auto 模式下，主 Agent 走 OpenAI 兼容 Chat API（如 vLLM），否则走 Ollama /api/generate
def _openai_compat_base_url() -> str:
    for key in ("USER_RAG_OPENAI_BASE_URL", "OPENAI_BASE_URL"):
        raw = os.environ.get(key, "").strip()
        if raw:
            return raw.rstrip("/")
    host = os.environ.get("USER_RAG_VLLM_HOST", "").strip()
    port = os.environ.get("USER_RAG_VLLM_PORT", "").strip()
    if host and port:
        return f"http://{host}:{port}/v1".rstrip("/")
    return ""


def _openai_compat_model() -> str:
    return (
        os.environ.get("USER_RAG_OPENAI_MODEL", "").strip()
        or os.environ.get("OPENAI_MODEL", "").strip()
        or "Qwen3-30B"
    )


def _openai_compat_api_key() -> str:
    return (
        os.environ.get("OPENAI_API_KEY", "").strip()
        or os.environ.get("USER_RAG_OPENAI_API_KEY", "").strip()
        or "vllm-is-awesome"
    )


def agent_llm_public_config() -> dict[str, str | bool | None]:
    """供 health 等展示；不发起网络请求。"""
    mode = _llm_backend_mode()
    openai_url = _openai_compat_base_url()
    resolved_openai = mode == "openai" or (mode == "auto" and bool(openai_url))
    if mode == "ollama" or not resolved_openai:
        return {
            "backend": "ollama",
            "model": OLLAMA_MODEL,
            "base_url": OLLAMA_BASE_URL,
            "config_ok": True,
            "config_message": None,
        }
    if not openai_url:
        return {
            "backend": "openai_compat",
            "model": _openai_compat_model(),
            "base_url": "",
            "config_ok": False,
            "config_message": "USER_RAG_LLM_BACKEND 为 openai/vllm 但未配置 USER_RAG_OPENAI_BASE_URL（或 USER_RAG_VLLM_HOST+PORT）",
        }
    return {
        "backend": "openai_compat",
        "model": _openai_compat_model(),
        "base_url": openai_url,
        "config_ok": True,
        "config_message": None,
    }


# ReAct 外层循环；默认 8 步，够用且减少空转；可用 USER_RAG_AGENT_MAX_STEPS 覆盖
AGENT_MAX_STEPS = _env_int("USER_RAG_AGENT_MAX_STEPS", 8, lo=1, hi=48)
# 默认跳过重写以省一轮 LLM、少跑偏；需要时设 USER_RAG_USE_REWRITE=1
USE_QUESTION_REWRITE = os.environ.get("USER_RAG_USE_REWRITE", "").strip().lower() in ("1", "true", "yes", "on")
# 未显式选表时，最多自动纳入前 N 张相关表，避免把整个库都塞进 prompt。
AUTO_CONTEXT_TABLE_MAX = _env_int("USER_RAG_AUTO_CONTEXT_TABLE_MAX", 6, lo=1, hi=12)
# 仅裁剪 ReAct 主循环 history
REACT_LOOP_HISTORY_MAX_LINES = 28
AGENT_TRANSCRIPT_MAX_CHARS = 120_000
# 低温度 + 收紧 top_p/top_k，减少胡编格式与乱跳工具；可用环境变量微调
LLM_GENERATE_OPTIONS = {
    "temperature": _env_float("USER_RAG_LLM_TEMPERATURE", 0.08, lo=0.0, hi=1.5),
    "top_p": _env_float("USER_RAG_LLM_TOP_P", 0.82, lo=0.05, hi=1.0),
    "top_k": _env_int("USER_RAG_LLM_TOP_K", 40, lo=1, hi=200),
    "repeat_penalty": _env_float("USER_RAG_LLM_REPEAT_PENALTY", 1.08, lo=1.0, hi=1.5),
}

REWRITE_PROMPT_TEMPLATE = """
把下面用户问题改写成一行、便于生成 SQL 的表述。保留原意与全量/时间范围等约束，不要加新条件，不要 Thought/Action。

{question}
""".strip()

REACT_PROMPT_TEMPLATE = """
你是 **MySQL** 数据分析 Agent（目标库为 MySQL 8+）。目标是把用户问题转成**单条可执行 SELECT SQL**。
**禁止**使用 SQLite 专用写法（如 `date('now')`、`strftime`、`PRAGMA`、双引号标识符等）；日期时间用 `CURDATE()`、`NOW()`、`DATE_SUB` / `DATE_ADD`、`INTERVAL` 等 MySQL 函数。
你只能调用下列工具做探查，最终答案必须通过 **Finish** 输出；不要把最终答案写成工具调用。

工具:
{tools}

{context_tables_block}
工作方式：
1. 先确定相关表：优先 **Search_relevant_schema**，必要时看 **Get_database_tables**。
2. 再看表结构：用 **Get_table_schema**，必要时用 **Profile_table_columns** 了解列分布。
3. 不确定字段时优先 **Find_relevant_columns / Profile_table_semantics / Infer_filter_columns / Find_time_columns / Profile_time_column**。
4. 不确定真实值长什么样时，优先 **Inspect_rows / Profile_column / Search_value_examples / Search_keyword_across_columns / Search_keyword_in_tables / Search_similar_values**。
5. 需要跨表时先 **Get_table_relationships / Infer_join_candidates / Validate_join_candidate / Find_join_path**，确认 JOIN 键后再写 SQL。
6. **Validate_sql / Explain_sql** 仅作可选自检；信息足够时直接 Finish，不要空转。

硬性规则：
- 每轮只输出两行：
  Thought: 简短推理
  Action: ToolName[JSON参数] 或 Finish[UNDERSTANDING: ... SQL: SELECT ... SUMMARY: ...]
- 禁止虚构工具名；不存在 Generate_SQL / Build_Query / Construct_WHERE 等工具。整条 SELECT 只能写在 **Finish 的 SQL:** 行。
- 最终 SQL 只能有一条 SELECT；列名用反引号；多表时列名冲突必须写 `` `表名`.`列名` ``。
- 类型转换必须写 MySQL 合法语法；整数转换优先 `CAST(expr AS SIGNED)`，不要写 `CAST(expr AS BIGINT)` 这类非标准 MySQL cast。
- 用户要全量时不要加 LIMIT；用户只要前 N 条/样例时再加 LIMIT。
- 明确 ID/编号时优先等值过滤；纯日期区间不要走关键词搜索。
- 不要把业务规则、国家别名、平台别名等外部假设硬写进 SQL；先用 schema、样本和值搜索工具取证，再写 WHERE/JOIN。
- 不要靠无限增大 Inspect_rows 的 limit 猜值域；若要定位“某个词出现在哪些列/哪张表”，应优先用 Search_keyword_* 工具。
- History 里已给出的 schema 不要重复探查；同一个工具与相同 JSON 参数也不要重复调用。

Finish 要求：
- 必须同时包含 **UNDERSTANDING:**、**SQL:**、**SUMMARY:** 三个标签，且各占一行。
- **SQL:** 后只能是一条 SELECT。没有命中时也要给出合法 SQL（例如 `WHERE 1=0`），不能只写文字结论。
- 同时有时间条件与多列 OR 条件时，要写成 `(时间条件) AND (OR 条件整段)`，避免 AND/OR 优先级错误。

Question: {question}
Knowledge:
{knowledge_context}
History:
{history}
""".strip()

STALL_ESCALATION = (
    "Observation: （系统）已连续无效步。本轮**只能**输出一条 Action，**禁止再调工具**。\n"
    "Finish 必须包含 UNDERSTANDING / SQL / SUMMARY 三个标签，且 SQL 必须单独成行。示例：\n"
    "Action: Finish[\n"
    "UNDERSTANDING: 当前表暂无匹配数据\n"
    "SQL: SELECT 1 WHERE 1=0\n"
    "SUMMARY: 一句话说明\n"
    "]\n"
    "若暂时无法确认列名或条件，也不要省略 SQL。"
)


def _resolve_context_tables(requested: list[str] | None, question: str | None = None) -> list[str]:
    """过滤非法表名；未显式选表时按问题从全库自动挑相关表。"""
    allowed = queryable_table_names()
    if not allowed:
        return []
    allow_set = set(allowed)
    if requested:
        out: list[str] = []
        for raw in requested:
            t = str(raw or "").strip()
            if t in allow_set and t not in out:
                out.append(t)
        return out or [allowed[0]]

    ranked: list[str] = []
    q = str(question or "").strip()
    if q:
        try:
            schema_hint = search_relevant_schema(q, max_tables=min(len(allowed), AUTO_CONTEXT_TABLE_MAX), max_columns_per_table=6)
            score_map = {
                str(name): int(score)
                for name, score in dict(schema_hint.get("table_scores") or {}).items()
                if str(name) in allow_set
            }
            best_score = max(score_map.values()) if score_map else 0
            keep_threshold = max(4, int(best_score * 0.45)) if best_score > 0 else 0
            for table in schema_hint.get("tables", []):
                t = str(table or "").strip()
                if t not in allow_set or t in ranked:
                    continue
                score = score_map.get(t, 0)
                if best_score > 0 and score < keep_threshold:
                    continue
                ranked.append(t)
        except Exception as exc:  # noqa: BLE001
            logger.info("auto context table selection fallback question=%r error=%s", q, exc)
    if ranked:
        return ranked[: min(len(ranked), AUTO_CONTEXT_TABLE_MAX)]
    for table in allowed:
        if table not in ranked:
            ranked.append(table)
    return ranked[: min(len(ranked), AUTO_CONTEXT_TABLE_MAX)]


def _format_context_tables_prompt_block(context_tables: list[str]) -> str:
    """每轮 LLM 提示中显式注入前端传入的表范围（不靠事后改 SQL）。"""
    if not context_tables:
        return (
            "【前端传入的查询范围·务必遵守】当前无可查业务表；仅可在 Finish 中说明需先导入数据，"
            "SQL 可写 `SELECT 1 WHERE 1=0`。\n"
        )
    listed = ", ".join(f"`{name}`" for name in context_tables)
    primary = context_tables[0]
    lines = [
        f"【前端传入的查询范围·务必遵守】本轮**仅允许**使用下列 {len(context_tables)} 张表：{listed}。",
        f"- **缺省主表**（工具 JSON 省略 `table` / `table_name` 时）：`{primary}`。",
        "- 凡工具参数里的 `table` / `table_name` / `from_table` / `to_table`：**只能**是上列之一（与上列拼写一致；大小写可忽略时以本列表为准）。",
        "- **Finish** 的 `SELECT` 中 `FROM` / `JOIN` **只能**出现上列中的表名；**禁止**事后依赖系统改 SQL，你必须直接写对表名。",
        "- 多表时列名冲突须写 `` `表名`.`列名` ``；跨表前可对**每张**相关表分别传 `table` 做 Inspect_rows / Search_keyword。",
    ]
    if len(context_tables) > 1:
        lines.append(
            "- 若问题要「两表（或多表）共同信息」：用业务公共键（如用户ID、订单号）在 **一条** SQL 里 `JOIN` 上列中的多张表，勿只查单表。"
        )
    return "\n".join(lines) + "\n"


class HelloAgentsLLM:
    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        timeout: float = 300.0,
        api_key: str | None = None,
    ):
        self.timeout = timeout
        mode = _llm_backend_mode()
        openai_from_env = _openai_compat_base_url()
        resolved_base = (base_url or "").strip()
        ctor_openai = bool(resolved_base and "/v1" in resolved_base.rstrip("/"))
        if mode == "ollama":
            self._use_openai = False
        elif mode == "openai":
            self._use_openai = True
            if not resolved_base and not openai_from_env:
                raise ValueError(
                    "USER_RAG_LLM_BACKEND=openai/vllm 但未设置 USER_RAG_OPENAI_BASE_URL、OPENAI_BASE_URL 或 USER_RAG_VLLM_HOST+PORT"
                    "也未向 HelloAgentsLLM 传入带 /v1 的 base_url"
                )
        else:
            self._use_openai = bool(openai_from_env or ctor_openai)
        if self._use_openai:
            root = (resolved_base or openai_from_env).rstrip("/")
            default_openai_model = _openai_compat_model()
            self.model = ((model or default_openai_model).strip() or default_openai_model)
            self.base_url = root
            key = (api_key or _openai_compat_api_key()).strip() or "vllm-is-awesome"
            from openai import OpenAI

            self._openai_client = OpenAI(
                base_url=root,
                api_key=key,
                http_client=httpx.Client(timeout=timeout, trust_env=False),
            )
        else:
            self.model = (model or OLLAMA_MODEL).strip() or OLLAMA_MODEL
            self.base_url = (resolved_base or OLLAMA_BASE_URL).rstrip("/")
            self._openai_client = None

    def think(self, prompt: str) -> str:
        started_at = time.perf_counter()
        if self._openai_client is not None:
            logger.info(
                "LLM think start backend=openai_compat model=%s prompt_chars=%s",
                self.model,
                len(prompt),
            )
            resp = self._openai_client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=LLM_GENERATE_OPTIONS["temperature"],
                top_p=LLM_GENERATE_OPTIONS["top_p"],
                stream=False,
            )
            content = (resp.choices[0].message.content or "").strip()
        else:
            logger.info("LLM think start backend=ollama model=%s prompt_chars=%s", self.model, len(prompt))
            response = httpx.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": LLM_GENERATE_OPTIONS,
                },
                timeout=self.timeout,
                trust_env=False,
            )
            response.raise_for_status()
            content = response.json()["response"].strip()
        elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        logger.info("LLM think done model=%s elapsed_ms=%s response_chars=%s", self.model, elapsed_ms, len(content))
        if not content:
            logger.warning("LLM returned empty response model=%s elapsed_ms=%s", self.model, elapsed_ms)
        return content

    def rewrite(self, question: str) -> str:
        return self.think(REWRITE_PROMPT_TEMPLATE.format(question=question))


def _parse_output(text: str) -> tuple[str, str]:
    thought_match = re.search(r"Thought:\s*(.*?)(?=\nAction:|$)", text, re.DOTALL)
    thought = thought_match.group(1).strip() if thought_match else ""
    action = ""
    if "Action:" in text:
        remainder = text.split("Action:", 1)[1].lstrip()
        buffer: list[str] = []
        depth = 0
        started = False
        for char in remainder:
            buffer.append(char)
            if char == "[":
                depth += 1
                started = True
            elif char == "]":
                depth -= 1
                if started and depth <= 0:
                    break
            elif char == "\n" and not started:
                cur = "".join(buffer).strip()
                # Action: 后直接跟多行 SELECT 时，若无 [] 则合并后续 SQL 行直到非 SQL 行
                if re.match(r"(?is)^SELECT\b", cur) and not re.search(r"(?i)\bLIMIT\s+\d+\s*$", cur):
                    rest_lines = remainder[len(buffer) :].splitlines()
                    for line in rest_lines:
                        lt = line.strip()
                        if not lt:
                            break
                        if re.match(
                            r"(?i)^(FROM|WHERE|GROUP|ORDER|HAVING|LIMIT|AND|OR|LEFT|RIGHT|INNER|JOIN|ON|\)\s*AND|\()\s",
                            lt,
                        ):
                            buffer.append("\n")
                            buffer.append(line.rstrip())
                            continue
                        break
                break
        action = "".join(buffer).strip()
    return thought, action


def _recover_action_from_text(text: str) -> str:
    finish_action = _extract_finish_anywhere(text)
    if finish_action:
        return finish_action

    tool_pattern = re.search(r"\b([A-Za-z_][A-Za-z0-9_]*)\[(.*?)\]", text, re.DOTALL)
    if tool_pattern:
        return f"{tool_pattern.group(1)}[{tool_pattern.group(2)}]"

    natural_tool = re.search(
        r"Call(?: the)?\s+['\"]?([A-Za-z_][A-Za-z0-9_]*)['\"]?\s+tool(?:\s+with\s+(.*?))?(?:\.|$)",
        text,
        re.DOTALL | re.IGNORECASE,
    )
    if natural_tool:
        tool_name = natural_tool.group(1).strip()
        tool_input = (natural_tool.group(2) or "").strip()
        return f"{tool_name}[{tool_input}]"

    has_structured_finish = any(tag in text for tag in ("UNDERSTANDING:", "SQL:", "SUMMARY:", "CLARIFICATION:"))
    if has_structured_finish:
        blocks = []
        for key in ("UNDERSTANDING", "SQL", "SUMMARY", "CLARIFICATION"):
            value = _match_block(text, key)
            if value:
                blocks.append(f"{key}: {value}")
        if blocks:
            return f"Finish[{chr(10).join(blocks)}]"

    sql_match = re.search(r"\bSELECT\s+.*?(?=$|\n)", text, re.DOTALL | re.IGNORECASE)
    if sql_match:
        sql = sql_match.group(0).strip()
        return f"Finish[SQL: {sql}]"

    return ""


def _normalize_finish_content(content: str) -> str:
    """去掉模型爱加的 markdown 围栏、**标签**，便于解析 UNDERSTANDING/SQL/SUMMARY。"""
    t = content.replace("\r\n", "\n")
    # 常见烂格式：SQL: ```sql} 或 ```SQL（无空白），原正则要求 ```sql 后必须空白故删不掉
    t = re.sub(r"```(?:sql|SQL)?[\s\}`]*", "", t)
    t = re.sub(r"```\w*[\s\}`]*", "", t)
    t = t.replace("```", "")
    t = re.sub(
        r"\*\*\s*(UNDERSTANDING|SQL|SUMMARY|CLARIFICATION)\s*\*\*\s*[:：]",
        r"\1:",
        t,
        flags=re.IGNORECASE,
    )
    t = re.sub(
        r"(?mi)^#+\s*(UNDERSTANDING|SQL|SUMMARY|CLARIFICATION)\s*[:：]?\s*",
        r"\1: ",
        t,
    )
    return t.strip()


def _parse_finish_bracket_payload(action_text: str) -> tuple[str | None, str | None]:
    """用括号深度解析 Finish[...]，避免 SQL 内含 ] 或非贪婪正则截断失败。"""
    s = action_text.strip()
    if not re.match(r"(?is)finish\s*(\[|$)", s):
        return None, None
    if re.fullmatch(r"(?is)finish\s*", s):
        return "Finish", ""
    idx = s.lower().find("finish[")
    if idx < 0:
        return None, None
    i = idx + len("finish[")
    depth = 1
    start = i
    while i < len(s):
        if s[i] == "[":
            depth += 1
        elif s[i] == "]":
            depth -= 1
            if depth == 0:
                inner = s[start:i]
                return "Finish", _normalize_finish_content(inner.strip())
        i += 1
    return "Finish", _normalize_finish_content(s[start:].strip())


def _extract_finish_anywhere(text: str) -> str | None:
    marker = "Finish["
    if marker not in text:
        return None
    start = text.rfind(marker)
    fragment = text[start:].strip()
    name, payload = _parse_finish_bracket_payload(fragment)
    if name != "Finish":
        return None
    return f"Finish[{payload}]" if payload is not None else None


def _parse_action(action_text: str) -> tuple[str | None, str | None]:
    fin_name, fin_payload = _parse_finish_bracket_payload(action_text)
    if fin_name == "Finish":
        return "Finish", fin_payload or ""
    tool = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\[(.*)\]\s*$", action_text, re.DOTALL)
    if tool:
        return tool.group(1).strip(), tool.group(2).strip()
    natural_tool = re.search(
        r"Call(?: the)?\s+['\"]?([A-Za-z_][A-Za-z0-9_]*)['\"]?\s+tool(?:\s+with\s+(.*?))?\.?$",
        action_text,
        re.DOTALL | re.IGNORECASE,
    )
    if natural_tool:
        return natural_tool.group(1).strip(), (natural_tool.group(2) or "").strip()
    # 模型常把整句 SQL 写在 Action: 后且省略 Finish[...]，此处收编成合法 Finish
    bare = action_text.strip()
    if re.match(r"(?is)^[`\"\s]*SELECT\b", bare) and re.search(r"\bFROM\b", bare, re.IGNORECASE):
        sql_clean = _strip_trailing_sql_noise(bare.rstrip(";"))
        payload = (
            "UNDERSTANDING: 根据对话与问题生成的查询\n"
            f"SQL: {sql_clean}\n"
            "SUMMARY: 见下方查询结果"
        )
        return "Finish", payload
    return None, None


def _parse_tool_input(tool_name: str, raw_input: str) -> dict[str, Any]:
    payload = raw_input.strip()
    if not payload:
        return {}
    try:
        parsed = json.loads(payload)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    except json.JSONDecodeError:
        kv_pairs = re.findall(r'(\w+)\s*=\s*"([^"]*)"|(\w+)\s*=\s*\'([^\']*)\'|(\w+)\s*=\s*([^,\s]+)', payload)
        if kv_pairs:
            parsed_pairs: dict[str, Any] = {}
            for match in kv_pairs:
                key = match[0] or match[2] or match[4]
                value = match[1] or match[3] or match[5]
                parsed_pairs[key] = value
            return parsed_pairs
        single_arg_map = {
            "Get_table_schema": "table_name",
            "Profile_column": "column",
            "Find_relevant_columns": "query",
            "Search_keyword_across_columns": "keyword",
            "Search_keyword_in_tables": "keyword",
            "Search_relevant_schema": "question",
        }
        natural_language_patterns = {
            "Get_table_schema": [r"table name ['\"]([^'\"]+)['\"]", r"table ['\"]([^'\"]+)['\"]"],
            "Profile_column": [r"(?:field|column) ['\"]([^'\"]+)['\"]"],
            "Find_relevant_columns": [r"query ['\"]([^'\"]+)['\"]", r"question ['\"]([^'\"]+)['\"]"],
            "Search_keyword_across_columns": [
                r"keyword ['\"]([^'\"]+)['\"]",
                r"search ['\"]([^'\"]+)['\"]",
                r"词 ['\"]([^'\"]+)['\"]",
            ],
            "Search_relevant_schema": [r"question ['\"]([^'\"]+)['\"]", r"query ['\"]([^'\"]+)['\"]"],
        }
        for pattern in natural_language_patterns.get(tool_name, []):
            match = re.search(pattern, payload, re.IGNORECASE | re.DOTALL)
            if match:
                key = single_arg_map.get(tool_name)
                if key:
                    return {key: match.group(1).strip()}
        key = single_arg_map.get(tool_name)
        return {key: payload} if key else {}


def _match_block(content: str, key: str) -> str | None:
    boundary = r"(?=\n\s*(?:UNDERSTANDING|SQL|SUMMARY|CLARIFICATION)\s*[:：]|(?<=[^\n])\s+(?:UNDERSTANDING|SQL|SUMMARY|CLARIFICATION)\s*[:：]|$)"
    match = re.search(rf"{key}\s*[:：]\s*(.*?){boundary}", content, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else None


def _strip_trailing_sql_noise(sql: str) -> str:
    """去掉句末句号、中文句号等，避免拼上 LIMIT 后变成 ...'2026-02-03'. LIMIT 导致语法错误。"""
    s = sql.strip().rstrip(";")
    while s and s[-1] in (".", "。", "．", "·"):
        s = s[:-1].rstrip()
    return s.strip()


def _merge_duplicate_where_clauses(sql: str) -> str:
    """
    模型常写出非法双 WHERE：`... ) WHERE 更多条件`。
    合并为 `... ) AND (更多条件)`，可多次迭代。
    """
    s = sql.strip().rstrip(";")
    for _ in range(8):
        m = re.search(r"(?is)\)\s+WHERE\s+", s)
        if not m:
            break
        prefix = s[: m.start() + 1].strip()
        tail = s[m.end() :].strip()
        s = f"{prefix} AND ({tail})"
    return s.strip()


def _extract_sql_block(content: str) -> str | None:
    """
    支持 SQL: / SQL：；允许多处出现（如误写在 SUMMARY 里）时取**最后一段**合法 SELECT。
    """
    last_good: str | None = None
    for m in re.finditer(r"SQL\s*[:：]\s*(SELECT[\s\S]+)", content, re.IGNORECASE):
        chunk = m.group(1).strip()
        chunk = re.split(r"(?i)\s+(?:UNDERSTANDING|SUMMARY|CLARIFICATION)\s*[:：]", chunk, 1)[0].strip()
        chunk = re.sub(r"\s+SUMMARY\s*[:：].*$", "", chunk, flags=re.DOTALL | re.IGNORECASE).strip()
        chunk = re.sub(r"\s+CLARIFICATION\s*[:：].*$", "", chunk, flags=re.DOTALL | re.IGNORECASE).strip()
        chunk = _strip_trailing_sql_noise(chunk)
        chunk = _merge_duplicate_where_clauses(chunk)
        if chunk.lower().startswith("select") and re.search(r"\bfrom\b", chunk, re.IGNORECASE):
            last_good = chunk
    if last_good:
        return last_good
    fence = re.search(r"(?is)\bSELECT\b[\s\S]+?(?=\n\s*(?:SUMMARY|UNDERSTANDING|CLARIFICATION)\s*[:：]|\*\*SUMMARY|\Z)", content)
    if fence:
        cand = _strip_trailing_sql_noise(fence.group(0).strip())
        cand = _merge_duplicate_where_clauses(cand)
        if cand.lower().startswith("select") and re.search(r"\bfrom\b", cand, re.IGNORECASE):
            return cand
    return None


def _parse_finish_block(content: str) -> tuple[str, str | None, str | None, str | None]:
    content = _normalize_finish_content(content)
    understanding = _match_block(content, "UNDERSTANDING") or "根据工具结果完成回答"
    sql = _extract_sql_block(content)
    summary = _match_block(content, "SUMMARY")
    clarification = _match_block(content, "CLARIFICATION")
    return understanding, sql, summary, clarification


def _full_data_intent(text: str) -> bool:
    """用户是否明确要求全量/所有匹配行（而非抽样）。"""
    if not (text and str(text).strip()):
        return False
    t = str(text).strip()
    strong = (
        "全量",
        "全部数据",
        "所有数据",
        "完整数据",
        "完整列表",
        "每条",
        "导出全部",
        "列出所有",
        "所有符合",
        "全部符合",
        "所有记录",
        "全部记录",
        "所有信息",
        "全部信息",
    )
    if any(s in t for s in strong):
        return True
    pairs = (
        ("所有", "数据"),
        ("所有", "用户"),
        ("所有", "记录"),
        ("全部", "数据"),
        ("全部", "用户"),
        ("全部", "记录"),
    )
    return any(a in t and b in t for a, b in pairs)


def _explicit_row_cap_intent(text: str) -> bool:
    """用户明确只要前 N 条/样例时，保留模型写的 LIMIT。"""
    t = str(text)
    if re.search(r"前\s*\d+", t):
        return True
    if re.search(r"\d+\s*条", t):
        return True
    if re.search(r"最多\s*\d+", t):
        return True
    if re.search(r"top\s*\d+", t, re.IGNORECASE):
        return True
    if re.search(r"limit\s*\d+", t, re.IGNORECASE):
        return True
    if any(w in t for w in ("样例", "示例", "看看", "一眼", "几条")):
        return True
    return False


def _strip_trailing_limit_for_full_intent(question: str, rewritten: str | None, sql: str) -> str:
    """全量意图且用户未要求前 N 条时，去掉 SQL 末尾的 LIMIT n，避免模型习惯性加 LIMIT。"""
    combined = f"{question or ''} {rewritten or ''}"
    if not _full_data_intent(combined) or _explicit_row_cap_intent(combined):
        return sql
    s = sql.strip().rstrip(";")
    stripped = re.sub(r"(?is)\s+limit\s+\d+\s*$", "", s).strip()
    return stripped if stripped else sql


def _looks_like_identifier_query(text: str) -> bool:
    """是否像是在按主键/编号/编码精确查一条或少量记录。"""
    if not text or not str(text).strip():
        return False
    t = str(text).strip()
    patterns = (
        r"\d+\s*号",
        r"(?:ID|id|Id|编号|编码|单号|订单号|客户号|料号|工号|uid)\b",
        r"第\s*\d+\s*(?:个|位|条)?",
    )
    return any(re.search(p, t) for p in patterns)


def _question_has_time_intent(text: str) -> bool:
    if not text or not str(text).strip():
        return False
    t = str(text)
    keywords = (
        "时间",
        "日期",
        "最近",
        "近",
        "按月",
        "按天",
        "按周",
        "趋势",
        "环比",
        "同比",
        "year",
        "month",
        "day",
        "date",
        "time",
    )
    return any(k.lower() in t.lower() for k in keywords)


def _question_has_join_intent(text: str) -> bool:
    if not text or not str(text).strip():
        return False
    t = str(text)
    keywords = (
        "关联",
        "join",
        "合并",
        "同时",
        "一起",
        "对应",
        "匹配",
        "两张表",
        "多张表",
        "来自",
    )
    return any(k.lower() in t.lower() for k in keywords)


def _trace_searched_keyword(tool_trace: list[ToolTrace], keyword: str) -> bool:
    """是否已用 Search_keyword_* / Search_similar_values 探查过该关键词。"""
    if not keyword:
        return False
    probe_tools = {"Search_keyword_across_columns", "Search_keyword_in_tables", "Search_similar_values"}
    for item in tool_trace:
        if item.tool not in probe_tools:
            continue
        args = item.arguments or {}
        raw_values: list[str] = []
        raw = args.get("keyword")
        if isinstance(raw, str):
            raw_values.append(raw)
        raw = args.get("value")
        if isinstance(raw, str):
            raw_values.append(raw)
        for raw_item in raw_values:
            k = raw_item.strip()
            if k == keyword or keyword in k or k in keyword:
                return True
    return False


def _has_search_probe(tool_trace: list[ToolTrace]) -> bool:
    return any(
        item.tool in {"Search_keyword_across_columns", "Search_keyword_in_tables", "Search_similar_values"}
        for item in tool_trace
    )


def _nudge_finish_after_duplicate_probe(question: str, tool_trace: list[ToolTrace], action_name: str) -> str:
    """重复探查工具时：引导换参/换工具或直接 Finish，避免空转。"""
    q = question or ""
    if action_name in {
        "Profile_column",
        "Profile_time_column",
        "Find_time_columns",
        "Profile_table_columns",
        "Find_relevant_columns",
        "Search_relevant_schema",
        "Get_table_schema",
        "Get_database_tables",
    }:
        parts = [
            " **禁止**原样重复同一探查工具与相同 JSON。",
            "若已有 schema / profile / column ranking，应直接据此写 Finish，或改用其它探查工具。",
            "时间范围类问题优先 Find_time_columns / Profile_time_column；值在哪列不确定时改用 Search_keyword_*、Profile_column 或 Inspect_rows。",
            "跨表问题优先 Infer_join_candidates / Find_join_path，而不是继续重复看同一列画像。",
            f"需要其它信息可换工具（{REACT_PROBE_TOOLS_DISPLAY}）或改 JSON 参数，勿空转。",
        ]
        return "".join(parts)
    if action_name not in {
        "Inspect_rows",
        "Search_keyword_across_columns",
        "Search_keyword_in_tables",
        "Search_similar_values",
        "Infer_join_candidates",
        "Validate_join_candidate",
        "Find_join_path",
    }:
        return ""
    if not _has_previewed_rows(tool_trace) and action_name == "Inspect_rows":
        return ""
    has_search = _has_search_probe(tool_trace)
    probe_desc = "History 里已有 schema"
    if _has_previewed_rows(tool_trace):
        probe_desc += " 与 Inspect_rows 样本"
    if has_search:
        probe_desc += "、Search_keyword 结果"
    extra = (
        f" **勿重复本次调用**：{probe_desc}。"
        f"若仍缺关键信息，可换用其它探查工具（已注册: {REACT_PROBE_TOOLS_DISPLAY}；"
        "或同一工具换不同 JSON 参数，如 Inspect_rows 换 columns/filters/order_by、Find_join_path 换 from_table/to_table）。"
        "若已能根据 History 写出合法 WHERE / JOIN，则 `Action: Finish[UNDERSTANDING: ... SQL: SELECT ... SUMMARY: ...]`。"
    )
    if re.search(r"多少|几个|数量|COUNT|计数", q, re.IGNORECASE):
        extra += " 数量类问题先确认统计粒度与主键；不要在没有依据时想当然地写 `COUNT(*)`。"
    if _looks_like_identifier_query(q):
        extra += " 编号/ID 类问题优先找到真实编号列做等值过滤，不要把编号当普通关键词反复全表模糊搜。"
    if _question_has_join_intent(q):
        extra += " 若问题明显跨表，下一步优先 Infer_join_candidates / Find_join_path，而不是继续扩大单表样本。"
    return extra


def _compact_observation(result: Any, limit: int = 1500) -> str:
    serialized = json.dumps(result, ensure_ascii=False)
    return serialized if len(serialized) <= limit else serialized[:limit] + "...(truncated)"


def _trim_react_history(history: list[str], max_lines: int = REACT_LOOP_HISTORY_MAX_LINES) -> None:
    """只裁剪 ReAct 主循环 history；前缀里的 schema 由调用方单独保留。"""
    if len(history) <= max_lines:
        return
    tail_keep = max_lines - 1
    omitted = len(history) - tail_keep
    tail = history[-tail_keep:]
    history.clear()
    history.append(f"Observation: （已省略较早 {omitted} 条 ReAct 记录；列名见对话开头的 schema。）")
    history.extend(tail)


def _extract_schema_columns(tool_trace: list[ToolTrace]) -> list[str]:
    """合并本轮所有 Get_table_schema 的列名（去重保序），供 Finish 校验与跨表 JOIN。"""
    seen: set[str] = set()
    out: list[str] = []
    for item in tool_trace:
        if item.tool == "Get_table_schema" and isinstance(item.result, dict):
            columns = item.result.get("columns", [])
            for column in columns:
                if isinstance(column, dict) and column.get("name"):
                    name = column["name"]
                    if name not in seen:
                        seen.add(name)
                        out.append(name)
    return out


def _has_previewed_rows(tool_trace: list[ToolTrace]) -> bool:
    return any(item.tool == "Inspect_rows" for item in tool_trace)


def _count_preview_rows_in_trace(tool_trace: list[ToolTrace]) -> int:
    return sum(1 for item in tool_trace if item.tool == "Inspect_rows")


def _question_directly_mentions_schema_column(question: str, schema_columns: list[str]) -> bool:
    lowered_question = question.lower()
    for column in schema_columns:
        lowered_column = str(column).strip().lower()
        if not lowered_column:
            continue
        if lowered_column in lowered_question:
            return True
        if len(lowered_column) >= 2 and any(part and part in lowered_question for part in re.split(r"[\s_]+", lowered_column)):
            return True
    return False


def _auto_preview_rows(tools: ToolExecutor, tool_trace: list[ToolTrace], history: list[str], step: int) -> None:
    args = {"limit": 5}
    result = tools.run("Inspect_rows", args)
    tool_trace.append(ToolTrace(step=step, tool="Inspect_rows", arguments=args, result=result))
    history.append(
        "Observation: schema 没有直接解决问题时，先预览了 5 行样本，判断目标信息是否藏在某些列值里。"
    )
    history.append(f"Observation: {_compact_observation(result)}")


def _find_unknown_sql_identifiers(sql: str, schema_columns: list[str]) -> list[str]:
    if not schema_columns:
        return []
    cleaned = re.sub(r"'(?:''|[^'])*'", " ", sql)
    quoted_identifiers = re.findall(r'["`]\s*([^"`]+?)\s*["`]', cleaned)
    cleaned = re.sub(r'["`]\s*([^"`]+?)\s*["`]', " ", cleaned)
    function_names = {
        match.group(1).lower()
        for match in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", cleaned)
        if match.group(1)
    }
    reserved_alias_words = {
        "where",
        "join",
        "left",
        "right",
        "inner",
        "outer",
        "cross",
        "full",
        "on",
        "group",
        "order",
        "limit",
        "having",
        "union",
        "offset",
    }
    table_aliases: set[str] = set()
    for match in re.finditer(
        r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_]*)\s+(?:as\s+)?([A-Za-z_][A-Za-z0-9_]*)\b",
        cleaned,
        re.IGNORECASE,
    ):
        alias = str(match.group(2) or "").strip().lower()
        if alias and alias not in reserved_alias_words:
            table_aliases.add(alias)
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", cleaned)
    # MySQL 常见关键字/函数，避免 LOWER(CAST(...)) 中的 lower 被当成列名
    keywords = {
        "select",
        "from",
        "where",
        "and",
        "or",
        "as",
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "distinct",
        "order",
        "by",
        "group",
        "limit",
        "between",
        "datetime",
        "date",
        "time",
        "now",
        "current_date",
        "current_timestamp",
        "asc",
        "desc",
        "null",
        "is",
        "not",
        "like",
        "in",
        "on",
        "join",
        "left",
        "right",
        "inner",
        "outer",
        "case",
        "when",
        "then",
        "else",
        "end",
        "cast",
        "curdate",
        "current_time",
        "date_add",
        "date_sub",
        "interval",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "users",
        "true",
        "false",
        "text",
        "integer",
        "bigint",
        "decimal",
        "real",
        "double",
        "blob",
        "numeric",
        "signed",
        "unsigned",
        "lower",
        "upper",
        "trim",
        "ltrim",
        "rtrim",
        "replace",
        "substr",
        "substring",
        "instr",
        "coalesce",
        "ifnull",
        "nullif",
        "length",
        "char_length",
        "unicode",
        "char",
        "round",
        "abs",
        "hex",
        "if",
        "concat",
        "group_concat",
        "summary",
        "id",
        "cnt",
    }
    schema_set = {column.lower() for column in schema_columns}
    allowed_tables_lower = {t.lower() for t in queryable_table_names()}
    unknown: list[str] = []
    for identifier in quoted_identifiers:
        lowered = identifier.strip().lower()
        if lowered in allowed_tables_lower:
            continue
        if lowered and lowered not in schema_set and lowered not in unknown:
            unknown.append(identifier.strip())
    previous = ""
    skip_next = False
    for token in tokens:
        lowered = token.lower()
        if skip_next:
            skip_next = False
            previous = lowered
            continue
        if lowered in keywords or lowered in function_names or lowered in table_aliases:
            if lowered in {"as", "from", "join"}:
                skip_next = True
            previous = lowered
            continue
        if previous == "from":
            previous = lowered
            continue
        if lowered in allowed_tables_lower:
            previous = lowered
            continue
        if lowered not in schema_set and lowered not in unknown:
            unknown.append(lowered)
        previous = lowered
    return unknown


def _optional_success_clarification(question: str, tool_trace: list[ToolTrace], history: list[str]) -> str | None:
    """成功执行 SQL 后仅补充与问题相关的轻提示；不把早前工具噪声当作「失败澄清」。"""
    columns = _extract_schema_columns(tool_trace)
    lowered_question = question.lower()
    lowered_columns = {column.lower() for column in columns}
    if any(keyword in lowered_question for keyword in ("邮箱", "邮件", "email")) and not {"邮箱", "email", "mail"} & lowered_columns:
        available = ", ".join(columns) if columns else "当前已选表暂无可用列信息"
        return f"当前已选表里没有明显的邮箱字段，可用列包括：{available}。"
    for entry in reversed(history):
        if "no such column:" in entry:
            match = re.search(r"no such column:\s*([A-Za-z_][A-Za-z0-9_]*)", entry)
            if match:
                return f"当前数据表里没有字段 `{match.group(1)}`，请先确认字段名或改查已有字段。"
        if "no such table:" in entry:
            match = re.search(r"no such table:\s*([A-Za-z_][A-Za-z0-9_]*)", entry)
            if match:
                return f"当前上下文中没有表 `{match.group(1)}`，请确认表范围或改用已选表。"
    return None


def _final_clarification(question: str, tool_trace: list[ToolTrace], history: list[str]) -> str:
    hint = _optional_success_clarification(question, tool_trace, history)
    if hint:
        return hint
    for item in reversed(tool_trace):
        error = item.result.get("error") if isinstance(item.result, dict) else None
        if error:
            return f"Agent 没有稳定完成查询，最后一次工具报错是：{error}"
    return "Agent 达到最大轮数，仍未得到稳定结果。"


class ReActAgent:
    def __init__(self, llm: HelloAgentsLLM | None = None, tools: ToolExecutor | None = None, max_steps: int = AGENT_MAX_STEPS):
        self.llm = llm or HelloAgentsLLM()
        self.tools = tools or ToolExecutor()
        self.max_steps = max_steps
        self.history: list[str] = []
        self.history_prefix: list[str] = []
        self._resolved_context_tables: list[str] = []

    def _build_prompt(self, question: str, knowledge_context: str) -> str:
        history_chunks = [*self.history_prefix, *self.history]
        history_block = "\n".join(history_chunks) if history_chunks else "暂无历史。"
        ctx_block = _format_context_tables_prompt_block(self._resolved_context_tables)
        return REACT_PROMPT_TEMPLATE.format(
            tools=self.tools.tools_prompt_react(),
            context_tables_block=ctx_block,
            question=question,
            knowledge_context=knowledge_context,
            history=history_block,
        )

    def _rewrite_question(self, question: str) -> str:
        try:
            rewritten = self.llm.rewrite(question).strip()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Question rewrite failed question=%r error=%s", question, exc)
            return question.strip()
        if not rewritten:
            return question.strip()
        first_line = next((line.strip() for line in rewritten.splitlines() if line.strip()), "")
        first_line = re.sub(r"^(重写结果|重写后的问题|改写后问题|Rewritten Question)\s*[:：]\s*", "", first_line, flags=re.IGNORECASE)
        cleaned = first_line.strip("` ").strip()
        if not cleaned:
            return question.strip()
        logger.info("Question rewrite done original=%r rewritten=%r", question, cleaned)
        return cleaned

    def _agent_transcript(self) -> str:
        text = "\n".join([*self.history_prefix, *self.history])
        if len(text) > AGENT_TRANSCRIPT_MAX_CHARS:
            return text[:AGENT_TRANSCRIPT_MAX_CHARS] + "\n\n...(agent_transcript 已截断)"
        return text

    def iter_events(self, question: str, context_tables: list[str] | None = None) -> Iterator[dict[str, Any]]:
        """逐步产出事件供 SSE；最后一条 type=done。未显式传表时会自动挑选相关表。"""
        self.history = []
        self.history_prefix = []
        tool_trace: list[ToolTrace] = []
        seen_tool_calls: set[str] = set()
        stall_bumps = 0

        if USE_QUESTION_REWRITE:
            rewritten_question = self._rewrite_question(question)
            if rewritten_question and rewritten_question != question.strip():
                self.history_prefix.append(f"Observation: 改写意图：{rewritten_question}")
        else:
            rewritten_question = question.strip()
        resolved_tables = _resolve_context_tables(context_tables, rewritten_question or question)
        knowledge_context = build_knowledge_context(question, rewritten_question, resolved_tables)
        self._resolved_context_tables = list(resolved_tables)
        logger.info(
            "ReAct run start question=%r max_steps=%s context_tables=%r resolved=%r",
            question,
            self.max_steps,
            context_tables,
            resolved_tables,
        )

        if not resolved_tables:
            self.history_prefix.append(
                "Observation: （系统）当前无可查询业务表。Finish 须含三标签，SQL 可写 `SELECT 1 WHERE 1=0`，SUMMARY 说明需先导入数据。"
            )
            yield {"type": "bootstrap", "step": 0, "tool": "Get_table_schema", "arguments": {}, "result": "{}"}
        else:
            if len(resolved_tables) > 1:
                tbl_line = ", ".join(f"`{t}`" for t in resolved_tables)
                self.history_prefix.append(
                    f"Observation: （系统）本轮查询**选定多表**：{tbl_line}。"
                    "跨表请用 JOIN（无外键时按业务公共键 ON）；探查工具须指定正确 `table`。"
                )
                rel_args: dict[str, Any] = {}
                rel_result = self.tools.run("Get_table_relationships", rel_args)
                tool_trace.append(ToolTrace(step=-1, tool="Get_table_relationships", arguments=rel_args, result=rel_result))
                seen_tool_calls.add(json.dumps({"tool": "Get_table_relationships", "arguments": rel_args}, ensure_ascii=False, sort_keys=True))
                self.history_prefix.append(f"Observation: （表关系）{_compact_observation(rel_result, limit=1600)}")
                yield {"type": "bootstrap", "step": -1, "tool": "Get_table_relationships", "arguments": rel_args, "result": _compact_observation(rel_result, limit=4000)}
            for idx, tbl in enumerate(resolved_tables):
                bootstrap_args = {"table_name": tbl}
                schema_result = self.tools.run("Get_table_schema", bootstrap_args)
                tool_trace.append(
                    ToolTrace(step=idx, tool="Get_table_schema", arguments=bootstrap_args, result=schema_result)
                )
                seen_tool_calls.add(
                    json.dumps({"tool": "Get_table_schema", "arguments": bootstrap_args}, ensure_ascii=False, sort_keys=True)
                )
                self.history_prefix.append(f"Observation: （表 `{tbl}`）{_compact_observation(schema_result, limit=1600)}")
                yield {
                    "type": "bootstrap",
                    "step": idx,
                    "tool": "Get_table_schema",
                    "arguments": bootstrap_args,
                    "result": _compact_observation(schema_result, limit=4000),
                }
            bootstrap_columns = _extract_schema_columns(tool_trace)
            if bootstrap_columns:
                self.history_prefix.append(
                    "Observation: 允许使用的列名（SQL 用反引号；多表建议 `表名`.`列名`）："
                    + ", ".join(bootstrap_columns)
                    + "。探查后 Finish 写 SELECT。"
                )
                for item in tool_trace:
                    if item.tool != "Get_table_schema" or not isinstance(item.result, dict):
                        continue
                    rc = item.result.get("row_count")
                    tnm = item.result.get("table")
                    if isinstance(rc, int) and rc == 0 and tnm:
                        self.history_prefix.append(
                            f"Observation: （系统）表 `{tnm}` 当前 **0 行**，Search_keyword 的 match_count 可能全为 0。"
                            "等值过滤/范围过滤仍可写合法 SQL；勿对同一 keyword 无限换列重试。"
                        )
            else:
                primary = resolved_tables[0]
                self.history_prefix.append(
                    "Observation: （系统）选定表暂无列信息（可能未导入）。Finish 仍须含三标签；"
                    f"SQL 可写 `SELECT * FROM `{primary}` WHERE 1=0`。"
                )
        combined_q = f"{question} {rewritten_question or ''}"
        if _question_has_time_intent(combined_q):
            self.history_prefix.append(
                "Observation: （系统）问题像时间范围/时间趋势类。优先 Find_time_columns / Profile_time_column，"
                "确认真实时间列与格式后，再写 WHERE / GROUP BY。"
            )
        if _looks_like_identifier_query(combined_q):
            self.history_prefix.append(
                "Observation: （系统）问题含编号/ID 意图。优先找到主键/编号列做等值过滤，"
                "不要把编号当普通文本值在全库反复模糊搜。"
            )
        if _question_has_join_intent(combined_q) or len(resolved_tables) > 1:
            self.history_prefix.append(
                "Observation: （系统）问题可能涉及跨表。优先用 Infer_join_candidates / Find_join_path 确认 JOIN 键，"
                "再在一条 SELECT 中完成查询。"
            )


        for step in range(1, self.max_steps + 1):
            if step == self.max_steps - 1:
                self.history.append(
                    "Observation: （系统）ReAct 剩余步数已很少。"
                    "若还未输出最终 SQL，本轮或下一轮**必须 Finish**（三标签齐全）；禁止再只用 Explain_sql/Validate_sql。"
                )
            _trim_react_history(self.history)
            if stall_bumps >= 2:
                self.history.append(STALL_ESCALATION)
                stall_bumps = 0
            logger.info("ReAct step start step=%s history_items=%s", step, len(self.history))
            response_text = self.llm.think(self._build_prompt(rewritten_question or question, knowledge_context))
            thought, action = _parse_output(response_text)
            finish_action = _extract_finish_anywhere(response_text)
            logger.info(
                "ReAct step parsed step=%s thought=%r action=%r finish_found=%s",
                step,
                thought[:160],
                action[:160],
                bool(finish_action),
            )
            if thought:
                self.history.append(f"Thought: {thought}")
            if finish_action:
                action = finish_action
            elif not action:
                action = _recover_action_from_text(response_text)
            if not action:
                logger.warning("ReAct step missing action step=%s", step)
                logger.warning("ReAct raw response step=%s response=%r", step, response_text[:800])
                stall_bumps += 1
                yield {
                    "type": "step",
                    "step": step,
                    "thought": thought,
                    "action": None,
                    "raw_response_preview": response_text[:1200],
                }
                self.history.append(
                    "Observation: 你的上一条回复没有给出合法 Action。"
                    " 请严格只输出 Thought 和 Action，并且 Action 必须是 ToolName[...] 或 Finish[...]."
                )
                continue
            self.history.append(f"Action: {action}")
            yield {"type": "step", "step": step, "thought": thought, "action": action}

            action_name, action_input = _parse_action(action)
            if action_name == "CLARIFICATION":
                schema_columns = _extract_schema_columns(tool_trace)
                if (
                    schema_columns
                    and not _has_previewed_rows(tool_trace)
                    and not _question_directly_mentions_schema_column(rewritten_question or question, schema_columns)
                ):
                    _auto_preview_rows(self.tools, tool_trace, self.history, step)
                    self.history.append(
                        "Observation: 你刚才准备直接澄清，但在澄清前应先根据表明细判断目标信息是否藏在列值中。请基于 schema 和样本继续推理。"
                    )
                    continue
                clarification = (action_input or "").strip() or "需要更多信息才能完成查询。"
                qlog = log_query(question, "根据工具结果需要澄清", None, [item.model_dump() for item in tool_trace])
                resp = QueryResponse(
                    question=question,
                    understanding="根据工具结果需要进一步澄清",
                    clarification=clarification,
                    called_tools=[item.tool for item in tool_trace],
                    tool_trace=tool_trace,
                    query_log_id=qlog,
                    agent_transcript=self._agent_transcript(),
                )
                yield {"type": "done", "response": resp.model_dump()}
                return
            if (
                action_name
                and action_name not in ("Finish", "CLARIFICATION")
                and action_name not in REACT_PROBE_TOOL_NAMES
            ):
                logger.warning("ReAct disallowed tool step=%s tool=%r", step, action_name)
                stall_bumps += 1
                extra_hallucination = ""
                an = action_name or ""
                if re.search(r"(?i)construct|generate|build|compose|assemble", an) and re.search(
                    r"(?i)sql|where|query|select", an
                ):
                    extra_hallucination = (
                        " **WHERE / SELECT 只能写在 `Finish` 的 `SQL:` 标签下**，不存在分步「拼 SQL」类工具；"
                        "不要用 `Construct_*` / `Generate_*` 等虚构名称。"
                    )
                self.history.append(
                    "Observation: 不允许使用虚构工具 `"
                    + action_name
                    + "`。只能用: "
                    + REACT_PROBE_TOOLS_DISPLAY
                    + "，或 **Finish[UNDERSTANDING: ... SQL: SELECT ... WHERE ... SUMMARY: ...]** / CLARIFICATION[...]。"
                    "没有 system、数据库控制台。要找「某词在哪些列」用 Search_keyword_across_columns。"
                    + extra_hallucination
                )
                continue
            if action_name == "Finish":
                logger.info("ReAct finish step=%s", step)
                understanding, sql, summary, clarification = _parse_finish_block(action_input or "")
                if clarification and not sql:
                    logger.info("ReAct clarification step=%s clarification=%r", step, clarification[:200])
                    qlog = log_query(question, understanding, None, [item.model_dump() for item in tool_trace])
                    resp = QueryResponse(
                        question=question,
                        understanding=understanding,
                        clarification=clarification,
                        called_tools=[item.tool for item in tool_trace],
                        tool_trace=tool_trace,
                        summary=summary,
                        query_log_id=qlog,
                        agent_transcript=self._agent_transcript(),
                    )
                    yield {"type": "done", "response": resp.model_dump()}
                    return
                if not sql:
                    logger.warning("ReAct finish missing sql step=%s", step)
                    stall_bumps += 1
                    self.history.append("Observation: 你的 Finish 缺少 SQL 或 CLARIFICATION。请严格输出 Finish[UNDERSTANDING: ... SQL: SELECT ... SUMMARY: ...]。")
                    continue
                schema_columns = _extract_schema_columns(tool_trace)
                if not schema_columns:
                    stall_bumps += 1
                    self.history.append(
                        "Observation: 未找到 schema 列名，请用 History 开头的列名写 SQL，输出 Finish。"
                    )
                    continue
                unknown_identifiers = _find_unknown_sql_identifiers(sql, schema_columns)
                if unknown_identifiers:
                    if not _has_previewed_rows(tool_trace) and not _question_directly_mentions_schema_column(
                        rewritten_question or question, schema_columns
                    ):
                        _auto_preview_rows(self.tools, tool_trace, self.history, step)
                    stall_bumps += 1
                    self.history.append(
                        "Observation: SQL 含未知列: "
                        + ", ".join(unknown_identifiers)
                        + "。只能用: "
                        + ", ".join(schema_columns)
                        + "。请改 Finish。"
                    )
                    continue
                sql_to_run = _strip_trailing_limit_for_full_intent(question, rewritten_question, sql)
                if sql_to_run != sql:
                    logger.info("ReAct stripped trailing LIMIT for full-data intent step=%s", step)
                logger.info("ReAct execute final sql step=%s sql=%r", step, sql_to_run[:300])
                yield {"type": "sql", "step": step, "sql": sql_to_run}
                combined_q = f"{question or ''} {rewritten_question or ''}"
                exec_cap: int | None = (
                    -1
                    if _full_data_intent(combined_q) and not _explicit_row_cap_intent(combined_q)
                    else None
                )
                try:
                    result = execute_sql(sql_to_run, max_rows=exec_cap)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("ReAct final sql failed step=%s error=%r sql=%r", step, str(exc), sql[:300])
                    stall_bumps += 1
                    yield {"type": "sql_error", "step": step, "error": str(exc)}
                    self.history.append(f"Observation: SQL 执行失败: {exc}。请修正后重新 Finish。")
                    continue
                qlog = log_query(question, understanding, result["sql"], [item.model_dump() for item in tool_trace])
                clarification_hint = _optional_success_clarification(
                    question, tool_trace, self.history_prefix + self.history
                )
                sum_line = summary or f"返回 {result.get('row_count', len(result['rows']))} 行结果。"
                if result.get("truncated"):
                    sum_line += "（结果已达系统行数上限被截断，可缩小条件或在问题中明确要求全量。）"
                resp = QueryResponse(
                    question=question,
                    understanding=understanding,
                    sql=result["sql"],
                    columns=result["columns"],
                    rows=result["rows"],
                    called_tools=[item.tool for item in tool_trace],
                    tool_trace=tool_trace,
                    clarification=clarification_hint,
                    summary=sum_line,
                    query_log_id=qlog,
                    agent_transcript=self._agent_transcript(),
                )
                yield {"type": "done", "response": resp.model_dump()}
                return

            if not action_name:
                logger.warning("ReAct invalid action step=%s raw_action=%r", step, action[:200])
                stall_bumps += 1
                self.history.append(
                    f"Observation: Action 无法解析（禁止自然语言如 Analyze/Determine，必须用 JSON 工具调用）。"
                    f" 合法探查工具: {REACT_PROBE_TOOLS_DISPLAY}。"
                    " 日期/注册区间请 **Profile_time_column**[{{\"column\":\"真实列名\"}}] 或 **Find_time_columns**；"
                    "只看少数列请 **Inspect_rows**[{{\"columns\":[\"列1\",\"列2\"],\"limit\":10}}]。"
                    f" 原始片段: {action[:200]}"
                )
                continue

            arguments = _parse_tool_input(action_name, action_input or "")
            if (
                action_name == "Inspect_rows"
                and _count_preview_rows_in_trace(tool_trace) >= 1
                and not _question_directly_mentions_schema_column(rewritten_question or question, _extract_schema_columns(tool_trace))
                and not _has_search_probe(tool_trace)
            ):
                logger.info("ReAct block repeated preview step=%s", step)
                stall_bumps += 1
                self.history.append(
                    "Observation: 已经用过 Inspect_rows 看过样本，**不要**继续只靠增大 limit 猜字段/值。"
                    "下一步请改用 Search_keyword_in_tables、Search_keyword_across_columns、Find_relevant_columns 或 Profile_column 之一，"
                    "根据返回结果定位真实字段后再 Finish。"
                )
                continue
            canon_args = self.tools.canonical_arguments(action_name, arguments)
            signature = json.dumps({"tool": action_name, "arguments": canon_args}, ensure_ascii=False, sort_keys=True)
            if signature in seen_tool_calls:
                logger.info("ReAct skip duplicate tool step=%s tool=%s arguments=%s", step, action_name, arguments)
                stall_bumps += 1
                cq = f"{question} {rewritten_question or ''}".strip()
                nudge = _nudge_finish_after_duplicate_probe(cq, tool_trace, action_name)
                self.history.append(
                    "Observation: 该工具与参数已执行过，**禁止**原样重试。"
                    "请**改用其它探查工具**（不同 ToolName 或不同 JSON 参数），或在信息已够时 **Finish**。"
                    + nudge
                )
                continue
            seen_tool_calls.add(signature)

            logger.info("ReAct tool call step=%s tool=%s arguments=%s", step, action_name, arguments)
            yield {"type": "tool_start", "step": step, "tool": action_name, "arguments": arguments}
            result = self.tools.run(action_name, arguments)
            logger.info("ReAct tool result step=%s tool=%s error=%s", step, action_name, result.get("error"))
            tool_trace.append(ToolTrace(step=step, tool=action_name, arguments=arguments, result=result))
            yield {
                "type": "tool_result",
                "step": step,
                "tool": action_name,
                "arguments": arguments,
                "result": _compact_observation(result, limit=8000),
            }
            self.history.append(f"Observation: {_compact_observation(result)}")
            stall_bumps = 0

            if action_name in ("Validate_sql", "Explain_sql"):
                ev_total = sum(1 for x in tool_trace if x.tool in ("Validate_sql", "Explain_sql"))
                if ev_total >= 2:
                    self.history.append(
                        "Observation: （系统）校验与执行计划已重复占用步数。"
                        "下一轮**必须** `Finish[UNDERSTANDING: ... SQL: SELECT ... SUMMARY: ...]` 输出最终查询；勿再 Explain_sql/Validate_sql。"
                    )
                elif not _has_search_probe(tool_trace) and not _question_directly_mentions_schema_column(
                    rewritten_question or question, _extract_schema_columns(tool_trace)
                ):
                    self.history.append(
                        "Observation: （系统）当前还缺少值定位证据。"
                        "下一轮优先用 Search_keyword_in_tables / Search_keyword_across_columns / Find_relevant_columns 做探查，再 Finish。"
                    )

            if action_name == "Get_table_schema":
                schema_columns = _extract_schema_columns(tool_trace)
                if schema_columns:
                    self.history.append(
                        "Observation: 列名: " + ", ".join(schema_columns) + "。可 Inspect_rows 后 Finish。"
                    )

        logger.warning("ReAct run max steps reached question=%r", question)
        resp = QueryResponse(
            question=question,
            understanding="这次查询没有稳定完成",
            clarification=_final_clarification(question, tool_trace, self.history_prefix + self.history),
            called_tools=[item.tool for item in tool_trace],
            tool_trace=tool_trace,
            agent_transcript=self._agent_transcript(),
        )
        yield {"type": "done", "response": resp.model_dump()}

    def run(self, question: str, context_tables: list[str] | None = None) -> QueryResponse:
        final: QueryResponse | None = None
        for event in self.iter_events(question, context_tables):
            if event.get("type") == "done":
                payload = event.get("response")
                if isinstance(payload, dict):
                    final = QueryResponse.model_validate(payload)
                else:
                    final = payload  # type: ignore[assignment]
        if final is None:
            return QueryResponse(
                question=question,
                understanding="这次查询没有稳定完成",
                clarification="Agent 未返回结果",
                called_tools=[],
            )
        return final


tool_executor = ToolExecutor()


def list_tools() -> list[ToolDefinition]:
    return tool_executor.list_tools()


def run_tool(name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return tool_executor.run(name, arguments)


def run_sql_agent(question: str, context_tables: list[str] | None = None) -> QueryResponse:
    resolved = _resolve_context_tables(context_tables, question)
    fallback_tables = queryable_table_names()
    default_table = resolved[0] if resolved else (fallback_tables[0] if fallback_tables else USERS_TABLE)
    tools = ToolExecutor(default_table=default_table, context_tables=resolved)
    try:
        return ReActAgent(tools=tools).run(question, resolved)
    except httpx.HTTPError as exc:
        logger.warning("run_sql_agent http error question=%r error=%s", question, exc)
        return QueryResponse(question=question, understanding="LLM 当前不可用", clarification=f"模型调用失败：{exc}", called_tools=[])
    except Exception as exc:  # noqa: BLE001
        logger.warning("run_sql_agent failed question=%r error=%s", question, exc)
        return QueryResponse(question=question, understanding="这次查询没有稳定完成", clarification=str(exc), called_tools=[])


def iter_sql_agent_events(question: str, context_tables: list[str] | None = None) -> Iterator[dict[str, Any]]:
    """供 SSE 使用；异常时产出单条 done 且 clarification 为错误信息。"""
    resolved = _resolve_context_tables(context_tables, question)
    fallback_tables = queryable_table_names()
    default_table = resolved[0] if resolved else (fallback_tables[0] if fallback_tables else USERS_TABLE)
    tools = ToolExecutor(default_table=default_table, context_tables=resolved)
    try:
        yield from ReActAgent(tools=tools).iter_events(question, resolved)
    except httpx.HTTPError as exc:
        logger.warning("iter_sql_agent_events http error question=%r error=%s", question, exc)
        resp = QueryResponse(
            question=question,
            understanding="LLM 当前不可用",
            clarification=f"模型调用失败：{exc}",
            called_tools=[],
        )
        yield {"type": "done", "response": resp.model_dump()}
    except Exception as exc:  # noqa: BLE001
        logger.warning("iter_sql_agent_events failed question=%r error=%s", question, exc)
        resp = QueryResponse(
            question=question,
            understanding="这次查询没有稳定完成",
            clarification=str(exc),
            called_tools=[],
        )
        yield {"type": "done", "response": resp.model_dump()}
