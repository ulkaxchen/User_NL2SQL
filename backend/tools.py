from __future__ import annotations

import inspect
import json
import os
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import httpx

from . import db
from .country_match import resolve_country_intent
from .db import (
    USERS_TABLE,
    explain_sql,
    find_join_path,
    find_relevant_columns,
    find_time_columns,
    get_table_relationships,
    get_table_row_count,
    get_table_schema,
    infer_join_candidates,
    inspect_rows,
    list_columns,
    list_tables,
    profile_column,
    profile_table_columns,
    profile_time_column,
    queryable_table_names,
    recent_query_examples,
    search_similar_values,
    validate_join_candidate,
    validate_sql,
)
from .schemas import ToolDefinition


# 默认精简知识块长度；需要完整上下文时设 USER_RAG_FULL_KNOWLEDGE=1
_KNOWLEDGE_VERBOSE = os.environ.get("USER_RAG_FULL_KNOWLEDGE", "").strip().lower() in ("1", "true", "yes", "on")

ASSETS_PATH = Path(__file__).resolve().parent / "knowledge_assets.json"
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://127.0.0.1:11434").strip() or "http://127.0.0.1:11434"
SEMANTIC_LLM_MODEL = (
    os.environ.get("USER_RAG_SEMANTIC_LLM_MODEL")
    or os.environ.get("OLLAMA_MODEL")
    or "qwen2.5:3b"
).strip() or "qwen2.5:3b"
SEMANTIC_LLM_MODE = (os.environ.get("USER_RAG_SEMANTIC_LLM", "auto") or "auto").strip().lower()
SEMANTIC_LLM_TIMEOUT = min(max(int(os.environ.get("USER_RAG_SEMANTIC_LLM_TIMEOUT", "12")), 3), 30)

_SEMANTIC_ALIASES: dict[str, tuple[str, ...]] = {
    "预测": ("pred", "predict", "forecast", "prob", "result"),
    "实际": ("actual", "real", "qty", "sale", "sales", "sum"),
    "销量": ("qty", "sale", "sales", "sum"),
    "数量": ("qty", "count", "sum", "amount"),
    "准确率": ("accuracy", "acc", "rate", "ratio"),
    "准确": ("accuracy", "acc", "rate"),
    "月份": ("month", "date_month"),
    "日期": ("date", "day", "dt", "time"),
    "时间": ("time", "date", "dt"),
    "国家": ("country", "nation", "region", "area"),
    "地区": ("region", "area", "country"),
    "用户": ("user", "uid", "nickname", "email", "phone"),
    "邮箱": ("email", "mail"),
    "平台": ("platform", "channel"),
}
_PLATFORM_TOKENS = {
    "app", "pc", "web", "h5", "ios", "android", "mac", "windows", "desktop", "mobile",
    "miniapp", "小程序", "微信", "wechat", "browser", "api",
}
_PLATFORM_COLUMN_HINTS = ("平台", "渠道", "platform", "channel", "device", "terminal", "终端", "source", "os")
_COUNTRY_COLUMN_HINTS = ("国家", "地区", "国别", "区域", "country", "region", "nation", "market", "city", "province")
_DERIVED_DURATION_TOKENS = ("多久", "多长时间", "时长", "年龄", "tenure", "lifetime", "since", "elapsed", "duration", "间隔", "耗时", "距今")
_DERIVED_RATIO_TOKENS = ("占比", "比例", "rate", "ratio", "转化率", "百分比", "%")
_DERIVED_GROWTH_TOKENS = ("同比", "环比", "yoy", "mom", "增长率", "增幅", "变化率")
_MEASURE_NAME_HINTS = ("数量", "count", "qty", "sum", "amount", "revenue", "cost", "price", "sales", "sale", "score", "bcr", "rate", "ratio", "accuracy", "acc", "金额", "费用", "收入", "利润")
_SEMANTIC_ROLE_NAMES = (
    "entity_key",
    "time",
    "country",
    "platform",
    "category",
    "measure",
    "ratio",
    "email",
    "phone",
    "free_text",
)


def _normalize(text: str) -> str:
    return str(text or "").strip().lower()


def _tokenize(text: str) -> list[str]:
    normalized = _normalize(text)
    raw_latin = re.findall(r"[a-zA-Z0-9_]+", normalized)
    latin_tokens: list[str] = []
    for token in raw_latin:
        latin_tokens.append(token)
        latin_tokens.extend(part for part in re.split(r"_+", token) if part and part != token)
    han_tokens: list[str] = []
    for seq in re.findall(r"[一-鿿]+", normalized):
        seq = seq.strip()
        if not seq:
            continue
        if len(seq) == 1:
            han_tokens.append(seq)
            continue
        # 中文不做分词，改用 2~3 字滑窗，减少单字碰撞带来的误召回。
        for n in (2, 3):
            if len(seq) < n:
                continue
            han_tokens.extend(seq[i : i + n] for i in range(len(seq) - n + 1))
    alias_tokens: list[str] = []
    for trigger, aliases in _SEMANTIC_ALIASES.items():
        if trigger in normalized:
            alias_tokens.extend(aliases)
    return latin_tokens + han_tokens + alias_tokens


def _overlap_score(question: str, candidate: str) -> int:
    q = _normalize(question)
    c = _normalize(candidate)
    if not q or not c:
        return 0
    score = 0
    if c in q or q in c:
        score += 8
    q_tokens = set(_tokenize(q))
    c_tokens = set(_tokenize(c))
    score += len(q_tokens & c_tokens)
    return score


def _load_assets() -> dict[str, list[dict[str, Any]]]:
    if not ASSETS_PATH.exists():
        return {"glossary": [], "metrics": [], "rules": []}
    try:
        payload = json.loads(ASSETS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"glossary": [], "metrics": [], "rules": []}
    return {
        "glossary": list(payload.get("glossary", [])),
        "metrics": list(payload.get("metrics", [])),
        "rules": list(payload.get("rules", [])),
    }


def _resolve_knowledge_tables(context_tables: list[str] | None = None) -> list[str]:
    allowed = queryable_table_names()
    if not allowed:
        return []
    allow_set = set(allowed)
    if not context_tables:
        return list(allowed)
    seen: set[str] = set()
    picked: list[str] = []
    for raw in context_tables:
        name = str(raw or "").strip()
        if name and name in allow_set and name not in seen:
            seen.add(name)
            picked.append(name)
    return picked or list(allowed)


def _schema_map_for_tables(tables: list[str]) -> dict[str, list[dict[str, Any]]]:
    return {table: list_columns(table) for table in tables}


def _resolve_relevant_schema(question: str, tables: list[str], max_tables: int, max_columns_per_table: int) -> dict[str, list[str]]:
    ranked = search_relevant_schema(
        question,
        max_tables=max(1, min(max_tables, len(tables) or 1)),
        max_columns_per_table=max_columns_per_table,
    )
    picked_tables = [t for t in ranked.get("tables", []) if t in set(tables)]
    if not picked_tables:
        picked_tables = list(tables[:max_tables])
    columns: dict[str, list[str]] = {}
    for table in picked_tables:
        cols = [str(c) for c in ranked.get("columns", {}).get(table, []) if str(c).strip()]
        if not cols:
            cols = [str(c.get("name") or "") for c in list_columns(table)[:max_columns_per_table] if c.get("name")]
        columns[table] = cols[:max_columns_per_table]
    return columns


def _relevant_columns(question: str, tables: list[str], max_tables: int = 4, max_columns_per_table: int = 5) -> list[dict[str, Any]]:
    schema_map = _schema_map_for_tables(tables)
    relevant = _resolve_relevant_schema(question, tables, max_tables=max_tables, max_columns_per_table=max_columns_per_table)
    out: list[dict[str, Any]] = []
    for table, names in relevant.items():
        meta_by_name = {str(col.get("name") or ""): col for col in schema_map.get(table, [])}
        for name in names:
            col = dict(meta_by_name.get(name) or {})
            if not col:
                continue
            col["table"] = table
            out.append(col)
    if out:
        return out
    fallback: list[dict[str, Any]] = []
    for table in tables[:max_tables]:
        for col in schema_map.get(table, [])[:max_columns_per_table]:
            item = dict(col)
            item["table"] = table
            fallback.append(item)
    return fallback


def _column_value_hints(columns: list[dict[str, Any]], max_fields: int = 4) -> list[str]:
    hints: list[str] = []
    for column in columns[:max_fields]:
        field = str(column.get("name") or "")
        table = str(column.get("table") or "")
        if not table or not field:
            continue
        # 仅对低基数/文本列给出值域提示，避免大列扫描噪音。
        if not column.get("is_text_like") and not column.get("enum_candidate") == "likely_low":
            continue
        try:
            pr = profile_column(table=table, field=field, top_k=5, include_top_values=True)
            values = (pr.get("data") or {}).get("top_values", []) if pr.get("ok") else []
        except Exception:  # noqa: BLE001
            values = []
        if not values:
            continue
        display = ", ".join(str(item.get("value")) for item in values[:5] if item.get("value") is not None)
        if display:
            hints.append(f"- `{table}`.`{field}`: 高频值有 {display}")
    return hints


def _is_current_schema_sql(sql: str, schema_map: dict[str, list[dict[str, Any]]]) -> bool:
    allowed_tables = set(schema_map)
    allowed_columns = {str(col.get("name") or "") for cols in schema_map.values() for col in cols if col.get("name")}

    quoted_identifiers = re.findall(r'["`]\s*([^"`]+?)\s*["`]', sql)
    for identifier in quoted_identifiers:
        stripped = identifier.strip()
        if "." in stripped:
            left, right = [x.strip() for x in stripped.split(".", 1)]
            if left not in allowed_tables or right not in allowed_columns:
                return False
            continue
        if stripped not in allowed_tables and stripped not in allowed_columns:
            return False

    cleaned = re.sub(r"'(?:''|[^'])*'", " ", sql)
    cleaned = re.sub(r'["`]\s*([^"`]+?)\s*["`]', " ", cleaned)
    ascii_tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", cleaned)
    keywords = {
        "select", "from", "where", "and", "or", "as", "count", "sum", "avg", "min", "max", "distinct",
        "order", "by", "group", "limit", "between", "datetime", "date", "now", "current_date",
        "current_timestamp", "asc", "desc", "null", "is", "not", "like", "in", "on", "join", "left",
        "right", "inner", "outer", "case", "when", "then", "else", "end", "cast", "strftime",
        "julianday", "true", "false", "text", "integer", "real", "blob", "numeric", "lower", "upper",
        "trim", "ltrim", "rtrim", "replace", "substr", "substring", "instr", "coalesce", "ifnull", "nullif",
        "typeof", "length", "round", "abs", "hex", "quote", "glob", "like", "date_sub",
    }
    for token in ascii_tokens:
        low = token.lower()
        if low in keywords or token in allowed_tables or token in allowed_columns:
            continue
        return False
    return True


def _retrieve_examples(question: str, schema_map: dict[str, list[dict[str, Any]]], max_items: int = 3) -> list[dict[str, Any]]:
    examples = recent_query_examples(limit=30)
    scored: list[tuple[int, dict[str, Any]]] = []
    for item in examples:
        sql = str(item.get("sql") or "")
        if sql and not _is_current_schema_sql(sql, schema_map):
            continue
        combined = " ".join(str(item.get(key) or "") for key in ("question", "understanding", "sql"))
        score = _overlap_score(question, combined)
        if score > 0:
            scored.append((score, item))
    ranked = [item for score, item in sorted(scored, key=lambda pair: pair[0], reverse=True)]
    return ranked[:max_items]


def _retrieve_assets(question: str, asset_type: str, max_items: int = 3) -> list[dict[str, Any]]:
    assets = _load_assets().get(asset_type, [])
    scored: list[tuple[int, dict[str, Any]]] = []
    for item in assets:
        text = " ".join(str(value or "") for value in item.values())
        score = _overlap_score(question, text)
        if score > 0:
            scored.append((score, item))
    ranked = [item for score, item in sorted(scored, key=lambda pair: pair[0], reverse=True)]
    return ranked[:max_items]


def _asset_schema_refs(item: dict[str, Any]) -> tuple[set[str], set[str]]:
    text = " ".join(str(value or "") for value in item.values())
    table_refs = {m.strip() for m in re.findall(r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_]*)", text, re.IGNORECASE)}
    column_refs: set[str] = set()
    for field in item.get("fields", []) or []:
        token = str(field or "").strip()
        if token:
            column_refs.add(token)
    for token in re.findall(r"`([^`]+)`", text):
        stripped = token.strip()
        if not stripped:
            continue
        if "." in stripped:
            left, right = [part.strip() for part in stripped.split(".", 1)]
            if left:
                table_refs.add(left)
            if right:
                column_refs.add(right)
        elif re.match(r"[A-Za-z_][A-Za-z0-9_]*$", stripped):
            table_refs.add(stripped)
        else:
            column_refs.add(stripped)
    return table_refs, column_refs


def _filter_assets_for_schema(items: list[dict[str, Any]], schema_map: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    if not schema_map:
        return items
    current_tables = set(schema_map)
    current_columns = {
        str(col.get("name") or "")
        for cols in schema_map.values()
        for col in cols
        if str(col.get("name") or "").strip()
    }
    filtered: list[dict[str, Any]] = []
    for item in items:
        table_refs, column_refs = _asset_schema_refs(item)
        if table_refs and table_refs.isdisjoint(current_tables):
            continue
        if column_refs and column_refs.isdisjoint(current_columns):
            continue
        filtered.append(item)
    return filtered


def search_relevant_schema(question: str, max_tables: int = 5, max_columns_per_table: int = 8) -> dict[str, Any]:
    """按问题从全库表/列名与注释相关度召回，供 agent 压缩 schema 上下文。"""
    q = str(question or "").strip()
    meta = list_tables()
    if not meta:
        return {"question": q, "tables": [], "columns": {}, "table_scores": {}}
    names = [str(m.get("name") or "") for m in meta if m.get("name")]
    max_tables = max(1, min(max_tables, len(names)))

    table_scores: list[tuple[int, str, list[tuple[int, str]]]] = []
    q_lower = q.lower()
    for t in names:
        cols = list_columns(t)
        scored_cols: list[tuple[int, str]] = []
        table_score = _overlap_score(q, t)
        for c in cols:
            cn = str(c.get("name") or "")
            lab = str(c.get("label") or cn) # distance between question and column name/label is a strong signal of relevance, so we take the max score of both to give the column a better chance to be selected if either matches well.
            score = max(_overlap_score(q, cn), _overlap_score(q, lab))
            if c.get("is_time_like") and any(k in q_lower for k in ("时间", "日期", "天", "月", "年", "最近", "趋势", "date", "time", "month", "day")):
                score += 3
            if c.get("is_numeric") and any(k in q_lower for k in ("数量", "多少", "合计", "总", "平均", "sum", "avg", "count", "accuracy", "rate", "ratio", "qty")):
                score += 2
            if score > 0 and cn:
                scored_cols.append((score, cn))
        scored_cols.sort(key=lambda item: (-item[0], item[1]))
        if scored_cols:
            table_score += scored_cols[0][0] * 4
            table_score += sum(score for score, _ in scored_cols[:3])
        table_scores.append((table_score, t, scored_cols))

    ranked_pairs = sorted(table_scores, key=lambda item: (-item[0], item[1]))
    ranked_tables = [name for _, name, _ in ranked_pairs]
    pick_tables = ranked_tables[:max_tables]
    score_map = {name: scored for _, name, scored in table_scores}
    table_score_map = {name: score for score, name, _ in ranked_pairs}

    columns: dict[str, list[str]] = {}
    for t in pick_tables:
        cols = list_columns(t)
        scored_cols = score_map.get(t, [])
        top_named = [c for s, c in scored_cols if s > 0][:max_columns_per_table]
        if not top_named:
            top_named = [str(c.get("name") or "") for c in cols[:max_columns_per_table]]
        columns[t] = top_named
    return {"question": q, "tables": pick_tables, "columns": columns, "table_scores": table_score_map}


def get_business_definitions(term: str) -> dict[str, Any]:
    """从 glossary / metrics / rules 资产中按相关度检索业务口径。"""
    text = str(term or "").strip()
    if not text:
        return {"term": "", "error": "term 不能为空", "glossary": [], "metrics": [], "rules": []}
    return {
        "term": text,
        "glossary": _retrieve_assets(text, "glossary", max_items=8),
        "metrics": _retrieve_assets(text, "metrics", max_items=8),
        "rules": _retrieve_assets(text, "rules", max_items=8),
    }


def search_similar_queries(question: str, limit: int = 5) -> dict[str, Any]:
    """检索用户标记为有帮助的历史问答+SQL，按与当前问题的字面相关度排序。"""
    q = str(question or "").strip()
    if not q:
        return {"question": "", "matches": []}
    lim = min(max(limit, 1), 20)
    pool = recent_query_examples(limit=max(lim * 4, 20))
    scored: list[tuple[int, dict[str, Any]]] = []
    for item in pool:
        blob = " ".join(str(item.get(k) or "") for k in ("question", "understanding", "sql"))
        scored.append((_overlap_score(q, blob), item))
    scored.sort(key=lambda pair: pair[0], reverse=True)
    high = [item for s, item in scored if s > 0]
    low = [item for s, item in scored if s == 0]
    ordered = high + low
    top = ordered[:lim]
    matches = [
        {
            "question": item.get("question"),
            "sql": item.get("sql"),
            "understanding": item.get("understanding"),
            "created_at": item.get("created_at"),
        }
        for item in top
    ]
    return {"question": q, "matches": matches}


def _extract_query_hints(question: str, tables: list[str]) -> list[str]:
    hints: list[str] = []
    if re.search(r"(?:\bID\b|\buid\b|编号|主键|订单号|用户ID|客户ID)\s*[为是=:：]?\s*[A-Za-z0-9_-]+", question, re.IGNORECASE):
        hints.append("- 精确实体: 问题含明显 ID/编号时，优先在对应 ID-like 列使用等值过滤，而不是做模糊搜索。")

    bulk_markers = ("所有", "全部", "每个", "每条", "完整", "全量", "列出所有", "符合条件", "所有数据", "所有记录")
    if any(m in question for m in bulk_markers):
        hints.append("- 结果范围: 用户要全量时，最终 SQL 不要在末尾追加 LIMIT；探查阶段仍可用小样本。")

    has_date_span = bool(
        re.search(r"\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日", question)
        or re.search(r"\d{4}-\d{1,2}-\d{1,2}", question)
        or re.search(r"\d{4}/\d{1,2}/\d{1,2}", question)
    )
    if has_date_span or any(w in question for w in ("最近", "本月", "上月", "本周", "季度", "趋势", "按月")):
        hints.append("- 时间条件: 先用 Find_time_columns / Profile_time_column 确认时间列与格式，再写范围过滤，不要把纯日期当关键词搜索。")

    if any(w.lower() in question.lower() for w in ("国家", "地区", "城市", "country", "region", "market", "印尼", "印度尼西亚")):
        hints.append("- 地域过滤: 先用 Find_relevant_columns、Profile_column、Search_keyword_across_columns 找真正承载国家/地区的列；在确认前不要把值直接写到任意文本列。")

    if any(w.lower() in question.lower() for w in ("多少", "几个", "数量", "count", "人数", "用户数", "订单数")):
        hints.append("- 查数口径: 先确认统计粒度与实体键；若不能确定一行就是一个实体，不要默认 COUNT(*)。")

    if len(tables) > 1 or any(w in question.lower() for w in ("join", "关联", "对比", "合并", "预测", "实际")):
        hints.append("- 多表查询: 先看 Get_table_relationships / Infer_join_candidates / Find_join_path；无外键时优先使用高置信同名键或时间键。")

    if any(w in question for w in ("分组", "每月", "每天", "趋势", "top", "排名", "占比", "平均")):
        hints.append("- 聚合分析: 明确 SELECT 维度列、聚合指标与 GROUP BY；排序后若只要前 N 条再加 LIMIT。")
    return hints


def build_knowledge_context(
    question: str,
    rewritten_question: str | None = None,
    context_tables: list[str] | None = None,
) -> str:
    effective_question = rewritten_question or question
    tables = _resolve_knowledge_tables(context_tables)
    schema_map = _schema_map_for_tables(tables)
    rcap_tables = 6 if _KNOWLEDGE_VERBOSE else 4
    rcap_cols = 6 if _KNOWLEDGE_VERBOSE else 4
    acap = 3 if _KNOWLEDGE_VERBOSE else 2
    ecap = 3 if _KNOWLEDGE_VERBOSE else 2
    vfields = 5 if _KNOWLEDGE_VERBOSE else 3

    relevant_columns = _relevant_columns(effective_question, tables, max_tables=rcap_tables, max_columns_per_table=rcap_cols)
    examples = _retrieve_examples(effective_question, schema_map, max_items=ecap)
    glossary_items = _filter_assets_for_schema(_retrieve_assets(effective_question, "glossary", max_items=max(acap * 3, 6)), schema_map)[:acap]
    metric_items = _filter_assets_for_schema(_retrieve_assets(effective_question, "metrics", max_items=max(acap * 3, 6)), schema_map)[:acap]
    rule_items = _filter_assets_for_schema(_retrieve_assets(effective_question, "rules", max_items=max(acap * 3, 6)), schema_map)[:acap]
    value_hints = _column_value_hints(relevant_columns, max_fields=vfields)
    table_hint_order = db._dedupe_preserve_text([str(item.get("table") or "") for item in relevant_columns if str(item.get("table") or "").strip()] + tables)
    focus_tables = table_hint_order[: min(3 if _KNOWLEDGE_VERBOSE else 2, len(table_hint_order))]
    slots = infer_query_slots(effective_question, tables=tables) if effective_question.strip() else None
    semantic_profiles: list[dict[str, Any]] = []
    for table in focus_tables:
        sem = profile_table_semantics(
            table=table,
            query=effective_question,
            max_columns=max(rcap_cols + 2, 8),
            sample_limit=4,
            distinct_limit=6,
        )
        if sem.get("ok"):
            semantic_profiles.append(sem)
    filter_groundings: list[dict[str, Any]] = []
    slot_filters = list(slots.get("filters", []) or []) if isinstance(slots, dict) else []
    for flt in slot_filters[: 3 if _KNOWLEDGE_VERBOSE else 2]:
        if not isinstance(flt, dict):
            continue
        raw_value = _norm(flt.get("raw_value"))
        semantic_type = _norm_low(flt.get("semantic_type")) or "generic"
        if not raw_value or semantic_type == "time":
            continue
        for table in focus_tables:
            inferred = infer_filter_columns(
                query=effective_question,
                table=table,
                value=raw_value,
                semantic_type=semantic_type,
                top_k=3,
                preview_limit=4,
            )
            picks = [item for item in (inferred.get("columns") or []) if isinstance(item, dict)][:2]
            if not picks:
                continue
            columns_for_examples = [str(item.get("column") or "") for item in picks if _norm(item.get("column"))]
            examples_result = search_value_examples(
                keyword_variants=flt.get("normalized_values") if isinstance(flt.get("normalized_values"), list) else None,
                keyword=raw_value,
                value=raw_value,
                table=table,
                columns=columns_for_examples,
                semantic_type=semantic_type,
                limit_per_column=2,
                max_columns=max(len(columns_for_examples), 2),
            )
            hits: list[str] = []
            if examples_result.get("ok"):
                for item in examples_result.get("columns", []) or []:
                    if not isinstance(item, dict):
                        continue
                    ex_vals = [str(v) for v in item.get("example_values", []) if _norm(v)]
                    if ex_vals:
                        hits.append(f"{item.get('column')}: {', '.join(ex_vals[:2])}")
            filter_groundings.append(
                {
                    "table": table,
                    "raw_value": raw_value,
                    "semantic_type": semantic_type,
                    "columns": picks,
                    "hits": hits,
                    "llm_used": bool(inferred.get("llm_used")),
                }
            )
    time_groundings: list[dict[str, Any]] = []
    has_time_intent = any(str(item.get("semantic_type") or "") == "time" for item in slot_filters) or any(
        tok in effective_question.lower() for tok in ("时间", "日期", "本月", "上月", "最近", "趋势", "按月", "按天")
    )
    if has_time_intent:
        for table in focus_tables:
            time_info = find_time_columns(table=table)
            time_columns = []
            if time_info.get("ok"):
                time_columns = list((time_info.get("data") or {}).get("time_columns") or [])
            if not time_columns:
                continue
            top_time = str((time_columns[0] or {}).get("column") or "")
            prof = profile_time_column(table=table, column=top_time) if top_time else {"ok": False}
            time_groundings.append({"table": table, "columns": time_columns[:3], "profile": prof})

    lines = ["## Data Overview"]
    if not tables:
        lines.append("- 当前没有可查询的业务表。")
    else:
        for table in tables[:rcap_tables]:
            cols = schema_map.get(table, [])
            lines.append(f"- `{table}`：约 {get_table_row_count(table)} 行、{len(cols)} 列")
        if len(tables) > rcap_tables:
            lines.append(f"- 其余 {len(tables) - rcap_tables} 张表已省略；必要时可用 Search_relevant_schema / Get_table_schema 继续探查。")

    if rewritten_question and rewritten_question.strip() and rewritten_question.strip() != question.strip():
        lines.extend(["", "## Rewritten Intent", f"- {rewritten_question.strip()}"])

    if isinstance(slots, dict) and slots.get("ok"):
        lines.extend(["", "## Intent Analysis"])
        lines.append(
            f"- 查询类型: `{slots.get('query_type') or 'list'}`；实体倾向: `{slots.get('entity_hint') or 'record'}`；"
            f"语义推理: {'LLM+规则' if slots.get('llm_used') else '规则'}"
        )
        filters_desc: list[str] = []
        for flt in slot_filters[: 4 if _KNOWLEDGE_VERBOSE else 3]:
            if not isinstance(flt, dict):
                continue
            raw_value = _norm(flt.get("raw_value"))
            semantic_type = _norm_low(flt.get("semantic_type")) or "generic"
            if not raw_value:
                continue
            extra = ""
            if semantic_type == "time" and _norm(flt.get("time_granularity")):
                extra = f" / {flt.get('time_granularity')}"
            filters_desc.append(f"`{raw_value}` -> {semantic_type}{extra}")
        if filters_desc:
            lines.append(f"- 抽取到的过滤意图: {', '.join(filters_desc)}")
        derived = slots.get("derived_metric") if isinstance(slots.get("derived_metric"), dict) else {}
        if derived.get("required"):
            lines.append(
                f"- 派生指标: 需要 `{derived.get('operation_type') or 'unknown'}`"
                + (f"（unit={derived.get('unit')}）" if derived.get("unit") else "")
            )
        next_tools = [str(x) for x in slots.get("recommended_next_tools", []) if _norm(x)]
        if next_tools:
            lines.append(f"- 语义建议优先探查: {', '.join(next_tools[:5])}")

    query_hints = _extract_query_hints(effective_question, tables)
    if query_hints:
        lines.extend(["", "## Query Hints"])
        lines.extend(query_hints)

    if relevant_columns:
        lines.extend(["", "## Relevant Schema"])
        grouped: dict[str, list[dict[str, Any]]] = {}
        for column in relevant_columns:
            grouped.setdefault(str(column.get("table") or ""), []).append(column)
        for table, cols in grouped.items():
            lines.append(f"- 表 `{table}`")
            for column in cols:
                name = str(column.get("name") or "")
                sql_type = str(column.get("sql_type") or "TEXT")
                flags: list[str] = []
                if column.get("is_time_like"):
                    flags.append("time")
                if column.get("is_numeric"):
                    flags.append("numeric")
                if column.get("is_text_like"):
                    flags.append("text")
                suffix = f" [{' / '.join(flags)}]" if flags else ""
                lines.append(f"  - `{name}` ({sql_type}){suffix}")

    if semantic_profiles:
        lines.extend(["", "## Semantic Table Focus"])
        for sem in semantic_profiles:
            table = str(sem.get("table") or "")
            summary = _norm(sem.get("llm_summary"))
            mode = "LLM+规则" if sem.get("llm_used") else "规则"
            lines.append(f"- 表 `{table}`（列角色推断: {mode}）")
            if summary:
                lines.append(f"  - 语义摘要: {summary}")
            for item in sem.get("columns", [])[: 5 if _KNOWLEDGE_VERBOSE else 4]:
                if not isinstance(item, dict):
                    continue
                roles = [f"{role.get('role')}:{int(role.get('score') or 0)}" for role in (item.get("roles") or [])[:3] if isinstance(role, dict)]
                samples = [str(v) for v in (item.get("top_values") or [])[:3] if _norm(v)]
                extra = f"；样例值: {', '.join(samples)}" if samples else ""
                lines.append(
                    f"  - `{item.get('column')}` -> {', '.join(roles) or '未判定'}"
                    f"；relevance={int(item.get('relevance_score') or 0)}{extra}"
                )

    if filter_groundings:
        lines.extend(["", "## Filter Grounding"])
        for item in filter_groundings[: 6 if _KNOWLEDGE_VERBOSE else 4]:
            column_parts = []
            for col in item.get("columns", []) or []:
                if not isinstance(col, dict):
                    continue
                reasons = ", ".join(str(r) for r in (col.get("reasons") or [])[:2] if _norm(r))
                column_parts.append(
                    f"`{item.get('table')}`.`{col.get('column')}`"
                    f"(score={int(col.get('score') or 0)}{'; ' + reasons if reasons else ''})"
                )
            lines.append(
                f"- `{item.get('raw_value')}` ({item.get('semantic_type')}) -> "
                + (" / ".join(column_parts) if column_parts else f"`{item.get('table')}` 中暂无高置信列")
                + (f"；依据: {'；'.join(item.get('hits') or [])}" if item.get("hits") else "")
                + ("；列选择含 LLM 重排" if item.get("llm_used") else "")
            )

    if time_groundings:
        lines.extend(["", "## Time Grounding"])
        for item in time_groundings:
            cols = [str(col.get("column") or "") for col in item.get("columns", []) if isinstance(col, dict) and _norm(col.get("column"))]
            if cols:
                lines.append(f"- 表 `{item.get('table')}` 的时间列候选: {', '.join(f'`{c}`' for c in cols)}")
            profile = item.get("profile") if isinstance(item.get("profile"), dict) else {}
            if profile.get("ok"):
                lines.append(
                    f"  - 优先时间列 `{profile.get('column')}`：min={profile.get('min_value')}, max={profile.get('max_value')}"
                )
                if profile.get("filter_hint"):
                    lines.append(f"  - 时间过滤提示: {profile.get('filter_hint')}")

    if len(tables) > 1:
        join_info = infer_join_candidates(tables=tables, limit=8)
        candidates = join_info.get("candidates", []) if isinstance(join_info, dict) else []
        if candidates:
            lines.extend(["", "## Join Candidates"])
            for item in candidates[:6]:
                reason = "; ".join(item.get("reasons", [])[:2])
                lines.append(
                    f"- `{item.get('from_table')}`.`{item.get('from_column')}` = `{item.get('to_table')}`.`{item.get('to_column')}`"
                    f"（{item.get('confidence', 'unknown')} / {reason or '启发式候选'}）"
                )

    if glossary_items:
        lines.extend(["", "## Glossary"])
        for item in glossary_items:
            lines.append(f"- 术语: {item.get('term', '')}")
            lines.append(f"  含义: {item.get('meaning', '')}")
            fields = item.get("fields", [])
            if fields:
                lines.append(f"  相关列: {', '.join(str(field) for field in fields)}")

    if metric_items:
        lines.extend(["", "## Metrics"])
        for item in metric_items:
            lines.append(f"- 指标: {item.get('name', '')}")
            lines.append(f"  定义: {item.get('definition', '')}")
            formula = item.get("formula")
            if formula:
                lines.append(f"  SQL提示: {formula}")

    if rule_items:
        lines.extend(["", "## Rules"])
        for item in rule_items:
            lines.append(f"- 规则: {item.get('name', '')}")
            lines.append(f"  说明: {item.get('description', '')}")

    if value_hints:
        lines.extend(["", "## Frequent Values"])
        lines.extend(value_hints)

    if examples:
        lines.extend(["", "## Similar Successful Queries"])
        for item in examples:
            lines.append(f"- 问题: {item.get('question', '')}")
            lines.append(f"  SQL: {item.get('sql', '')}")

    lines.extend(
        [
            "",
            "## Guidance",
            "- 优先根据已选表的 schema、语义画像、过滤 grounding 与 JOIN 候选写 SQL，不要假设只有某一张默认表。",
            "- 不确定表时先 Search_relevant_schema；不确定值在哪列时先看 Filter Grounding，再用 Search_keyword_across_columns 或 Search_keyword_in_tables。",
            "- 最终只在 Finish 的 SQL: 行输出一条 SELECT；确认信息已够时立即 Finish。",
        ]
    )
    return "\n".join(lines).strip()


def _norm(text: Any) -> str:
    return str(text or "").strip()


def _norm_low(text: Any) -> str:
    return _norm(text).lower()


def _clean_key(text: Any) -> str:
    cleaned = _norm_low(text)
    return re.sub(r"[\s\-_/:;,.()\[\]{}]+", "", cleaned)


def _parse_json_object(text: str) -> dict[str, Any] | None:
    raw = _norm(text)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else None
    except Exception:
        pass
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if not match:
        return None
    try:
        payload = json.loads(match.group(0))
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _confidence_rank(level: str) -> int:
    return {"high": 3, "medium": 2, "low": 1}.get(_norm_low(level), 0)


def _normalize_semantic_role(role: Any) -> str:
    raw = _norm_low(role).replace("-", "_").replace(" ", "_")
    alias_map = {
        "id": "entity_key",
        "key": "entity_key",
        "primary_key": "entity_key",
        "datetime": "time",
        "date": "time",
        "timestamp": "time",
        "geo": "country",
        "region": "country",
        "nation": "country",
        "channel": "platform",
        "device": "platform",
        "metric": "measure",
        "numeric": "measure",
        "number": "measure",
        "percentage": "ratio",
        "text": "free_text",
        "freetext": "free_text",
    }
    normalized = alias_map.get(raw, raw)
    return normalized if normalized in _SEMANTIC_ROLE_NAMES else ""


def _rebuild_role_buckets(columns: list[dict[str, Any]]) -> dict[str, list[str]]:
    role_buckets: dict[str, list[str]] = {role: [] for role in _SEMANTIC_ROLE_NAMES}
    for item in columns:
        cname = str(item.get("column") or "")
        if not cname:
            continue
        for role in item.get("roles", []) or []:
            if not isinstance(role, dict):
                continue
            rkey = _normalize_semantic_role(role.get("role"))
            if not rkey:
                continue
            if int(role.get("score") or 0) >= 72 and cname not in role_buckets[rkey]:
                role_buckets[rkey].append(cname)
    return role_buckets


def _resolve_semantic_type(query: str | None = None, value: str | None = None, semantic_type: str | None = None) -> str:
    explicit = _norm_low(semantic_type)
    if explicit and explicit != "generic":
        return explicit
    raw_value = _norm(value)
    if raw_value and resolve_country_intent(raw_value):
        return "country"
    if raw_value and _looks_like_platform_value(raw_value):
        return "platform"
    return db._infer_filter_semantic_type(query=query, value=value, semantic_type=semantic_type)


def _llm_backend_mode() -> str:
    raw = (os.environ.get("USER_RAG_LLM_BACKEND") or "").strip().lower()
    if raw in ("ollama", "local"):
        return "ollama"
    if raw in ("openai", "vllm", "remote"):
        return "openai"
    return "auto"


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
        os.environ.get("USER_RAG_SEMANTIC_LLM_MODEL", "").strip()
        or os.environ.get("USER_RAG_OPENAI_MODEL", "").strip()
        or os.environ.get("OPENAI_MODEL", "").strip()
        or SEMANTIC_LLM_MODEL
    )


def _openai_compat_api_key() -> str:
    return (
        os.environ.get("OPENAI_API_KEY", "").strip()
        or os.environ.get("USER_RAG_OPENAI_API_KEY", "").strip()
        or "vllm-is-awesome"
    )


def _semantic_llm_target() -> tuple[str, str]:
    mode = _llm_backend_mode()
    openai_url = _openai_compat_base_url()
    use_openai = mode == "openai" or (mode == "auto" and bool(openai_url))
    if use_openai and openai_url:
        return "openai", openai_url
    return "ollama", OLLAMA_BASE_URL


@lru_cache(maxsize=1)
def _semantic_llm_available() -> bool:
    if SEMANTIC_LLM_MODE in {"0", "off", "false", "no", "disabled"}:
        return False
    backend, base_url = _semantic_llm_target()
    try:
        with httpx.Client(timeout=min(1.5, SEMANTIC_LLM_TIMEOUT), trust_env=False) as client:
            if backend == "openai":
                resp = client.get(
                    f"{base_url}/models",
                    headers={"Authorization": f"Bearer {_openai_compat_api_key()}"},
                )
            else:
                resp = client.get(f"{base_url}/api/tags")
            resp.raise_for_status()
            return True
    except Exception:
        return False


def _call_semantic_llm_json(prompt: str) -> dict[str, Any] | None:
    if SEMANTIC_LLM_MODE in {"0", "off", "false", "no", "disabled"}:
        return None
    if SEMANTIC_LLM_MODE == "auto" and not _semantic_llm_available():
        return None
    backend, base_url = _semantic_llm_target()
    try:
        if backend == "openai":
            from openai import OpenAI

            client = OpenAI(
                base_url=base_url,
                api_key=_openai_compat_api_key(),
                http_client=httpx.Client(timeout=SEMANTIC_LLM_TIMEOUT, trust_env=False),
            )
            resp = client.chat.completions.create(
                model=_openai_compat_model(),
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                top_p=0.2,
                stream=False,
            )
            content = (resp.choices[0].message.content or "").strip()
            return _parse_json_object(content)
        with httpx.Client(timeout=SEMANTIC_LLM_TIMEOUT, trust_env=False) as client:
            resp = client.post(
                f"{base_url}/api/generate",
                json={
                    "model": SEMANTIC_LLM_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"temperature": 0.0, "top_p": 0.2, "top_k": 20, "repeat_penalty": 1.05},
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return _parse_json_object(str(data.get("response") or ""))
    except Exception:
        return None


def _semantic_llm_enabled() -> bool:
    if SEMANTIC_LLM_MODE in {"0", "off", "false", "no", "disabled"}:
        return False
    if SEMANTIC_LLM_MODE == "auto":
        return _semantic_llm_available()
    return True


@lru_cache(maxsize=96)
def _infer_query_slots_llm_cached(question_key: str, tables_key: str, heuristic_key: str) -> dict[str, Any]:
    if not _semantic_llm_enabled():
        return {}
    prompt = (
        "你是 SQL 查询意图拆解助手。请在启发式草稿基础上做语义修正，但不要凭空发明用户没提到的筛选条件。\n"
        "只返回 JSON，不要解释。\n"
        "输出格式："
        "{\"query_type\":str,\"entity_hint\":str,"
        "\"filters\":[{\"raw_value\":str,\"semantic_type\":str,\"confidence\":str}],"
        "\"derived_metric\":{\"required\":bool,\"operation_type\":str,\"unit\":str},"
        "\"recommended_next_tools\":[str]}\n"
        "semantic_type 只允许: country, platform, time, id, email, phone, category, generic。\n"
        "confidence 只允许: high, medium, low。\n"
        f"Question: {question_key}\n"
        f"Tables: {tables_key}\n"
        f"HeuristicDraft: {heuristic_key}\n"
    )
    return _call_semantic_llm_json(prompt) or {}


@lru_cache(maxsize=96)
def _profile_table_semantics_llm_cached(table_name: str, query_key: str, columns_key: str) -> dict[str, Any]:
    if not _semantic_llm_enabled() or not query_key.strip():
        return {}
    prompt = (
        "你是数据库 schema 语义分析助手，要帮 SQL Agent 判断哪些列最适合当前问题。\n"
        "只返回 JSON，不要解释。\n"
        "输出格式："
        "{\"columns\":[{\"column\":str,\"relevance\":int,"
        "\"roles\":[{\"role\":str,\"score\":int,\"reason\":str}],\"reason\":str}],\"summary\":str}\n"
        f"允许角色: {', '.join(_SEMANTIC_ROLE_NAMES)}。\n"
        "candidate_roles 只是启发式提示，不是真相；请结合列名、类型、top values、sample values 和用户问题综合判断。\n"
        "relevance 范围 0-100；role.score 范围 0-100。不要返回不存在的列。\n"
        f"Question: {query_key}\n"
        f"Table: {table_name}\n"
        f"Columns: {columns_key}\n"
    )
    return _call_semantic_llm_json(prompt) or {}


@lru_cache(maxsize=128)
def _infer_filter_columns_llm_cached(
    table_name: str,
    query_key: str,
    value_key: str,
    semantic_key: str,
    variants_key: str,
    candidates_key: str,
) -> dict[str, Any]:
    if not _semantic_llm_enabled() or not query_key.strip():
        return {}
    prompt = (
        "你是 SQL 过滤条件选列助手。目标是判断“这个筛选值最应该落在哪些列”。\n"
        "只返回 JSON，不要解释。\n"
        "输出格式："
        "{\"columns\":[{\"column\":str,\"score\":int,\"confidence\":str,\"reason\":str}],\"summary\":str}\n"
        "score 范围 0-100；confidence 只允许: high, medium, low。\n"
        "优先选择语义匹配、样例值/高频值能承载该值、且最适合写 WHERE 的列；不要返回不存在的列。\n"
        f"Question: {query_key}\n"
        f"TargetValue: {value_key}\n"
        f"SemanticType: {semantic_key}\n"
        f"Variants: {variants_key}\n"
        f"Table: {table_name}\n"
        f"Candidates: {candidates_key}\n"
    )
    return _call_semantic_llm_json(prompt) or {}


def _looks_like_email(value: Any) -> bool:
    return bool(re.search(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", _norm(value)))


def _looks_like_phone(value: Any) -> bool:
    text = _norm(value)
    if not text or _looks_like_datetime_value(text) or _looks_like_email(text):
        return False
    digits = re.sub(r"\D+", "", text)
    if len(digits) < 7 or len(digits) > 15:
        return False
    return bool(re.match(r"^\+?\d[\d\-\s()]{6,}$", text)) or text.isdigit()


def _looks_like_datetime_value(value: Any) -> bool:
    text = _norm(value)
    if not text:
        return False
    if re.match(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?$", text):
        return True
    if re.match(r"^\d{4}/\d{2}/\d{2}$", text):
        return True
    if re.match(r"^\d{8}$", text):
        return True
    if re.match(r"^\d{6}$", text):
        return True
    if re.match(r"^\d{4}年\d{1,2}月(?:\d{1,2}日)?$", text):
        return True
    try:
        datetime.fromisoformat(text.replace("Z", "+00:00"))
        return True
    except Exception:
        return False


def _looks_like_platform_value(value: Any) -> bool:
    text = _clean_key(value)
    return text in _PLATFORM_TOKENS or any(tok in text for tok in _PLATFORM_TOKENS)


def _looks_like_country_value(value: Any) -> bool:
    text = _norm(value)
    if not text:
        return False
    intent = resolve_country_intent(text)
    return bool(intent)


def _column_profile_data(table_name: str, column_name: str, top_k: int, sample_cap: int) -> dict[str, Any]:
    prof = db.profile_column(
        table=table_name,
        column=column_name,
        include_top_values=True,
        top_k=top_k,
        sample_cap=sample_cap,
    )
    return dict(prof.get("data") or {}) if prof.get("ok") else {}


def _column_role_scores(column_meta: dict[str, Any], profile_data: dict[str, Any]) -> list[dict[str, Any]]:
    name = str(column_meta.get("name") or "")
    low = name.lower()
    top_values = [str(item.get("value") or "") for item in profile_data.get("top_values", []) if item.get("value") is not None]
    sample_values = [str(v) for v in profile_data.get("sample_values", []) if v is not None]
    values = [v for v in [*top_values, *sample_values] if _norm(v)]
    distinct_ratio = float(profile_data.get("distinct_ratio") or 0.0)
    roles: dict[str, dict[str, Any]] = {}

    def add(role: str, score: int, reason: str) -> None:
        cur = roles.setdefault(role, {"role": role, "score": 0, "evidence": []})
        cur["score"] = max(int(cur["score"]), int(score))
        if reason and reason not in cur["evidence"]:
            cur["evidence"].append(reason)

    if column_meta.get("primary_key"):
        add("entity_key", 98, "主键列")
    if db._is_id_like_name(name):
        add("entity_key", 92, "列名像 ID/编号键")
    if column_meta.get("is_time_like"):
        add("time", 96, "schema 标注为时间列")
    if any(_looks_like_datetime_value(v) for v in values[:6]):
        add("time", 88, "样本值像日期/时间")
    if any(tok in low for tok in _COUNTRY_COLUMN_HINTS):
        add("country", 88, "列名像国家/地区字段")
    country_hits = sum(1 for v in values[:6] if _looks_like_country_value(v))
    if country_hits >= max(2, min(4, len(values[:6]))):
        add("country", 82, "高频值与国家/地区别名对齐")
    if any(tok in low for tok in _PLATFORM_COLUMN_HINTS):
        add("platform", 88, "列名像平台/渠道字段")
    platform_hits = sum(1 for v in values[:6] if _looks_like_platform_value(v))
    if platform_hits >= max(2, min(4, len(values[:6]))):
        add("platform", 80, "高频值像 APP/PC/iOS/Android 等平台")
    if "email" in low or "邮箱" in name or any(_looks_like_email(v) for v in values[:5]):
        add("email", 95, "列名或样本值像邮箱")
    if any(tok in low for tok in ("phone", "mobile", "tel", "联系电话", "手机")) or any(_looks_like_phone(v) for v in values[:5]):
        add("phone", 90, "列名或样本值像手机号/电话")
    if column_meta.get("is_numeric") and not column_meta.get("is_time_like") and not db._is_id_like_name(name):
        add("measure", 68, "数值列，可能可聚合")
    if any(tok in low for tok in _MEASURE_NAME_HINTS):
        add("measure", 82, "列名像指标/度量")
    if any(tok in low for tok in ("ratio", "rate", "accuracy", "pct", "%", "占比", "比例", "准确率", "bcr")):
        add("ratio", 90, "列名像比例/准确率")
    if column_meta.get("enum_candidate") == "likely_low" and column_meta.get("is_text_like"):
        add("category", 72, "低基数文本列，适合作筛选维度")
    if column_meta.get("is_text_like") and distinct_ratio >= 0.65:
        add("free_text", 70, "高基数文本列，更像自由文本")

    ranked = sorted(roles.values(), key=lambda item: (int(item.get("score") or 0), item.get("role") or ""), reverse=True)
    return ranked[:4]


def _merge_semantic_roles(
    base_roles: list[dict[str, Any]],
    llm_roles: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in base_roles or []:
        if not isinstance(item, dict):
            continue
        role = _normalize_semantic_role(item.get("role"))
        if not role:
            continue
        cur = merged.setdefault(role, {"role": role, "score": 0, "evidence": []})
        cur["score"] = max(int(cur.get("score") or 0), int(item.get("score") or 0))
        cur["evidence"] = db._dedupe_preserve_text([*cur.get("evidence", []), *(item.get("evidence") or [])])
    for item in llm_roles or []:
        if not isinstance(item, dict):
            continue
        role = _normalize_semantic_role(item.get("role"))
        if not role:
            continue
        cur = merged.setdefault(role, {"role": role, "score": 0, "evidence": []})
        cur["score"] = max(int(cur.get("score") or 0), min(max(int(item.get("score") or 0), 0), 100))
        llm_reason = _norm(item.get("reason"))
        if llm_reason:
            cur["evidence"] = db._dedupe_preserve_text([*cur.get("evidence", []), f"LLM: {llm_reason}"])
    ranked = sorted(merged.values(), key=lambda entry: (int(entry.get("score") or 0), str(entry.get("role") or "")), reverse=True)
    return ranked[:4]


@lru_cache(maxsize=64)
def _profile_table_semantics_cached(
    table_name: str,
    query_key: str,
    schema_signature: str,
    row_count: int,
    max_columns: int,
    sample_limit: int,
    distinct_limit: int,
) -> dict[str, Any]:
    del schema_signature, row_count
    columns_meta = list(db.list_columns(table_name))
    query_low = query_key.lower()
    scored: list[tuple[int, dict[str, Any]]] = []
    for meta in columns_meta:
        name = str(meta.get("name") or "")
        score = 0
        if meta.get("primary_key"):
            score += 60
        if db._is_id_like_name(name):
            score += 45
        if meta.get("is_time_like"):
            score += 42
        if meta.get("enum_candidate") == "likely_low":
            score += 28
        if db._is_measure_like_name(name):
            score += 22
        if meta.get("is_text_like"):
            score += 6
        if query_key:
            score += max(db._overlap_score_db(query_key, name), db._overlap_score_db(query_key, str(meta.get("label") or name))) * 12
            if any(tok in query_low for tok in _DERIVED_DURATION_TOKENS) and meta.get("is_time_like"):
                score += 25
            if any(tok in query_low for tok in ("国家", "地区", "country", "region")) and any(tok in name.lower() for tok in _COUNTRY_COLUMN_HINTS):
                score += 25
        scored.append((score, meta))
    scored.sort(key=lambda item: (item[0], str(item[1].get("name") or "")), reverse=True)
    selected = [meta for _, meta in scored[: max(1, min(max_columns, len(scored)))]]

    role_buckets: dict[str, list[str]] = {role: [] for role in ("entity_key", "time", "country", "platform", "category", "measure", "ratio", "email", "phone", "free_text")}
    profiled_columns: list[dict[str, Any]] = []
    for meta in selected:
        cname = str(meta.get("name") or "")
        pdata = _column_profile_data(table_name, cname, top_k=distinct_limit, sample_cap=sample_limit)
        roles = _column_role_scores(meta, pdata)
        for role in roles:
            if not isinstance(role, dict):
                continue
            rkey = str(role.get("role") or "")
            if not rkey or rkey not in role_buckets:
                continue
            if int(role.get("score") or 0) >= 72 and cname not in role_buckets[rkey]:
                role_buckets[rkey].append(cname)
        top_values = [str(item.get("value") or "") for item in pdata.get("top_values", []) if item.get("value") is not None][:distinct_limit]
        samples = [str(v) for v in pdata.get("sample_values", []) if v is not None][:sample_limit]
        profiled_columns.append(
            {
                "column": cname,
                "sql_type": meta.get("sql_type", "TEXT"),
                "is_text_like": bool(meta.get("is_text_like")),
                "is_numeric": bool(meta.get("is_numeric")),
                "is_time_like": bool(meta.get("is_time_like")),
                "primary_key": bool(meta.get("primary_key")),
                "distinct_count": int(pdata.get("distinct_count") or 0),
                "distinct_ratio": round(float(pdata.get("distinct_ratio") or 0.0), 6),
                "non_null_rows": int(pdata.get("non_null_rows") or 0),
                "roles": roles,
                "top_values": top_values,
                "sample_values": samples,
            }
        )
    return {"columns": profiled_columns, "role_buckets": role_buckets}


def _month_range_iso(year: int, month: int) -> tuple[str, str]:
    if month < 1 or month > 12:
        raise ValueError("month out of range")
    start = f"{year}-{month:02d}-01"
    if month == 12:
        y2, m2 = year + 1, 1
    else:
        y2, m2 = year, month + 1
    end = f"{y2}-{m2:02d}-01"
    return start, end


def _year_range_iso(year: int) -> tuple[str, str]:
    return f"{year}-01-01", f"{year + 1}-01-01"


def _chinese_calendar_time_filter_dicts(text: str) -> list[dict[str, Any]]:
    if not str(text or "").strip():
        return []
    candidates: list[tuple[int, int, dict[str, Any]]] = []

    day_pat = re.compile(
        r"\d{4}-\d{1,2}-\d{1,2}|\d{4}/\d{1,2}/\d{1,2}|\d{4}年\d{1,2}月\d{1,2}(?:日|号)"
    )
    for m in day_pat.finditer(text):
        match = m.group(0)
        iso = match.replace("/", "-")
        iso = re.sub(r"年", "-", iso)
        iso = re.sub(r"月", "-", iso)
        iso = iso.replace("日", "").replace("号", "")
        parts = [p.zfill(2) if idx > 0 else p for idx, p in enumerate(iso.split("-"))]
        day_iso = "-".join(parts)
        candidates.append(
            (
                m.start(),
                m.end(),
                {
                    "raw_value": match,
                    "semantic_type": "time",
                    "normalized_values": [day_iso, f"{day_iso} 00:00:00", f"{day_iso} 23:59:59"],
                    "confidence": "high",
                    "time_granularity": "day",
                    "time_range_hint": (
                        f"按自然日：`CAST(时间列 AS DATE) = DATE('{day_iso}')`，"
                        f"或 `>= '{day_iso} 00:00:00' AND < DATE_ADD('{day_iso}', INTERVAL 1 DAY)`"
                    ),
                },
            )
        )

    month_pat = re.compile(r"(\d{4})年(\d{1,2})月(?:份)?(?!\d{1,2}(?:日|号))")
    for m in month_pat.finditer(text):
        y, mo = int(m.group(1)), int(m.group(2))
        if mo < 1 or mo > 12:
            continue
        raw = m.group(0)
        start_iso, end_iso = _month_range_iso(y, mo)
        ym = f"{y}-{mo:02d}"
        candidates.append(
            (
                m.start(),
                m.end(),
                {
                    "raw_value": raw,
                    "semantic_type": "time",
                    "normalized_values": [ym, start_iso, end_iso, m.group(1), str(mo)],
                    "confidence": "high",
                    "time_granularity": "month",
                    "time_range_hint": (
                        f"整月统计：`CAST(时间列 AS DATETIME) >= '{start_iso}' AND CAST(时间列 AS DATETIME) < '{end_iso}'`，"
                        f"或 `DATE_FORMAT(时间列, '%Y-%m') = '{ym}'`（列须为可解析的日期时间文本）"
                    ),
                },
            )
        )

    year_pat = re.compile(r"(19\d{2}|20\d{2})年(?!\d{1,2}月)")
    for m in year_pat.finditer(text):
        y = int(m.group(1))
        raw = m.group(0)
        start_iso, end_iso = _year_range_iso(y)
        candidates.append(
            (
                m.start(),
                m.end(),
                {
                    "raw_value": raw,
                    "semantic_type": "time",
                    "normalized_values": [str(y), start_iso, end_iso],
                    "confidence": "high",
                    "time_granularity": "year",
                    "time_range_hint": (
                        f"整年统计：`CAST(时间列 AS DATETIME) >= '{start_iso}' AND CAST(时间列 AS DATETIME) < '{end_iso}'`，"
                        f"或 `YEAR(CAST(时间列 AS DATETIME)) = {y}`"
                    ),
                },
            )
        )

    candidates.sort(key=lambda x: -(x[1] - x[0]))
    out: list[dict[str, Any]] = []
    used: list[tuple[int, int]] = []
    for start, end, payload in candidates:
        if any(a < end and b > start for a, b in used):
            continue
        used.append((start, end))
        out.append(payload)
    out.sort(key=lambda p: text.find(p["raw_value"]) if p.get("raw_value") in text else 0)
    return out


def infer_query_slots(
    question: str,
    tables: list[str] | None = None,
    table: str | None = None,
) -> dict[str, Any]:
    text = _norm(question)
    if not text:
        return {"ok": False, "error": "question 不能为空", "question": question}
    low = text.lower()
    query_type = "list"
    if any(tok in low for tok in ("多少", "几个", "count", "人数", "用户数", "订单数", "数量")):
        query_type = "count"
    elif any(tok in low for tok in ("趋势", "走势", "按月", "按天", "每天", "每月", "变化")):
        query_type = "trend"
    elif any(tok in low for tok in ("对比", "比较", "vs", "相比", "差异")):
        query_type = "compare"
    elif any(tok in low for tok in ("top", "前", "排名", "最高", "最低", "rank")):
        query_type = "topk"

    filters: list[dict[str, Any]] = []
    country_intent = resolve_country_intent(text)
    if country_intent:
        filters.append(
            {
                "raw_value": country_intent.triggers[0],
                "semantic_type": "country",
                "normalized_values": db._dedupe_preserve_text([country_intent.cn_display, country_intent.canonical_en, *country_intent.triggers]),
                "confidence": "high",
            }
        )
    for token in sorted(_PLATFORM_TOKENS):
        if token and token in _clean_key(low):
            filters.append(
                {
                    "raw_value": token,
                    "semantic_type": "platform",
                    "normalized_values": [token],
                    "confidence": "medium",
                }
            )
            break
    filters.extend(_chinese_calendar_time_filter_dicts(text))
    for marker in ("今天", "昨日", "昨天", "本周", "本月", "上月", "今年", "去年", "最近7天", "最近30天"):
        if marker in text:
            filters.append({"raw_value": marker, "semantic_type": "time", "normalized_values": [marker], "confidence": "medium"})
            break
    id_match = re.search(r"(?:用户ID|客户ID|订单号|编号|ID|uid)\s*[=:：]?\s*([A-Za-z0-9_-]+)", text, re.IGNORECASE)
    if id_match:
        filters.append({"raw_value": id_match.group(1), "semantic_type": "id", "normalized_values": [id_match.group(1)], "confidence": "high"})

    derived_metric: dict[str, Any] = {"required": False, "operation_type": "none", "confidence": "low"}
    if any(tok in low for tok in _DERIVED_DURATION_TOKENS):
        derived_metric = {"required": True, "operation_type": "time_since_now", "unit": "day", "confidence": "medium"}
        if any(tok in low for tok in ("间隔", "耗时", "相差", "差值", "between")):
            derived_metric = {"required": True, "operation_type": "date_diff", "unit": "day", "confidence": "medium"}
    elif any(tok in low for tok in _DERIVED_RATIO_TOKENS):
        derived_metric = {"required": True, "operation_type": "ratio", "confidence": "medium"}
    elif any(tok in low for tok in _DERIVED_GROWTH_TOKENS):
        derived_metric = {"required": True, "operation_type": "growth", "confidence": "medium"}

    entity_hint = "record"
    if any(tok in low for tok in ("用户", "user", "member", "customer", "客户")):
        entity_hint = "user"
    elif any(tok in low for tok in ("订单", "order", "单号")):
        entity_hint = "order"
    elif any(tok in low for tok in ("商品", "物料", "sku", "matnr", "item", "product")):
        entity_hint = "material"

    next_tools: list[str] = ["Profile_table_semantics"]
    if filters:
        next_tools.append("Infer_filter_columns")
    if any(item.get("semantic_type") == "time" for item in filters) or any(
        tok in low for tok in ("时间", "日期", "本月", "上月", "趋势", "活跃", "登录", "访问")
    ):
        next_tools.append("Find_time_columns")
        next_tools.append("Profile_time_column")

    llm_used = False
    tables_for_prompt = [t for t in ([table] if table else []) + list(tables or []) if _norm(t)]
    heuristic_draft = json.dumps(
        {
            "query_type": query_type,
            "entity_hint": entity_hint,
            "filters": [{"raw_value": item.get("raw_value"), "semantic_type": item.get("semantic_type"), "confidence": item.get("confidence")} for item in filters],
            "derived_metric": derived_metric,
            "recommended_next_tools": db._dedupe_preserve_text(next_tools),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    payload = _infer_query_slots_llm_cached(text, json.dumps(tables_for_prompt, ensure_ascii=False), heuristic_draft)
    if payload:
        llm_used = True
        guessed_query_type = _norm_low(payload.get("query_type"))
        if guessed_query_type in {"list", "count", "trend", "compare", "topk"}:
            query_type = guessed_query_type
        guessed_entity = _norm_low(payload.get("entity_hint"))
        if guessed_entity:
            entity_hint = guessed_entity
        raw_filters = payload.get("filters") if isinstance(payload.get("filters"), list) else []
        for item in raw_filters:
            if not isinstance(item, dict):
                continue
            raw_value = _norm(item.get("raw_value"))
            semantic_type = _norm_low(item.get("semantic_type")) or "generic"
            if semantic_type not in {"country", "platform", "time", "id", "email", "phone", "category", "generic"}:
                semantic_type = "generic"
            if raw_value:
                filters.append(
                    {
                        "raw_value": raw_value,
                        "semantic_type": semantic_type,
                        "normalized_values": db._expand_keyword_variants(value=raw_value, query=text, semantic_type=semantic_type),
                        "confidence": _norm_low(item.get("confidence")) or "medium",
                    }
                )
        d = payload.get("derived_metric") if isinstance(payload.get("derived_metric"), dict) else {}
        if d:
            derived_metric = {
                "required": bool(d.get("required")),
                "operation_type": str(d.get("operation_type") or derived_metric.get("operation_type") or "none"),
                "unit": str(d.get("unit") or derived_metric.get("unit") or "day"),
                "confidence": _norm_low(d.get("confidence")) or "medium",
            }
        raw_tools = payload.get("recommended_next_tools") if isinstance(payload.get("recommended_next_tools"), list) else []
        for tool_name in raw_tools:
            tname = _norm(tool_name)
            if tname:
                next_tools.append(tname)

    filters_out: list[dict[str, Any]] = []
    seen = set()
    for item in filters:
        raw_value = _norm(item.get("raw_value"))
        semantic_type = _norm_low(item.get("semantic_type")) or "generic"
        key = (raw_value, semantic_type)
        if not raw_value or key in seen:
            continue
        seen.add(key)
        expanded = db._expand_keyword_variants(value=raw_value, query=text, semantic_type=semantic_type)
        if semantic_type == "time" and isinstance(item.get("normalized_values"), list) and item["normalized_values"]:
            normalized_values = db._dedupe_preserve_text([*item["normalized_values"], *expanded])
        else:
            normalized_values = expanded
        entry: dict[str, Any] = {
            "raw_value": raw_value,
            "semantic_type": semantic_type,
            "normalized_values": normalized_values,
            "confidence": item.get("confidence") or "medium",
        }
        if semantic_type == "time":
            for ek in ("time_granularity", "time_range_hint"):
                v = item.get(ek)
                if v:
                    entry[ek] = v
        filters_out.append(entry)

    return {
        "ok": True,
        "error": None,
        "question": text,
        "query_type": query_type,
        "entity_hint": entity_hint,
        "filters": filters_out,
        "derived_metric": derived_metric,
        "recommended_next_tools": db._dedupe_preserve_text(next_tools),
        "tables": tables_for_prompt,
        "llm_used": llm_used,
    }


def profile_table_semantics(
    table: str = db.USERS_TABLE,
    query: str | None = None,
    sample_limit: int = 5,
    distinct_limit: int = 8,
    max_columns: int = 18,
) -> dict[str, Any]:
    table_name = db.resolve_table_name(table)
    if not db.is_queryable_table(table_name):
        return {"ok": False, "error": f"Unknown or disallowed table: {table_name}", "table": table_name, "columns": []}
    safe_sample = min(max(int(sample_limit), 3), 12)
    safe_distinct = min(max(int(distinct_limit), 3), 12)
    safe_cols = min(max(int(max_columns), 6), 30)
    schema = db.list_columns(table_name)
    row_count = db.get_table_row_count(table_name)
    signature = json.dumps([(c.get("name"), c.get("sql_type"), c.get("enum_candidate")) for c in schema], ensure_ascii=False)
    payload = _profile_table_semantics_cached(table_name, _norm(query), signature, row_count, safe_cols, safe_sample, safe_distinct)
    slots = infer_query_slots(query or "", tables=[table_name]) if _norm(query) else None
    q_low = _norm_low(query)
    llm_columns = []
    for item in payload.get("columns", []):
        llm_columns.append(
            {
                "column": item.get("column"),
                "sql_type": item.get("sql_type"),
                "primary_key": bool(item.get("primary_key")),
                "distinct_count": int(item.get("distinct_count") or 0),
                "distinct_ratio": round(float(item.get("distinct_ratio") or 0.0), 6),
                "candidate_roles": [
                    {"role": role.get("role"), "score": int(role.get("score") or 0)}
                    for role in (item.get("roles") or [])
                    if isinstance(role, dict)
                ],
                "top_values": [str(v) for v in item.get("top_values", [])[:5] if _norm(v)],
                "sample_values": [str(v) for v in item.get("sample_values", [])[:5] if _norm(v)],
            }
        )
    llm_payload = _profile_table_semantics_llm_cached(
        table_name,
        _norm(query),
        json.dumps(llm_columns, ensure_ascii=False, sort_keys=True),
    )
    llm_map: dict[str, dict[str, Any]] = {}
    raw_llm_columns = llm_payload.get("columns") if isinstance(llm_payload.get("columns"), list) else []
    for item in raw_llm_columns:
        if not isinstance(item, dict):
            continue
        cname = _norm(item.get("column"))
        if cname:
            llm_map[cname] = item
    columns = []
    for item in payload.get("columns", []):
        relevance = 0
        cname = str(item.get("column") or "")
        llm_item = llm_map.get(cname)
        merged_roles = _merge_semantic_roles(item.get("roles", []), llm_item.get("roles") if isinstance(llm_item, dict) else None)
        if q_low:
            relevance += max(db._overlap_score_db(q_low, cname), 0) * 12
            role_names = {str(r.get("role") or "") for r in merged_roles}
            if slots:
                for flt in slots.get("filters", []):
                    st = str(flt.get("semantic_type") or "")
                    if st in role_names:
                        relevance += 32
                    if st == "country" and "phone" in role_names:
                        relevance += 10
            if any(tok in q_low for tok in ("时间", "日期", "趋势", "注册", "活跃")) and "time" in role_names:
                relevance += 16
        new_item = dict(item)
        new_item["roles"] = merged_roles
        llm_reason = _norm(llm_item.get("reason")) if isinstance(llm_item, dict) else ""
        llm_relevance = 0
        if isinstance(llm_item, dict):
            llm_relevance = min(max(int(llm_item.get("relevance") or 0), 0), 100)
            if llm_relevance:
                relevance += min(48, max(8, llm_relevance // 2))
        if llm_reason:
            new_item["llm_reason"] = llm_reason
        new_item["llm_relevance_score"] = llm_relevance
        new_item["relevance_score"] = relevance
        columns.append(new_item)
    columns.sort(key=lambda row: (int(row.get("relevance_score") or 0), sum(int(r.get("score") or 0) for r in row.get("roles", [])), str(row.get("column") or "")), reverse=True)
    role_buckets = _rebuild_role_buckets(columns)
    return {
        "ok": True,
        "error": None,
        "table": table_name,
        "query": _norm(query) or None,
        "row_count": row_count,
        "columns": columns,
        "role_buckets": role_buckets,
        "query_slots": slots,
        "llm_used": bool(llm_map),
        "llm_summary": _norm(llm_payload.get("summary")) if isinstance(llm_payload, dict) else "",
    }


def infer_geography_columns(
    table: str = db.USERS_TABLE,
    query: str | None = None,
    semantic_type: str | None = None,
) -> dict[str, Any]:
    table_name = db.resolve_table_name(table)
    if not db.is_queryable_table(table_name):
        return {"ok": False, "error": f"Unknown or disallowed table: {table_name}", "table": table_name, "candidates": []}
    text = _norm(query)
    st = _norm_low(semantic_type) or "country"
    max_cols = min(max(int(getattr(db, "USER_RAG_PROFILE_TABLE_MAX_COLUMNS", 24)), 6), 30)
    sem = profile_table_semantics(table=table_name, query=text, max_columns=max_cols)
    if not sem.get("ok"):
        return {"ok": False, "error": sem.get("error") or "语义画像失败", "table": table_name, "candidates": []}
    buckets = sem.get("role_buckets") or {}
    slots = sem.get("query_slots") if isinstance(sem.get("query_slots"), dict) else {}

    def rank_conf(conf: str) -> int:
        return {"high": 0, "medium": 1, "low": 2}.get(conf, 3)

    collected: list[dict[str, Any]] = []

    def push(column: str, kind: str, confidence: str, reasons: list[str]) -> None:
        c = str(column or "").strip()
        if not c:
            return
        collected.append({"column": c, "kind": kind, "confidence": confidence, "reasons": list(reasons)})

    for c in buckets.get("country") or []:
        push(str(c), "semantic_country", "high", ["语义画像将列标为 country"])
    for c in buckets.get("phone") or []:
        push(str(c), "semantic_phone", "medium", ["语义画像将列标为 phone；可用国际冠码近似地区"])
    for c in (buckets.get("free_text") or [])[:4]:
        push(str(c), "semantic_free_text", "low", ["高基数文本列；仅适合关键词/LIKE 近似，需在 SUMMARY 说明口径"])

    for meta in db.list_columns(table_name):
        nm = str(meta.get("name") or "")
        if not nm:
            continue
        low = nm.lower()
        blob = f"{nm} {low}"
        if any(h in blob for h in _COUNTRY_COLUMN_HINTS):
            push(nm, "schema_country", "high", ["列名包含国家/地区类关键词"])
        if any(tok in low for tok in ("phone", "mobile", "tel")) or any(tok in nm for tok in ("电话", "手机", "联系")):
            push(nm, "schema_phone", "medium", ["列名像电话/联系方式，可用 +区号 等弱匹配地区"])

    merged: dict[str, dict[str, Any]] = {}
    for item in collected:
        col = item["column"]
        cur = merged.get(col)
        if cur is None:
            merged[col] = {
                "column": col,
                "kind": item["kind"],
                "confidence": item["confidence"],
                "reasons": list(item["reasons"]),
            }
            continue
        if rank_conf(item["confidence"]) < rank_conf(cur["confidence"]):
            merged[col] = {
                "column": col,
                "kind": item["kind"],
                "confidence": item["confidence"],
                "reasons": list(item["reasons"]),
            }
        elif rank_conf(item["confidence"]) == rank_conf(cur["confidence"]):
            prev_kinds = cur["kind"].split("|") if cur.get("kind") else []
            kinds = sorted({*prev_kinds, item["kind"]})
            cur["kind"] = "|".join(kinds)
            cur["reasons"] = db._dedupe_preserve_text([*cur["reasons"], *item["reasons"]])

    candidates = sorted(merged.values(), key=lambda x: (rank_conf(str(x.get("confidence") or "low")), str(x.get("column") or "")))

    phone_prefixes: list[str] = []
    country_resolution: dict[str, Any] | None = None
    for flt in slots.get("filters") or []:
        if not isinstance(flt, dict):
            continue
        if str(flt.get("semantic_type") or "") != "country":
            continue
        for v in flt.get("normalized_values") or []:
            s = str(v).strip()
            if s.startswith("+"):
                phone_prefixes.append(s)
        raw = str(flt.get("raw_value") or "")
        ci = resolve_country_intent(f"{raw} {text}".strip())
        if ci:
            country_resolution = {
                "cn_display": ci.cn_display,
                "canonical_en": ci.canonical_en,
                "triggers": list(ci.triggers),
            }

    kinds_blob = " ".join(str(c.get("kind") or "") for c in candidates)
    has_structured_country = "semantic_country" in kinds_blob or "schema_country" in kinds_blob
    has_phone_col = "phone" in kinds_blob

    recommended: list[str] = []
    if has_structured_country:
        recommended.append("存在较明确的国家/地区相关列：优先与 Preview_distinct_values 对齐后用 `=` / `IN (...)`，少用宽泛 LIKE。")
    if has_phone_col and phone_prefixes:
        px = phone_prefixes[0]
        recommended.append(f"电话列可补充谓词（弱证据）：例如 `联系电话` LIKE '{px}%'，须在 SUMMARY 写明为近似口径。")
    if not has_structured_country:
        recommended.append("无专用国家列时：避免裸写 `A OR B AND C`；每列内 `(LIKE... OR LIKE...)`，多列之间 `((列1组) OR (列2组))`。")
    recommended.append("与日期/其它条件组合：`(地理整块) AND (时间或其它条件)`，整块外层必须加括号。")

    return {
        "ok": True,
        "error": None,
        "table": table_name,
        "semantic_type": st,
        "query": text or None,
        "candidates": candidates,
        "phone_prefix_hints": db._dedupe_preserve_text(phone_prefixes),
        "country_resolution": country_resolution,
        "recommended_strategies": recommended,
        "where_parenthesis_hint": "单列: `(c LIKE ... OR c LIKE ...)`。多列: `((c1...) OR (c2...))`。混合: `(地理) AND (其它)`。",
    }


def infer_filter_columns(
    query: str,
    table: str = db.USERS_TABLE,
    value: str | None = None,
    semantic_type: str | None = None,
    top_k: int = 8,
    preview_limit: int = 6,
) -> dict[str, Any]:
    initial_semantic = _resolve_semantic_type(query=query, value=value, semantic_type=semantic_type)
    base = db.infer_filter_columns(query=query, table=table, value=value, semantic_type=initial_semantic, top_k=max(top_k, 8), preview_limit=preview_limit)
    if not base.get("ok"):
        return base
    table_name = str(base.get("table") or db.resolve_table_name(table))
    text = _norm(query)
    slots = infer_query_slots(text, tables=[table_name])
    resolved_semantic = _resolve_semantic_type(query=text, value=value, semantic_type=base.get("semantic_type") or initial_semantic)
    if resolved_semantic == "generic":
        for flt in slots.get("filters", []):
            st = _norm_low(flt.get("semantic_type"))
            if st and st != "generic":
                resolved_semantic = st
                break
    semantic_profile = profile_table_semantics(table=table_name, query=text, max_columns=max(10, preview_limit * 3))
    profile_map = {str(item.get("column") or ""): item for item in semantic_profile.get("columns", [])}
    selected_value = _norm(value)
    if not selected_value and slots.get("filters"):
        selected_value = _norm((slots.get("filters") or [{}])[0].get("raw_value"))
    variants = db._expand_keyword_variants(value=selected_value or None, query=text, semantic_type=resolved_semantic)
    merged: list[dict[str, Any]] = []
    for item in base.get("columns", []):
        cname = str(item.get("column") or "")
        score = int(item.get("score") or 0)
        reasons = list(item.get("reasons") or [])
        prof = profile_map.get(cname, {})
        roles = {str(role.get("role") or ""): int(role.get("score") or 0) for role in prof.get("roles", [])}
        if resolved_semantic in roles:
            score += min(40, max(12, roles[resolved_semantic] // 2))
            reasons.append(f"表语义画像显示该列像 {resolved_semantic}")
        if resolved_semantic == "country" and "phone" in roles:
            score += 8
            reasons.append("手机号列可近似表达国家码")
        top_values = [str(v) for v in prof.get("top_values", []) if _norm(v)]
        matched_variants = []
        for variant in variants[:8]:
            needle = _clean_key(variant)
            if not needle:
                continue
            if any(needle and needle in _clean_key(tv) for tv in top_values):
                matched_variants.append(variant)
        if matched_variants:
            score += min(30, 10 + 5 * len(matched_variants))
            reasons.append("高频值中已出现目标值写法")
        new_item = dict(item)
        new_item["score"] = score
        new_item["confidence"] = "high" if score >= 95 else ("medium" if score >= 60 else "low")
        new_item["reasons"] = db._dedupe_preserve_text(reasons)
        new_item["matched_variants"] = db._dedupe_preserve_text([*(item.get("matched_variants") or []), *matched_variants])
        new_item["example_values"] = db._dedupe_preserve_text([*(item.get("example_values") or []), *top_values[:3]])[:5]
        merged.append(new_item)
    llm_payload = _infer_filter_columns_llm_cached(
        table_name,
        text,
        selected_value,
        resolved_semantic,
        json.dumps(variants[:8], ensure_ascii=False),
        json.dumps(
            [
                {
                    "column": item.get("column"),
                    "score": int(item.get("score") or 0),
                    "confidence": item.get("confidence"),
                    "reasons": item.get("reasons", []),
                    "matched_variants": item.get("matched_variants", []),
                    "match_count": int(item.get("match_count") or 0),
                    "example_values": item.get("example_values", []),
                    "roles": [
                        {"role": role.get("role"), "score": int(role.get("score") or 0)}
                        for role in (profile_map.get(str(item.get("column") or ""), {}).get("roles") or [])
                        if isinstance(role, dict)
                    ],
                }
                for item in merged[: min(max(top_k, 8), 10)]
            ],
            ensure_ascii=False,
            sort_keys=True,
        ),
    )
    llm_map: dict[str, dict[str, Any]] = {}
    raw_llm_columns = llm_payload.get("columns") if isinstance(llm_payload.get("columns"), list) else []
    for item in raw_llm_columns:
        if not isinstance(item, dict):
            continue
        cname = _norm(item.get("column"))
        if cname:
            llm_map[cname] = item
    llm_used = bool(llm_map)
    reranked: list[dict[str, Any]] = []
    for item in merged:
        cname = str(item.get("column") or "")
        llm_item = llm_map.get(cname)
        new_item = dict(item)
        if isinstance(llm_item, dict):
            llm_score = min(max(int(llm_item.get("score") or 0), 0), 100)
            boost = min(48, max(-18, (llm_score - 50)))
            new_item["score"] = int(new_item.get("score") or 0) + boost
            llm_reason = _norm(llm_item.get("reason"))
            if llm_reason:
                new_item["reasons"] = db._dedupe_preserve_text([*(new_item.get("reasons") or []), f"LLM: {llm_reason}"])
            new_item["llm_score"] = llm_score
            llm_conf = _norm_low(llm_item.get("confidence"))
            if _confidence_rank(llm_conf) > _confidence_rank(str(new_item.get("confidence") or "")):
                new_item["confidence"] = llm_conf
        new_item["confidence"] = "high" if int(new_item.get("score") or 0) >= 95 else ("medium" if int(new_item.get("score") or 0) >= 60 else "low")
        reranked.append(new_item)
    merged = reranked
    merged.sort(key=lambda row: (int(row.get("score") or 0), int(row.get("match_count") or 0), str(row.get("column") or "")), reverse=True)
    return {
        **base,
        "semantic_type": resolved_semantic or base.get("semantic_type") or "generic",
        "variants": variants,
        "columns": merged[: min(max(int(top_k), 1), 20)],
        "query_slots": slots,
        "semantic_profile_excerpt": {
            role: cols[:4] for role, cols in (semantic_profile.get("role_buckets") or {}).items() if cols
        },
        "llm_used": llm_used,
        "llm_summary": _norm(llm_payload.get("summary")) if isinstance(llm_payload, dict) else "",
    }


def search_value_examples(
    keyword_variants: list[str] | None = None,
    keyword: str | None = None,
    value: str | None = None,
    table: str = db.USERS_TABLE,
    columns: list[str] | None = None,
    semantic_type: str | None = None,
    limit_per_column: int = 3,
    max_columns: int = 12,
) -> dict[str, Any]:
    table_name = db.resolve_table_name(table)
    if not db.is_queryable_table(table_name):
        return {"ok": False, "error": f"Unknown or disallowed table: {table_name}", "table": table_name, "columns": []}
    resolved_semantic = _resolve_semantic_type(query=keyword, value=value or keyword, semantic_type=semantic_type)
    variants = db._expand_keyword_variants(keyword_variants=keyword_variants, keyword=keyword, value=value, query=keyword, semantic_type=resolved_semantic)
    requested = [str(x or "").strip() for x in (columns or []) if str(x or "").strip()]
    selection_source = "user_columns"
    targets = [col for col in requested if col in db.column_names_for_table(table_name)]
    if not targets:
        selection_source = "semantic_profile"
        sem = profile_table_semantics(table=table_name, query=" ".join(variants), max_columns=max(12, max_columns))
        role_buckets = sem.get("role_buckets") or {}
        if resolved_semantic in role_buckets:
            targets = list(role_buckets.get(resolved_semantic) or [])
        elif resolved_semantic == "country":
            targets = db._dedupe_preserve_text([*(role_buckets.get("country") or []), *(role_buckets.get("phone") or []), *(role_buckets.get("free_text") or [])])
        elif resolved_semantic == "platform":
            targets = db._dedupe_preserve_text([*(role_buckets.get("platform") or []), *(role_buckets.get("category") or [])])
        elif resolved_semantic == "category":
            targets = db._dedupe_preserve_text([*(role_buckets.get("category") or []), *(role_buckets.get("free_text") or [])])
        if not targets:
            selection_source = "db_default"
            cols_meta = db.list_columns(table_name)
            probe_seed = " ".join(variants)
            targets, probe_meta = db._keyword_probe_target_columns(cols_meta, probe_seed, None)
            selection_source = str(probe_meta.get("scope") or "db_default")
    targets = targets[: min(max(int(max_columns), 1), 24)]
    if not targets:
        return {"ok": False, "error": "没有可搜索的候选列", "table": table_name, "columns": []}
    safe_limit = min(max(int(limit_per_column), 1), 10)
    out = []
    for column_name in targets:
        item = db._search_value_examples_in_column(table_name, column_name, variants, safe_limit)
        example_values: list[str] = []
        seen_values: set[str] = set()
        for hit in item.get("variant_hits", []):
            for ex in hit.get("examples", []):
                v = str(ex.get("value") or "")
                if v and v not in seen_values:
                    seen_values.add(v)
                    example_values.append(v)
        item["example_values"] = example_values[:safe_limit]
        item["confidence"] = "high" if item.get("total_match_count", 0) >= 5 else ("medium" if item.get("total_match_count", 0) > 0 else "low")
        out.append(item)
    out.sort(key=lambda row: (int(row.get("total_match_count") or 0), row.get("column") or ""), reverse=True)
    return {
        "ok": True,
        "error": None,
        "table": table_name,
        "semantic_type": resolved_semantic,
        "variants": variants,
        "columns": out,
        "meta": {"searched_columns": targets, "selection_source": selection_source, "searched_variants": len(variants)},
    }


def search_keyword_across_columns(
    keyword: str | None = None,
    keywords: list[str] | None = None,
    column_names: list[str] | None = None,
    columns: list[str] | None = None,
    table: str = db.USERS_TABLE,
    semantic_type: str | None = None,
) -> dict[str, Any]:
    resolved_semantic = _resolve_semantic_type(query=keyword, value=keyword, semantic_type=semantic_type)
    variants = db._expand_keyword_variants(keyword_variants=keywords, keyword=keyword, value=keyword, query=keyword, semantic_type=resolved_semantic)
    requested = column_names or columns
    res = search_value_examples(
        keyword_variants=variants,
        keyword=keyword,
        table=table,
        columns=requested,
        semantic_type=resolved_semantic,
        limit_per_column=1,
        max_columns=max(len(requested or []), 12),
    )
    if not res.get("ok", True):
        return {
            "ok": False,
            "error": res.get("error") or "search_value_examples 失败",
            "table": str(res.get("table") or db.resolve_table_name(table)),
            "keyword": _norm(keyword) if keyword else "",
            "keywords": variants,
            "matches": [],
            "meta": res.get("meta", {}),
        }
    matches = []
    for item in res.get("columns", []):
        example_value = None
        for hit in item.get("variant_hits", []):
            examples = hit.get("examples") or []
            if examples:
                ex0 = examples[0]
                if isinstance(ex0, dict):
                    example_value = ex0.get("value")
                break
        matches.append(
            {
                "column": item.get("column"),
                "match_count": int(item.get("total_match_count") or 0),
                "example_value": example_value,
                "matched_variants": item.get("matched_variants", []),
            }
        )
    matches.sort(key=lambda item: (int(item.get("match_count") or 0), str(item.get("column") or "")), reverse=True)
    return {
        "ok": True,
        "error": None,
        "table": str(res.get("table") or db.resolve_table_name(table)),
        "keyword": _norm(keyword) if keyword else (variants[0] if variants else ""),
        "keywords": variants,
        "matches": matches,
        "meta": res.get("meta", {}),
    }


def search_keyword_in_tables(
    keyword: str,
    tables: list[str] | None = None,
    limit_per_table: int = 8,
    positive_only: bool = True,
) -> dict[str, Any]:
    text = _norm(keyword)
    if not text:
        return {"ok": False, "error": "keyword 不能为空", "keyword": text, "tables": []}
    allowed = set(db.queryable_table_names())
    selected: list[str] = []
    for raw in tables or db.queryable_table_names():
        name = _norm(raw)
        if name and name in allowed and name not in selected:
            selected.append(name)
    out: list[dict[str, Any]] = []
    for table_name in selected:
        res = search_keyword_across_columns(keyword=text, table=table_name)
        matches = list(res.get("matches", []) or [])
        positive = [m for m in matches if int(m.get("match_count") or 0) > 0]
        total_match_count = sum(int(m.get("match_count") or 0) for m in positive)
        if positive_only and total_match_count == 0:
            continue
        out.append(
            {
                "table": table_name,
                "total_match_count": total_match_count,
                "matched_columns": positive[:limit_per_table] if positive else matches[:limit_per_table],
                "meta": res.get("meta", {}),
            }
        )
    out.sort(key=lambda item: (int(item.get("total_match_count") or 0), str(item.get("table") or "")), reverse=True)
    return {"ok": True, "error": None, "keyword": text, "tables": out, "meta": {"searched_tables": selected, "positive_only": bool(positive_only)}}


REACT_TOOL_LINES = [
    ("Get_database_tables", '列出当前可查询业务表：Get_database_tables[{}]。'),
    ("Get_table_schema", '查看指定表的列与类型：Get_table_schema[{"table_name":"表名"}]。'),
    ("Search_relevant_schema", '按问题召回相关表与列：Search_relevant_schema[{"question":"用户问题"}]。'),
    ("Inspect_rows", '查看样本行，可附带 columns/filters/order_by：Inspect_rows[{"table":"表名","limit":5}]。'),
    ("Profile_column", '查看单列画像、top values、样例：Profile_column[{"table":"表名","column":"列名"}]。'),
    ("Profile_table_columns", '查看整表列概况，适合宽表：Profile_table_columns[{"table":"表名"}]。'),
    ("Profile_table_semantics", '结合列名和值样例推断列角色：Profile_table_semantics[{"table":"表名","query":"用户问题"}]。'),
    ("Find_relevant_columns", '按问题找最相关列：Find_relevant_columns[{"table":"表名","query":"用户问题"}]。'),
    ("Infer_filter_columns", '推断过滤条件最可能落在哪些列：Infer_filter_columns[{"table":"表名","query":"国家是印尼"}]。'),
    ("Find_time_columns", '找时间列：Find_time_columns[{"table":"表名"}]。'),
    ("Profile_time_column", '分析时间列取值范围与样例：Profile_time_column[{"table":"表名","column":"时间列"}]。'),
    ("Search_value_examples", '在候选列中搜索目标值的真实样例：Search_value_examples[{"table":"表名","keyword":"印尼"}]。'),
    ("Search_keyword_across_columns", '在单表多列中搜索值或关键词：Search_keyword_across_columns[{"table":"表名","keyword":"关键词"}]。'),
    ("Search_keyword_in_tables", '跨表搜索关键词，定位在哪张表：Search_keyword_in_tables[{"keyword":"关键词","tables":["t1","t2"]}]。'),
    ("Search_similar_values", '在列的真实值中做相似匹配：Search_similar_values[{"table":"表名","field":"列名","query":"错拼值"}]。'),
    ("Get_table_relationships", '查看数据库里的外键关系：Get_table_relationships[{}]。'),
    ("Infer_join_candidates", '无外键时推测可能的 JOIN 键：Infer_join_candidates[{"tables":["a","b"]}]。'),
    ("Validate_join_candidate", '校验某组 JOIN 键是否靠谱：Validate_join_candidate[{"from_table":"a","from_column":"用户ID","to_table":"b","to_column":"用户ID"}]。'),
    ("Find_join_path", '查两表之间的 JOIN 路径：Find_join_path[{"from_table":"a","to_table":"b"}]。'),
    ("Validate_sql", '在执行前校验 SQL：Validate_sql[{"sql":"SELECT ..."}]。'),
    ("Explain_sql", '查看 SQL 的 EXPLAIN 结果：Explain_sql[{"sql":"SELECT ..."}]。'),
]
REACT_PROBE_TOOL_NAMES = frozenset(name for name, _ in REACT_TOOL_LINES)
REACT_PROBE_TOOLS_DISPLAY = ", ".join(sorted(REACT_PROBE_TOOL_NAMES))


class ToolExecutor:
    def __init__(self, default_table: str | None = None, context_tables: list[str] | None = None) -> None:
        ctx = list(context_tables or [])
        self.context_tables = ctx
        if ctx:
            dt = (default_table or ctx[0]).strip() or ctx[0]
        else:
            fallback_tables = queryable_table_names()
            fallback_default = fallback_tables[0] if fallback_tables else USERS_TABLE
            dt = (default_table or fallback_default).strip() or fallback_default
        self.default_table = dt
        self.tools: dict[str, dict[str, Any]] = {}
        self.register_tool("Get_database_tables", "列出可查询业务表。", lambda: {"tables": list_tables()}, {})
        self.register_tool("Get_table_schema", "查看指定表的列、类型和行数。", get_table_schema, {"table_name": "string"})
        self.register_tool("Get_table_relationships", "查看数据库里的外键关系。", get_table_relationships, {})
        self.register_tool("Search_relevant_schema", "按问题召回相关表与列。", search_relevant_schema, {"question": "string", "max_tables": "int", "max_columns_per_table": "int"})
        self.register_tool("Inspect_rows", "查看样本行，可按列、过滤条件和排序查看。", inspect_rows, {"table": "string", "limit": "int", "filters": "object", "as_dict": "bool", "order_by": "string", "columns": "list"})
        self.register_tool("Profile_column", "查看单列画像、top values 和样例。", profile_column, {"table": "string", "column": "string", "field": "string", "include_top_values": "bool", "top_k": "int", "keyword": "string", "sample_cap": "int"})
        self.register_tool("Profile_table_columns", "查看整表列概况。", profile_table_columns, {"sample_per_column": "int", "table": "string"})
        self.register_tool("Profile_table_semantics", "根据列名和值样例推断列角色。", profile_table_semantics, {"table": "string", "query": "string", "sample_limit": "int", "distinct_limit": "int", "max_columns": "int"})
        self.register_tool("Find_relevant_columns", "按问题找相关列。", find_relevant_columns, {"query": "string", "table": "string", "top_k": "int"})
        self.register_tool("Infer_filter_columns", "推断过滤条件应落在哪些列。", infer_filter_columns, {"query": "string", "table": "string", "value": "string", "semantic_type": "string", "top_k": "int", "preview_limit": "int"})
        self.register_tool("Find_time_columns", "列出疑似时间列。", find_time_columns, {"table": "string"})
        self.register_tool("Profile_time_column", "分析时间列的样例和范围。", profile_time_column, {"table": "string", "column": "string", "field": "string", "sample_limit": "int"})
        self.register_tool("Search_value_examples", "在候选列中搜索真实值样例。", search_value_examples, {"keyword_variants": "list", "keyword": "string", "value": "string", "table": "string", "columns": "list", "semantic_type": "string", "limit_per_column": "int", "max_columns": "int"})
        self.register_tool("Search_keyword_across_columns", "在单表多列里搜索关键词或值。", search_keyword_across_columns, {"keyword": "string", "keywords": "list", "column_names": "list", "columns": "list", "table": "string", "semantic_type": "string"})
        self.register_tool("Search_keyword_in_tables", "跨表搜索关键词。", search_keyword_in_tables, {"keyword": "string", "tables": "list", "limit_per_table": "int", "positive_only": "bool"})
        self.register_tool("Search_similar_values", "在列的真实值中做相似匹配。", search_similar_values, {"field": "string", "query": "string", "limit": "int", "table": "string"})
        self.register_tool("Infer_join_candidates", "推测可用的 JOIN 键。", infer_join_candidates, {"tables": "list", "limit": "int"})
        self.register_tool("Validate_join_candidate", "校验某组 JOIN 键是否可用。", validate_join_candidate, {"from_table": "string", "from_column": "string", "to_table": "string", "to_column": "string", "sample_limit": "int"})
        self.register_tool("Find_join_path", "查找两表之间的 JOIN 路径。", find_join_path, {"from_table": "string", "to_table": "string"})
        self.register_tool("Validate_sql", "校验 SQL。", validate_sql, {"sql": "string"})
        self.register_tool("Explain_sql", "查看 SQL 的 EXPLAIN 结果。", explain_sql, {"sql": "string"})

    def register_tool(self, name: str, description: str, func: Any, input_schema: dict[str, Any]) -> None:
        self.tools[name] = {"description": description, "func": func, "input_schema": input_schema}

    def list_tools(self) -> list[ToolDefinition]:
        return [
            ToolDefinition(name=name, description=info["description"], input_schema=info["input_schema"])
            for name, info in self.tools.items()
        ]

    def tools_prompt(self) -> str:
        return "\n".join(f"- {name}: {info['description']}" for name, info in self.tools.items())

    @staticmethod
    def tools_prompt_react() -> str:
        return "\n".join(f"- {name}: {blurb}" for name, blurb in REACT_TOOL_LINES)

    def canonical_arguments(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_arguments(name, arguments)
        tool = self.tools.get(name, {}).get("func")
        if not tool:
            return normalized
        accepted = set(inspect.signature(tool).parameters.keys())
        return {k: v for k, v in normalized.items() if k in accepted}

    def run(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        tool = self.tools.get(name, {}).get("func")
        if not tool:
            return {"error": f"Unknown tool: {name}"}
        arguments = self._normalize_arguments(name, arguments)
        accepted_params = set(inspect.signature(tool).parameters.keys())
        filtered_arguments = {key: value for key, value in arguments.items() if key in accepted_params}
        try:
            return tool(**filtered_arguments)
        except TypeError:
            if len(filtered_arguments) == 1:
                try:
                    return tool(next(iter(filtered_arguments.values())))
                except Exception as exc:  # noqa: BLE001
                    return {"error": str(exc)}
            if not filtered_arguments:
                try:
                    return tool()
                except Exception as exc:  # noqa: BLE001
                    return {"error": str(exc)}
            return {"error": f"Invalid tool arguments for {name}: {arguments}"}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc)}

    def _normalize_arguments(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(arguments)
        if name == "Get_table_schema":
            table_name = str(normalized.get("table_name", "")).strip()
            if not table_name or table_name == "{tool_input}":
                normalized["table_name"] = self.default_table
        elif name == "Profile_column":
            if not str(normalized.get("column") or "").strip():
                for alias in ("field", "column_name", "field_name"):
                    if str(normalized.get(alias) or "").strip():
                        normalized["column"] = normalized[alias]
                        break
            if not str(normalized.get("column") or "").strip() and isinstance(normalized.get("column_names"), list) and normalized["column_names"]:
                normalized["column"] = normalized["column_names"][0]
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
            try:
                normalized["top_k"] = int(normalized.get("top_k", 8))
            except (TypeError, ValueError):
                normalized["top_k"] = 8
            try:
                normalized["sample_cap"] = int(normalized.get("sample_cap", 8))
            except (TypeError, ValueError):
                normalized["sample_cap"] = 8
        elif name == "Inspect_rows":
            val = normalized.get("value")
            if isinstance(val, list) and val and all(isinstance(x, dict) for x in val):
                merged: dict[str, Any] = {}
                for item in val:
                    merged.update(item)
                normalized.pop("value", None)
                if "limit" in merged:
                    try:
                        normalized["limit"] = int(merged["limit"])
                    except (TypeError, ValueError):
                        normalized["limit"] = 5
                if isinstance(merged.get("filters"), dict):
                    normalized["filters"] = merged["filters"]
                if merged.get("table"):
                    normalized["table"] = merged["table"]
                if "as_dict" in merged:
                    normalized["as_dict"] = bool(merged["as_dict"])
                if merged.get("order_by"):
                    normalized["order_by"] = merged["order_by"]
                if isinstance(merged.get("columns"), list):
                    normalized["columns"] = merged["columns"]
                elif str(merged.get("column") or "").strip():
                    normalized["columns"] = [str(merged["column"]).strip()]
            normalized.pop("field", None)
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
            if "as_dict" not in normalized:
                normalized["as_dict"] = True
            try:
                normalized["limit"] = int(normalized.get("limit", 5))
            except (TypeError, ValueError):
                normalized["limit"] = 5
            cn = normalized.get("columns")
            if isinstance(cn, str):
                cn = [x.strip() for x in cn.split(",") if x.strip()]
                normalized["columns"] = cn or None
            elif isinstance(cn, list):
                normalized["columns"] = [str(x).strip() for x in cn if str(x).strip()] or None
            else:
                normalized.pop("columns", None)
            if not normalized.get("columns") and str(normalized.get("column") or "").strip():
                normalized["columns"] = [str(normalized["column"]).strip()]
            normalized.pop("column", None)
        elif name == "Profile_time_column":
            if not str(normalized.get("column") or "").strip():
                for alias in ("field", "column_name", "field_name"):
                    if str(normalized.get(alias) or "").strip():
                        normalized["column"] = normalized[alias]
                        break
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
            try:
                normalized["sample_limit"] = int(normalized.get("sample_limit", 12))
            except (TypeError, ValueError):
                normalized["sample_limit"] = 12
        elif name == "Profile_table_columns":
            sp = normalized.get("sample_per_column", 3)
            try:
                normalized["sample_per_column"] = int(sp)
            except (TypeError, ValueError):
                normalized["sample_per_column"] = 3
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
        elif name == "Profile_table_semantics":
            if "query" not in normalized:
                for alias in ("question", "q", "text"):
                    if alias in normalized:
                        normalized["query"] = normalized[alias]
                        break
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
            for key, default in (("sample_limit", 5), ("distinct_limit", 8), ("max_columns", 18)):
                try:
                    normalized[key] = int(normalized.get(key, default))
                except (TypeError, ValueError):
                    normalized[key] = default
        elif name == "Find_relevant_columns":
            if "query" not in normalized:
                for alias in ("question", "q", "text"):
                    if alias in normalized:
                        normalized["query"] = normalized[alias]
                        break
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
            try:
                normalized["top_k"] = int(normalized.get("top_k", 5))
            except (TypeError, ValueError):
                normalized["top_k"] = 5
        elif name == "Infer_filter_columns":
            if "query" not in normalized:
                for alias in ("question", "q", "text", "keyword"):
                    if alias in normalized:
                        normalized["query"] = normalized[alias]
                        break
            if "value" not in normalized:
                for alias in ("keyword", "term", "search"):
                    if alias in normalized and str(normalized.get(alias) or "").strip():
                        normalized["value"] = normalized[alias]
                        break
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
            for key, default in (("top_k", 8), ("preview_limit", 6)):
                try:
                    normalized[key] = int(normalized.get(key, default))
                except (TypeError, ValueError):
                    normalized[key] = default
        elif name == "Find_time_columns":
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
        elif name == "Find_join_path":
            for a, b in (("from_table", "from"), ("to_table", "to")):
                if b in normalized and a not in normalized:
                    normalized[a] = normalized[b]
        elif name == "Infer_join_candidates":
            tv = normalized.get("tables")
            if isinstance(tv, str):
                try:
                    parsed = json.loads(tv)
                    if isinstance(parsed, list):
                        normalized["tables"] = [str(x).strip() for x in parsed if str(x).strip()]
                except json.JSONDecodeError:
                    normalized["tables"] = [x.strip() for x in tv.split(",") if x.strip()]
            elif isinstance(tv, list):
                normalized["tables"] = [str(x).strip() for x in tv if str(x).strip()]
            try:
                normalized["limit"] = int(normalized.get("limit", 20))
            except (TypeError, ValueError):
                normalized["limit"] = 20
        elif name == "Validate_join_candidate":
            for a, b in (("from_table", "from"), ("to_table", "to"), ("from_column", "left_column"), ("to_column", "right_column")):
                if b in normalized and a not in normalized:
                    normalized[a] = normalized[b]
            try:
                normalized["sample_limit"] = int(normalized.get("sample_limit", 5))
            except (TypeError, ValueError):
                normalized["sample_limit"] = 5
        elif name == "Search_keyword_in_tables":
            if not str(normalized.get("keyword") or "").strip():
                for alias in ("q", "text", "term", "search", "query"):
                    if str(normalized.get(alias) or "").strip():
                        normalized["keyword"] = normalized[alias]
                        break
            tv = normalized.get("tables")
            if isinstance(tv, str):
                try:
                    parsed = json.loads(tv)
                    if isinstance(parsed, list):
                        normalized["tables"] = [str(x).strip() for x in parsed if str(x).strip()]
                    else:
                        normalized["tables"] = [x.strip() for x in tv.split(",") if x.strip()]
                except json.JSONDecodeError:
                    normalized["tables"] = [x.strip() for x in tv.split(",") if x.strip()]
            elif isinstance(tv, list):
                normalized["tables"] = [str(x).strip() for x in tv if str(x).strip()]
            try:
                normalized["limit_per_table"] = int(normalized.get("limit_per_table", 8))
            except (TypeError, ValueError):
                normalized["limit_per_table"] = 8
            if "positive_only" in normalized:
                normalized["positive_only"] = bool(normalized.get("positive_only"))
        elif name == "Search_similar_values":
            if not str(normalized.get("field") or "").strip() and str(normalized.get("column") or "").strip():
                normalized["field"] = normalized["column"]
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
            try:
                normalized["limit"] = int(normalized.get("limit", 10))
            except (TypeError, ValueError):
                normalized["limit"] = 10
        elif name == "Search_value_examples":
            if not str(normalized.get("keyword") or "").strip():
                for alias in ("q", "text", "term", "search", "query", "value"):
                    if str(normalized.get(alias) or "").strip():
                        normalized["keyword"] = normalized[alias]
                        break
            cols = normalized.get("columns")
            if isinstance(cols, str):
                normalized["columns"] = [x.strip() for x in cols.split(",") if x.strip()]
            elif isinstance(cols, list):
                normalized["columns"] = [str(x).strip() for x in cols if str(x).strip()]
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
            for key, default in (("limit_per_column", 3), ("max_columns", 12)):
                try:
                    normalized[key] = int(normalized.get(key, default))
                except (TypeError, ValueError):
                    normalized[key] = default
        elif name == "Search_keyword_across_columns":
            for noise in ("table_name", "limit"):
                normalized.pop(noise, None)
            if not str(normalized.get("keyword") or "").strip():
                for alias in ("q", "text", "term", "search", "query"):
                    if str(normalized.get(alias) or "").strip():
                        normalized["keyword"] = normalized[alias]
                        break
            kv = normalized.get("keywords")
            if isinstance(kv, str):
                try:
                    parsed = json.loads(kv)
                    if isinstance(parsed, list):
                        normalized["keywords"] = [str(x).strip() for x in parsed if str(x).strip()]
                    else:
                        normalized["keywords"] = [x.strip() for x in kv.split(",") if x.strip()]
                except json.JSONDecodeError:
                    normalized["keywords"] = [x.strip() for x in kv.split(",") if x.strip()]
            elif isinstance(kv, list):
                normalized["keywords"] = [str(x).strip() for x in kv if str(x).strip()]
            if "columns" in normalized and "column_names" not in normalized:
                normalized["column_names"] = normalized["columns"]
            cn = normalized.get("column_names")
            if isinstance(cn, str):
                try:
                    parsed = json.loads(cn)
                    if isinstance(parsed, list):
                        normalized["column_names"] = [str(x).strip() for x in parsed if str(x).strip()]
                    else:
                        normalized["column_names"] = [x.strip() for x in cn.split(",") if x.strip()]
                except json.JSONDecodeError:
                    normalized["column_names"] = [x.strip() for x in cn.split(",") if x.strip()]
            elif isinstance(cn, list):
                normalized["column_names"] = [str(x).strip() for x in cn if str(x).strip()]
            if str(normalized.get("semantic_type") or "").strip():
                normalized["semantic_type"] = str(normalized["semantic_type"]).strip()
            if not str(normalized.get("table") or "").strip() or str(normalized.get("table")) == "{tool_input}":
                normalized["table"] = self.default_table
        elif name == "Search_relevant_schema":
            if "question" not in normalized:
                for alias in ("q", "text", "query"):
                    if alias in normalized:
                        normalized["question"] = normalized[alias]
                        break
            try:
                normalized["max_tables"] = int(normalized.get("max_tables", 5))
            except (TypeError, ValueError):
                normalized["max_tables"] = 5
            try:
                normalized["max_columns_per_table"] = int(normalized.get("max_columns_per_table", 8))
            except (TypeError, ValueError):
                normalized["max_columns_per_table"] = 8
        elif name in {"Validate_sql", "Explain_sql"}:
            if not str(normalized.get("sql") or "").strip():
                for alias in ("sql_query", "query"):
                    if alias in normalized and str(normalized.get(alias) or "").strip():
                        normalized["sql"] = normalized[alias]
                        break
        self._apply_context_tables_to_tool_args(name, normalized)
        return normalized

    def _canonical_tool_table_name(self, value: str) -> str:
        v = str(value or "").strip()
        if not v:
            return self.default_table
        allowed = self.context_tables
        if not allowed:
            return v
        if v in allowed:
            return v
        by_lower = {t.lower(): t for t in allowed}
        lowered = v.lower()
        if lowered in by_lower:
            return by_lower[lowered]
        if lowered in {"main", "default", "primary", "users"}:
            return self.default_table
        if len(allowed) == 1:
            return allowed[0]
        return v

    def _canonical_tool_table_list(self, values: list[str] | tuple[str, ...] | None) -> list[str]:
        out: list[str] = []
        for raw in list(values or []):
            name = self._canonical_tool_table_name(str(raw or ""))
            if name and name not in out:
                out.append(name)
        return out

    def _apply_context_tables_to_tool_args(self, name: str, normalized: dict[str, Any]) -> None:
        keys: tuple[str, ...] = ()
        if name == "Get_table_schema":
            keys = ("table_name",)
        elif name in (
            "Inspect_rows",
            "Profile_column",
            "Profile_time_column",
            "Profile_table_columns",
            "Profile_table_semantics",
            "Find_relevant_columns",
            "Infer_filter_columns",
            "Find_time_columns",
            "Search_value_examples",
            "Search_keyword_across_columns",
            "Search_similar_values",
        ):
            keys = ("table",)
        elif name == "Find_join_path":
            keys = ("from_table", "to_table")
        for key in keys:
            if key not in normalized:
                continue
            raw = normalized.get(key)
            if raw is None:
                continue
            vs = str(raw).strip()
            if not vs:
                normalized[key] = self.default_table
                continue
            normalized[key] = self._canonical_tool_table_name(vs)

        if name in {"Infer_join_candidates", "Search_keyword_in_tables"}:
            if not normalized.get("tables") and self.context_tables:
                normalized["tables"] = list(self.context_tables)
            elif normalized.get("tables"):
                normalized["tables"] = self._canonical_tool_table_list(normalized.get("tables"))
