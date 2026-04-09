#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd


NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
AHA_TYPE_ORDER = [
    "服务流程启动",
    "设备绑定成功",
    "内容获取感",
    "快速入口使用",
]
BEHAVIOR_COMPARE_SHEET = "行为汇总对比"
BEHAVIOR_SIGNAL_DIFF_THRESHOLD = 0.5
METRIC_DEFINITION_SHEET = "分析指标定义"
SERVICE_ENTRY_CLICK_PATTERNS = ("我要召请按钮0331", "服务召请按钮")
SPARE_PART_ENTRY_CLICK_PATTERNS = ("我要配件按钮",)
MAINTENANCE_ENTRY_CLICK_PATTERNS = ("设备保养", "预约保养")
PRODUCT_CENTER_ENTRY_CLICK_PATTERNS = ("产品中心按钮",)
DATA_REPORT_CLICK_PATTERNS = ("数据报表",)
DATA_ANALYSIS_CLICK_PATTERNS = ("数据分析",)
QUOTE_SUBMIT_CLICK_PATTERNS = ("询价", "加入询价单")
SPARE_PART_LIST_PAGE_PATTERNS = ("配件列表页",)
SPARE_PART_DETAIL_PAGE_PATTERNS = ("配件详情页",)
MAINTENANCE_LIST_PAGE_PATTERNS = ("设备保养列表页", "保养记录")
PRODUCT_LIST_PAGE_PATTERNS = ("产品列表页",)
COLLEAGUE_CIRCLE_PAGE_PATTERNS = ("同事圈",)
RENAME_MAP = {
    "用户ID": "user_id",
    "时间": "dt_hour",
    "事件名称": "event_name",
    "点击事件": "click_name",
    "界面名称_PV": "page_name",
    "页面停留时间_PV": "page_stay",
    "次数": "cnt",
    "用户类型": "user_type",
    "sy_btn_click_name": "click_name",
    "sy_page_name_pv": "page_name",
    "sy_standing_time": "page_stay",
    "sy_bind_device_count": "bound_device_total",
    "sy_api_loaded_time": "api_loaded_time",
    "sy_api_url": "api_url",
    "sy_api_params": "api_params",
    "sy_service_api": "service_api",
    "sy_parent_page_name": "parent_page_name",
    "sy_page_source": "page_source",
    "page_loaded_time": "page_loaded_time",
    "page_start_time": "page_start_time",
    "page_end_time": "page_end_time",
    "page_receipt_time": "page_receipt_time",
    "page_platform": "page_platform",
    "page_version": "page_version",
    "user_load_time": "user_load_time",
    "user_ip": "user_ip",
    "user_country": "user_country",
    "user_network_latency": "user_network_latency",
    "user_network_speed": "user_network_speed",
    "user_network_type": "user_network_type",
    "user_platform": "user_platform",
    "sy_error": "api_error_message",
    "启动时间": "user_load_time",
    "用户ip": "user_ip",
    "用户IP": "user_ip",
    "用户国家": "user_country",
    "用户延时": "user_network_latency",
    "用户延迟": "user_network_latency",
    "用户网速": "user_network_speed",
    "用户网络类型": "user_network_type",
    "用户平台": "user_platform",
    "加载时间": "page_loaded_time",
    "页面停留时间": "page_stay",
    "页面名称": "page_name",
    "手机系统（ios\\android）": "page_platform",
    "手机系统（ios/android）": "page_platform",
    "小程序版本x.x.x.x": "page_version",
    "页面加载开始时间": "page_start_time",
    "页面加载开始时间（原生传入）": "page_start_time",
    "页面加载完成时间": "page_end_time",
    "接收开始时间": "page_receipt_time",
    "接收开始时间节点": "page_receipt_time",
    "运行场景": "page_source",
    "曝光页面名称": "page_name",
    "按钮点击事件名称": "click_name",
    "上一个页面名称": "parent_page_name",
    "服务API名称": "service_api",
    "接口加载时间": "api_loaded_time",
    "接口地址": "api_url",
    "接口参数": "api_params",
    "绑定设备数量": "bound_device_total",
    "接口报错内容": "api_error_message",
}
BEHAVIOR_COMPARE_RENAME_MAP = {
    "事件类型": "behavior_event_type",
    "具体行为名称": "behavior_name",
    "所属页面": "behavior_page",
    "所属子模块": "behavior_module",
    "低活完成用户数": "low_active_user_count",
    "高活完成用户数": "high_active_user_count",
    "低活完成次数": "low_active_total_count",
    "高活完成次数": "high_active_total_count",
    "低活停留时间": "low_active_stay_time",
    "高活停留时间": "high_active_stay_time",
    "高活用户平均使用频次": "high_active_avg_freq",
    "低活用户平均使用频次": "low_active_avg_freq",
}
METRIC_DEFINITION_RENAME_MAP = {
    "功能模块（大模块）": "metric_module",
    "功能名称": "metric_function_name",
    "分析维度": "metric_dimension",
    "指标定义和公式": "metric_formula",
    "可视化需求 （作为筛选条件）": "metric_visual_requirement",
    "涉及埋点页面（可在评论区截图）": "metric_pages",
    "涉及埋点入口（关联原始表格的埋点名称）": "metric_entry_events",
    "备注": "metric_note",
    "PM": "metric_pm",
}


def read_events(path: Path, sheet_name: str | None = None) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        try:
            return pd.read_excel(path, sheet_name=sheet_name or 0)
        except ImportError:
            return read_xlsx_without_engine(path, sheet_name=sheet_name)
    raise ValueError(f"Unsupported file type: {path.suffix}")


def load_behavior_compare(
    *,
    input_path: Path,
    behavior_compare_input: str | None = None,
    behavior_compare_sheet: str | None = None,
) -> pd.DataFrame | None:
    compare_path = Path(behavior_compare_input) if behavior_compare_input else input_path
    compare_sheet = behavior_compare_sheet
    if compare_sheet is None and compare_path.suffix.lower() in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        compare_sheet = BEHAVIOR_COMPARE_SHEET

    try:
        raw_df = read_events(compare_path, sheet_name=compare_sheet)
    except Exception:
        return None

    normalized = normalize_behavior_compare(raw_df)
    if normalized.empty:
        return None
    return normalized


def load_metric_definition_catalog(metric_input: str | None = None) -> pd.DataFrame | None:
    path = Path(metric_input) if metric_input else Path("user_analysis/metric.xlsx")
    if not path.exists():
        return None
    try:
        raw_df = read_events(path, sheet_name=METRIC_DEFINITION_SHEET)
    except Exception:
        return None
    df = raw_df.rename(columns={col: METRIC_DEFINITION_RENAME_MAP.get(col, col) for col in raw_df.columns}).copy()
    if "metric_module" not in df.columns and "功能模块（大模块）" not in raw_df.columns:
        return None
    if "metric_module" not in df.columns:
        return None
    for column in [
        "metric_module",
        "metric_function_name",
        "metric_dimension",
        "metric_formula",
        "metric_visual_requirement",
        "metric_pages",
        "metric_entry_events",
        "metric_note",
        "metric_pm",
    ]:
        if column not in df.columns:
            df[column] = ""
        df[column] = df[column].astype("string").fillna("").str.strip()
    df = df[(df["metric_module"] != "") | (df["metric_function_name"] != "")].copy()
    return df.reset_index(drop=True)


def normalize_behavior_compare(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={col: BEHAVIOR_COMPARE_RENAME_MAP.get(col, col) for col in df.columns}).copy()
    required = {"behavior_name", "high_active_avg_freq", "low_active_avg_freq"}
    if not required.issubset(df.columns):
        return pd.DataFrame()

    for column in [
        "low_active_user_count",
        "high_active_user_count",
        "low_active_total_count",
        "high_active_total_count",
        "low_active_stay_time",
        "high_active_stay_time",
        "high_active_avg_freq",
        "low_active_avg_freq",
    ]:
        if column not in df.columns:
            df[column] = 0
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)

    if "behavior_event_type" not in df.columns:
        df["behavior_event_type"] = ""
    if "behavior_page" not in df.columns:
        df["behavior_page"] = ""
    if "behavior_module" not in df.columns:
        df["behavior_module"] = ""

    df["behavior_name"] = df["behavior_name"].astype("string").fillna("").str.strip()
    df = df[df["behavior_name"] != ""].copy()
    df["behavior_event_type"] = df["behavior_event_type"].astype("string").fillna("").str.strip()
    df["freq_diff"] = (df["high_active_avg_freq"] - df["low_active_avg_freq"]).round(3)
    df["signal_strength"] = df["freq_diff"].abs().clip(lower=BEHAVIOR_SIGNAL_DIFF_THRESHOLD).round(3)
    df["signal_direction"] = np.where(
        df["freq_diff"] >= BEHAVIOR_SIGNAL_DIFF_THRESHOLD,
        "high_active",
        np.where(
            df["freq_diff"] <= -BEHAVIOR_SIGNAL_DIFF_THRESHOLD,
            "low_active",
            "neutral",
        ),
    )
    df["match_field"] = np.where(
        df["behavior_event_type"].str.contains("浏览|页面|曝光", regex=True),
        "page_name",
        "click_name",
    )
    return df.reset_index(drop=True)


def read_xlsx_without_engine(path: Path, sheet_name: str | None = None) -> pd.DataFrame:
    with zipfile.ZipFile(path) as workbook:
        shared_strings = build_shared_strings(workbook)
        target = resolve_sheet_target(workbook, sheet_name)
        root = ET.fromstring(workbook.read(f"xl/{target.lstrip('/')}"))
        sheet_data = root.find("a:sheetData", NS)
        if sheet_data is None:
            return pd.DataFrame()

        rows: list[dict[int, str]] = []
        for row in sheet_data.findall("a:row", NS):
            row_values: dict[int, str] = {}
            for cell in row.findall("a:c", NS):
                column_letters = re.match(r"[A-Z]+", cell.attrib["r"]).group(0)
                column_index = excel_column_to_index(column_letters)
                row_values[column_index] = read_excel_cell(cell, shared_strings)
            rows.append(row_values)

    if not rows:
        return pd.DataFrame()

    header_row = rows[0]
    max_col = max(max(row.keys(), default=0) for row in rows)
    headers = [header_row.get(idx, f"column_{idx}") for idx in range(1, max_col + 1)]

    records = []
    for row in rows[1:]:
        records.append({headers[idx - 1]: row.get(idx) for idx in range(1, max_col + 1)})
    return pd.DataFrame(records)


def build_shared_strings(workbook: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in workbook.namelist():
        return []
    root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
    return [
        "".join(t.text or "" for t in item.iterfind(".//a:t", NS))
        for item in root.findall("a:si", NS)
    ]


def read_excel_cell(cell: ET.Element, shared_strings: list[str]) -> str | None:
    cell_type = cell.attrib.get("t")
    value_node = cell.find("a:v", NS)
    if cell_type == "s" and value_node is not None and value_node.text is not None:
        return shared_strings[int(value_node.text)]
    if cell_type == "inlineStr":
        inline = cell.find("a:is", NS)
        if inline is not None:
            return "".join(t.text or "" for t in inline.iterfind(".//a:t", NS))
    if value_node is not None:
        return value_node.text
    return None


def resolve_sheet_target(workbook: zipfile.ZipFile, sheet_name: str | None) -> str:
    root = ET.fromstring(workbook.read("xl/workbook.xml"))
    rels = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
    rel_map = {rel.attrib["Id"]: rel.attrib["Target"] for rel in rels}
    sheets = list(root.find("a:sheets", NS))
    selected = sheets[0]
    if sheet_name:
        for sheet in sheets:
            if sheet.attrib["name"] == sheet_name:
                selected = sheet
                break
        else:
            raise ValueError(f"Sheet not found: {sheet_name}")
    rel_id = selected.attrib[
        "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
    ]
    return rel_map[rel_id]


def excel_column_to_index(letters: str) -> int:
    value = 0
    for char in letters:
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={col: RENAME_MAP.get(col, col) for col in df.columns})
    required = {"user_id", "event_name", "cnt"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    if "dt_hour" not in df.columns:
        df["dt_hour"] = pd.NaT
    if "user_type" not in df.columns:
        df["user_type"] = pd.NA
    if "click_name" not in df.columns:
        df["click_name"] = pd.NA
    if "page_name" not in df.columns:
        df["page_name"] = pd.NA
    if "page_stay" not in df.columns:
        df["page_stay"] = 0.0
    if "bound_device_total" not in df.columns:
        df["bound_device_total"] = pd.NA
    if "api_loaded_time" not in df.columns:
        df["api_loaded_time"] = pd.NA
    if "user_load_time" not in df.columns:
        df["user_load_time"] = pd.NA
    if "user_network_latency" not in df.columns:
        df["user_network_latency"] = pd.NA
    return df


def clean_events(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = normalize_columns(df)
    df = df.replace("(not set)", np.nan)
    df["dt_hour"] = pd.to_datetime(
        df["dt_hour"].astype("string"),
        format="%Y%m%d%H",
        errors="coerce",
    )
    df["page_stay"] = pd.to_numeric(df["page_stay"], errors="coerce").fillna(0.0)
    df["cnt"] = pd.to_numeric(df["cnt"], errors="coerce").fillna(0.0)
    for column in [
        "bound_device_total",
        "api_loaded_time",
        "page_loaded_time",
        "user_load_time",
        "user_network_latency",
        "user_network_speed",
    ]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    df["date"] = df["dt_hour"].dt.date
    df["event_type"] = df["event_name"].map(map_event_type).fillna("other")
    return df


def map_event_type(event_name: object) -> str:
    if pd.isna(event_name):
        return "other"
    value = str(event_name)
    if value == "sy_home_loaded":
        return "app_launch"
    if value == "sy_page_loaded":
        return "page_loaded"
    if value == "sy_btn_click":
        return "click"
    if value in {"screen_view", "sy_page_pv"}:
        return "page_view"
    if value == "sy_bind_device":
        return "bind_device_snapshot"
    if value == "sy_api_loaded":
        return "api_loaded"
    if value == "sy_api_error":
        return "api_error"
    if value in {"user_engagement"}:
        return "system_or_view"
    return "other"


def sum_series(df: pd.DataFrame, mask: pd.Series, value_col: str, output_name: str) -> pd.Series:
    return df.loc[mask].groupby("user_id")[value_col].sum().rename(output_name)


def max_series(df: pd.DataFrame, mask: pd.Series, value_col: str, output_name: str) -> pd.Series:
    return df.loc[mask].groupby("user_id")[value_col].max().rename(output_name)


def mean_series(df: pd.DataFrame, mask: pd.Series, value_col: str, output_name: str) -> pd.Series:
    return df.loc[mask].groupby("user_id")[value_col].mean().rename(output_name)


def contains_any(series: pd.Series, patterns: tuple[str, ...]) -> pd.Series:
    values = series.fillna("").astype("string")
    mask = pd.Series(False, index=series.index)
    for pattern in patterns:
        mask = mask | values.str.contains(re.escape(pattern), regex=True)
    return mask


def sum_pattern_series(
    df: pd.DataFrame,
    target_col: str,
    patterns: tuple[str, ...],
    value_col: str,
    output_name: str,
    extra_mask: pd.Series | None = None,
) -> pd.Series:
    mask = contains_any(df[target_col], patterns)
    if extra_mask is not None:
        mask = mask & extra_mask
    return sum_series(df, mask, value_col, output_name)


def build_user_features(df: pd.DataFrame) -> pd.DataFrame:
    users = pd.DataFrame({"user_id": sorted(df["user_id"].dropna().astype(str).unique())})
    base = users.set_index("user_id")
    page_view_mask = df["event_type"] == "page_view"
    click_mask = df["event_type"] == "click"
    bind_snapshot_mask = df["event_type"] == "bind_device_snapshot"
    api_loaded_mask = df["event_type"] == "api_loaded"
    app_launch_mask = df["event_type"] == "app_launch"
    if "bound_device_total" in df.columns and df["bound_device_total"].notna().any():
        bind_device_metric = max_series(df, bind_snapshot_mask, "bound_device_total", "bind_device_cnt")
    else:
        # 兼容旧表：如果没有 sy_bind_device_count，则退回到 sy_bind_device 事件次数。
        bind_device_metric = sum_series(df, bind_snapshot_mask, "cnt", "bind_device_cnt")

    feature_list: list[pd.Series] = [
        df.groupby("user_id")["date"].nunique().rename("active_days"),
        df.groupby("user_id")["cnt"].sum().rename("total_event_cnt"),
        df.loc[page_view_mask].groupby("user_id")["page_stay"].sum().rename("total_page_stay"),
        df.loc[page_view_mask].groupby("user_id")["page_name"].nunique(dropna=True).rename("unique_pages"),
        df.loc[click_mask].groupby("user_id")["click_name"].nunique(dropna=True).rename("unique_clicks"),
        df.groupby("user_id")["dt_hour"].min().rename("first_seen_at"),
        df.groupby("user_id")["dt_hour"].max().rename("last_seen_at"),
        df.dropna(subset=["user_type"]).groupby("user_id")["user_type"].agg(lambda s: s.iloc[0]).rename("user_type"),
        sum_series(df, bind_snapshot_mask, "cnt", "bind_device_snapshot_event_cnt"),
        bind_device_metric,
        sum_series(df, df["click_name"] == "绑定设备按钮", "cnt", "bind_btn_click_cnt"),
        sum_series(df, df["click_name"] == "新增弹窗-极速绑定按钮首页", "cnt", "fast_bind_click_cnt"),
        sum_series(df, df["click_name"] == "虚拟挖机弹窗引导-绑定真实设备", "cnt", "virtual_to_real_bind_cnt"),
        sum_series(df, df["click_name"] == "底部设备按钮", "cnt", "bottom_device_btn_cnt"),
        sum_series(df, df["click_name"] == "我要召请按钮0331", "cnt", "summon_entry_click_cnt"),
        sum_pattern_series(df, "click_name", SERVICE_ENTRY_CLICK_PATTERNS, "cnt", "service_entry_click_cnt"),
        sum_series(df, df["click_name"] == "我要召请-选择设备按钮", "cnt", "summon_select_device_cnt"),
        sum_series(df, df["click_name"] == "我要召请-提交按钮", "cnt", "summon_submit_cnt"),
        sum_series(df, page_view_mask & (df["page_name"] == "首页"), "cnt", "home_pv"),
        sum_series(df, page_view_mask & (df["page_name"] == "首页"), "page_stay", "home_stay"),
        sum_series(df, page_view_mask & (df["page_name"] == "设备列表页"), "cnt", "device_list_pv"),
        sum_series(df, page_view_mask & (df["page_name"] == "设备列表页"), "page_stay", "device_list_stay"),
        sum_series(df, page_view_mask & (df["page_name"] == "设备详情页"), "cnt", "device_detail_pv"),
        sum_series(df, page_view_mask & (df["page_name"] == "设备详情页"), "page_stay", "device_detail_stay"),
        sum_series(df, page_view_mask & (df["page_name"] == "圈子推荐-首页"), "cnt", "circle_pv"),
        sum_series(df, page_view_mask & (df["page_name"] == "圈子推荐-首页"), "page_stay", "circle_stay"),
        sum_pattern_series(df, "click_name", ("圈子",), "cnt", "circle_entry_click_cnt"),
        sum_pattern_series(df, "page_name", COLLEAGUE_CIRCLE_PAGE_PATTERNS, "cnt", "colleague_circle_pv", extra_mask=page_view_mask),
        sum_pattern_series(df, "page_name", COLLEAGUE_CIRCLE_PAGE_PATTERNS, "page_stay", "colleague_circle_stay", extra_mask=page_view_mask),
        sum_series(df, page_view_mask & (df["page_name"] == "服务召请页"), "cnt", "service_page_pv"),
        sum_series(df, page_view_mask & (df["page_name"] == "服务召请页"), "page_stay", "service_page_stay"),
        sum_pattern_series(df, "click_name", SPARE_PART_ENTRY_CLICK_PATTERNS, "cnt", "spare_part_entry_click_cnt"),
        sum_pattern_series(df, "page_name", SPARE_PART_LIST_PAGE_PATTERNS, "cnt", "spare_part_list_pv", extra_mask=page_view_mask),
        sum_pattern_series(df, "page_name", SPARE_PART_LIST_PAGE_PATTERNS, "page_stay", "spare_part_list_stay", extra_mask=page_view_mask),
        sum_pattern_series(df, "page_name", SPARE_PART_DETAIL_PAGE_PATTERNS, "cnt", "spare_part_detail_pv", extra_mask=page_view_mask),
        sum_pattern_series(df, "click_name", QUOTE_SUBMIT_CLICK_PATTERNS, "cnt", "quote_submit_cnt"),
        sum_pattern_series(df, "click_name", MAINTENANCE_ENTRY_CLICK_PATTERNS, "cnt", "maintenance_entry_click_cnt"),
        sum_pattern_series(df, "page_name", MAINTENANCE_LIST_PAGE_PATTERNS, "cnt", "maintenance_list_pv", extra_mask=page_view_mask),
        sum_pattern_series(df, "page_name", MAINTENANCE_LIST_PAGE_PATTERNS, "page_stay", "maintenance_list_stay", extra_mask=page_view_mask),
        sum_pattern_series(df, "click_name", PRODUCT_CENTER_ENTRY_CLICK_PATTERNS, "cnt", "product_center_entry_click_cnt"),
        sum_pattern_series(df, "page_name", PRODUCT_LIST_PAGE_PATTERNS, "cnt", "product_list_pv", extra_mask=page_view_mask),
        sum_pattern_series(df, "page_name", PRODUCT_LIST_PAGE_PATTERNS, "page_stay", "product_list_stay", extra_mask=page_view_mask),
        sum_pattern_series(df, "click_name", DATA_REPORT_CLICK_PATTERNS, "cnt", "data_report_click_cnt"),
        sum_pattern_series(df, "click_name", DATA_ANALYSIS_CLICK_PATTERNS, "cnt", "data_analysis_click_cnt"),
        sum_series(df, df["event_name"] == "sy_api_error", "cnt", "api_error_cnt"),
        sum_series(df, api_loaded_mask, "cnt", "api_loaded_cnt"),
        mean_series(df, api_loaded_mask, "api_loaded_time", "avg_api_loaded_time"),
        max_series(df, api_loaded_mask, "api_loaded_time", "max_api_loaded_time"),
        sum_series(df, app_launch_mask, "cnt", "app_launch_cnt"),
        mean_series(df, app_launch_mask, "user_load_time", "avg_app_load_time"),
        mean_series(df, app_launch_mask, "user_network_latency", "avg_network_latency"),
        sum_series(df, df["click_name"] == "全部设备列表切换按钮", "cnt", "switch_device_cnt"),
    ]

    user_features = pd.concat([base] + feature_list, axis=1).reset_index()
    user_features = user_features.fillna(
        {
            "user_type": "",
            "active_days": 0,
            "total_event_cnt": 0,
            "total_page_stay": 0,
            "unique_pages": 0,
            "unique_clicks": 0,
            "bind_device_cnt": 0,
            "bind_device_snapshot_event_cnt": 0,
            "bind_btn_click_cnt": 0,
            "fast_bind_click_cnt": 0,
            "virtual_to_real_bind_cnt": 0,
            "bottom_device_btn_cnt": 0,
            "summon_entry_click_cnt": 0,
            "service_entry_click_cnt": 0,
            "summon_select_device_cnt": 0,
            "summon_submit_cnt": 0,
            "home_pv": 0,
            "home_stay": 0,
            "device_list_pv": 0,
            "device_list_stay": 0,
            "device_detail_pv": 0,
            "device_detail_stay": 0,
            "circle_pv": 0,
            "circle_stay": 0,
            "circle_entry_click_cnt": 0,
            "colleague_circle_pv": 0,
            "colleague_circle_stay": 0,
            "service_page_pv": 0,
            "service_page_stay": 0,
            "spare_part_entry_click_cnt": 0,
            "spare_part_list_pv": 0,
            "spare_part_list_stay": 0,
            "spare_part_detail_pv": 0,
            "quote_submit_cnt": 0,
            "maintenance_entry_click_cnt": 0,
            "maintenance_list_pv": 0,
            "maintenance_list_stay": 0,
            "product_center_entry_click_cnt": 0,
            "product_list_pv": 0,
            "product_list_stay": 0,
            "data_report_click_cnt": 0,
            "data_analysis_click_cnt": 0,
            "api_error_cnt": 0,
            "api_loaded_cnt": 0,
            "avg_api_loaded_time": 0,
            "max_api_loaded_time": 0,
            "app_launch_cnt": 0,
            "avg_app_load_time": 0,
            "avg_network_latency": 0,
            "switch_device_cnt": 0,
        }
    )

    user_features["days_observed"] = (
        user_features["last_seen_at"] - user_features["first_seen_at"]
    ).dt.days.fillna(0)
    user_features["avg_stay_per_event"] = (
        user_features["total_page_stay"] / user_features["total_event_cnt"].replace(0, np.nan)
    ).fillna(0)
    service_detail_base_cnt = user_features[["service_page_pv", "summon_select_device_cnt"]].max(axis=1)
    user_features["service_detail_engaged_user_flag"] = (
        (service_detail_base_cnt > 0) | (user_features["summon_submit_cnt"] > 0)
    ).astype(int)
    user_features["service_detail_submit_cvr_proxy"] = (
        user_features["summon_submit_cnt"] / service_detail_base_cnt.replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0)
    user_features["circle_avg_stay_per_view"] = (
        user_features["circle_stay"] / user_features["circle_pv"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0)
    user_features["spare_part_list_avg_stay_per_view"] = (
        user_features["spare_part_list_stay"] / user_features["spare_part_list_pv"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0)
    user_features["product_list_avg_stay_per_view"] = (
        user_features["product_list_stay"] / user_features["product_list_pv"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0)
    user_features["maintenance_list_avg_stay_per_view"] = (
        user_features["maintenance_list_stay"] / user_features["maintenance_list_pv"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0)
    user_features["spare_part_quote_cvr_proxy"] = (
        user_features["quote_submit_cnt"] / user_features["spare_part_detail_pv"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0)
    user_features["maintenance_entry_to_list_cvr_proxy"] = (
        user_features["maintenance_list_pv"] / user_features["maintenance_entry_click_cnt"].replace(0, np.nan)
    ).replace([np.inf, -np.inf], np.nan).fillna(0)
    return user_features


def tag_bind_status(row: pd.Series) -> str:
    if row["bind_device_cnt"] > 0:
        return "已绑定"
    if row["bind_btn_click_cnt"] > 0 or row["fast_bind_click_cnt"] > 0 or row["virtual_to_real_bind_cnt"] > 0:
        return "尝试绑定未完成"
    return "未触发绑定"


def tag_circle_engagement(row: pd.Series) -> str:
    if row["circle_stay"] >= 300:
        return "高社区参与"
    if row["circle_stay"] > 0:
        return "低社区参与"
    return "无社区参与"


def tag_service_intent(row: pd.Series) -> str:
    if row["summon_submit_cnt"] > 0:
        return "服务提交用户"
    if row["summon_select_device_cnt"] > 0 or row["service_entry_click_cnt"] > 0:
        return "服务意向用户"
    return "无明显服务意向"


def tag_risk(row: pd.Series) -> str:
    if row["api_error_cnt"] > 0 or row["switch_device_cnt"] >= 5:
        return "高受阻风险"
    if row.get("max_api_loaded_time", 0) >= 5:
        return "高受阻风险"
    if row["device_detail_stay"] >= 300 and row["summon_submit_cnt"] == 0:
        return "疑似流程受阻"
    return "正常"


def detect_aha_moments(user_features: pd.DataFrame) -> pd.DataFrame:
    user_features = user_features.copy()
    user_features["aha_content_flag"] = (
        (user_features["circle_stay"] >= 300)
        | ((user_features["circle_pv"] >= 3) & (user_features["circle_stay"] > 0))
    ).astype(int)
    user_features["aha_bind_success_flag"] = (
        (user_features["bind_device_cnt"] > 0)
        & (
            (user_features["virtual_to_real_bind_cnt"] > 0)
            | (user_features["bind_btn_click_cnt"] > 0)
            | (user_features["fast_bind_click_cnt"] > 0)
        )
    ).astype(int)
    user_features["aha_service_start_flag"] = (
        (user_features["summon_select_device_cnt"] > 0)
        | (user_features["summon_submit_cnt"] > 0)
    ).astype(int)
    user_features["aha_fast_entry_flag"] = (
        (user_features["fast_bind_click_cnt"] > 0)
        | (user_features["bottom_device_btn_cnt"] >= 3)
    ).astype(int)
    user_features["aha_moment_count"] = (
        user_features[
            [
                "aha_content_flag",
                "aha_bind_success_flag",
                "aha_service_start_flag",
                "aha_fast_entry_flag",
            ]
        ]
        .sum(axis=1)
        .astype(int)
    )
    user_features["aha_types"] = user_features.apply(join_aha_types, axis=1)
    user_features["primary_aha_type"] = user_features.apply(select_primary_aha_type, axis=1)
    user_features["aha_detail"] = user_features.apply(build_aha_detail, axis=1)
    return user_features


def join_aha_types(row: pd.Series) -> str:
    aha_types: list[str] = []
    if row["aha_service_start_flag"] == 1:
        aha_types.append("服务流程启动")
    if row["aha_bind_success_flag"] == 1:
        aha_types.append("设备绑定成功")
    if row["aha_content_flag"] == 1:
        aha_types.append("内容获取感")
    if row["aha_fast_entry_flag"] == 1:
        aha_types.append("快速入口使用")
    return "|".join(aha_types) if aha_types else "未识别"


def select_primary_aha_type(row: pd.Series) -> str:
    for aha_type in AHA_TYPE_ORDER:
        if aha_type == "服务流程启动" and row["aha_service_start_flag"] == 1:
            return aha_type
        if aha_type == "设备绑定成功" and row["aha_bind_success_flag"] == 1:
            return aha_type
        if aha_type == "内容获取感" and row["aha_content_flag"] == 1:
            return aha_type
        if aha_type == "快速入口使用" and row["aha_fast_entry_flag"] == 1:
            return aha_type
    return "未识别"


def build_aha_detail(row: pd.Series) -> str:
    if row["primary_aha_type"] == "内容获取感":
        return f"circle_pv={int(row['circle_pv'])}, circle_stay={round(float(row['circle_stay']), 2)}"
    if row["primary_aha_type"] == "设备绑定成功":
        return (
            f"bind_device_cnt={round(float(row['bind_device_cnt']), 2)}, "
            f"virtual_to_real_bind_cnt={round(float(row['virtual_to_real_bind_cnt']), 2)}"
        )
    if row["primary_aha_type"] == "服务流程启动":
        return (
            f"summon_select_device_cnt={round(float(row['summon_select_device_cnt']), 2)}, "
            f"summon_submit_cnt={round(float(row['summon_submit_cnt']), 2)}"
        )
    if row["primary_aha_type"] == "快速入口使用":
        return (
            f"fast_bind_click_cnt={round(float(row['fast_bind_click_cnt']), 2)}, "
            f"bottom_device_btn_cnt={round(float(row['bottom_device_btn_cnt']), 2)}"
        )
    return ""


def apply_behavior_compare_signals(
    user_features: pd.DataFrame,
    event_df: pd.DataFrame,
    behavior_compare_df: pd.DataFrame | None,
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    user_features = user_features.copy()
    default_values = {
        "high_active_behavior_hit_cnt": 0,
        "low_active_behavior_hit_cnt": 0,
        "high_active_behavior_score": 0.0,
        "low_active_behavior_score": 0.0,
        "behavior_net_signal_score": 0.0,
        "high_active_signal_behaviors": "",
        "low_active_signal_behaviors": "",
        "behavior_signal_tag": "未提供行为对比表",
    }
    for column, value in default_values.items():
        user_features[column] = value

    if behavior_compare_df is None or behavior_compare_df.empty:
        return user_features, None

    score_board: dict[str, dict[str, object]] = {
        str(user_id): {
            "high_score": 0.0,
            "low_score": 0.0,
            "high_behaviors": [],
            "low_behaviors": [],
        }
        for user_id in user_features["user_id"].astype(str)
    }

    for row in behavior_compare_df.itertuples(index=False):
        if row.signal_direction == "neutral":
            continue
        match_field = row.match_field
        if match_field not in event_df.columns:
            continue
        matched = (
            event_df.loc[event_df[match_field] == row.behavior_name]
            .groupby("user_id")["cnt"]
            .sum()
        )
        if matched.empty:
            continue

        for user_id, raw_count in matched.items():
            key = str(user_id)
            if key not in score_board:
                continue
            capped_count = min(float(raw_count), 3.0)
            weighted_score = capped_count * float(row.signal_strength)
            formatted = f"{row.behavior_name}:{round(float(raw_count), 2)}"
            if row.signal_direction == "high_active":
                score_board[key]["high_score"] += weighted_score
                score_board[key]["high_behaviors"].append(formatted)
            elif row.signal_direction == "low_active":
                score_board[key]["low_score"] += weighted_score
                score_board[key]["low_behaviors"].append(formatted)

    summary_rows = []
    for user_id, score in score_board.items():
        high_behaviors = sorted(set(score["high_behaviors"]))
        low_behaviors = sorted(set(score["low_behaviors"]))
        high_score = round(float(score["high_score"]), 3)
        low_score = round(float(score["low_score"]), 3)
        net_score = round(high_score - low_score, 3)
        if high_score - low_score >= 1.5 and len(high_behaviors) >= 1:
            behavior_signal_tag = "高活倾向行为"
        elif low_score - high_score >= 1.5 and len(low_behaviors) >= 1:
            behavior_signal_tag = "低活倾向行为"
        else:
            behavior_signal_tag = "中性行为"

        summary_rows.append(
            {
                "user_id": user_id,
                "high_active_behavior_hit_cnt": len(high_behaviors),
                "low_active_behavior_hit_cnt": len(low_behaviors),
                "high_active_behavior_score": high_score,
                "low_active_behavior_score": low_score,
                "behavior_net_signal_score": net_score,
                "high_active_signal_behaviors": "|".join(high_behaviors),
                "low_active_signal_behaviors": "|".join(low_behaviors),
                "behavior_signal_tag": behavior_signal_tag,
            }
        )

    summary_df = pd.DataFrame(summary_rows)
    user_features = user_features.drop(
        columns=list(default_values.keys()),
        errors="ignore",
    ).merge(summary_df, on="user_id", how="left")
    for column, value in default_values.items():
        user_features[column] = user_features[column].fillna(value)
    return user_features, behavior_compare_df


def add_rule_tags(
    user_features: pd.DataFrame,
    event_df: pd.DataFrame | None = None,
    behavior_compare_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame | None]:
    user_features = user_features.copy()
    user_features["bind_tag"] = user_features.apply(tag_bind_status, axis=1)
    user_features["circle_tag"] = user_features.apply(tag_circle_engagement, axis=1)
    user_features["service_tag"] = user_features.apply(tag_service_intent, axis=1)
    user_features["risk_tag"] = user_features.apply(tag_risk, axis=1)
    normalized_behavior_compare = None
    if event_df is not None:
        user_features, normalized_behavior_compare = apply_behavior_compare_signals(
            user_features,
            event_df=event_df,
            behavior_compare_df=behavior_compare_df,
        )
    user_features["high_active_label"] = (
        (user_features["active_days"] >= 2)
        & (user_features["total_event_cnt"] >= 20)
        & (
            (user_features["bind_device_cnt"] > 0)
            | (user_features["circle_stay"] >= 300)
            | (user_features["summon_submit_cnt"] > 0)
            | (
                (user_features["behavior_signal_tag"] == "高活倾向行为")
                & (user_features["high_active_behavior_hit_cnt"] >= 2)
            )
        )
    ).astype(int)
    user_features["blocked_flag"] = (
        (
            (user_features["device_detail_stay"] >= 300)
            & (user_features["summon_submit_cnt"] == 0)
        )
        | (user_features["api_error_cnt"] > 0)
        | (user_features["max_api_loaded_time"] >= 5)
        | (user_features["switch_device_cnt"] >= 5)
        | (
            (user_features["behavior_signal_tag"] == "低活倾向行为")
            & (user_features["low_active_behavior_hit_cnt"] >= 2)
        )
    ).astype(int)
    user_features["persona"] = user_features.apply(build_persona, axis=1)
    return detect_aha_moments(user_features), normalized_behavior_compare


def build_persona(row: pd.Series) -> str:
    parts = [
        "高活" if row["high_active_label"] == 1 else "低活",
        row["bind_tag"],
        row["service_tag"],
        row["circle_tag"],
        row["risk_tag"],
    ]
    return "|".join(parts)


def run_clustering(user_features: pd.DataFrame, clusters: int) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    try:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler
    except ModuleNotFoundError:
        return None

    cluster_cols = [
        "active_days",
        "total_event_cnt",
        "bind_device_cnt",
        "service_entry_click_cnt",
        "summon_select_device_cnt",
        "summon_submit_cnt",
        "circle_stay",
        "device_detail_stay",
        "api_error_cnt",
        "switch_device_cnt",
    ]
    if len(user_features) < clusters:
        return None

    X = user_features[cluster_cols].copy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    kmeans = KMeans(n_clusters=clusters, random_state=42, n_init=10)
    clustered = user_features.copy()
    clustered["cluster"] = kmeans.fit_predict(X_scaled)
    cluster_summary = (
        clustered.groupby("cluster")[cluster_cols + ["high_active_label", "blocked_flag"]]
        .mean()
        .round(2)
        .reset_index()
    )
    return clustered, cluster_summary


def train_high_active_model(user_features: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]] | None:
    try:
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.metrics import classification_report, roc_auc_score
        from sklearn.model_selection import train_test_split
    except ModuleNotFoundError:
        return None

    if user_features["high_active_label"].nunique() < 2 or len(user_features) < 10:
        return None

    model_cols = [
        "active_days",
        "total_event_cnt",
        "bind_device_cnt",
        "bind_btn_click_cnt",
        "fast_bind_click_cnt",
        "virtual_to_real_bind_cnt",
        "service_entry_click_cnt",
        "summon_select_device_cnt",
        "summon_submit_cnt",
        "circle_pv",
        "circle_stay",
        "device_detail_pv",
        "device_detail_stay",
        "api_error_cnt",
        "switch_device_cnt",
    ]

    X = user_features[model_cols]
    y = user_features["high_active_label"]
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.3,
        random_state=42,
        stratify=y,
    )

    clf = RandomForestClassifier(
        n_estimators=200,
        max_depth=6,
        random_state=42,
    )
    clf.fit(X_train, y_train)
    pred = clf.predict(X_test)
    proba = clf.predict_proba(X_test)[:, 1]

    feature_importance = (
        pd.DataFrame({"feature": model_cols, "importance": clf.feature_importances_})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )
    report = classification_report(y_test, pred, output_dict=True, zero_division=0)
    metrics = {
        "auc": float(roc_auc_score(y_test, proba)),
        "accuracy": float(report["accuracy"]),
        "positive_count": int(y.sum()),
        "negative_count": int((1 - y).sum()),
    }
    return feature_importance, metrics


def build_funnel(user_features: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    funnel = pd.DataFrame(
        {
            "step_home": (user_features["home_pv"] > 0).astype(int),
            "step_device_list": (user_features["device_list_pv"] > 0).astype(int),
            "step_bind": (user_features["bind_device_cnt"] > 0).astype(int),
            "step_summon_entry": (user_features["service_entry_click_cnt"] > 0).astype(int),
            "step_select_device": (user_features["summon_select_device_cnt"] > 0).astype(int),
            "step_submit": (user_features["summon_submit_cnt"] > 0).astype(int),
        }
    )
    funnel_rate = funnel.mean().round(3).rename("rate").reset_index()
    funnel_rate.columns = ["step", "rate"]
    funnel_with_label = pd.concat(
        [user_features[["user_id", "high_active_label"]], funnel],
        axis=1,
    )
    group_funnel = (
        funnel_with_label.groupby("high_active_label")
        .mean(numeric_only=True)
        .round(3)
        .reset_index()
    )
    return funnel_rate, group_funnel


def build_blocked_summary(user_features: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "active_days",
        "total_event_cnt",
        "bind_device_cnt",
        "service_entry_click_cnt",
        "summon_submit_cnt",
        "circle_stay",
        "device_detail_stay",
        "api_error_cnt",
        "switch_device_cnt",
    ]
    return (
        user_features.groupby("blocked_flag")[cols]
        .mean()
        .round(2)
        .reset_index()
    )


def build_aha_summary(user_features: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    aha_summary = pd.DataFrame(
        {
            "aha_type": [
                "内容获取感",
                "设备绑定成功",
                "服务流程启动",
                "快速入口使用",
            ],
            "user_count": [
                int(user_features["aha_content_flag"].sum()),
                int(user_features["aha_bind_success_flag"].sum()),
                int(user_features["aha_service_start_flag"].sum()),
                int(user_features["aha_fast_entry_flag"].sum()),
            ],
        }
    )
    aha_summary["user_ratio"] = (
        aha_summary["user_count"] / max(len(user_features), 1)
    ).round(3)

    aha_by_active_label = (
        user_features.groupby("high_active_label")[
            [
                "aha_content_flag",
                "aha_bind_success_flag",
                "aha_service_start_flag",
                "aha_fast_entry_flag",
                "aha_moment_count",
            ]
        ]
        .mean()
        .round(3)
        .reset_index()
    )
    return aha_summary, aha_by_active_label


def build_behavior_signal_summary(user_features: pd.DataFrame) -> pd.DataFrame:
    summary = (
        user_features.groupby("behavior_signal_tag")[
            [
                "high_active_behavior_hit_cnt",
                "low_active_behavior_hit_cnt",
                "high_active_behavior_score",
                "low_active_behavior_score",
                "behavior_net_signal_score",
            ]
        ]
        .agg(["count", "mean"])
        .round(3)
        .reset_index()
    )
    flattened_columns = ["behavior_signal_tag"]
    for metric, stat in summary.columns.tolist()[1:]:
        flattened_columns.append(f"{metric}_{stat}")
    summary.columns = flattened_columns
    return summary


def safe_ratio(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return round(float(numerator) / float(denominator), 3)


def build_metric_summary(user_features: pd.DataFrame) -> pd.DataFrame:
    total_users = max(len(user_features), 1)

    service_entry_users = int((user_features["service_entry_click_cnt"] > 0).sum())
    service_entry_clicks = round(float(user_features["service_entry_click_cnt"].sum()), 3)
    service_detail_users = int(user_features["service_detail_engaged_user_flag"].sum())
    service_submit_users = int((user_features["summon_submit_cnt"] > 0).sum())

    circle_entry_users = int((user_features["circle_entry_click_cnt"] > 0).sum())
    circle_entry_clicks = round(float(user_features["circle_entry_click_cnt"].sum()), 3)
    circle_browse_users = int((user_features["circle_pv"] > 0).sum())
    circle_avg_browse_duration = safe_ratio(
        float(user_features["circle_stay"].sum()),
        circle_browse_users,
    )

    colleague_circle_users = int((user_features["colleague_circle_pv"] > 0).sum())
    colleague_circle_avg_stay = safe_ratio(
        float(user_features["colleague_circle_stay"].sum()),
        colleague_circle_users,
    )

    spare_part_entry_users = int((user_features["spare_part_entry_click_cnt"] > 0).sum())
    spare_part_entry_clicks = round(float(user_features["spare_part_entry_click_cnt"].sum()), 3)
    spare_part_list_users = int((user_features["spare_part_list_pv"] > 0).sum())
    spare_part_detail_users = int((user_features["spare_part_detail_pv"] > 0).sum())
    quote_submit_users = int((user_features["quote_submit_cnt"] > 0).sum())

    maintenance_entry_users = int((user_features["maintenance_entry_click_cnt"] > 0).sum())
    maintenance_entry_clicks = round(float(user_features["maintenance_entry_click_cnt"].sum()), 3)
    maintenance_list_users = int((user_features["maintenance_list_pv"] > 0).sum())

    product_list_users = int((user_features["product_list_pv"] > 0).sum())
    device_tab_users = int((user_features["bottom_device_btn_cnt"] > 0).sum())
    device_tab_clicks = round(float(user_features["bottom_device_btn_cnt"].sum()), 3)
    device_detail_users = int((user_features["device_detail_pv"] > 0).sum())
    data_report_users = int((user_features["data_report_click_cnt"] > 0).sum())
    data_analysis_users = int((user_features["data_analysis_click_cnt"] > 0).sum())

    rows = [
        {
            "metric_module": "服务召请",
            "metric_function_name": "服务召请（入口）",
            "metric_dimension": "功能点击趋势-入口点击用户数",
            "metric_formula": "去重点击入口用户数",
            "metric_value": service_entry_users,
            "metric_unit": "user",
            "metric_note": "按多入口口径聚合：我要召请按钮0331、服务召请按钮",
        },
        {
            "metric_module": "服务召请",
            "metric_function_name": "服务召请（入口）",
            "metric_dimension": "功能点击趋势-入口点击次数",
            "metric_formula": "所有入口点击总次数",
            "metric_value": service_entry_clicks,
            "metric_unit": "cnt",
            "metric_note": "按多入口口径聚合：我要召请按钮0331、服务召请按钮",
        },
        {
            "metric_module": "服务召请",
            "metric_function_name": "服务召请（详情页）",
            "metric_dimension": "召请-转化率 CVR",
            "metric_formula": "点击提交按钮用户数 / 服务召请详情页访问用户数",
            "metric_value": safe_ratio(service_submit_users, service_detail_users),
            "metric_unit": "ratio",
            "metric_note": f"submit_users={service_submit_users}, detail_view_users={service_detail_users}",
        },
        {
            "metric_module": "服务召请",
            "metric_function_name": "路径分析 服务召请（入口）-详情页-提交",
            "metric_dimension": "路径漏斗分析-入口到提交转化率",
            "metric_formula": "点击提交按钮用户数 / 去重点击入口用户数",
            "metric_value": safe_ratio(service_submit_users, service_entry_users),
            "metric_unit": "ratio",
            "metric_note": f"entry_users={service_entry_users}, submit_users={service_submit_users}",
        },
        {
            "metric_module": "圈子",
            "metric_function_name": "圈子（入口）",
            "metric_dimension": "功能点击趋势-入口点击用户数",
            "metric_formula": "去重点击Tab“圈子”的用户数",
            "metric_value": circle_entry_users if circle_entry_users else circle_browse_users,
            "metric_unit": "user",
            "metric_note": "若未采到圈子tab点击，则回退使用圈子页面访问用户数",
        },
        {
            "metric_module": "圈子",
            "metric_function_name": "推荐/关注/话题页",
            "metric_dimension": "平均浏览时长",
            "metric_formula": "浏览总时长 / 浏览人数",
            "metric_value": circle_avg_browse_duration,
            "metric_unit": "sec_per_user",
            "metric_note": f"browse_users={circle_browse_users}",
        },
        {
            "metric_module": "圈子",
            "metric_function_name": "同事圈聚合页",
            "metric_dimension": "平均浏览时长",
            "metric_formula": "浏览总时长 / 浏览人数",
            "metric_value": colleague_circle_avg_stay,
            "metric_unit": "sec_per_user",
            "metric_note": f"browse_users={colleague_circle_users}",
        },
        {
            "metric_module": "我要配件",
            "metric_function_name": "我要配件（入口）",
            "metric_dimension": "功能点击趋势-入口点击用户数",
            "metric_formula": "去重点击入口用户数",
            "metric_value": spare_part_entry_users,
            "metric_unit": "user",
            "metric_note": "按包含“我要配件按钮”的点击聚合",
        },
        {
            "metric_module": "我要配件",
            "metric_function_name": "我要配件（入口）",
            "metric_dimension": "功能点击趋势-入口点击次数",
            "metric_formula": "所有入口点击总次数",
            "metric_value": spare_part_entry_clicks,
            "metric_unit": "cnt",
            "metric_note": "按包含“我要配件按钮”的点击聚合",
        },
        {
            "metric_module": "我要配件",
            "metric_function_name": "配件列表页",
            "metric_dimension": "平均浏览时长",
            "metric_formula": "浏览总时长 / 浏览人数",
            "metric_value": safe_ratio(float(user_features["spare_part_list_stay"].sum()), spare_part_list_users),
            "metric_unit": "sec_per_user",
            "metric_note": f"browse_users={spare_part_list_users}",
        },
        {
            "metric_module": "我要配件",
            "metric_function_name": "配件详情页",
            "metric_dimension": "询价转化率",
            "metric_formula": "询价提交人数 / 配件详情页访问UV",
            "metric_value": safe_ratio(quote_submit_users, spare_part_detail_users),
            "metric_unit": "ratio",
            "metric_note": f"submit_users={quote_submit_users}, detail_view_users={spare_part_detail_users}",
        },
        {
            "metric_module": "设备保养",
            "metric_function_name": "设备保养入口",
            "metric_dimension": "功能点击趋势-入口点击用户数",
            "metric_formula": "去重点击入口用户数",
            "metric_value": maintenance_entry_users,
            "metric_unit": "user",
            "metric_note": "按包含“设备保养/预约保养”的点击聚合",
        },
        {
            "metric_module": "设备保养",
            "metric_function_name": "设备保养入口",
            "metric_dimension": "功能点击趋势-入口点击次数",
            "metric_formula": "所有入口点击总次数",
            "metric_value": maintenance_entry_clicks,
            "metric_unit": "cnt",
            "metric_note": "按包含“设备保养/预约保养”的点击聚合",
        },
        {
            "metric_module": "设备保养",
            "metric_function_name": "设备保养列表页",
            "metric_dimension": "功能转化率",
            "metric_formula": "列表页访问用户数 / 入口点击用户数",
            "metric_value": safe_ratio(maintenance_list_users, maintenance_entry_users),
            "metric_unit": "ratio",
            "metric_note": f"list_users={maintenance_list_users}, entry_users={maintenance_entry_users}",
        },
        {
            "metric_module": "设备保养",
            "metric_function_name": "设备保养列表页",
            "metric_dimension": "访问跳出率",
            "metric_formula": "当前埋点不足，未实现",
            "metric_value": np.nan,
            "metric_unit": "ratio",
            "metric_note": "需要更明确的下一步行为或离开事件定义，当前脚本暂未支持",
        },
        {
            "metric_module": "产品中心",
            "metric_function_name": "产品列表页",
            "metric_dimension": "平均浏览时长",
            "metric_formula": "浏览总时长 / 浏览人数",
            "metric_value": safe_ratio(float(user_features["product_list_stay"].sum()), product_list_users),
            "metric_unit": "sec_per_user",
            "metric_note": f"browse_users={product_list_users}",
        },
        {
            "metric_module": "设备",
            "metric_function_name": "设备tab",
            "metric_dimension": "功能点击趋势-入口点击用户数",
            "metric_formula": "去重点击设备tab用户数",
            "metric_value": device_tab_users,
            "metric_unit": "user",
            "metric_note": "当前按底部设备按钮口径近似",
        },
        {
            "metric_module": "设备",
            "metric_function_name": "设备tab",
            "metric_dimension": "功能点击趋势-入口点击次数",
            "metric_formula": "所有入口点击总次数",
            "metric_value": device_tab_clicks,
            "metric_unit": "cnt",
            "metric_note": "当前按底部设备按钮口径近似",
        },
        {
            "metric_module": "设备",
            "metric_function_name": "工况详情页",
            "metric_dimension": "功能访问率",
            "metric_formula": "功能访问用户数 / 总活跃用户数",
            "metric_value": safe_ratio(device_detail_users, total_users),
            "metric_unit": "ratio",
            "metric_note": f"detail_users={device_detail_users}, total_users={total_users}",
        },
        {
            "metric_module": "设备",
            "metric_function_name": "数据报表",
            "metric_dimension": "功能访问率",
            "metric_formula": "功能访问用户数 / 总活跃用户数",
            "metric_value": safe_ratio(data_report_users, total_users),
            "metric_unit": "ratio",
            "metric_note": f"report_users={data_report_users}, total_users={total_users}",
        },
        {
            "metric_module": "设备",
            "metric_function_name": "数据分析",
            "metric_dimension": "功能访问率",
            "metric_formula": "功能访问用户数 / 总活跃用户数",
            "metric_value": safe_ratio(data_analysis_users, total_users),
            "metric_unit": "ratio",
            "metric_note": f"analysis_users={data_analysis_users}, total_users={total_users}",
        },
    ]
    return pd.DataFrame(rows)


def write_outputs(
    *,
    user_features: pd.DataFrame,
    output_dir: Path,
    clustered_result: tuple[pd.DataFrame, pd.DataFrame] | None,
    model_result: tuple[pd.DataFrame, dict[str, float]] | None,
    behavior_compare_df: pd.DataFrame | None,
    metric_summary_df: pd.DataFrame | None,
    metric_definition_df: pd.DataFrame | None,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    user_features.to_csv(output_dir / "user_features.csv", index=False, encoding="utf-8-sig")
    user_features[
        [
            "user_id",
            "persona",
            "bind_tag",
            "circle_tag",
            "service_tag",
            "risk_tag",
            "high_active_label",
            "blocked_flag",
            "aha_types",
            "primary_aha_type",
            "aha_moment_count",
            "aha_detail",
            "behavior_signal_tag",
            "high_active_behavior_hit_cnt",
            "low_active_behavior_hit_cnt",
            "high_active_behavior_score",
            "low_active_behavior_score",
            "behavior_net_signal_score",
            "high_active_signal_behaviors",
            "low_active_signal_behaviors",
        ]
    ].to_csv(output_dir / "user_persona.csv", index=False, encoding="utf-8-sig")

    blocked_summary = build_blocked_summary(user_features)
    blocked_summary.to_csv(output_dir / "blocked_summary.csv", index=False, encoding="utf-8-sig")

    aha_summary, aha_by_active_label = build_aha_summary(user_features)
    aha_summary.to_csv(output_dir / "aha_summary.csv", index=False, encoding="utf-8-sig")
    aha_by_active_label.to_csv(output_dir / "aha_by_active_label.csv", index=False, encoding="utf-8-sig")

    behavior_signal_summary = build_behavior_signal_summary(user_features)
    behavior_signal_summary.to_csv(output_dir / "behavior_signal_summary.csv", index=False, encoding="utf-8-sig")
    if behavior_compare_df is not None:
        behavior_compare_df.to_csv(output_dir / "behavior_compare_catalog.csv", index=False, encoding="utf-8-sig")
    if metric_summary_df is not None:
        metric_summary_df.to_csv(output_dir / "metric_summary.csv", index=False, encoding="utf-8-sig")
    if metric_definition_df is not None:
        metric_definition_df.to_csv(output_dir / "metric_definition_catalog.csv", index=False, encoding="utf-8-sig")

    funnel_rate, group_funnel = build_funnel(user_features)
    funnel_rate.to_csv(output_dir / "funnel_rate.csv", index=False, encoding="utf-8-sig")
    group_funnel.to_csv(output_dir / "group_funnel.csv", index=False, encoding="utf-8-sig")

    if clustered_result is not None:
        clustered_users, cluster_summary = clustered_result
        clustered_users.to_csv(output_dir / "user_features_with_cluster.csv", index=False, encoding="utf-8-sig")
        cluster_summary.to_csv(output_dir / "cluster_summary.csv", index=False, encoding="utf-8-sig")

    if model_result is not None:
        feature_importance, metrics = model_result
        feature_importance.to_csv(output_dir / "feature_importance.csv", index=False, encoding="utf-8-sig")
        (output_dir / "model_metrics.json").write_text(
            json.dumps(metrics, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="从用户事件明细构建用户特征表、画像标签、分群和漏斗结果。")
    parser.add_argument("--input", required=True, help="输入 CSV 或 XLSX 路径")
    parser.add_argument("--sheet-name", help="当输入为 XLSX 时使用的工作表名称")
    parser.add_argument("--behavior-compare-input", help="可选：行为汇总对比表路径，支持 CSV 或 XLSX")
    parser.add_argument("--behavior-compare-sheet", help="当行为对比输入为 XLSX 时使用的工作表名称，默认 行为汇总对比")
    parser.add_argument("--metric-definition-input", help="可选：指标说明表路径，默认 user_analysis/metric.xlsx")
    parser.add_argument("--output-dir", default="output/persona_pipeline", help="输出目录")
    parser.add_argument("--clusters", type=int, default=4, help="KMeans 聚类数，默认 4")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    df = read_events(input_path, sheet_name=args.sheet_name)
    cleaned = clean_events(df)
    behavior_compare_df = load_behavior_compare(
        input_path=input_path,
        behavior_compare_input=args.behavior_compare_input,
        behavior_compare_sheet=args.behavior_compare_sheet,
    )
    metric_definition_df = load_metric_definition_catalog(args.metric_definition_input)
    user_features, normalized_behavior_compare = add_rule_tags(
        build_user_features(cleaned),
        event_df=cleaned,
        behavior_compare_df=behavior_compare_df,
    )
    metric_summary_df = build_metric_summary(user_features)

    clustered_result = run_clustering(user_features, clusters=args.clusters)
    model_result = train_high_active_model(user_features)
    write_outputs(
        user_features=user_features,
        output_dir=output_dir,
        clustered_result=clustered_result,
        model_result=model_result,
        behavior_compare_df=normalized_behavior_compare,
        metric_summary_df=metric_summary_df,
        metric_definition_df=metric_definition_df,
    )

    print(f"Input rows: {len(cleaned)}")
    print(f"Users: {len(user_features)}")
    print(f"High active ratio: {user_features['high_active_label'].mean():.2%}")
    print(f"Blocked ratio: {user_features['blocked_flag'].mean():.2%}")
    print(f"Output dir: {output_dir}")
    if normalized_behavior_compare is None:
        print("Behavior compare logic skipped: no valid 行为汇总对比 input found.")
    if metric_definition_df is None:
        print("Metric definition catalog skipped: no valid metric.xlsx found.")
    if clustered_result is None:
        print("Clustering skipped: scikit-learn missing or not enough users.")
    if model_result is None:
        print("Model training skipped: scikit-learn missing, label single-class, or sample too small.")


if __name__ == "__main__":
    main()
