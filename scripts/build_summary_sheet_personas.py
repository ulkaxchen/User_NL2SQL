#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd


HIGH_ACTIVE_SHEET = "高活用户明细"
LOW_ACTIVE_SHEET = "低活用户明细"
DETAIL_RENAME_MAP = {
    "用户ID": "user_id",
    "时间": "dt_hour",
    "事件名称": "event_name",
    "点击事件": "click_name",
    "界面名称_PV": "page_name",
    "页面停留时间_PV": "page_stay",
    "次数": "cnt",
}
REQUIRED_COLUMNS = tuple(DETAIL_RENAME_MAP)


def ratio_for_keywords(counter: Counter[str], keywords: tuple[str, ...]) -> float:
    total = sum(counter.values()) or 1.0
    hit = sum(
        count
        for name, count in counter.items()
        if any(keyword in name for keyword in keywords)
    )
    return hit / total


def summarize_counter(counter: Counter[str], top_n: int = 3) -> str:
    if not counter:
        return ""
    return " | ".join(
        f"{name}:{int(value) if value.is_integer() else round(value, 2)}"
        for name, value in counter.most_common(top_n)
    )


def read_detail_sheet(
    workbook_path: Path,
    *,
    sheet_name: str,
    activity_label: str,
    is_high_active: int,
) -> pd.DataFrame:
    try:
        raw = pd.read_excel(workbook_path, sheet_name=sheet_name)
    except ImportError as exc:
        raise SystemExit(
            "读取 xlsx 需要 `openpyxl`。请先执行 `uv pip install openpyxl`，"
            "或者安装 `requirements.txt` 里的依赖。"
        ) from exc

    missing_columns = [column for column in REQUIRED_COLUMNS if column not in raw.columns]
    if missing_columns:
        raise ValueError(
            f"{sheet_name} 缺少必要列: {missing_columns}。"
            f" 现有列为: {list(raw.columns)}"
        )

    detail = raw.loc[:, REQUIRED_COLUMNS].rename(columns=DETAIL_RENAME_MAP).copy()
    detail = detail.replace("(not set)", pd.NA)

    for column in ("user_id", "event_name", "click_name", "page_name"):
        detail[column] = detail[column].astype("string").str.strip()

    detail["cnt"] = pd.to_numeric(detail["cnt"], errors="coerce").fillna(0.0)
    detail["page_stay"] = pd.to_numeric(detail["page_stay"], errors="coerce").fillna(0.0)
    detail["dt_hour"] = pd.to_datetime(
        detail["dt_hour"].astype("string"),
        format="%Y%m%d%H",
        errors="coerce",
    )

    detail = detail.loc[detail["user_id"].notna() & detail["user_id"].ne("")].copy()
    detail["activity_label"] = activity_label
    detail["is_high_active"] = is_high_active
    detail["source_sheet"] = sheet_name
    return detail

# 高低活跃用户表应该通过用户登陆时间来确定，使用`登陆表`来做分桶
def read_detail_rows(
    workbook_path: Path,
    *,
    high_sheet: str,
    low_sheet: str,
) -> pd.DataFrame:
    high_detail = read_detail_sheet(
        workbook_path,
        sheet_name=high_sheet,
        activity_label="高活跃",
        is_high_active=1,
    )
    low_detail = read_detail_sheet(
        workbook_path,
        sheet_name=low_sheet,
        activity_label="低活跃",
        is_high_active=0,
    )
    return pd.concat([high_detail, low_detail], ignore_index=True)


def build_user_profiles(detail_df: pd.DataFrame) -> list[dict[str, str | float | int]]:
    profiles: list[dict[str, str | float | int]] = []
    for user_id, group in detail_df.groupby("user_id", sort=False):
        pages = Counter()
        clicks = Counter()
        print(group)

        for row in group.itertuples(index=False):
            count = float(row.cnt)
            if pd.notna(row.page_name):
                pages[str(row.page_name)] += count
            if pd.notna(row.click_name):
                clicks[str(row.click_name)] += count
        # print(pages)
        # pages = Counter({'首页': 5.0, '设备列表页': 4.0, '圈子推荐-首页': 1.0, '我的': 1.0})

        page_total = sum(pages.values()) or 1.0
        print(page_total)
        total_count = float(group["cnt"].sum())
        print(total_count)
        total_stay_time = float(group["page_stay"].sum())
        circle_ratio = ratio_for_keywords(pages, ("圈子",))
        device_ratio = ratio_for_keywords(pages, ("设备",))
        my_ratio = ratio_for_keywords(pages, ("我的",))
        service_ratio = ratio_for_keywords(pages, ("召请", "工单", "服务"))
        bind_ratio = ratio_for_keywords(clicks, ("绑定",))
        top_pages = pages.most_common(3)
        top_clicks = clicks.most_common(3)
        top3_ratio = sum(value for _, value in top_pages) / page_total if pages else 0.0
        avg_stay = total_stay_time / total_count if total_count else 0.0

        activity_label = str(group["activity_label"].mode().iat[0])
        is_high_active = int(group["is_high_active"].mode().iat[0])
        source_sheet = str(group["source_sheet"].mode().iat[0])

        persona, reason = assign_persona(
            is_high_active=is_high_active,
            total_count=total_count,
            avg_stay=avg_stay,
            circle_ratio=circle_ratio,
            device_ratio=device_ratio,
            my_ratio=my_ratio,
            service_ratio=service_ratio,
            bind_ratio=bind_ratio,
            top_pages=top_pages,
        )

        profiles.append(
            {
                "user_id": str(user_id),
                "activity_label": activity_label,
                "is_high_active": is_high_active,
                "source_sheet": source_sheet,
                "persona_label": persona,
                "persona_reason": reason,
                "total_count": round(total_count, 1),
                "total_stay_time": round(total_stay_time, 1),
                "avg_stay_per_count": round(avg_stay, 2),
                "page_concentration_top3": round(top3_ratio, 2),
                "circle_ratio": round(circle_ratio, 2),
                "device_ratio": round(device_ratio, 2),
                "my_ratio": round(my_ratio, 2),
                "service_ratio": round(service_ratio, 2),
                "bind_ratio": round(bind_ratio, 2),
                "top_pages": summarize_counter(pages),
                "top_clicks": summarize_counter(clicks),
            }
        )

    return sorted(
        profiles,
        key=lambda item: (
            -int(item["is_high_active"]),
            str(item["persona_label"]),
            -float(item["total_count"]),
            str(item["user_id"]),
        ),
    )


def assign_persona(
    *,
    is_high_active: int,
    total_count: float,
    avg_stay: float,
    circle_ratio: float,
    device_ratio: float,
    my_ratio: float,
    service_ratio: float,
    bind_ratio: float,
    top_pages: list[tuple[str, float]],
) -> tuple[str, str]:
    top_page_names = {name for name, _ in top_pages}
    detail_heavy = any("详情" in name for name in top_page_names)

    if (
        is_high_active == 0
        and total_count >= 80
        and bind_ratio < 0.10
        and service_ratio < 0.10
        and (avg_stay >= 2.5 or detail_heavy or my_ratio >= 0.15)
    ):
        return "探索受阻型", "行为很多但核心转化弱，更多停留在详情/我的/设备探索链路。"
    if service_ratio >= 0.15:
        return "服务推进型", "服务召请、工单、服务相关页面占比较高，目标明确。"
    if device_ratio >= 0.45 and bind_ratio >= 0.15:
        return "设备管理型", "设备列表、设备详情和绑定动作集中，偏设备运营任务。"
    if circle_ratio >= 0.10:
        return "社区浏览型", "圈子相关页面占比明显，偏内容/社区浏览。"
    return "轻度尝鲜型", "行为量较轻或入口分散，更像在试用产品和浅层浏览。"


def write_csv(profiles: list[dict[str, str | float | int]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "user_id",
                "activity_label",
                "is_high_active",
                "source_sheet",
                "persona_label",
                "persona_reason",
                "total_count",
                "total_stay_time",
                "avg_stay_per_count",
                "page_concentration_top3",
                "circle_ratio",
                "device_ratio",
                "my_ratio",
                "service_ratio",
                "bind_ratio",
                "top_pages",
                "top_clicks",
            ],
        )
        writer.writeheader()
        writer.writerows(profiles)


def render_report(
    profiles: list[dict[str, str | float | int]],
    workbook_path: Path,
    *,
    high_sheet: str,
    low_sheet: str,
) -> str:
    persona_counts = Counter(str(item["persona_label"]) for item in profiles)
    activity_counts = Counter(str(item["activity_label"]) for item in profiles)
    persona_to_users: dict[str, list[dict[str, str | float | int]]] = defaultdict(list)
    for profile in profiles:
        persona_to_users[str(profile["persona_label"])].append(profile)

    lines = [
        "# 高活/低活用户明细画像结果",
        "",
        f"- 数据来源: `{workbook_path}` 的 `{high_sheet}` + `{low_sheet}` 工作表",
        f"- 用户数: `{len(profiles)}`",
        f"- 来源分层: `{dict(activity_counts)}`",
        f"- 画像分布: `{dict(persona_counts)}`",
        "- `is_high_active` 不是原始字段，而是脚本按工作表来源自行定义：高活表=1，低活表=0。",
        "",
        "## 画像规则",
        "",
        "- `探索受阻型`: 低活跃样本里，行为次数高但绑定/服务转化弱，且明显停留在详情、设备探索或我的页。",
        "- `服务推进型`: 服务召请、工单、服务相关页面占比较高。",
        "- `设备管理型`: 设备相关页面和绑定行为都较集中。",
        "- `社区浏览型`: 圈子相关页面占比较高。",
        "- `轻度尝鲜型`: 行为量较轻，或入口分散，更多是浅层试用。",
        "",
        "## 画像样本",
        "",
    ]

    for persona in sorted(persona_to_users):
        users = sorted(
            persona_to_users[persona],
            key=lambda item: (-float(item["total_count"]), str(item["user_id"])),
        )
        lines.append(f"### {persona}")
        lines.append("")
        lines.append(f"- 人数: `{len(users)}`")
        for sample in users[:4]:
            lines.append(
                "- 用户 `{user_id}` / `{activity_label}` / `is_high_active={is_high_active}`: "
                "top_pages=`{top_pages}`; top_clicks=`{top_clicks}`; reason={persona_reason}".format(
                    **sample
                )
            )
        lines.append("")

    lines.extend(
        [
            "## 使用建议",
            "",
            "- 这是一版基于高活/低活样本明细重建出来的可解释画像，不再依赖额外的 `汇总用户明细` 工作表。",
            "- 当前 `activity_label` / `is_high_active` 更适合作为样本标签或验证标签，不代表真实线上全量用户自动分类结果。",
            "- 如果后续补到注册时间、设备绑定结果、召请提交结果，可以把这版行为画像继续升级为生命周期画像。",
            "",
        ]
    )
    return "\n".join(lines)


def write_report(report: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="从高活/低活用户明细工作表生成用户画像结果。"
    )
    parser.add_argument(
        "--input",
        default="user_analysis/users_analysis_oct.xlsx",
        help="Excel 文件路径，默认 user_analysis/users_analysis_oct.xlsx",
    )
    parser.add_argument(
        "--high-sheet-name",
        default=HIGH_ACTIVE_SHEET,
        help=f"高活用户明细工作表名，默认 {HIGH_ACTIVE_SHEET}",
    )
    parser.add_argument(
        "--low-sheet-name",
        default=LOW_ACTIVE_SHEET,
        help=f"低活用户明细工作表名，默认 {LOW_ACTIVE_SHEET}",
    )
    parser.add_argument(
        "--csv-output",
        default="user_analysis/summary_sheet_personas.csv",
        help="画像 CSV 输出路径",
    )
    parser.add_argument(
        "--report-output",
        default="user_analysis/summary_sheet_persona_report.md",
        help="画像 Markdown 报告输出路径",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    workbook_path = Path(args.input)
    detail_df = read_detail_rows(
        workbook_path,
        high_sheet=args.high_sheet_name,
        low_sheet=args.low_sheet_name,
    )
    profiles = build_user_profiles(detail_df)
    write_csv(profiles, Path(args.csv_output))
    write_report(
        render_report(
            profiles,
            workbook_path,
            high_sheet=args.high_sheet_name,
            low_sheet=args.low_sheet_name,
        ),
        Path(args.report_output),
    )
    print(
        f"Built {len(profiles)} user personas from "
        f"{workbook_path} ({args.high_sheet_name} + {args.low_sheet_name})"
    )
    print(f"CSV: {args.csv_output}")
    print(f"Report: {args.report_output}")


if __name__ == "__main__":
    main()
