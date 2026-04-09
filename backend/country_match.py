"""
国家/地区匹配：以英文规范名为「主键」解析意图，在 SQL 中同时使用
LOWER(列) 的英文子串与中文子串条件，避免纯中文 LIKE 的子串冲突（如 印度⊂印度尼西亚）。
UNDERSTANDING/SUMMARY 仍用中文国名向用户说明。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CountryIntent:
    """单个国家：规范英文名、中文展示名、问题里可能出现的触发词、SQL 片段生成规则。"""

    canonical_en: str
    cn_display: str
    triggers: tuple[str, ...]
    # 中文：每项为 (必须 LIKE 的模式列表, 必须 NOT LIKE 的模式列表)，组内 AND，多项之间 OR
    zh_branches: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...]
    # 英文小写：每项为 (必须包含的子串, 必须不包含的子串)，组内 AND，多项之间 OR
    en_branches: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...]


def _col_expr(col: str) -> str:
    return f"`{col}`"


def _sql_zh_branch(col: str, positives: tuple[str, ...], negatives: tuple[str, ...]) -> str:
    c = _col_expr(col)
    parts: list[str] = []
    for p in positives:
        parts.append(f"{c} LIKE '{p}'")
    for n in negatives:
        parts.append(f"{c} NOT LIKE '{n}'")
    return "(" + " AND ".join(parts) + ")"


def _sql_en_branch(col: str, positives: tuple[str, ...], negatives: tuple[str, ...]) -> str:
    low = f"LOWER(CAST({_col_expr(col)} AS TEXT))"
    parts: list[str] = []
    for p in positives:
        parts.append(f"{low} LIKE '%{p}%'")
    for n in negatives:
        parts.append(f"{low} NOT LIKE '%{n}%'")
    return "(" + " AND ".join(parts) + ")"


def sql_country_predicate_for_column(col: str, intent: CountryIntent) -> str:
    """单列：中文若干分支 OR + 英文若干分支 OR，再总 OR。"""
    zh_parts = [_sql_zh_branch(col, pos, neg) for pos, neg in intent.zh_branches]
    en_parts = [_sql_en_branch(col, pos, neg) for pos, neg in intent.en_branches]
    all_parts = [p for p in zh_parts + en_parts if p != "()"]
    if not all_parts:
        return "1=1"
    if len(all_parts) == 1:
        return all_parts[0]
    return "(" + " OR ".join(all_parts) + ")"


def sql_country_predicate_nickname_or_tag(intent: CountryIntent) -> str:
    """常用两列：用户昵称 OR 目标用户标签。"""
    nick = sql_country_predicate_for_column("用户昵称", intent)
    tag = sql_country_predicate_for_column("目标用户标签", intent)
    return f"(({nick}) OR ({tag}))"


# 顺序重要：先匹配更长/更易冲突的国名，避免「印度」吃掉「印度尼西亚」。
COUNTRY_INTENTS: tuple[CountryIntent, ...] = (
    CountryIntent(
        canonical_en="Saudi Arabia",
        cn_display="沙特阿拉伯",
        triggers=("沙特阿拉伯", "沙特"),
        zh_branches=((("%沙特阿拉伯%",), tuple()), (("%沙特%",), tuple())),
        en_branches=((("saudi arabia",), tuple()), (("saudi",), ("israel",))),
    ),
    CountryIntent(
        canonical_en="Indonesia",
        cn_display="印度尼西亚",
        triggers=("印度尼西亚", "印尼"),
        zh_branches=(
            (("%印度尼西亚%",), tuple()),
            (("%印尼%",), ("%印度%",)),  # 避免与「印度」单列混淆时可再收紧；此处排除仅「印度」二字
        ),
        en_branches=((("indonesia",), tuple()),),
    ),
    CountryIntent(
        canonical_en="India",
        cn_display="印度",
        triggers=("印度",),
        zh_branches=((("%印度%",), ("%印度尼西亚%",)),),
        en_branches=((("india",), ("indonesia",)),),
    ),
    CountryIntent(
        canonical_en="United Arab Emirates",
        cn_display="阿联酋",
        triggers=("阿联酋",),
        zh_branches=((("%阿联酋%",), tuple()),),
        en_branches=((("united arab emirates",), tuple()), (("uae",), tuple())),
    ),
    CountryIntent(
        canonical_en="United States",
        cn_display="美国",
        triggers=("美国",),
        zh_branches=((("%美国%",), tuple()),),
        en_branches=((("united states",), tuple()), (("u.s.",), tuple()), (("usa",), tuple())),
    ),
    CountryIntent(
        canonical_en="China",
        cn_display="中国",
        triggers=("中国",),
        zh_branches=((("%中国%",), tuple()),),
        en_branches=((("china",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Japan",
        cn_display="日本",
        triggers=("日本",),
        zh_branches=((("%日本%",), tuple()),),
        en_branches=((("japan",), tuple()),),
    ),
    CountryIntent(
        canonical_en="South Korea",
        cn_display="韩国",
        triggers=("韩国",),
        zh_branches=((("%韩国%",), tuple()),),
        en_branches=((("south korea",), tuple()), (("korea",), ("north korea", "dprk"))),
    ),
    CountryIntent(
        canonical_en="North Korea",
        cn_display="朝鲜",
        triggers=("朝鲜",),
        zh_branches=((("%朝鲜%",), tuple()),),
        en_branches=((("north korea",), tuple()), (("dprk",), tuple())),
    ),
    CountryIntent(
        canonical_en="United Kingdom",
        cn_display="英国",
        triggers=("英国",),
        zh_branches=((("%英国%",), tuple()),),
        en_branches=((("united kingdom",), tuple()), (("u.k.",), tuple()), (("britain",), tuple()), (("uk",), tuple())),
    ),
    CountryIntent(
        canonical_en="France",
        cn_display="法国",
        triggers=("法国",),
        zh_branches=((("%法国%",), tuple()),),
        en_branches=((("france",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Germany",
        cn_display="德国",
        triggers=("德国",),
        zh_branches=((("%德国%",), tuple()),),
        en_branches=((("germany",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Russia",
        cn_display="俄罗斯",
        triggers=("俄罗斯",),
        zh_branches=((("%俄罗斯%",), tuple()),),
        en_branches=((("russia",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Brazil",
        cn_display="巴西",
        triggers=("巴西",),
        zh_branches=((("%巴西%",), tuple()),),
        en_branches=((("brazil",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Vietnam",
        cn_display="越南",
        triggers=("越南",),
        zh_branches=((("%越南%",), tuple()),),
        en_branches=((("vietnam",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Thailand",
        cn_display="泰国",
        triggers=("泰国",),
        zh_branches=((("%泰国%",), tuple()),),
        en_branches=((("thailand",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Malaysia",
        cn_display="马来西亚",
        triggers=("马来西亚",),
        zh_branches=((("%马来西亚%",), tuple()),),
        en_branches=((("malaysia",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Singapore",
        cn_display="新加坡",
        triggers=("新加坡",),
        zh_branches=((("%新加坡%",), tuple()),),
        en_branches=((("singapore",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Philippines",
        cn_display="菲律宾",
        triggers=("菲律宾",),
        zh_branches=((("%菲律宾%",), tuple()),),
        en_branches=((("philippines",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Australia",
        cn_display="澳大利亚",
        triggers=("澳大利亚",),
        zh_branches=((("%澳大利亚%",), tuple()),),
        en_branches=((("australia",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Canada",
        cn_display="加拿大",
        triggers=("加拿大",),
        zh_branches=((("%加拿大%",), tuple()),),
        en_branches=((("canada",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Mexico",
        cn_display="墨西哥",
        triggers=("墨西哥",),
        zh_branches=((("%墨西哥%",), tuple()),),
        en_branches=((("mexico",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Italy",
        cn_display="意大利",
        triggers=("意大利",),
        zh_branches=((("%意大利%",), tuple()),),
        en_branches=((("italy",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Spain",
        cn_display="西班牙",
        triggers=("西班牙",),
        zh_branches=((("%西班牙%",), tuple()),),
        en_branches=((("spain",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Netherlands",
        cn_display="荷兰",
        triggers=("荷兰",),
        zh_branches=((("%荷兰%",), tuple()),),
        en_branches=((("netherlands",), tuple()), (("holland",), tuple())),
    ),
    CountryIntent(
        canonical_en="Switzerland",
        cn_display="瑞士",
        triggers=("瑞士",),
        zh_branches=((("%瑞士%",), tuple()),),
        en_branches=((("switzerland",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Sweden",
        cn_display="瑞典",
        triggers=("瑞典",),
        zh_branches=((("%瑞典%",), tuple()),),
        en_branches=((("sweden",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Poland",
        cn_display="波兰",
        triggers=("波兰",),
        zh_branches=((("%波兰%",), tuple()),),
        en_branches=((("poland",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Turkey",
        cn_display="土耳其",
        triggers=("土耳其",),
        zh_branches=((("%土耳其%",), tuple()),),
        en_branches=((("turkey",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Egypt",
        cn_display="埃及",
        triggers=("埃及",),
        zh_branches=((("%埃及%",), tuple()),),
        en_branches=((("egypt",), tuple()),),
    ),
    CountryIntent(
        canonical_en="South Africa",
        cn_display="南非",
        triggers=("南非",),
        zh_branches=((("%南非%",), tuple()),),
        en_branches=((("south africa",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Argentina",
        cn_display="阿根廷",
        triggers=("阿根廷",),
        zh_branches=((("%阿根廷%",), tuple()),),
        en_branches=((("argentina",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Chile",
        cn_display="智利",
        triggers=("智利",),
        zh_branches=((("%智利%",), tuple()),),
        en_branches=((("chile",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Colombia",
        cn_display="哥伦比亚",
        triggers=("哥伦比亚",),
        zh_branches=((("%哥伦比亚%",), tuple()),),
        en_branches=((("colombia",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Peru",
        cn_display="秘鲁",
        triggers=("秘鲁",),
        zh_branches=((("%秘鲁%",), tuple()),),
        en_branches=((("peru",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Pakistan",
        cn_display="巴基斯坦",
        triggers=("巴基斯坦",),
        zh_branches=((("%巴基斯坦%",), tuple()),),
        en_branches=((("pakistan",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Bangladesh",
        cn_display="孟加拉国",
        triggers=("孟加拉国", "孟加拉"),
        zh_branches=((("%孟加拉国%",), tuple()), (("%孟加拉%",), tuple())),
        en_branches=((("bangladesh",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Myanmar",
        cn_display="缅甸",
        triggers=("缅甸",),
        zh_branches=((("%缅甸%",), tuple()),),
        en_branches=((("myanmar",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Cambodia",
        cn_display="柬埔寨",
        triggers=("柬埔寨",),
        zh_branches=((("%柬埔寨%",), tuple()),),
        en_branches=((("cambodia",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Laos",
        cn_display="老挝",
        triggers=("老挝",),
        zh_branches=((("%老挝%",), tuple()),),
        en_branches=((("laos",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Nepal",
        cn_display="尼泊尔",
        triggers=("尼泊尔",),
        zh_branches=((("%尼泊尔%",), tuple()),),
        en_branches=((("nepal",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Sri Lanka",
        cn_display="斯里兰卡",
        triggers=("斯里兰卡",),
        zh_branches=((("%斯里兰卡%",), tuple()),),
        en_branches=((("sri lanka",), tuple()),),
    ),
    CountryIntent(
        canonical_en="New Zealand",
        cn_display="新西兰",
        triggers=("新西兰",),
        zh_branches=((("%新西兰%",), tuple()),),
        en_branches=((("new zealand",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Iran",
        cn_display="伊朗",
        triggers=("伊朗",),
        zh_branches=((("%伊朗%",), tuple()),),
        en_branches=((("iran",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Israel",
        cn_display="以色列",
        triggers=("以色列",),
        zh_branches=((("%以色列%",), tuple()),),
        en_branches=((("israel",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Iraq",
        cn_display="伊拉克",
        triggers=("伊拉克",),
        zh_branches=((("%伊拉克%",), tuple()),),
        en_branches=((("iraq",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Ukraine",
        cn_display="乌克兰",
        triggers=("乌克兰",),
        zh_branches=((("%乌克兰%",), tuple()),),
        en_branches=((("ukraine",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Czech Republic",
        cn_display="捷克",
        triggers=("捷克",),
        zh_branches=((("%捷克%",), tuple()),),
        en_branches=((("czech",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Belgium",
        cn_display="比利时",
        triggers=("比利时",),
        zh_branches=((("%比利时%",), tuple()),),
        en_branches=((("belgium",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Austria",
        cn_display="奥地利",
        triggers=("奥地利",),
        zh_branches=((("%奥地利%",), tuple()),),
        en_branches=((("austria",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Portugal",
        cn_display="葡萄牙",
        triggers=("葡萄牙",),
        zh_branches=((("%葡萄牙%",), tuple()),),
        en_branches=((("portugal",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Greece",
        cn_display="希腊",
        triggers=("希腊",),
        zh_branches=((("%希腊%",), tuple()),),
        en_branches=((("greece",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Denmark",
        cn_display="丹麦",
        triggers=("丹麦",),
        zh_branches=((("%丹麦%",), tuple()),),
        en_branches=((("denmark",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Norway",
        cn_display="挪威",
        triggers=("挪威",),
        zh_branches=((("%挪威%",), tuple()),),
        en_branches=((("norway",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Finland",
        cn_display="芬兰",
        triggers=("芬兰",),
        zh_branches=((("%芬兰%",), tuple()),),
        en_branches=((("finland",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Ireland",
        cn_display="爱尔兰",
        triggers=("爱尔兰",),
        zh_branches=((("%爱尔兰%",), tuple()),),
        en_branches=((("ireland",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Romania",
        cn_display="罗马尼亚",
        triggers=("罗马尼亚",),
        zh_branches=((("%罗马尼亚%",), tuple()),),
        en_branches=((("romania",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Hungary",
        cn_display="匈牙利",
        triggers=("匈牙利",),
        zh_branches=((("%匈牙利%",), tuple()),),
        en_branches=((("hungary",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Venezuela",
        cn_display="委内瑞拉",
        triggers=("委内瑞拉",),
        zh_branches=((("%委内瑞拉%",), tuple()),),
        en_branches=((("venezuela",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Nigeria",
        cn_display="尼日利亚",
        triggers=("尼日利亚",),
        zh_branches=((("%尼日利亚%",), tuple()),),
        en_branches=((("nigeria",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Kenya",
        cn_display="肯尼亚",
        triggers=("肯尼亚",),
        zh_branches=((("%肯尼亚%",), tuple()),),
        en_branches=((("kenya",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Ethiopia",
        cn_display="埃塞俄比亚",
        triggers=("埃塞俄比亚",),
        zh_branches=((("%埃塞俄比亚%",), tuple()),),
        en_branches=((("ethiopia",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Morocco",
        cn_display="摩洛哥",
        triggers=("摩洛哥",),
        zh_branches=((("%摩洛哥%",), tuple()),),
        en_branches=((("morocco",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Belarus",
        cn_display="白俄罗斯",
        triggers=("白俄罗斯",),
        zh_branches=((("%白俄罗斯%",), tuple()),),
        en_branches=((("belarus",), tuple()),),
    ),
    CountryIntent(
        canonical_en="Kazakhstan",
        cn_display="哈萨克斯坦",
        triggers=("哈萨克斯坦",),
        zh_branches=((("%哈萨克斯坦%",), tuple()),),
        en_branches=((("kazakhstan",), tuple()),),
    ),
)


def resolve_country_intent(question: str) -> CountryIntent | None:
    """从问题中解析唯一国家意图；先按 intent 声明顺序，再按触发词长度降序。"""
    if not (question and question.strip()):
        return None
    q = question.strip()
    for intent in COUNTRY_INTENTS:
        for trig in sorted(intent.triggers, key=len, reverse=True):
            if trig and trig in q:
                return intent
    return None



def country_literal_candidates(question: str) -> list[str]:
    """按国家意图返回可用于值对齐/搜索的候选字面值（去重保序）。"""
    intent = resolve_country_intent(question)
    if not intent:
        return []
    items: list[str] = [intent.cn_display, intent.canonical_en, *intent.triggers]
    low = intent.canonical_en.strip().lower()
    if low == "indonesia":
        items.extend(["ID", "IDN", "+62"])
    elif low == "india":
        items.extend(["IN", "IND", "+91"])
    elif low == "china":
        items.extend(["CN", "CHN", "+86"])
    elif low == "japan":
        items.extend(["JP", "JPN", "+81"])
    elif low == "south korea":
        items.extend(["KR", "KOR", "+82"])
    elif low == "vietnam":
        items.extend(["VN", "VNM", "+84"])
    elif low == "thailand":
        items.extend(["TH", "THA", "+66"])
    elif low == "malaysia":
        items.extend(["MY", "MYS", "+60"])
    elif low == "singapore":
        items.extend(["SG", "SGP", "+65"])
    elif low == "philippines":
        items.extend(["PH", "PHL", "+63"])
    elif low == "united states":
        items.extend(["US", "USA", "+1"])
    elif low == "saudi arabia":
        items.extend(["SA", "SAU", "+966"])
    elif low == "united arab emirates":
        items.extend(["AE", "ARE", "+971", "UAE"])
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out

def build_country_match_knowledge_section(question: str) -> str | None:
    """供注入 Knowledge：规范英文名 + 推荐 SQL 片段（昵称 OR 标签）。"""
    intent = resolve_country_intent(question)
    if not intent:
        return None
    sql_2col = sql_country_predicate_nickname_or_tag(intent)
    lines = [
        "## Country resolution (use in SQL WHERE)",
        f"- **Canonical (EN)**: {intent.canonical_en}",
        f"- **User-facing (CN)**: {intent.cn_display}（UNDERSTANDING/SUMMARY 用中文说明即可）",
        "- **Rule**: 国家条件不要只用中文 `LIKE '%…%'`；须**同时**覆盖英文小写列值（`LOWER(CAST(`列` AS TEXT))`）与中文，按下列模板组合到 AND 其它条件。",
        "- **Count-only**（如「印尼有多少用户」）: `SELECT COUNT(*) AS cnt FROM users WHERE` + 下列模板即可；**不要**加 `所属平台`（那是 APP/PC 等渠道，不是 android/ios）；**不要**编造问题里未出现的 `用户ID IN (...)`；同一国家条件不要重复 AND 两遍。",
        "- **列选择**: 模板里的「用户昵称 / 目标用户标签」是常见示例；**须**先用 `Search_keyword_across_columns` 看国名/中文国名在哪些列 `match_count>0`，**只**在那些列上套模板。**禁止**把国名写进 `注册时间`、`最近活跃时间` 等时间列的 `LIKE`/`CAST`（易恒为 0 行）。Finish 前可用工具 **Validate_where_semantics** 检查 WHERE 是否错绑列。",
        "- **Template (用户昵称 OR 目标用户标签)**:",
        sql_2col,
    ]
    return "\n".join(lines)
