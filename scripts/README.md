# 用户画像脚本说明

这个目录里目前有两类脚本，目标都是为了从现有埋点/行为表里快速产出一版可解释的用户画像。

- `build_summary_sheet_personas.py`
- `build_user_event_personas.py`

它们的区别是：

- `build_summary_sheet_personas.py` 适合只有 `高活用户明细` 和 `低活用户明细` 两张样本表，想先重建一版可解释的画像汇总。
- `build_user_event_personas.py` 适合拿到更细粒度的事件明细表，想先做用户特征表，再继续做规则标签、分群、漏斗和高活预测。

## 1. build_summary_sheet_personas.py

### 这个脚本在干嘛

这个脚本会直接读取 Excel 里的 `高活用户明细` 和 `低活用户明细` 两个工作表，
再把原本一行一条行为明细的数据，聚合成“一行一个用户”的画像结果。

它会做这些事：

1. 用 `pandas.read_excel` 读取 `高活用户明细` 和 `低活用户明细`
2. 按 `user_id` 聚合页面和点击行为
3. 根据工作表来源自己生成 `activity_label` 和 `is_high_active`
4. 计算几个简单但可解释的指标
5. 用规则给用户打标签
6. 输出 CSV 结果表和 Markdown 报告

### 它适合什么场景

- 你手里只有 `高活用户明细` / `低活用户明细` 两张样本明细
- 你想先快速得到一版业务可读的画像
- 你更关心“可解释”而不是“模型精度”

### 脚本里用到的主要指标

- `activity_label`：样本来源标签，高活表记为 `高活跃`，低活表记为 `低活跃`
- `is_high_active`：脚本自行定义的二值字段，高活表=1，低活表=0
- `total_count`：总行为次数
- `total_stay_time`：总停留时长
- `circle_ratio`：圈子相关页面占比
- `device_ratio`：设备相关页面占比
- `my_ratio`：我的页相关占比
- `service_ratio`：服务/召请/工单相关页面占比
- `bind_ratio`：绑定相关点击占比
- `page_concentration_top3`：TOP3 页面行为占比

### 输出的画像类型

当前规则里会打成这几类：

- `探索受阻型`
- `服务推进型`
- `设备管理型`
- `社区浏览型`
- `轻度尝鲜型`

### 运行方式

```bash
python scripts/build_summary_sheet_personas.py
```

如果要指定输入或输出路径：

```bash
python scripts/build_summary_sheet_personas.py \
  --input user_analysis/users_analysis_oct.xlsx \
  --high-sheet-name 高活用户明细 \
  --low-sheet-name 低活用户明细 \
  --csv-output user_analysis/summary_sheet_personas.csv \
  --report-output user_analysis/summary_sheet_persona_report.md
```

### 输出文件

- `user_analysis/summary_sheet_personas.csv`
- `user_analysis/summary_sheet_persona_report.md`

### 适用限制

- 这是规则法，不是聚类或监督学习模型
- `is_high_active` 是按工作表来源定义出来的样本标签，不是模型自动识别结果
- 依赖页面名、点击名里是否包含业务关键词
- 更适合做“行为画像原型”，不适合直接当完整用户画像结论
- 运行 `xlsx` 读取时依赖 `openpyxl`

## 2. build_user_event_personas.py

### 这个脚本在干嘛

这个脚本是完整一点的用户画像流水线。

它的思路是：

1. 先把原始事件表清洗干净
2. 聚合成 `user_features`
3. 打规则标签
4. 生成组合画像
5. 做漏斗分析和受阻分析
6. 如果环境里有 `scikit-learn`，再继续做聚类和高活预测
7. 如果提供了 `行为汇总对比` 表，再把行为差异转成高活/低活信号
8. 如果存在 `metric.xlsx`，再按指标说明输出标准指标汇总

### 它适合什么场景

- 你拿到的是事件级或接近事件级的数据
- 你想把画像分析做成一个可重复执行的流程
- 你后面还想继续做分群、预测、特征重要性分析

### 支持的输入

- `csv`
- `xlsx`

如果输入是 `xlsx`，可以通过 `--sheet-name` 指定工作表。

另外，这个脚本还支持一张可选的“行为汇总对比”表：

- `--behavior-compare-input`
- `--behavior-compare-sheet`

如果你不传这两个参数，但输入本身是一个包含 `行为汇总对比` 工作表的 Excel，脚本会默认尝试自动读取这张表。

这个脚本还支持一张可选的“指标说明表”：

- `--metric-definition-input`

如果不传，脚本会默认尝试读取：

- `user_analysis/metric.xlsx`

并优先使用里面的 `分析指标定义` 工作表作为指标口径说明来源。

### 期望字段

脚本会优先识别这些中文列，并自动映射成英文字段：

- `用户ID` -> `user_id`
- `时间` -> `dt_hour`
- `事件名称` -> `event_name`
- `点击事件` -> `click_name`
- `界面名称_PV` -> `page_name`
- `页面停留时间_PV` -> `page_stay`
- `次数` -> `cnt`
- `用户类型` -> `user_type`

同时也支持直接用埋点参数表里的标准 `key` 字段，例如：

- `sy_btn_click_name` -> `click_name`
- `sy_page_name_pv` -> `page_name`
- `sy_standing_time` -> `page_stay`
- `sy_bind_device_count` -> `bound_device_total`
- `sy_api_loaded_time` -> `api_loaded_time`
- `sy_api_url` -> `api_url`
- `sy_api_params` -> `api_params`
- `sy_service_api` -> `service_api`
- `sy_parent_page_name` -> `parent_page_name`
- `user_load_time` -> `user_load_time`
- `user_network_latency` -> `user_network_latency`
- `user_network_speed` -> `user_network_speed`
- `user_network_type` -> `user_network_type`
- `user_platform` -> `user_platform`
- `sy_error` -> `api_error_message`

也支持图片里右侧那种中文参数名别名，例如：

- `启动时间`
- `用户国家`
- `用户网络类型`
- `页面名称`
- `页面停留时间`
- `按钮点击事件名称`
- `接口加载时间`
- `绑定设备数量`
- `接口报错内容`

其中真正必需字段现在只有：

- `user_id`
- `event_name`
- `cnt`

下面这些字段是可选的，缺失时脚本会自动补空列或默认值：

- `dt_hour`
- `click_name`
- `page_name`
- `page_stay`
- `bound_device_total`
- `api_loaded_time`
- `user_load_time`
- `user_network_latency`
- `user_type`

### 它会做哪些事情

#### 1. 数据清洗

- 把 `(not set)` 转成缺失值
- 把 `2025100109` 这种时间解析成 datetime
- 把 `page_stay`、`cnt` 转成数值
- 把 `bound_device_total`、`api_loaded_time`、`user_load_time`、`user_network_latency` 等参数转成数值
- 按 `event_name` 映射出统一的 `event_type`

当前事件映射口径是：

- `sy_home_loaded` -> `app_launch`
- `sy_page_loaded` -> `page_loaded`
- `sy_btn_click` -> `click`
- `sy_page_pv` 或 `screen_view` -> `page_view`
- `sy_bind_device` -> `bind_device_snapshot`
- `sy_api_loaded` -> `api_loaded`
- `sy_api_error` -> `api_error`
- `user_engagement` -> `system_or_view`
- 其他未命中事件 -> `other`

这里要注意两点：

- 页面浏览和停留相关指标，脚本只会优先按 `page_view` 口径去聚合。
- `sy_bind_device` 在当前脚本里被当成“绑定设备状态快照/绑定信息事件”，不是简单的“绑定成功一次”。

#### 2. 构建用户特征表

输出一张一行一个用户的 `user_features.csv`，里面会有：

- 基础活跃特征
  - `active_days`
  - `total_event_cnt`
  - `total_page_stay`
  - `unique_pages`
  - `unique_clicks`
  - `first_seen_at`
  - `last_seen_at`
  - `days_observed`
  - `avg_stay_per_event`
- 绑定相关特征
  - `bind_device_snapshot_event_cnt`
  - `bind_device_cnt`
  - `bind_btn_click_cnt`
  - `fast_bind_click_cnt`
  - `virtual_to_real_bind_cnt`
- 服务相关特征
  - `summon_entry_click_cnt`
  - `service_entry_click_cnt`
  - `summon_select_device_cnt`
  - `summon_submit_cnt`
- 页面浏览特征
  - `home_pv`
  - `device_list_pv`
  - `device_detail_pv`
  - `circle_pv`
  - `service_page_pv`
  - `colleague_circle_pv`
  - `spare_part_list_pv`
  - `spare_part_detail_pv`
  - `maintenance_list_pv`
  - `product_list_pv`
- 停留时长特征
  - `home_stay`
  - `device_list_stay`
  - `device_detail_stay`
  - `circle_stay`
  - `service_page_stay`
  - `colleague_circle_stay`
  - `spare_part_list_stay`
  - `maintenance_list_stay`
  - `product_list_stay`
- 指标口径相关特征
  - `service_detail_engaged_user_flag`
  - `service_detail_submit_cvr_proxy`
  - `circle_avg_stay_per_view`
  - `spare_part_list_avg_stay_per_view`
  - `spare_part_quote_cvr_proxy`
  - `maintenance_entry_to_list_cvr_proxy`
- 风险特征
  - `api_error_cnt`
  - `switch_device_cnt`
  - `api_loaded_cnt`
  - `avg_api_loaded_time`
  - `max_api_loaded_time`
  - `app_launch_cnt`
  - `avg_app_load_time`
  - `avg_network_latency`

更细一点说，脚本当前的特征构造口径是：

- `total_event_cnt`
  - 所有事件的 `cnt` 求和
- `total_page_stay`
  - 只统计 `page_view` 事件里的 `page_stay`
- `unique_pages`
  - 只统计 `page_view` 事件里的去重页面数
- `unique_clicks`
  - 只统计 `click` 事件里的去重点击名
- `bind_device_snapshot_event_cnt`
  - `sy_bind_device` 事件行的 `cnt` 总和
- `bind_device_cnt`
  - 优先取 `sy_bind_device_count` / `绑定设备数量` 的最大值
  - 如果源数据没有这个字段，则退回到 `sy_bind_device` 事件次数求和
- `service_entry_click_cnt`
  - 不是只看首页一个入口
  - 会按多入口口径聚合：
    - `我要召请按钮0331`
    - `服务召请按钮`
- `home_pv` / `device_list_pv` / `device_detail_pv` / `circle_pv` / `service_page_pv`
  - 都只从 `page_view` 事件里按页面名统计
- `colleague_circle_pv`
  - 用页面名包含 `同事圈` 的 `page_view` 事件统计
- `spare_part_entry_click_cnt`
  - 用点击名包含 `我要配件按钮` 的点击事件统计
- `spare_part_list_pv` / `spare_part_detail_pv`
  - 用页面名包含 `配件列表页` / `配件详情页` 的 `page_view` 事件统计
- `quote_submit_cnt`
  - 用点击名包含 `询价` / `加入询价单` 的点击事件统计
- `maintenance_entry_click_cnt`
  - 用点击名包含 `设备保养` / `预约保养` 的点击事件统计
- `maintenance_list_pv`
  - 用页面名包含 `设备保养列表页` / `保养记录` 的 `page_view` 事件统计
- `product_center_entry_click_cnt`
  - 用点击名包含 `产品中心按钮` 的点击事件统计
- `product_list_pv`
  - 用页面名包含 `产品列表页` 的 `page_view` 事件统计
- `data_report_click_cnt`
  - 用点击名包含 `数据报表` 的点击事件统计
- `data_analysis_click_cnt`
  - 用点击名包含 `数据分析` 的点击事件统计
- `home_stay` / `device_list_stay` / `device_detail_stay` / `circle_stay` / `service_page_stay`
  - 都只从 `page_view` 事件里按页面名累计停留时间
- `api_loaded_cnt`
  - `sy_api_loaded` 事件次数
- `avg_api_loaded_time` / `max_api_loaded_time`
  - `sy_api_loaded_time` 的均值和最大值
- `app_launch_cnt`
  - `sy_home_loaded` 事件次数
- `avg_app_load_time`
  - `user_load_time` 的均值
- `avg_network_latency`
  - `user_network_latency` 的均值
- `service_detail_engaged_user_flag`
  - 只要服务详情页访问、选择设备或提交任一发生，就记为服务详情参与用户
- `service_detail_submit_cvr_proxy`
  - `summon_submit_cnt / max(service_page_pv, summon_select_device_cnt)`
  - 这是用户级近似代理值，用来避免详情页 PV 埋点缺失时分母失真
- `circle_avg_stay_per_view`
  - `circle_stay / circle_pv`
- `spare_part_list_avg_stay_per_view`
  - `spare_part_list_stay / spare_part_list_pv`
- `spare_part_quote_cvr_proxy`
  - `quote_submit_cnt / spare_part_detail_pv`
- `maintenance_entry_to_list_cvr_proxy`
  - `maintenance_list_pv / maintenance_entry_click_cnt`

#### 3. 规则标签

脚本会先打四类业务标签：

- `bind_tag`
  - `已绑定`
  - `尝试绑定未完成`
  - `未触发绑定`
- `circle_tag`
  - `高社区参与`
  - `低社区参与`
  - `无社区参与`
- `service_tag`
  - `服务提交用户`
  - `服务意向用户`
  - `无明显服务意向`
- `risk_tag`
  - `高受阻风险`
  - `疑似流程受阻`
  - `正常`

这些标签的具体逻辑是：

- `bind_tag`
  - `已绑定`
    - `bind_device_cnt > 0`
  - `尝试绑定未完成`
    - `bind_device_cnt == 0`
    - 且 `bind_btn_click_cnt > 0` 或 `fast_bind_click_cnt > 0` 或 `virtual_to_real_bind_cnt > 0`
  - `未触发绑定`
    - 上面都不满足

- `circle_tag`
  - `高社区参与`
    - `circle_stay >= 300`
  - `低社区参与`
    - `0 < circle_stay < 300`
  - `无社区参与`
    - `circle_stay == 0`

- `service_tag`
  - `服务提交用户`
    - `summon_submit_cnt > 0`
  - `服务意向用户`
    - `summon_submit_cnt == 0`
    - 且 `summon_select_device_cnt > 0` 或 `service_entry_click_cnt > 0`
  - `无明显服务意向`
    - 上面都不满足

- `risk_tag`
  - `高受阻风险`
    - `api_error_cnt > 0`
    - 或 `switch_device_cnt >= 5`
    - 或 `max_api_loaded_time >= 5`
  - `疑似流程受阻`
    - `device_detail_stay >= 300`
    - 且 `summon_submit_cnt == 0`
  - `正常`
    - 上面都不满足

然后会再生成：

- `high_active_label`
- `blocked_flag`
- `persona`
- `aha_content_flag`
- `aha_bind_success_flag`
- `aha_service_start_flag`
- `aha_fast_entry_flag`
- `aha_types`
- `primary_aha_type`
- `aha_moment_count`
- `aha_detail`

`persona` 是把上面几个标签拼起来的组合画像，例如：

- `高活|已绑定|服务提交用户|高社区参与|正常`
- `低活|尝试绑定未完成|服务意向用户|无社区参与|疑似流程受阻`

其中这几个衍生字段的逻辑是：

- `high_active_label`
  - 当前是一版启发式规则标签，不是模型真值
  - 满足以下条件时记为 `1`，否则为 `0`
  - `active_days >= 2`
  - 且 `total_event_cnt >= 20`
  - 且满足下面任一核心价值信号
    - `bind_device_cnt > 0`
    - 或 `circle_stay >= 300`
    - 或 `summon_submit_cnt > 0`
    - 或 `behavior_signal_tag == 高活倾向行为` 且 `high_active_behavior_hit_cnt >= 2`

- `blocked_flag`
  - 满足以下任一条件记为 `1`
    - `device_detail_stay >= 300` 且 `summon_submit_cnt == 0`
    - `api_error_cnt > 0`
    - `max_api_loaded_time >= 5`
    - `switch_device_cnt >= 5`
    - `behavior_signal_tag == 低活倾向行为` 且 `low_active_behavior_hit_cnt >= 2`
  - 否则记为 `0`

- `persona`
  - 由 5 个标签直接拼接而成
  - 结构固定为：
    - `高活/低活|bind_tag|service_tag|circle_tag|risk_tag`

#### 3.1 Aha 时刻识别

脚本把 Aha 时刻也做成了规则字段，口径对应你给的那张表：

- `内容获取感`
  - 相关行为/页面：`圈子推荐-首页`
  - 业务理解：用户在社区内容里有明显停留，可能通过内容建立连接
- `设备绑定成功`
  - 相关行为/页面：`虚拟挖机弹窗引导-绑定真实设备`、绑定按钮、极速绑定
  - 业务理解：用户不仅进入了绑定引导，而且已经形成绑定结果
- `服务流程启动`
  - 相关行为/页面：`我要召请-选择设备按钮`、`我要召请-提交按钮`
  - 业务理解：用户已经启动甚至推进了服务流程
- `快速入口使用`
  - 相关行为/页面：`新增弹窗-极速绑定按钮首页`、`底部设备按钮`
  - 业务理解：用户开始依赖快速入口，操作路径更熟练

具体字段与规则如下：

- `aha_content_flag`
  - 满足任一条件记为 `1`
  - `circle_stay >= 300`
  - 或 `circle_pv >= 3` 且 `circle_stay > 0`

- `aha_bind_success_flag`
  - 满足全部条件记为 `1`
  - `bind_device_cnt > 0`
  - 且满足以下任一项
    - `virtual_to_real_bind_cnt > 0`
    - `bind_btn_click_cnt > 0`
    - `fast_bind_click_cnt > 0`

- `aha_service_start_flag`
  - 满足任一条件记为 `1`
  - `summon_select_device_cnt > 0`
  - 或 `summon_submit_cnt > 0`

- `aha_fast_entry_flag`
  - 满足任一条件记为 `1`
  - `fast_bind_click_cnt > 0`
  - 或 `bottom_device_btn_cnt >= 3`

- `aha_moment_count`
  - 上述 4 个 Aha flag 的求和

- `aha_types`
  - 把命中的 Aha 类型按规则拼接
  - 可能的值例如：
    - `服务流程启动|设备绑定成功|快速入口使用`
    - `内容获取感`
    - `未识别`

- `primary_aha_type`
  - 当一个用户同时命中多个 Aha 时刻时，按固定优先级选一个主类型
  - 当前优先级为：
    - `服务流程启动`
    - `设备绑定成功`
    - `内容获取感`
    - `快速入口使用`

- `aha_detail`
  - 输出主 Aha 对应的关键指标，方便排查
  - 例如：
    - 内容获取感：`circle_pv`、`circle_stay`
    - 设备绑定成功：`bind_device_cnt`、`virtual_to_real_bind_cnt`
    - 服务流程启动：`summon_select_device_cnt`、`summon_submit_cnt`
    - 快速入口使用：`fast_bind_click_cnt`、`bottom_device_btn_cnt`

#### 4. 分群

如果环境里安装了 `scikit-learn`，脚本会自动做 KMeans 聚类，输出：

- `user_features_with_cluster.csv`
- `cluster_summary.csv`

如果没有装 `scikit-learn`，脚本会直接跳过，不会报错退出。

当前聚类用到的字段是：

- `active_days`
- `total_event_cnt`
- `bind_device_cnt`
- `service_entry_click_cnt`
- `summon_select_device_cnt`
- `summon_submit_cnt`
- `circle_stay`
- `device_detail_stay`
- `api_error_cnt`
- `switch_device_cnt`

处理方式是：

- 先用 `StandardScaler` 标准化
- 再用 `KMeans(n_clusters=4, random_state=42, n_init=10)` 分群
- 最后输出每个 cluster 的均值汇总，方便人工命名

#### 5. 高活预测

如果环境里安装了 `scikit-learn`，并且样本足够、标签有正负两类，脚本会训练一个简单的 `RandomForestClassifier`，输出：

- `feature_importance.csv`
- `model_metrics.json`

当前模型使用的特征包括：

- `active_days`
- `total_event_cnt`
- `bind_device_cnt`
- `bind_btn_click_cnt`
- `fast_bind_click_cnt`
- `virtual_to_real_bind_cnt`
- `service_entry_click_cnt`
- `summon_select_device_cnt`
- `summon_submit_cnt`
- `circle_pv`
- `circle_stay`
- `device_detail_pv`
- `device_detail_stay`
- `api_error_cnt`
- `switch_device_cnt`

目标变量是：

- `high_active_label`

训练逻辑是：

- `train_test_split(test_size=0.3, random_state=42, stratify=y)`
- `RandomForestClassifier(n_estimators=200, max_depth=6, random_state=42)`
- 输出 `AUC`、`accuracy`、正负样本数和特征重要性

#### 6. 漏斗分析

脚本会输出一条基础漏斗：

- 首页
- 设备列表
- 绑定
- 召请入口
- 选择设备
- 提交

对应输出：

- `funnel_rate.csv`
- `group_funnel.csv`

当前漏斗口径是：

- `step_home`
  - `home_pv > 0`
- `step_device_list`
  - `device_list_pv > 0`
- `step_bind`
  - `bind_device_cnt > 0`
- `step_summon_entry`
  - `service_entry_click_cnt > 0`
- `step_select_device`
  - `summon_select_device_cnt > 0`
- `step_submit`
  - `summon_submit_cnt > 0`

其中：

- `funnel_rate.csv` 是全量用户每一步的覆盖率
- `group_funnel.csv` 是按 `high_active_label` 分组后的漏斗覆盖率

#### 7. 受阻分析

脚本会按照 `blocked_flag` 输出一张受阻分组汇总：

- `blocked_summary.csv`

当前分组汇总会比较这些字段：

- `active_days`
- `total_event_cnt`
- `bind_device_cnt`
- `service_entry_click_cnt`
- `summon_submit_cnt`
- `circle_stay`
- `device_detail_stay`
- `api_error_cnt`
- `switch_device_cnt`

#### 7.1 指标说明表逻辑

如果脚本能读到 `metric.xlsx` 的 `分析指标定义` 工作表，它会额外生成一层“标准指标汇总”。

当前已经按指标说明落地的核心指标包括：

- `服务召请（入口）`
  - 入口点击用户数
  - 入口点击次数
- `服务召请（详情页）`
  - 召请 CVR = 提交用户数 / 详情页访问用户数
- `服务召请路径分析`
  - 入口到提交转化率
- `圈子（入口）`
  - 入口点击用户数
  - 如果没有圈子 tab 点击埋点，会回退到圈子页面访问用户数
- `圈子推荐/关注/话题页`
  - 平均浏览时长 = 浏览总时长 / 浏览人数
- `同事圈聚合页`
  - 平均浏览时长
- `我要配件（入口）`
  - 入口点击用户数
  - 入口点击次数
- `配件列表页`
  - 平均浏览时长
- `配件详情页`
  - 询价转化率 = 询价提交人数 / 配件详情页访问 UV
- `设备保养入口`
  - 入口点击用户数
  - 入口点击次数
- `设备保养列表页`
  - 功能转化率 = 列表页访问用户数 / 入口点击用户数
  - 跳出率目前仍是未实现状态，因为现有埋点不足以可靠识别“进入后直接离开”
- `产品中心 产品列表页`
  - 平均浏览时长
- `设备tab`
  - 入口点击用户数
  - 入口点击次数
- `工况详情页 / 数据报表 / 数据分析`
  - 功能访问率 = 功能访问用户数 / 总活跃用户数

这部分结果会输出成：

- `metric_summary.csv`
- `metric_definition_catalog.csv`

#### 3.2 行为汇总对比逻辑

如果你提供了 `行为汇总对比` 这张表，脚本会把它当成一张“行为信号字典”。

这张表预期字段包括：

- `事件类型`
- `具体行为名称`
- `所属页面`
- `所属子模块`
- `低活完成用户数`
- `高活完成用户数`
- `低活完成次数`
- `高活完成次数`
- `低活停留时间`
- `高活停留时间`
- `高活用户平均使用频次`
- `低活用户平均使用频次`

脚本会先把它标准化成：

- `behavior_event_type`
- `behavior_name`
- `behavior_page`
- `behavior_module`
- `low_active_user_count`
- `high_active_user_count`
- `low_active_total_count`
- `high_active_total_count`
- `low_active_stay_time`
- `high_active_stay_time`
- `high_active_avg_freq`
- `low_active_avg_freq`

然后做下面几步：

##### 第一步：计算行为差异

对每一行行为，计算：

- `freq_diff = high_active_avg_freq - low_active_avg_freq`

这代表：

- `freq_diff > 0`
  - 高活用户更常用这个行为
- `freq_diff < 0`
  - 低活用户更常用这个行为

##### 第二步：给行为定方向

当前阈值是：

- `freq_diff >= 0.5`
  - 记为 `high_active`
- `freq_diff <= -0.5`
  - 记为 `low_active`
- `-0.5 < freq_diff < 0.5`
  - 记为 `neutral`

同时会生成：

- `signal_strength = max(abs(freq_diff), 0.5)`

也就是说：

- 差异越大，这个行为的信号强度越高
- 即使差异刚好踩线，也至少保留 `0.5` 的强度

##### 第三步：决定怎么匹配用户行为

脚本会根据 `事件类型` 决定这行行为应该去匹配用户表里的哪一列：

- 如果 `事件类型` 包含 `浏览 / 页面 / 曝光`
  - 用 `page_name` 匹配
- 否则
  - 默认用 `click_name` 匹配

也就是说：

- `底部设备按钮`
  - 会拿去匹配用户点击事件
- 如果后面有 `设备详情页` 这类浏览行为
  - 会拿去匹配用户页面浏览

##### 第四步：把行为差异打到用户身上

对每个用户，如果命中了某个行为，脚本会累计行为信号分。

当前逻辑是：

- 先统计这个用户该行为的 `cnt`
- 为了避免极端高频行为把分数拉爆，单个行为的计数会做上限截断：
  - `capped_count = min(cnt, 3)`
- 然后：
  - `weighted_score = capped_count * signal_strength`

如果这个行为是 `high_active`：

- 加到 `high_active_behavior_score`
- 行为名记到 `high_active_signal_behaviors`

如果这个行为是 `low_active`：

- 加到 `low_active_behavior_score`
- 行为名记到 `low_active_signal_behaviors`

同时脚本还会统计：

- `high_active_behavior_hit_cnt`
- `low_active_behavior_hit_cnt`
- `behavior_net_signal_score = high_active_behavior_score - low_active_behavior_score`

##### 第五步：生成用户行为信号标签

当前规则是：

- 如果 `high_active_behavior_score - low_active_behavior_score >= 1.5`
  - 且命中了至少 1 个高活行为
  - 记为 `高活倾向行为`
- 如果 `low_active_behavior_score - high_active_behavior_score >= 1.5`
  - 且命中了至少 1 个低活行为
  - 记为 `低活倾向行为`
- 否则
  - 记为 `中性行为`

如果没有提供行为对比表，则记为：

- `未提供行为对比表`

##### 第六步：并入主标签逻辑

这层行为信号会进一步影响两个核心标签：

- `high_active_label`
  - 除了原本的活跃天数、总事件数、绑定/社区/召请条件外
  - 还会额外接受这一条：
    - `behavior_signal_tag == 高活倾向行为`
    - 且 `high_active_behavior_hit_cnt >= 2`

- `blocked_flag`
  - 除了原本的设备详情停留、接口报错、接口慢、设备切换条件外
  - 还会额外接受这一条：
    - `behavior_signal_tag == 低活倾向行为`
    - 且 `low_active_behavior_hit_cnt >= 2`

这意味着：

- 行为对比表不只是拿来展示
- 它会实际参与高活和受阻判断

#### 8. Aha 输出文件

脚本还会额外输出两张 Aha 结果表：

- `aha_summary.csv`
  - 统计每种 Aha 类型命中的用户数和占比
- `aha_by_active_label.csv`
  - 按 `high_active_label` 对比各类 Aha 的平均命中率和平均 Aha 数量

### 运行方式

最常见的运行方式：

```bash
python scripts/build_user_event_personas.py \
  --input data/user_events.csv \
  --output-dir output/persona_pipeline
```

如果要把“行为汇总对比”也接进来：

```bash
python scripts/build_user_event_personas.py \
  --input data/user_events.csv \
  --behavior-compare-input data/behavior_compare.xlsx \
  --behavior-compare-sheet 行为汇总对比 \
  --output-dir output/persona_pipeline
```

如果要显式指定指标说明表：

```bash
python scripts/build_user_event_personas.py \
  --input data/user_events.csv \
  --metric-definition-input user_analysis/metric.xlsx \
  --output-dir output/persona_pipeline
```

如果输入是 Excel：

```bash
python scripts/build_user_event_personas.py \
  --input user_analysis/users_analysis_oct.xlsx \
  --sheet-name 高活用户明细 \
  --output-dir output/persona_pipeline_high
```

### 输出文件

脚本默认会往 `output/persona_pipeline/` 写这些文件：

- `user_features.csv`
- `user_persona.csv`
- `blocked_summary.csv`
- `aha_summary.csv`
- `aha_by_active_label.csv`
- `behavior_signal_summary.csv`
- `metric_summary.csv`
- `funnel_rate.csv`
- `group_funnel.csv`

如果启用了行为对比逻辑：

- `behavior_compare_catalog.csv`

如果启用了指标说明表逻辑：

- `metric_definition_catalog.csv`

如果启用了聚类：

- `user_features_with_cluster.csv`
- `cluster_summary.csv`

如果启用了模型：

- `feature_importance.csv`
- `model_metrics.json`

### 依赖说明

基础依赖：

- `pandas`
- `numpy`

可选依赖：

- `scikit-learn`

说明：

- 没有 `scikit-learn` 时，聚类和模型训练会自动跳过
- 读 `xlsx` 时优先走 pandas；如果当前环境没有对应 Excel 引擎，脚本里也做了 fallback 读取逻辑

## 3. 推荐怎么用

如果你现在数据不完整，建议这样用：

### 快速版

先跑：

```bash
python scripts/build_summary_sheet_personas.py
```

适合先看一版业务画像原型。

### 完整版

如果你能拿到原始事件表，再跑：

```bash
python scripts/build_user_event_personas.py --input your_events.csv
```

适合继续做：

- 用户特征工程
- 分群
- 高活预测
- 漏斗和受阻分析

## 4. 当前脚本的定位

这两份脚本都偏“分析工具”，不是线上服务代码。

它们的定位是：

- 帮你快速把行为数据整理成用户画像结果
- 帮你验证画像假设
- 帮你把业务洞察落成可复用脚本

它们不负责：

- 实时在线打标
- 生产级模型服务
- 自动调参
- 复杂因果分析

## 5. 后续可以怎么扩

如果后面你拿到更多数据，可以继续往下扩：

- 合并注册时间，做生命周期画像
- 合并设备绑定结果，做转化画像
- 合并留存标签，做高活/低活监督学习
- 增加渠道、地区、角色字段，做更完整的业务画像

如果后面要继续扩，建议优先保留并持续维护的中间表是：

- `user_features.csv`

因为它是后面规则标签、聚类、预测、漏斗分析的共同基础。
