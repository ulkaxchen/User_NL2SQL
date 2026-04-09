# User RAG — 自然语言查数（React + FastAPI + ReAct Agent）

业务数据落在 **MySQL** 当前库中的可查询业务表（默认主表名 **`users`**，可与实际库中表名一致）；查询意图经 **LLM + ReAct** 转成 **MySQL `SELECT`**，结果回前端。元数据与审计在同库 **`query_logs` 表**（启动时自动建表），经反馈回流到 **知识上下文**（轻量 RAG）。下面按 **数据从哪里来、经过谁、写回哪里** 把整条链路写清楚；**连接与配置**见第 3 节，**Agent / SQL / Finish 约束**见 **第 12 节**。
