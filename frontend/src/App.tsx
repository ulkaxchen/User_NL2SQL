import { useEffect, useState } from "react";

import { deleteTable, fetchHealth, fetchTables, importFile, runQueryStream } from "./api/client";
import { DataTable } from "./components/DataTable";
import { ImportPanel } from "./components/ImportPanel";
import { QueryForm } from "./components/QueryForm";
import { ResultPanel } from "./components/ResultPanel";
import { SqlPanel } from "./components/SqlPanel";
import { AgentTranscriptPanel } from "./components/AgentTranscriptPanel";
import { ToolTracePanel } from "./components/ToolTracePanel";
import type { AgentStreamEvent, HealthResponse, QueryResponse, TableListEntry, ToolTrace } from "./types";

const DEFAULT_QUESTION = "印尼有多少用户";

function App() {
  const [question, setQuestion] = useState(DEFAULT_QUESTION);
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [importing, setImporting] = useState(false);
  const [tables, setTables] = useState<TableListEntry[]>([]);
  const [importTargetTable, setImportTargetTable] = useState("users");
  const [contextTableNames, setContextTableNames] = useState<string[]>([]);
  const [deletingTable, setDeletingTable] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [liveTraces, setLiveTraces] = useState<ToolTrace[]>([]);
  const [liveTranscript, setLiveTranscript] = useState("");

  async function refreshHealth() {
    try {
      const nextHealth = await fetchHealth();
      setHealth(nextHealth);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "加载系统状态失败");
    }
  }

  async function loadTables() {
    const list = await fetchTables();
    setTables(list);
    setContextTableNames((prev) => {
      const names = new Set(list.map((x) => x.name));
      const kept = prev.filter((n) => names.has(n));
      if (kept.length > 0) {
        return kept;
      }
      const fallback = list.find((t) => t.is_default_table) ?? list[0];
      return fallback ? [fallback.name] : [];
    });
  }

  useEffect(() => {
    void (async () => {
      try {
        await refreshHealth();
      } catch (nextError) {
        setError(nextError instanceof Error ? nextError.message : "加载系统状态失败");
      }
      try {
        await loadTables();
      } catch (nextError) {
        const msg = nextError instanceof Error ? nextError.message : "加载表列表失败";
        setError((prev) => (prev ? `${prev}；${msg}` : msg));
      }
    })();
  }, []);

  function parseObservationJson(raw: string): Record<string, unknown> {
    try {
      return JSON.parse(raw) as Record<string, unknown>;
    } catch {
      return { preview: raw };
    }
  }

  function applyStreamEvent(ev: AgentStreamEvent) {
    if (ev.type === "bootstrap") {
      setLiveTraces((prev) => [
        ...prev,
        {
          step: ev.step,
          tool: ev.tool,
          arguments: ev.arguments,
          result: parseObservationJson(ev.result),
        },
      ]);
      setLiveTranscript((s) => `${s}Observation: ${ev.result}\n\n`);
      return;
    }
    if (ev.type === "step") {
      let block = "";
      if (ev.thought) {
        block += `Thought: ${ev.thought}\n`;
      }
      if (ev.action) {
        block += `Action: ${ev.action}\n`;
      } else if (ev.raw_response_preview) {
        block += `Action: (缺失，原始片段)\n${ev.raw_response_preview}\n`;
      }
      if (block) {
        setLiveTranscript((s) => `${s}${block}\n`);
      }
      return;
    }
    if (ev.type === "tool_result") {
      setLiveTraces((prev) => [
        ...prev,
        {
          step: ev.step,
          tool: ev.tool,
          arguments: ev.arguments,
          result: parseObservationJson(ev.result),
        },
      ]);
      setLiveTranscript((s) => `${s}Observation: ${ev.result}\n\n`);
      return;
    }
    if (ev.type === "sql") {
      setLiveTranscript((s) => `${s}--- 执行 SQL ---\n${ev.sql}\n\n`);
      return;
    }
    if (ev.type === "sql_error") {
      setLiveTranscript((s) => `${s}SQL 执行失败: ${ev.error}\n\n`);
    }
  }

  async function handleSubmit() {
    setLoading(true);
    setError(null);
    setLiveTraces([]);
    setLiveTranscript("");
    setResult(null);
    try {
      const response = await runQueryStream(question, applyStreamEvent, contextTableNames);
      setResult(response);
      setLiveTraces([]);
      setLiveTranscript("");
      await refreshHealth();
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "查询失败");
    } finally {
      setLoading(false);
    }
  }

  async function handleDeleteTable(tableName: string) {
    setDeletingTable(tableName);
    setError(null);
    try {
      await deleteTable(tableName);
      setResult(null);
      await loadTables();
      await refreshHealth();
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "删除表失败");
    } finally {
      setDeletingTable(null);
    }
  }

  async function handleImport(file: File) {
    setImporting(true);
    setError(null);
    try {
      await importFile(file, importTargetTable);
      setResult(null);
      await refreshHealth();
      await loadTables();
      const tname = importTargetTable.trim() || "users";
      setContextTableNames((prev) => Array.from(new Set([...prev, tname])));
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "导入失败");
    } finally {
      setImporting(false);
    }
  }

  return (
    <main className="page">
      <header className="hero">
        <div>
          <h1>User RAG SQL Agent</h1>
          <p>React + FastAPI + SQL Agent + tools。前端只保留提问、SQL、结果和工具轨迹。</p>
        </div>
        <div className="hero-stats">
          <div className="stat-card">
            <span className="label">系统状态</span>
            <strong>{health?.status ?? "loading"}</strong>
          </div>
          <div className="stat-card">
            <span className="label">已导入数据</span>
            <strong>{health?.row_count ?? 0}</strong>
          </div>
          {health?.llm_backend ? (
            <div className="stat-card" title={health.llm_base_url || undefined}>
              <span className="label">Agent LLM</span>
              <strong>{health.llm_backend}</strong>
              {health.llm_model ? (
                <span className="label" style={{ display: "block", marginTop: 4 }}>
                  {health.llm_model}
                </span>
              ) : null}
              {health.llm_config_ok === false && health.llm_config_message ? (
                <span className="label" style={{ display: "block", marginTop: 4, color: "#c0392b" }}>
                  {health.llm_config_message}
                </span>
              ) : null}
            </div>
          ) : null}
        </div>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}

      <div className="layout">
        <div className="left-column">
          <ImportPanel
            importing={importing}
            tables={tables}
            targetTable={importTargetTable}
            onTargetTableChange={setImportTargetTable}
            onImport={handleImport}
          />
          <QueryForm
            question={question}
            loading={loading}
            tables={tables}
            contextTableNames={contextTableNames}
            onContextTablesChange={setContextTableNames}
            onChange={setQuestion}
            onSubmit={handleSubmit}
            onDeleteTable={handleDeleteTable}
            deletingTable={deletingTable}
          />
          <ToolTracePanel traces={loading ? liveTraces : (result?.tool_trace ?? [])} />
          <AgentTranscriptPanel
            transcript={loading ? (liveTranscript.trim() ? liveTranscript : null) : result?.agent_transcript}
          />
        </div>
        <div className="right-column">
          <ResultPanel result={result} onFeedbackError={(message) => setError(message)} />
          <SqlPanel sql={result?.sql ?? null} />
          <DataTable
            columns={result?.columns ?? []}
            rows={result?.rows ?? []}
            sql={result?.sql ?? null}
            onExportError={(message) => setError(message)}
          />
        </div>
      </div>
    </main>
  );
}

export default App;
