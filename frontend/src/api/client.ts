import type { AgentStreamEvent, HealthResponse, QueryResponse, TableListEntry } from "../types";

/** 开发环境默认 `/api` 走 Vite 代理到后端；直连后端时可在 .env 设 VITE_API_BASE=http://127.0.0.1:8000/api */
function apiBase(): string {
  const raw = import.meta.env.VITE_API_BASE;
  if (typeof raw === "string" && raw.trim()) {
    return raw.trim().replace(/\/$/, "");
  }
  return "/api";
}

const API_BASE = apiBase();

export async function fetchHealth(): Promise<HealthResponse> {
  const response = await fetch(`${API_BASE}/system/health`);
  if (!response.ok) {
    throw new Error("Failed to fetch system health.");
  }
  return response.json();
}

export async function fetchTables(): Promise<TableListEntry[]> {
  const response = await fetch(`${API_BASE}/system/tables`);
  const text = await response.text();
  if (!response.ok) {
    throw new Error(text || `加载表列表失败（HTTP ${response.status}）。请确认后端已启动且与前端 API 地址一致。`);
  }
  let data: unknown;
  try {
    data = JSON.parse(text) as { tables?: unknown };
  } catch {
    throw new Error("表列表接口返回非 JSON，请检查是否连错地址或后端异常。");
  }
  const tables = (data as { tables?: TableListEntry[] }).tables;
  if (!Array.isArray(tables)) {
    throw new Error("表列表接口格式异常（缺少 tables 数组）。");
  }
  return tables;
}

export async function deleteTable(tableName: string): Promise<void> {
  const enc = encodeURIComponent(tableName);
  const response = await fetch(`${API_BASE}/system/tables/${enc}`, { method: "DELETE" });
  const text = await response.text();
  if (!response.ok) {
    let msg = "删除表失败。";
    try {
      const j = JSON.parse(text) as { detail?: string };
      if (typeof j.detail === "string") {
        msg = j.detail;
      }
    } catch {
      if (text) {
        msg = text;
      }
    }
    throw new Error(msg);
  }
}

export async function downloadQueryExport(sql: string, format: "csv" | "xlsx"): Promise<void> {
  const response = await fetch(`${API_BASE}/chat/export`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ sql, format }),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || "导出失败。");
  }
  const blob = await response.blob();
  const defaultName = format === "xlsx" ? "query_result.xlsx" : "query_result.csv";
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = defaultName;
  anchor.click();
  URL.revokeObjectURL(url);
}

export async function submitQueryFeedback(logId: number, helpful: boolean): Promise<void> {
  const response = await fetch(`${API_BASE}/chat/feedback`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ log_id: logId, helpful }),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || "反馈提交失败。");
  }
}

export async function runQuery(question: string, contextTables?: string[]): Promise<QueryResponse> {
  const response = await fetch(`${API_BASE}/chat/query`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question, context_tables: contextTables ?? [] }),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || "Failed to run query.");
  }
  return response.json();
}

function extractSseDataLines(buffer: string): { lines: string[]; rest: string } {
  const lines: string[] = [];
  let rest = buffer;
  const sep = "\n\n";
  for (;;) {
    const idx = rest.indexOf(sep);
    if (idx < 0) {
      break;
    }
    const block = rest.slice(0, idx);
    rest = rest.slice(idx + sep.length);
    for (const rawLine of block.split("\n")) {
      if (rawLine.startsWith("data: ")) {
        lines.push(rawLine.slice(6).trim());
      }
    }
  }
  return { lines, rest };
}

/**
 * 流式查询：通过 onEvent 实时收到事件，Promise 在收到 type=done 后 resolve。
 */
export async function runQueryStream(
  question: string,
  onEvent: (event: AgentStreamEvent) => void,
  contextTables?: string[],
): Promise<QueryResponse> {
  const response = await fetch(`${API_BASE}/chat/query/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      question,
      context_tables: contextTables ?? [],
    }),
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || "Failed to run query stream.");
  }
  const reader = response.body?.getReader();
  if (!reader) {
    throw new Error("No response body.");
  }
  const decoder = new TextDecoder();
  let carry = "";
  let finalResponse: QueryResponse | null = null;
  for (;;) {
    const { done, value } = await reader.read();
    carry += decoder.decode(value ?? new Uint8Array(), { stream: !done });
    const { lines, rest } = extractSseDataLines(carry);
    carry = rest;
    for (const line of lines) {
      if (!line) {
        continue;
      }
      const event = JSON.parse(line) as AgentStreamEvent;
      onEvent(event);
      if (event.type === "done") {
        finalResponse = event.response;
      }
    }
    if (done) {
      break;
    }
  }
  if (!finalResponse) {
    throw new Error("Stream ended without a final result.");
  }
  return finalResponse;
}

export async function importFile(file: File, targetTable: string): Promise<void> {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("table", targetTable.trim() || "users");
  const response = await fetch(`${API_BASE}/system/import`, {
    method: "POST",
    body: formData,
  });
  if (!response.ok) {
    const detail = await response.text();
    throw new Error(detail || "Failed to import file.");
  }
}
