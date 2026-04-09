export type ToolTrace = {
  step: number;
  tool: string;
  arguments: Record<string, unknown>;
  result: unknown;
};

export type QueryResponse = {
  question: string;
  understanding: string;
  sql: string | null;
  columns: string[];
  rows: Array<Array<string | number | null>>;
  called_tools: string[];
  tool_trace: ToolTrace[];
  clarification: string | null;
  summary: string | null;
  query_log_id?: number | null;
  agent_transcript?: string | null;
};

export type HealthResponse = {
  status: string;
  imported: boolean;
  row_count: number;
  tools: Array<{
    name: string;
    description: string;
    input_schema?: Record<string, string>;
  }>;
  llm_backend?: string;
  llm_model?: string;
  llm_base_url?: string;
  llm_config_ok?: boolean;
  llm_config_message?: string | null;
};

export type TableListEntry = {
  name: string;
  row_count: number;
  column_count: number;
  is_default_table: boolean;
};

/** SSE 事件，与 backend agent.iter_events 对齐 */
export type AgentStreamEvent =
  | {
      type: "bootstrap";
      step: number;
      tool: string;
      arguments: Record<string, unknown>;
      result: string;
    }
  | {
      type: "step";
      step: number;
      thought: string;
      action: string | null;
      raw_response_preview?: string;
    }
  | {
      type: "tool_start";
      step: number;
      tool: string;
      arguments: Record<string, unknown>;
    }
  | {
      type: "tool_result";
      step: number;
      tool: string;
      arguments: Record<string, unknown>;
      result: string;
    }
  | { type: "sql"; step: number; sql: string }
  | { type: "sql_error"; step: number; error: string }
  | { type: "done"; response: QueryResponse };
