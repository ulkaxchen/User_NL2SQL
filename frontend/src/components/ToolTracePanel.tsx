import type { ToolTrace } from "../types";

type ToolTracePanelProps = {
  traces: ToolTrace[];
};

const RESULT_PREVIEW_CHARS = 8000;

function stringifyLimited(value: unknown, maxChars: number): string {
  try {
    const s = JSON.stringify(value, null, 2);
    if (s.length <= maxChars) {
      return s;
    }
    return `${s.slice(0, maxChars)}\n...(truncated, ${s.length} chars total)`;
  } catch {
    return String(value);
  }
}

export function ToolTracePanel({ traces }: ToolTracePanelProps) {
  return (
    <section className="card">
      <div className="card-header">
        <h2>工具调用与返回</h2>
      </div>
      {!traces.length ? (
        <p className="muted">没有工具调用记录。</p>
      ) : (
        <div className="trace-list">
          {traces.map((trace, index) => (
            <div className="trace-item" key={`${trace.step}-${trace.tool}-${index}`}>
              <div className="label">
                Step {trace.step}: {trace.tool}
              </div>
              <div className="trace-block-label">入参</div>
              <pre>{JSON.stringify(trace.arguments, null, 2)}</pre>
              <div className="trace-block-label">返回</div>
              <pre className="trace-result-pre">{stringifyLimited(trace.result, RESULT_PREVIEW_CHARS)}</pre>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
