type AgentTranscriptPanelProps = {
  transcript: string | null | undefined;
};

export function AgentTranscriptPanel({ transcript }: AgentTranscriptPanelProps) {
  if (!transcript?.trim()) {
    return (
      <section className="card">
        <div className="card-header">
          <h2>ReAct 思考过程</h2>
        </div>
        <p className="muted">本次响应没有可用的中间过程文本（例如模型未进入 ReAct 循环）。</p>
      </section>
    );
  }

  return (
    <section className="card">
      <details className="debug-details" open>
        <summary className="debug-summary">
          <span className="debug-summary-title">ReAct 思考过程</span>
          <span className="muted debug-summary-hint">Thought / Action / Observation 全文</span>
        </summary>
        <pre className="sql-block agent-transcript-pre">{transcript}</pre>
      </details>
    </section>
  );
}
