type SqlPanelProps = {
  sql: string | null;
};

export function SqlPanel({ sql }: SqlPanelProps) {
  return (
    <section className="card">
      <div className="card-header">
        <h2>执行 SQL</h2>
      </div>
      <pre className="sql-block">{sql || "当前没有 SQL。可能需要先澄清问题。"}</pre>
    </section>
  );
}
