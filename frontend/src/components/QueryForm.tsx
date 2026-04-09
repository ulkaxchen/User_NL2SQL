import type { TableListEntry } from "../types";

type QueryFormProps = {
  question: string;
  loading: boolean;
  tables: TableListEntry[];
  contextTableNames: string[];
  onContextTablesChange: (names: string[]) => void;
  onChange: (value: string) => void;
  onSubmit: () => void;
  onDeleteTable: (tableName: string) => void;
  deletingTable: string | null;
};

function toggleName(names: string[], tableName: string, checked: boolean): string[] {
  if (checked) {
    return names.includes(tableName) ? names : [...names, tableName];
  }
  return names.filter((n) => n !== tableName);
}

export function QueryForm({
  question,
  loading,
  tables,
  contextTableNames,
  onContextTablesChange,
  onChange,
  onSubmit,
  onDeleteTable,
  deletingTable,
}: QueryFormProps) {
  const allNames = tables.map((t) => t.name);
  const allSelected = allNames.length > 0 && allNames.every((n) => contextTableNames.includes(n));

  return (
    <section className="card">
      <div className="card-header">
        <h2>提问</h2>
      </div>
      <div className="context-tables-block">
        <div className="context-tables-header">
          <span className="field-label">查询范围（可多选，用于跨表 JOIN）</span>
          <button
            type="button"
            className="btn-text"
            disabled={loading || tables.length === 0}
            onClick={() => {
              if (allSelected) {
                const fallback = tables.find((t) => t.is_default_table) ?? tables[0];
                onContextTablesChange(fallback ? [fallback.name] : []);
              } else {
                onContextTablesChange([...allNames]);
              }
            }}
          >
            {allSelected ? "仅默认表" : "全选"}
          </button>
        </div>
        {tables.length === 0 ? (
          <p className="muted-hint">
            未加载到业务表。请确认后端已配置 <code>MYSQL_DATABASE</code>（或 <code>USER_RAG_MYSQL_DATABASE</code>）且与库中表在同一库；也可查看后端日志中{" "}
            <code>queryable_table_names</code> 相关告警。有数据时仍为空请尝试刷新页面。
          </p>
        ) : (
          <ul className="table-checkbox-list">
            {tables.map((t) => (
              <li key={t.name} className="table-list-row">
                <label className="table-checkbox-label">
                  <input
                    type="checkbox"
                    checked={contextTableNames.includes(t.name)}
                    disabled={loading || !!deletingTable}
                    onChange={(e) => onContextTablesChange(toggleName(contextTableNames, t.name, e.target.checked))}
                  />
                  <span>
                    <code className="table-name-code">{t.name}</code>
                    <span className="table-meta">
                      {t.row_count} 行 · {t.column_count} 列
                      {t.is_default_table ? " · 默认" : ""}
                    </span>
                  </span>
                </label>
                <button
                  type="button"
                  className="btn-danger-inline"
                  disabled={loading || !!deletingTable}
                  title="整表删除，不可恢复"
                  onClick={() => {
                    if (
                      !window.confirm(
                        `确定删除表「${t.name}」？该表所有数据将永久删除且不可恢复。`,
                      )
                    ) {
                      return;
                    }
                    onDeleteTable(t.name);
                  }}
                >
                  {deletingTable === t.name ? "删除中…" : "删除表"}
                </button>
              </li>
            ))}
          </ul>
        )}
        <p className="muted-hint">不选任何表时由后端默认使用 users（若存在）。</p>
      </div>
      <textarea
        className="query-input"
        value={question}
        onChange={(event) => onChange(event.target.value)}
        placeholder="例如：印尼有多少用户？订单表与用户表按用户ID关联后的金额合计。"
        rows={4}
      />
      <button className="primary-button" disabled={loading || !question.trim()} onClick={onSubmit}>
        {loading ? "查询中..." : "提交查询"}
      </button>
    </section>
  );
}
