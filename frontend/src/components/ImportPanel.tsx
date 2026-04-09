import type { TableListEntry } from "../types";

type ImportPanelProps = {
  importing: boolean;
  tables: TableListEntry[];
  targetTable: string;
  onTargetTableChange: (value: string) => void;
  onImport: (file: File) => void;
};

export function ImportPanel({
  importing,
  tables,
  targetTable,
  onTargetTableChange,
  onImport,
}: ImportPanelProps) {
  const listId = "import-table-suggestions";
  return (
    <section className="card">
      <div className="card-header">
        <h2>导入数据</h2>
      </div>
      <p className="muted-hint">
        上传 <strong>.xlsx / .csv</strong> 将<strong>整表重建</strong>并写入所选表名（首行为列名）。表名限英文字母、数字、下划线；可输入新表名创建第二张业务表。
      </p>
      <label className="field-label" htmlFor="import-target-table">
        导入到表
      </label>
      <input
        id="import-target-table"
        className="text-input"
        list={listId}
        value={targetTable}
        disabled={importing}
        placeholder="users"
        onChange={(e) => onTargetTableChange(e.target.value)}
      />
      <datalist id={listId}>
        {tables.map((t) => (
          <option key={t.name} value={t.name} />
        ))}
      </datalist>
      <label className="upload-box upload-box-spaced">
        <span>{importing ? "导入中..." : "选择 xlsx / csv 文件上传"}</span>
        <input
          type="file"
          accept=".xlsx,.xlsm,.csv,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
          disabled={importing}
          onChange={(event) => {
            const file = event.target.files?.[0];
            if (file) {
              onImport(file);
              event.target.value = "";
            }
          }}
        />
      </label>
    </section>
  );
}
