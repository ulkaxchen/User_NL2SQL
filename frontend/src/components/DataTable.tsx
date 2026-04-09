import { useState } from "react";

import { downloadQueryExport } from "../api/client";
import { downloadRowsAsCsv } from "../utils/downloadCsv";

type DataTableProps = {
  columns: string[];
  rows: Array<Array<string | number | null>>;
  sql?: string | null;
  onExportError?: (message: string) => void;
};

export function DataTable({ columns, rows, sql, onExportError }: DataTableProps) {
  const [exporting, setExporting] = useState(false);

  if (!columns.length) {
    return (
      <section className="card">
        <div className="card-header">
          <h2>最终数据</h2>
        </div>
        <p className="muted">暂无数据。</p>
      </section>
    );
  }

  async function handleExcel() {
    if (!sql?.trim()) {
      return;
    }
    setExporting(true);
    try {
      await downloadQueryExport(sql, "xlsx");
    } catch (err) {
      onExportError?.(err instanceof Error ? err.message : "Excel 导出失败。");
    } finally {
      setExporting(false);
    }
  }

  return (
    <section className="card">
      <div className="card-header">
        <h2>最终数据</h2>
        <div className="download-actions">
          <button
            type="button"
            className="btn-secondary"
            disabled={exporting}
            onClick={() => downloadRowsAsCsv(columns, rows)}
          >
            下载 CSV
          </button>
          {sql?.trim() ? (
            <button type="button" className="btn-secondary" disabled={exporting} onClick={() => void handleExcel()}>
              {exporting ? "导出中…" : "下载 Excel"}
            </button>
          ) : null}
        </div>
      </div>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, rowIndex) => (
              <tr key={`${rowIndex}-${row.join("|")}`}>
                {row.map((cell, cellIndex) => (
                  <td key={`${rowIndex}-${cellIndex}`}>{cell === null ? "-" : String(cell)}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
