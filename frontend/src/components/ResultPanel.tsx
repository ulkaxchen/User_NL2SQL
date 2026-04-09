import { useEffect, useState } from "react";

import { submitQueryFeedback } from "../api/client";
import type { QueryResponse } from "../types";

type ResultPanelProps = {
  result: QueryResponse | null;
  onFeedbackError?: (message: string) => void;
};

export function ResultPanel({ result, onFeedbackError }: ResultPanelProps) {
  const [feedbackSent, setFeedbackSent] = useState<"up" | "down" | null>(null);
  const [feedbackBusy, setFeedbackBusy] = useState(false);

  useEffect(() => {
    setFeedbackSent(null);
    setFeedbackBusy(false);
  }, [result?.query_log_id]);

  async function sendFeedback(helpful: boolean) {
    const logId = result?.query_log_id;
    if (typeof logId !== "number" || logId < 1) {
      return;
    }
    setFeedbackBusy(true);
    try {
      await submitQueryFeedback(logId, helpful);
      setFeedbackSent(helpful ? "up" : "down");
    } catch (err) {
      onFeedbackError?.(err instanceof Error ? err.message : "反馈失败");
    } finally {
      setFeedbackBusy(false);
    }
  }

  if (!result) {
    return (
      <section className="card">
        <div className="card-header">
          <h2>结果</h2>
        </div>
        <p className="muted">提交问题后，这里会显示 Agent 的理解、SQL 和最终数据。</p>
      </section>
    );
  }

  return (
    <section className="card result-stack">
      <div className="card-header">
        <h2>结果</h2>
      </div>
      <div>
        <div className="label">系统理解</div>
        <div>{result.understanding}</div>
      </div>
      {result.clarification ? (
        <div className="clarification">{result.clarification}</div>
      ) : null}
      {result.summary ? (
        <div>
          <div className="label">结果总结</div>
          <div>{result.summary}</div>
        </div>
      ) : null}
      {typeof result.query_log_id === "number" && result.query_log_id > 0 ? (
        <div className="feedback-bar">
          <div className="label">结果是否有帮助？</div>
          <p className="muted feedback-hint">
            点「有帮助」后，本次问答与 SQL 会作为范例供后续相似问题参考；「无帮助」则不会收录。
          </p>
          {feedbackSent ? (
            <div className="feedback-done">
              {feedbackSent === "up" ? "已记录为有帮助，感谢反馈。" : "已记录为无帮助。"}
            </div>
          ) : (
            <div className="feedback-actions">
              <button
                type="button"
                className="btn-secondary"
                disabled={feedbackBusy}
                onClick={() => void sendFeedback(true)}
              >
                有帮助
              </button>
              <button
                type="button"
                className="btn-secondary"
                disabled={feedbackBusy}
                onClick={() => void sendFeedback(false)}
              >
                无帮助
              </button>
            </div>
          )}
        </div>
      ) : null}
    </section>
  );
}
