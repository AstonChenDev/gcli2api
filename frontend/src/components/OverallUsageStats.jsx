import { useEffect, useMemo, useState } from "react";
import { Loader2 } from "lucide-react";
import api from "../api";
import Modal from "./Modal";

export const successRate = (success, total) => total ? `${(success / total * 100).toFixed(2)}%` : "—";
const codeLabel = code => code === 0 ? "无状态码" : code === 200 ? "200（记录为失败）" : String(code);
const errorTitle = message => {
  try {
    const error = JSON.parse(message).error;
    for (const value of [typeof error === "string" ? error : null, error?.blockReason, error?.status, error?.message]) {
      if (typeof value === "string" && value) return value;
    }
  } catch { /* Older logs may contain truncated JSON. Keep the raw detail below. */ }
  for (const field of ["blockReason", "status", "message"]) {
    const match = (message || "").match(new RegExp(`"${field}"\\s*:\\s*("(?:[^"\\\\]|\\\\.)*")`));
    if (match) {
      try { return JSON.parse(match[1]); } catch { /* Fall back to original text. */ }
    }
  }
  return message || "日志未记录错误详情";
};

export default function OverallUsageStats({ filters }) {
  const params = useMemo(() => ({ start_timestamp: filters.start_timestamp,
    end_timestamp: filters.end_timestamp, model_name: filters.model_name }),
  [filters.start_timestamp, filters.end_timestamp, filters.model_name]);
  const [scope, setScope] = useState("requests");
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [detail, setDetail] = useState(null);
  const [page, setPage] = useState(1);
  const [details, setDetails] = useState(null);
  const [detailError, setDetailError] = useState("");
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true); setError(""); setData(null); setDetail(null);
    api.get("/usage-logs/overall-stats", { params, signal: controller.signal })
      .then(res => { if (!controller.signal.aborted) setData(res.data); })
      .catch(err => { if (!controller.signal.aborted) setError(err.response?.data?.detail || "加载整体统计失败"); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [params]);

  useEffect(() => {
    if (!detail) return;
    const controller = new AbortController();
    setDetails(null); setDetailError(""); setDetailLoading(true);
    api.get("/usage-logs/overall-errors", { params: { ...params, scope,
      ...(detail.code !== null ? { status_code: detail.code } : {}), offset: (page - 1) * 20, limit: 20 }, signal: controller.signal })
      .then(res => { if (!controller.signal.aborted) setDetails(res.data); })
      .catch(err => { if (!controller.signal.aborted) setDetailError(err.response?.data?.detail || "加载整体错误详情失败"); })
      .finally(() => { if (!controller.signal.aborted) setDetailLoading(false); });
    return () => controller.abort();
  }, [detail, page, scope, params]);

  const current = data?.[scope];
  const summary = current?.summary;
  const openErrors = code => { setPage(1); setDetail({ code }); };
  return <section className="space-y-4 rounded-xl border border-slate-200 bg-slate-50 p-4 sm:p-5" aria-label="整体调用统计">
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div><h3 className="text-lg font-semibold text-slate-900">整体成功率与错误情况</h3>
        <p className="text-xs text-slate-500 mt-1">全部凭证 · 跟随时间和模型筛选 · 邮箱筛选仅影响下方凭证统计</p></div>
      <div className="flex gap-2 flex-wrap" role="group" aria-label="整体统计口径">
        {[["requests", "用户请求（合并重试）"], ["attempts", "上游调用（含重试）"]].map(([value, label]) =>
          <button key={value} aria-pressed={scope === value} onClick={() => { setScope(value); setDetail(null); }}
            className={`btn text-xs ${scope === value ? "btn-primary" : "btn-secondary"}`}>{label}</button>)}
      </div>
    </div>
    <p className="text-xs text-slate-600">{scope === "requests"
      ? "相同 Request ID 合并，按最新已记录的结果计算成功率；重试后成功只计一次成功。缺少 ID 的旧记录各自计数，进行中的请求结束后再刷新。"
      : "每次上游调用单独计数，包含重试。成功率 = 成功调用数 ÷ 调用总数，失败的重试也计入失败。"}</p>
    {data?.logging_enabled === false && <p role="alert" className="text-sm text-amber-800">日志记录已关闭，以下仅包含保留的历史数据。</p>}
    {loading && <div role="status" className="py-6 flex items-center justify-center gap-2 text-sm text-slate-500"><Loader2 size={18} className="animate-spin" />加载整体统计…</div>}
    {error && <p role="alert" className="text-red-700 text-sm">{error}</p>}
    {summary && <>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        {[[scope === "requests" ? "请求总数" : "调用总数", summary.total_requests, "text-slate-900"],
          ["成功", summary.success_count, "text-emerald-700"], ["失败", summary.failure_count, "text-red-700"],
          ["整体成功率", successRate(summary.success_count, summary.total_requests), "text-purple-700"]].map(([label, value, color]) =>
          <div key={label} className="card p-4"><p className="text-xs text-slate-500">{label}</p><p className={`text-2xl font-bold mt-2 ${color}`}>{value}</p></div>)}
      </div>
      {!summary.total_requests ? <p className="text-sm text-slate-500 py-4">所选范围暂无记录，成功率暂不计算。</p> : <>
        <div className="rounded-lg border border-slate-200 bg-white p-4 space-y-3">
          <div className="flex items-center justify-between gap-2"><h4 className="font-semibold text-sm">整体错误分布</h4>
            {summary.failure_count > 0 && <button className="text-purple-700 text-xs underline" onClick={() => openErrors(null)}>全部错误明细</button>}</div>
          <p className="text-xs text-slate-500">按日志中的结果状态码汇总；流中途报错时，HTTP 连接可能已返回 200。点击错误码查看具体原因。</p>
          {current.error_codes.length ? current.error_codes.map(row => <button key={row.status_code} onClick={() => openErrors(row.status_code)}
            className="block w-full text-left rounded-lg p-2 hover:bg-red-50 focus-visible:ring-2 focus-visible:ring-purple-500">
            <div className="flex justify-between gap-3 text-sm"><span className="font-medium text-red-700">{codeLabel(row.status_code)}</span>
              <span>{row.count} 次 <span className="text-xs text-slate-500">· 占失败 {row.failure_percentage.toFixed(2)}% · 占全部 {row.total_percentage.toFixed(2)}%</span></span></div>
            <div className="h-2 bg-slate-100 rounded-full mt-2 overflow-hidden"><div className="bg-red-400 h-full rounded-full" style={{ width: `${row.failure_percentage}%` }} /></div>
          </button>) : <p className="text-sm text-emerald-700">所选范围没有失败记录。</p>}
        </div>
        <div><h4 className="font-semibold text-sm mb-2">各模型整体表现</h4>
          <div className="table-container"><table><thead><tr>{["模型", scope === "requests" ? "请求数" : "调用数", "成功", "失败", "成功率"].map(h => <th key={h}>{h}</th>)}</tr></thead>
            <tbody>{current.by_model.map(row => <tr key={row.model_name}><td className="break-all">{row.model_name}</td><td>{row.total_requests}</td>
              <td className="text-emerald-700">{row.success_count}</td><td className="text-red-700">{row.failure_count}</td>
              <td className="font-medium whitespace-nowrap">{successRate(row.success_count, row.total_requests)}</td></tr>)}</tbody></table></div>
        </div>
      </>}
    </>}
    {detail && <Modal title={`整体错误明细${detail.code !== null ? ` · ${codeLabel(detail.code)}` : ""}`} onClose={() => setDetail(null)}>
      <p className="text-xs text-slate-500 mb-3">{scope === "requests" ? "用户请求（合并重试）" : "上游调用（含重试）"} · 全部凭证 · {params.model_name || "全部模型"}</p>
      {detailLoading && <p role="status">正在加载…</p>}{detailError && <p role="alert" className="text-red-700">{detailError}</p>}
      {details?.items.map((row, i) => <details key={i} className="border border-red-200 bg-red-50 rounded-lg p-3 my-2">
        <summary className="cursor-pointer text-sm text-red-800 break-words">{codeLabel(row.status_code)} · {row.count} 次 · {errorTitle(row.error_message).slice(0, 160)}</summary>
        <pre className="whitespace-pre-wrap break-words text-xs mt-2">{row.error_message || "日志未记录错误详情"}</pre>
        <p className="text-xs text-slate-500 mt-2">最近发生：{new Date(row.last_seen * 1000).toLocaleString("zh-CN")}</p>
      </details>)}
      {details && <div className="flex flex-wrap gap-2 items-center text-xs mt-3"><span>{details.total} 类报错 · 第 {page} / {Math.max(1, Math.ceil(details.total / 20))} 页</span>
        <button disabled={page <= 1 || detailLoading} onClick={() => setPage(p => p - 1)} className="btn btn-secondary disabled:opacity-40">上一页</button>
        <button disabled={page * 20 >= details.total || detailLoading} onClick={() => setPage(p => p + 1)} className="btn btn-secondary disabled:opacity-40">下一页</button>
      </div>}
    </Modal>}
  </section>;
}
