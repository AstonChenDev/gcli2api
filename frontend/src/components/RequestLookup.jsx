import { useEffect, useState } from "react";
import api from "../api";

export default function RequestLookup({ onOpen }) {
  const [identifier, setIdentifier] = useState("");
  const [kind, setKind] = useState("auto");
  const [query, setQuery] = useState(null);
  const [page, setPage] = useState(1);
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!query) return;
    const controller = new AbortController();
    setLoading(true); setError(""); setResult(null);
    api.get("/usage-logs/lookup", { params: { ...query, offset: (page - 1) * 20, limit: 20 }, signal: controller.signal })
      .then(res => { if (!controller.signal.aborted) setResult(res.data); })
      .catch(err => { if (!controller.signal.aborted) setError(err.response?.data?.detail || "查询失败"); })
      .finally(() => { if (!controller.signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [query, page]);
  return <section className="card p-4 space-y-3" aria-label="按 ID 查询请求">
    <h3 className="font-semibold text-purple-700">按 ID 查询请求 JSON</h3>
    <form className="flex flex-wrap gap-2" onSubmit={e => { e.preventDefault(); setPage(1); setQuery({ identifier: identifier.trim(), id_type: kind }); }}>
      <select aria-label="查询 ID 类型" value={kind} onChange={e => setKind(e.target.value)} className="border border-slate-300 rounded-lg px-3 py-2 bg-white text-sm">
        <option value="auto">自动识别 ID</option><option value="request_id">Request ID</option><option value="response_id">上游 responseId</option><option value="log_id">日志数字 ID</option>
      </select>
      <input aria-label="查询 ID" maxLength={160} required value={identifier} onChange={e => setIdentifier(e.target.value)} placeholder="输入完整 Request ID、responseId 或日志数字 ID" className="min-w-0 flex-1 border border-slate-300 rounded-lg px-3 py-2 text-sm" />
      <button disabled={loading || !identifier.trim()} className="btn btn-primary text-sm">{loading ? "查询中…" : "按 ID 查询"}</button>
    </form>
    <p className="text-xs text-slate-500">精确匹配全部保留日志，不受下方时间筛选影响。同一请求的重试分别列出。旧记录未保存的 JSON 或 responseId 无法补回。</p>
    {error && <p role="alert" className="text-sm text-red-700">{error}</p>}
    {result && <div className="space-y-2 text-sm">
      <p role="status">找到 {result.total} 条记录</p>
      {result.items.map(row => <button key={row.id} onClick={() => onOpen(row)} className="w-full text-left border border-slate-200 rounded-lg p-3 hover:bg-purple-50 space-y-1">
        <div className="break-all">日志 #{row.id} · {row.success ? "成功" : `失败 HTTP ${row.status_code}`} · {row.credential_email || "未分配凭证"} · {row.model_name}</div>
        <div className="text-xs text-slate-500 break-all">Request ID: {row.request_id || "—"} · responseId: {row.response_id || "未记录"}</div>
        <div className="text-xs text-purple-700">{row.request_log_file ? "查看详情 / 请求 JSON" : "查看详情（无请求 JSON）"}</div>
      </button>)}
      {result.total > 20 && <div className="flex gap-3 items-center"><button disabled={page <= 1} onClick={() => setPage(p => p - 1)} className="btn btn-secondary">上一页</button><span>第 {page} 页</span><button disabled={page * 20 >= result.total} onClick={() => setPage(p => p + 1)} className="btn btn-secondary">下一页</button></div>}
    </div>}
  </section>;
}
