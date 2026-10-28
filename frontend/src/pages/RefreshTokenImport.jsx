import { KeyRound, Loader2, Upload } from "lucide-react";
import { useRef, useState } from "react";
import api from "../api";

const MAX_BYTES = 1024 * 1024;

export default function RefreshTokenImport() {
  const [text, setText] = useState("");
  const [file, setFile] = useState(null);
  const [busy, setBusy] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const fileRef = useRef(null);
  const tokens = text.split(/\r?\n/).map(v => v.trim()).filter(Boolean);
  const clear = () => {
    setText(""); setFile(null);
    if (fileRef.current) fileRef.current.value = "";
  };

  const submit = async (event) => {
    event.preventDefault();
    if (busy) return;
    setError(""); setResult(null);
    if (!tokens.length && !file) return setError("请粘贴 RT，或选择 Manager 备份 JSON。");
    if (tokens.length > 100) return setError("单次最多导入 100 项，请分批提交。");
    if (file?.size > MAX_BYTES) return setError("备份文件不能超过 1 MiB。");
    // Lock before reading files to prevent duplicate submissions.
    setBusy(true);
    try {
      let backup = null;
      if (file) {
        try { backup = JSON.parse((await file.text()).replace(/^\uFEFF/, "")); }
        catch { setError("备份文件不是有效的 JSON。"); return; }
      }
      const body = JSON.stringify({ refresh_tokens: tokens, manager_backup: backup });
      if (new Blob([body]).size > MAX_BYTES) { setError("导入内容合计不能超过 1 MiB。"); return; }
      clear();
      const response = await api.post("/creds/import-antigravity-refresh-tokens", body, {
        headers: { "Content-Type": "application/json" }, timeout: 360000,
      });
      setResult(response.data);
    } catch (err) {
      setError(err.response?.data?.detail || "连接中断或导入超时。请查看凭证管理确认结果；重新提交会跳过已导入账号。");
    } finally { setBusy(false); }
  };

  return <div className="space-y-5">
    <div className="rounded-xl border border-purple-200 bg-purple-50 p-5">
      <h2 className="flex items-center gap-2 text-lg font-semibold text-purple-800"><KeyRound size={20} /> RT 导入</h2>
      <p className="mt-2 text-sm text-slate-700">粘贴 Antigravity Refresh Token，自动验证并获取邮箱、Project ID 和订阅等级。</p>
      <p className="mt-2 text-xs text-slate-600">支持单条、批量 RT 和 Antigravity Manager 备份。重复 RT 或邮箱会跳过，保留已有凭证及其禁用、冷却状态。</p>
    </div>
    <form onSubmit={submit} className="card space-y-4">
      <div>
        <label htmlFor="refresh-tokens" className="block text-sm font-medium text-slate-800 mb-2">Refresh Token（每行一条）</label>
        <textarea id="refresh-tokens" value={text} onChange={e => setText(e.target.value)} disabled={busy}
          rows={6} autoComplete="off" autoCorrect="off" spellCheck={false}
          placeholder="在这里粘贴 RT，每行一条"
          className="w-full rounded-lg border border-slate-300 bg-white p-3 text-sm font-mono text-slate-900 disabled:bg-slate-50" />
        <p className="text-xs text-slate-500 mt-1">已填写 {tokens.length} 条 · 单次最多 100 项，合计不超过 1 MiB</p>
      </div>
      <div>
        <label htmlFor="manager-backup" className="block text-sm font-medium text-slate-800 mb-2">Manager 备份 JSON（可选）</label>
        <input ref={fileRef} id="manager-backup" type="file" accept=".json,application/json" disabled={busy}
          onChange={e => { setFile(e.target.files?.[0] || null); setError(""); }}
          className="block w-full text-sm text-slate-600 file:mr-3 file:rounded-lg file:border-0 file:bg-slate-100 file:px-3 file:py-2 file:text-slate-700" />
        <p className="text-xs text-slate-500 mt-2">支持账号数组或包含 accounts 数组的备份；可与上方 RT 一起导入。</p>
      </div>
      <div className="flex gap-2 flex-wrap">
        <button type="submit" disabled={busy || (!tokens.length && !file)} className="btn btn-primary flex items-center gap-2 text-sm disabled:opacity-50">
          {busy ? <Loader2 size={16} className="animate-spin" /> : <Upload size={16} />}
          {busy ? "正在验证并导入…" : "验证并导入"}
        </button>
        <button type="button" onClick={clear} disabled={busy} className="btn btn-secondary text-sm disabled:opacity-50">清空输入</button>
      </div>
      {busy && <p role="status" className="text-sm text-slate-600">正在逐项验证，请保持本页打开。较大的批次可能需要几分钟。</p>}
    </form>
    {error && <div role="alert" className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700">{error}</div>}
    {result && <section className="card space-y-4" aria-live="polite">
      <h3 className="font-semibold text-slate-800">导入完成 · 共 {result.total} 项</h3>
      <div className="flex flex-wrap gap-4 text-sm">
        <span className="text-emerald-700">成功 {result.imported_count}</span>
        <span className="text-slate-600">跳过 {result.skipped_count}</span>
        <span className="text-red-700">失败 {result.failed_count}</span>
      </div>
      <div className="space-y-2 max-h-96 overflow-auto">
        {result.results?.map(row => <div key={row.item} className={`rounded-lg border p-3 text-sm ${row.status === "success" ? "bg-emerald-50 border-emerald-200" : row.status === "skipped" ? "bg-slate-50 border-slate-200" : "bg-red-50 border-red-200"}`}>
          <div className="font-medium">第 {row.item} 项 · {({ success: "成功", skipped: "已跳过", failed: "失败" })[row.status]}</div>
          {row.email && <p className="break-all mt-1">{row.email}</p>}
          {(row.tier || row.project_id) && <p className="text-xs text-slate-600 mt-1 break-all">{row.tier && `等级：${row.tier === "ultra_lite" ? "Ultra Lite" : row.tier}`} {row.project_id && ` · Project：${row.project_id}`}</p>}
          {row.error && <p className="text-xs text-slate-600 mt-1">{row.error}</p>}
        </div>)}
      </div>
      {result.imported_count > 0 && <p className="text-sm text-slate-600">导入成功的账号已加入凭证池，可到「凭证管理」查看。</p>}
    </section>}
  </div>;
}
