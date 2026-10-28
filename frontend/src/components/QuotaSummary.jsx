export function quotaGroups(data) {
  if (data.quotaGroups?.length) return data.quotaGroups;
  return Object.entries(data.models || {}).map(([name, value]) => ({
    displayName: name,
    buckets: [{ remainingFraction: value.remaining, resetTime: value.resetTimeRaw, window: value.window || "" }],
  }));
}

export default function QuotaSummary({ data, loading = false }) {
  if (!data) return <p className="text-xs text-slate-500">{loading ? "正在查询额度…" : "额度待查询"}</p>;
  return <div className="space-y-2 text-xs">
    {loading && <p className="text-slate-500">正在刷新额度…</p>}
    {data.error && <p className="text-red-700">{data.error}</p>}
    {(data.groups || []).map((group, i) => <div key={`${group.displayName}-${i}`} className="space-y-1.5">
      <div className="font-medium text-slate-700">{group.displayName || "模型额度"}</div>
      {(group.buckets || []).map((bucket, j) => {
        const raw = bucket.remainingFraction;
        const known = typeof raw === "number" && Number.isFinite(raw);
        const pct = known ? Math.min(100, Math.max(0, raw * 100)) : null;
        const window = String(bucket.window || "").toLowerCase();
        const label = window.includes("week") ? "周额度" : /5|five/.test(window) ? "5 小时额度" : bucket.displayName || bucket.window || "剩余额度";
        const reset = bucket.resetTime ? new Date(bucket.resetTime) : null;
        return <div key={bucket.bucketId || j}>
          <div className="flex justify-between gap-2"><span className="text-slate-500">{label}</span><span className={pct !== null && pct < 20 ? "text-red-700" : "text-emerald-700"}>{known ? `${pct.toFixed(1)}%` : "未知"}</span></div>
          {known && <div className="h-1.5 rounded-full bg-slate-200 my-1"><div className={`h-full rounded-full ${pct < 20 ? "bg-red-500" : "bg-emerald-500"}`} style={{ width: `${pct}%` }} /></div>}
          {reset && !Number.isNaN(reset.getTime()) && <div className="text-slate-500">重置：{reset.toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" })}</div>}
        </div>;
      })}
    </div>)}
    {!data.error && !data.groups?.length && <p className="text-slate-500">上游未返回额度信息</p>}
    {data.updatedAt && <p className="text-slate-400">更新于 {new Date(data.updatedAt).toLocaleTimeString("zh-CN")}</p>}
  </div>;
}
