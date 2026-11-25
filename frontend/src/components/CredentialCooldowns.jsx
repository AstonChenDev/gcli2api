import { credentialQuotaCooldowns, formatCooldownRemaining } from "../utils/credentialCooldowns.js";

export default function CredentialCooldowns({ cooldowns, now }) {
  const { text, image } = credentialQuotaCooldowns(cooldowns, now);
  const deadline = (until) => new Date(until * 1000).toLocaleString();

  return (
    <dl className="space-y-1">
      <div className="flex items-start justify-between gap-2">
        <dt className="shrink-0 text-slate-600">文本配额 CD</dt>
        <dd className="flex flex-wrap justify-end gap-x-2 gap-y-1 text-right">
          {text.length ? text.map(({ group, label, until }) => (
            <span key={group} className="text-cyan-700" title={deadline(until)}>
              {label} {formatCooldownRemaining(until, now)}
            </span>
          )) : <span className="text-slate-500">无冷却</span>}
        </dd>
      </div>
      <div className="flex items-start justify-between gap-2">
        <dt className="shrink-0 text-slate-600">图片生成 CD</dt>
        <dd className="text-right">
          {image === null ? <span className="text-slate-500">无冷却</span> : (
            <span className="text-cyan-700" title={deadline(image)}>{formatCooldownRemaining(image, now)}</span>
          )}
        </dd>
      </div>
    </dl>
  );
}
