const textLabels = {
  argon: "Argon",
  gemini: "Gemini",
  claude_gpt: "Claude / GPT",
};

export function credentialQuotaCooldowns(cooldowns, now) {
  const text = [];
  let image = null;
  for (const [group, until] of Object.entries(cooldowns || {})) {
    if (!Number.isFinite(until) || until <= now) continue;
    if (group === "image") {
      image = until;
    } else {
      text.push({ group, label: Object.hasOwn(textLabels, group) ? textLabels[group] : group, until });
    }
  }
  return { text, image };
}

export function formatCooldownRemaining(until, now) {
  const seconds = Math.ceil(until - now);
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor(seconds / 3600) % 24;
  const minutes = Math.floor(seconds / 60) % 60;
  if (days) return `${days}天 ${hours}小时`;
  if (hours) return `${hours}小时 ${minutes}分`;
  if (minutes) return `${minutes}分 ${seconds % 60}秒`;
  return `${seconds}秒`;
}
