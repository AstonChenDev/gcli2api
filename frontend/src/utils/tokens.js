export function formatMillions(value) {
  return `${(Number(value || 0) / 1000000).toLocaleString("en-US", { minimumFractionDigits: 3, maximumFractionDigits: 6 })} M`;
}
