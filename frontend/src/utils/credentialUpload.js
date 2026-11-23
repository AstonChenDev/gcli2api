import api from "../api.js";
import { expirePanelSession, getPanelToken } from "../panelSession.js";

async function sameFileContents(left, right) {
  if (left === right) return true;
  const chunkSize = 256 * 1024;
  try {
    for (let offset = 0; offset < left.size; offset += chunkSize) {
      const [a, b] = await Promise.all([left, right].map(file =>
        file.slice(offset, offset + chunkSize).arrayBuffer()
      ));
      const bytes = new Uint8Array(b);
      if (!new Uint8Array(a).every((value, index) => value === bytes[index])) return false;
    }
    return true;
  } catch {
    // Keep both entries if the browser cannot establish that they are identical.
    return false;
  }
}

export async function appendCredentialFiles(current, additions) {
  const result = [...current];
  for (const entry of additions) {
    const { file } = entry;
    let duplicate = false;
    for (const existing of result) {
      if (existing.file.name === file.name && existing.file.size === file.size &&
          await sameFileContents(existing.file, file)) {
        duplicate = true;
        break;
      }
    }
    if (!duplicate) result.push(entry);
  }
  return result;
}

// Stream results as each credential finishes; no two-minute Axios request timeout.
export async function uploadCredentialFiles(formData, { signal, onEvent }) {
  const token = getPanelToken();
  const authorization = token ? `Bearer ${token}` : null;
  const response = await fetch(api.getUri({ url: "/creds/upload-stream" }), {
    method: "POST", body: formData, signal,
    headers: authorization ? { Authorization: authorization } : {},
  });
  if (!response.ok) {
    if (response.status === 401) expirePanelSession(authorization);
    const error = await response.json().catch(() => ({}));
    throw new Error(typeof error.detail === "string" ? error.detail : `导入请求失败（HTTP ${response.status}）`);
  }
  if (!response.body) throw new Error("浏览器不支持读取导入进度，请更新浏览器后重试");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let pending = "", complete = false;
  const consume = line => {
    if (!line.trim()) return;
    const event = JSON.parse(line);
    if (event.type === "complete") complete = true;
    onEvent(event);
  };
  try {
    while (true) {
      const { done, value } = await reader.read();
      pending += decoder.decode(value, { stream: !done });
      const lines = pending.split("\n"); pending = lines.pop();
      lines.forEach(consume);
      if (done) { consume(pending); break; }
    }
    if (!complete) throw new Error("导入连接中断。已保存的凭证会保留，请核对进度后重试未完成项");
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
