import { useState } from 'react';
import api from '../api';

export default function CredentialProxy({ credential, proxies, onSaved }) {
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const current = credential.proxy_id || '';

  async function selectProxy(event) {
    const proxyId = event.target.value || null;
    setSaving(true);
    setError('');
    try {
      const { data } = await api.patch(
        `/creds/proxy/${encodeURIComponent(credential.email)}`,
        { proxy_id: proxyId },
      );
      onSaved(data);
    } catch (err) {
      const detail = err.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : '保存失败，请重新选择。');
    } finally {
      setSaving(false);
    }
  }

  return <div className="space-y-1 text-xs">
    <label className="block text-slate-600">
      选择代理 IP
      <select
        aria-label={`${credential.email} 的代理 IP`}
        className="mt-1 w-full rounded-lg border border-slate-300 bg-white px-2 py-1.5"
        value={current} disabled={!proxies || saving} onChange={selectProxy}
      >
        <option value="">跟随项目代理</option>
        {current && !proxies?.some(proxy => proxy.id === current) &&
          <option value={current} disabled>当前已绑定代理</option>}
        {proxies?.map(proxy =>
          <option key={proxy.id} value={proxy.id}>{proxy.name} · {proxy.address}</option>
        )}
      </select>
    </label>
    {saving && <span role="status">正在保存…</span>}
    {error && <p role="alert" className="text-red-700">{error}</p>}
  </div>;
}
