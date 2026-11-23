import { useState } from 'react';
import api from '../api';
import Modal from './Modal';

export default function CredentialLimits({ credential, onSaved }) {
  const [editing, setEditing] = useState(false);
  const [concurrency, setConcurrency] = useState('');
  const [rpm, setRpm] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');

  function open() {
    setConcurrency(String(credential.concurrency_limit || 0));
    setRpm(String(credential.rpm_limit || 0));
    setError('');
    setEditing(true);
  }

  async function save(event) {
    event.preventDefault();
    setBusy(true);
    setError('');
    try {
      const { data } = await api.patch(`/creds/limits/${encodeURIComponent(credential.email)}`, {
        concurrency_limit: Number(concurrency), rpm_limit: Number(rpm),
      });
      onSaved(data);
      setEditing(false);
    } catch (err) {
      const detail = err.response?.data?.detail;
      setError(typeof detail === 'string' ? detail : '保存失败，请重试。');
    } finally {
      setBusy(false);
    }
  }

  return <div className="text-xs text-slate-600">
    并发 {credential.concurrency_limit || '不限'} · RPM {credential.rpm_limit || '不限'}
    <button className="ml-2 text-purple-700 underline" onClick={open}>设置调用限制</button>
    {editing && <Modal title="凭证调用限制" onClose={() => { if (!busy) setEditing(false); }}>
      <form onSubmit={save} className="space-y-4 text-sm">
        <p className="break-all">{credential.email}</p>
        <label className="block">最大并发数
          <input type="number" required min="0" max="2147483647" step="1" value={concurrency}
            onChange={e => setConcurrency(e.target.value)} className="block w-full rounded border p-2" />
        </label>
        <label className="block">每分钟请求数（RPM）
          <input type="number" required min="0" max="2147483647" step="1" value={rpm}
            onChange={e => setRpm(e.target.value)} className="block w-full rounded border p-2" />
        </label>
        <p className="text-slate-500">0 表示不限。所有模型共用此凭证的限额。并发统计进行中的生成请求，RPM 统计最近 60 秒内派发的生成请求，包含重试和凭证测试。</p>
        {error && <p role="alert" className="text-red-700">{error}</p>}
        <button disabled={busy} className="btn btn-primary">{busy ? '保存中…' : '保存限制'}</button>
      </form>
    </Modal>}
  </div>;
}
