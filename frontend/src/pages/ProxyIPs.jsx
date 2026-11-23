import { useEffect, useState } from 'react';
import api from '../api';
import Modal from '../components/Modal';

function ProxyTest({ proxyId }) {
  const [testing, setTesting] = useState(false);
  const [result, setResult] = useState(null);

  const test = async () => {
    setTesting(true);
    setResult(null);
    try {
      const { data } = await api.post(`/proxies/${proxyId}/test`, null, { timeout: 18000 });
      setResult(data);
    } catch (err) {
      const detail = err.response?.data?.detail;
      setResult({ ok: false, message: typeof detail === 'string' ? detail : '未收到面板检测结果，请重试。' });
    } finally {
      setTesting(false);
    }
  };

  return <div className="space-y-1">
    <button className="text-purple-700 disabled:text-slate-500" disabled={testing} onClick={test}>
      {testing ? '测试中…' : '测试连接'}
    </button>
    <div role="status" aria-live="polite">
      {result && <span className={result.ok ? 'text-emerald-700' : 'text-red-700'}>
        {result.message}{result.elapsed_ms != null && ` · ${result.elapsed_ms} ms`}
      </span>}
    </div>
  </div>;
}

export default function ProxyIPs() {
  const [items, setItems] = useState([]), [loading, setLoading] = useState(true);
  const [error, setError] = useState(''), [editing, setEditing] = useState(null);
  const [name, setName] = useState(''), [url, setUrl] = useState('');
  const [busy, setBusy] = useState(false), [formError, setFormError] = useState('');
  const [deleting, setDeleting] = useState(null);
  const [revision, setRevision] = useState(0);
  const refresh = () => setRevision(previous => previous + 1);
  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    setLoading(true); setError('');
    api.get('/proxies', { signal })
      .then(({ data }) => { if (!signal.aborted) setItems(data.items); })
      .catch(() => {
        if (signal.aborted) return;
        setItems([]);
        setError('代理列表加载失败，请重试');
      })
      .finally(() => { if (!signal.aborted) setLoading(false); });
    return () => controller.abort();
  }, [revision]);
  const open = row => { setEditing(row); setName(row.name || ''); setUrl(''); setFormError(''); };
  const save = async event => {
    event.preventDefault(); setBusy(true); setFormError('');
    try {
      const body = { name: name.trim(), ...(url.trim() ? { url: url.trim() } : {}) };
      if (editing.id) await api.patch(`/proxies/${editing.id}`, body);
      else await api.post('/proxies', body);
      setEditing(null); setUrl(''); refresh();
    } catch (err) { setFormError(typeof err.response?.data?.detail === 'string' ? err.response.data.detail : '保存失败，请检查名称和代理地址'); }
    finally { setBusy(false); }
  };
  const remove = async () => {
    setBusy(true); setFormError('');
    try { await api.delete(`/proxies/${deleting.id}`); setDeleting(null); refresh(); }
    catch (err) { setFormError(typeof err.response?.data?.detail === 'string' ? err.response.data.detail : '删除失败'); }
    finally { setBusy(false); }
  };
  return <section className="space-y-4">
    <div className="flex justify-between gap-3 items-center"><h2 className="text-xl font-semibold">IP 管理</h2>
      <div className="flex gap-2"><button className="btn btn-secondary" disabled={loading} onClick={refresh}>刷新</button><button className="btn btn-primary" onClick={() => open({})}>添加代理 IP</button></div></div>
    <p className="text-sm text-slate-600">添加 HTTP、HTTPS 或 SOCKS5 代理，然后在凭证管理中为每个凭证选择出口。多个凭证可以使用同一个代理。</p>
    <p className="text-xs text-slate-500">未绑定的凭证沿用全局代理设置。已绑定代理连接失败时不会自动直连。修改代理地址后，新请求使用新地址，已开始的请求继续使用原连接。</p>
    <p className="text-xs text-slate-500">点击“测试连接”，项目会通过该代理访问 Google 连通性检测地址，最长等待 12 秒。</p>
    {error && <p role="alert" className="text-red-700">{error}</p>}
    {loading ? <p role="status">加载中…</p> : error ? null : !items.length ? <p className="card text-slate-500">暂无代理，请点击“添加代理 IP”。</p> : <div className="table-container"><table aria-label="代理 IP 列表">
      <thead><tr><th>名称</th><th>代理地址</th><th>认证</th><th>绑定凭证数</th><th>连通性</th><th>操作</th></tr></thead>
      <tbody>{items.map(row => <tr key={row.id}><td>{row.name}</td><td className="break-all">{row.address}</td><td>{row.has_auth ? '已配置' : '无'}</td><td>{row.bound_count}</td>
        <td><ProxyTest proxyId={row.id} /></td>
        <td><div className="flex gap-3"><button className="text-purple-700" onClick={() => open(row)}>编辑</button><button className="text-red-700 disabled:text-slate-400" disabled={row.bound_count > 0} title={row.bound_count ? '请先在凭证管理中解除绑定' : '删除代理'} onClick={() => { setDeleting(row); setFormError(''); }}>删除</button></div></td></tr>)}</tbody>
    </table></div>}
    {editing && <Modal title={editing.id ? '编辑代理 IP' : '添加代理 IP'} onClose={() => { if (!busy) { setEditing(null); setUrl(''); } }}>
      <form onSubmit={save} className="space-y-4">
        <label className="block">名称<input autoFocus required maxLength={100} value={name} onChange={e => setName(e.target.value)} className="w-full mt-1 rounded-lg border border-slate-300 bg-white px-3 py-2" placeholder="例如：香港代理 01" /></label>
        {editing.id && <p className="text-xs text-slate-500">当前地址：{editing.address}</p>}
        <label className="block">代理地址<input type="password" autoComplete="new-password" required={!editing.id} maxLength={2048} value={url} onChange={e => setUrl(e.target.value)} className="w-full mt-1 rounded-lg border border-slate-300 bg-white px-3 py-2" placeholder={editing.id ? '留空保留原地址和认证信息' : 'http://用户名:密码@主机:端口'} /></label>
        <p className="text-xs text-slate-500">支持 http://、https://、socks5://、socks5h://，必须填写端口。用户名、密码中的 @、:、# 等特殊字符需进行 URL 编码。地址从运行项目的服务器连接。</p>
        {formError && <p role="alert" className="text-red-700">{formError}</p>}
        <button disabled={busy} className="btn btn-primary">{busy ? '保存中…' : '保存代理'}</button>
      </form>
    </Modal>}
    {deleting && <Modal title="删除代理 IP" onClose={() => { if (!busy) setDeleting(null); }}>
      <p>确认删除“{deleting.name}”？</p>{formError && <p role="alert" className="text-red-700">{formError}</p>}
      <button disabled={busy} onClick={remove} className="btn btn-danger mt-4">{busy ? '删除中…' : '确认删除'}</button>
    </Modal>}
  </section>;
}
