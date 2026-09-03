/* Credential tier x model success-rate table for the statistics dashboard. */
(function () {
    'use strict';

    const credentialModes = new Set(['geminicli', 'antigravity']);
    const tiers = [
        { key: 'ultra', label: 'Ultra' },
        { key: 'pro', label: 'Pro' },
        { key: 'free', label: 'Free' },
        { key: 'unknown', label: '未识别' }
    ];
    const knownTierKeys = new Set(tiers.map(item => item.key));

    function escapeHtml(value) {
        return String(value ?? '')
            .replaceAll('&', '&amp;')
            .replaceAll('<', '&lt;')
            .replaceAll('>', '&gt;')
            .replaceAll('"', '&quot;')
            .replaceAll("'", '&#039;');
    }

    function formatNumber(value) {
        const number = Number(value) || 0;
        if (number >= 1000000) return `${(number / 1000000).toFixed(1)}M`;
        if (number >= 1000) return `${(number / 1000).toFixed(1)}K`;
        return String(number);
    }

    function rateColor(success, total) {
        if (!total) return 'var(--text-muted)';
        const rate = success / total * 100;
        if (rate >= 95) return '#28a745';
        if (rate >= 85) return '#f0ad4e';
        return '#dc3545';
    }

    function ensureSection() {
        const existing = document.getElementById('statsTierModelSection');
        if (existing) return existing;

        const credentialTable = document.getElementById('statsCredTable');
        const scrollContainer = credentialTable?.closest('div');
        const credentialSection = scrollContainer?.parentElement;
        if (!credentialSection?.parentElement) return null;

        const section = document.createElement('div');
        section.id = 'statsTierModelSection';
        section.style.marginBottom = '24px';
        section.innerHTML = `
            <h4 style="margin-bottom:8px;">🎫 凭证等级 × 模型成功率</h4>
            <div style="margin-bottom:10px;color:var(--text-muted);font-size:12px;line-height:1.6;">
                按凭证当前等级汇总 API 调用（含重试）；等级变更会重新归类历史数据，已删除或无法关联的凭证计入“未识别”。
            </div>
            <div style="overflow-x:auto;background:var(--card-bg);border:1px solid var(--border-color);border-radius:10px;padding:8px 10px;">
                <table id="statsTierModelTable" style="width:100%;min-width:920px;border-collapse:collapse;font-size:13px;">
                    <thead><tr id="statsTierModelHead" style="border-bottom:2px solid var(--border-color);"></tr></thead>
                    <tbody id="statsTierModelBody"></tbody>
                </table>
            </div>`;
        credentialSection.parentElement.insertBefore(section, credentialSection);
        return section;
    }

    function emptyCounts() {
        return { total: 0, success: 0, fail: 0 };
    }

    function addCounts(target, row) {
        target.total += Number(row.total) || 0;
        target.success += Number(row.success) || 0;
        target.fail += Number(row.fail) || 0;
    }

    function metricCell(counts) {
        if (!counts.total) {
            return '<td style="text-align:center;padding:10px 12px;color:var(--text-muted);">--</td>';
        }
        const rate = counts.success / counts.total * 100;
        return `<td style="text-align:center;padding:8px 12px;white-space:nowrap;">
            <div style="font-weight:700;color:${rateColor(counts.success, counts.total)};">${rate.toFixed(1)}%</div>
            <div style="margin-top:2px;color:var(--text-muted);font-size:11px;">${formatNumber(counts.success)}成 / ${formatNumber(counts.fail)}败 / ${formatNumber(counts.total)}次</div>
        </td>`;
    }

    function render(rows, mode) {
        const section = ensureSection();
        if (!section) return;

        if (!credentialModes.has(mode)) {
            section.style.display = 'none';
            return;
        }
        section.style.display = '';

        const head = document.getElementById('statsTierModelHead');
        const body = document.getElementById('statsTierModelBody');
        if (!head || !body) return;

        head.innerHTML = '<th style="text-align:left;padding:10px 12px;">模型</th>' +
            tiers.map(tier => `<th style="text-align:center;padding:10px 12px;">${tier.label}</th>`).join('') +
            '<th style="text-align:center;padding:10px 12px;">合计</th>';

        const models = new Map();
        (Array.isArray(rows) ? rows : []).forEach(row => {
            const modelName = String(row.model_name || 'unknown');
            const tier = knownTierKeys.has(String(row.tier).toLowerCase())
                ? String(row.tier).toLowerCase()
                : 'unknown';
            if (!models.has(modelName)) {
                const tierCounts = Object.fromEntries(tiers.map(item => [item.key, emptyCounts()]));
                models.set(modelName, { modelName, tierCounts, total: emptyCounts() });
            }
            const model = models.get(modelName);
            addCounts(model.tierCounts[tier], row);
            addCounts(model.total, row);
        });

        const sorted = [...models.values()].sort((a, b) =>
            b.total.total - a.total.total || a.modelName.localeCompare(b.modelName)
        );
        if (!sorted.length) {
            body.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:18px;color:var(--text-muted);">当前筛选范围暂无等级统计</td></tr>';
            return;
        }

        body.innerHTML = sorted.map(model => `
            <tr style="border-bottom:1px solid var(--border-color);">
                <td style="padding:10px 12px;font-weight:600;">${escapeHtml(model.modelName)}</td>
                ${tiers.map(tier => metricCell(model.tierCounts[tier.key])).join('')}
                ${metricCell(model.total)}
            </tr>`).join('');
    }

    document.addEventListener('gcli:stats-loaded', event => {
        const detail = event.detail || {};
        render(detail.tierModels || [], detail.mode || 'geminicli');
    });

    window.TierModelStats = { render };
})();
