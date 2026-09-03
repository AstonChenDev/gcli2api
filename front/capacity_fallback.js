/* Capacity fallback control-panel extension.
 *
 * Kept separate from common.js so upstream dashboard changes only require the
 * two small populate/collect hooks below to remain in place.
 */
(function () {
    'use strict';

    const fields = {
        enabled: ['capacityFallbackEnabled', 'antigravity_capacity_fallback_enabled'],
        proxyUrl: ['capacityFallbackProxyUrl', 'antigravity_capacity_fallback_proxy_url'],
        httpStatuses: ['capacityFallbackHttpStatuses', 'antigravity_capacity_fallback_http_statuses'],
        errorStatuses: ['capacityFallbackErrorStatuses', 'antigravity_capacity_fallback_error_statuses'],
        reasons: ['capacityFallbackReasons', 'antigravity_capacity_fallback_reasons'],
        models: ['capacityFallbackModels', 'antigravity_capacity_fallback_models'],
        maxAttempts: ['capacityFallbackMaxAttempts', 'antigravity_capacity_fallback_max_attempts'],
        connectTimeout: ['capacityFallbackConnectTimeout', 'antigravity_capacity_fallback_connect_timeout_seconds'],
        requestTimeout: ['capacityFallbackRequestTimeout', 'antigravity_capacity_fallback_request_timeout_seconds'],
        statsEnabled: ['capacityFallbackStatsEnabled', 'antigravity_capacity_fallback_stats_enabled'],
        routeName: ['capacityFallbackRouteName', 'antigravity_capacity_fallback_route_name']
    };

    function csv(value, fallback) {
        if (Array.isArray(value)) return value.join(',');
        if (typeof value === 'string' && value.trim()) return value;
        return fallback;
    }

    function setValue(name, value, lockedKeys) {
        const [id, key] = fields[name];
        const element = document.getElementById(id);
        if (!element) return;
        if (element.type === 'checkbox') element.checked = Boolean(value);
        else element.value = value;
        const locked = lockedKeys && lockedKeys.has(key);
        element.disabled = Boolean(locked);
        element.classList.toggle('env-locked', Boolean(locked));
    }

    function parseNumber(id, minimum, maximum, label) {
        const raw = document.getElementById(id)?.value.trim() || '';
        const value = Number(raw);
        if (!Number.isFinite(value) || value < minimum || value > maximum) {
            throw new Error(`${label}必须在 ${minimum} - ${maximum} 之间`);
        }
        return value;
    }

    function validateProxyUrl(value) {
        if (!value) return;
        let parsed;
        try {
            parsed = new URL(value);
        } catch (_) {
            throw new Error('备用代理地址格式无效');
        }
        const protocols = new Set(['http:', 'https:', 'socks5:', 'socks5h:']);
        if (!protocols.has(parsed.protocol) || !parsed.hostname || !parsed.port) {
            throw new Error('备用代理必须包含协议、主机和端口');
        }
    }

    function insertUi() {
        if (document.getElementById('capacityFallbackEnabled')) return;
        const cooldownField = document.getElementById('antigravityResourceExhaustedCooldownMinutes');
        const anchor = cooldownField?.closest('.config-group, .card');
        if (!anchor) return;

        const panel = document.createElement('div');
        panel.className = anchor.classList.contains('config-group') ? 'config-group' : 'card';
        panel.innerHTML = `
            <h4 style="margin-top:0">容量503备用出口</h4>
            <div class="form-group">
                <label><input type="checkbox" id="capacityFallbackEnabled" /> 启用 MODEL_CAPACITY_EXHAUSTED 备用出口</label>
                <small class="config-note" style="display:block">仅在响应尚未输出时，通过配置的代理重试；支持热更新</small>
            </div>
            <div class="form-group">
                <label for="capacityFallbackProxyUrl">备用代理地址:</label>
                <input type="text" id="capacityFallbackProxyUrl" class="config-input" placeholder="socks5h://capacity-egress:1080" />
            </div>
            <div class="form-group">
                <label for="capacityFallbackModels">允许模型:</label>
                <input type="text" id="capacityFallbackModels" class="config-input" placeholder="* 或 gemini-*-image" />
                <small class="config-note" style="display:block">支持逗号分隔和 * 通配符</small>
            </div>
            <div class="form-group">
                <label for="capacityFallbackRouteName">统计路由名称:</label>
                <input type="text" id="capacityFallbackRouteName" class="config-input" placeholder="capacity-egress" />
            </div>
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px">
                <div class="form-group">
                    <label for="capacityFallbackMaxAttempts">每请求最大次数:</label>
                    <input type="number" id="capacityFallbackMaxAttempts" class="config-input" min="1" max="3" />
                </div>
                <div class="form-group">
                    <label for="capacityFallbackConnectTimeout">连接超时(秒):</label>
                    <input type="number" id="capacityFallbackConnectTimeout" class="config-input" min="0.1" max="60" step="0.1" />
                </div>
                <div class="form-group">
                    <label for="capacityFallbackRequestTimeout">请求超时(秒):</label>
                    <input type="number" id="capacityFallbackRequestTimeout" class="config-input" min="1" max="3600" step="1" />
                </div>
            </div>
            <details style="margin-top:8px">
                <summary style="cursor:pointer">高级触发条件</summary>
                <div class="form-group">
                    <label for="capacityFallbackHttpStatuses">HTTP状态码:</label>
                    <input type="text" id="capacityFallbackHttpStatuses" class="config-input" />
                </div>
                <div class="form-group">
                    <label for="capacityFallbackErrorStatuses">Google error.status:</label>
                    <input type="text" id="capacityFallbackErrorStatuses" class="config-input" />
                </div>
                <div class="form-group">
                    <label for="capacityFallbackReasons">Google ErrorInfo.reason:</label>
                    <input type="text" id="capacityFallbackReasons" class="config-input" />
                </div>
            </details>
            <div class="form-group">
                <label><input type="checkbox" id="capacityFallbackStatsEnabled" /> 记录按模型重试统计和成功率</label>
            </div>`;
        anchor.insertAdjacentElement('afterend', panel);

        const modeSelect = document.getElementById('statsModeSelect');
        if (modeSelect && !modeSelect.querySelector('option[value="capacity_fallback"]')) {
            const option = document.createElement('option');
            option.value = 'capacity_fallback';
            option.textContent = '容量503出口重试';
            modeSelect.appendChild(option);

            const note = document.createElement('div');
            note.id = 'capacityFallbackStatsNote';
            note.style.cssText = 'display:none;width:100%;padding:8px 10px;border-radius:6px;background:var(--hover-bg);color:var(--text-muted);font-size:12px;line-height:1.6';
            note.textContent = '统计口径：错误503=直连触发次数；总请求=实际备用出口重试次数；成功/失败及成功率=备用出口完整请求结果。';
            modeSelect.parentElement?.insertAdjacentElement('afterend', note);
            modeSelect.addEventListener('change', updateStatsHint);
        }
        updateStatsHint();
    }

    function updateStatsHint() {
        const selected = document.getElementById('statsModeSelect')?.value;
        const note = document.getElementById('capacityFallbackStatsNote');
        if (note) note.style.display = selected === 'capacity_fallback' ? 'block' : 'none';

        const total = document.getElementById('statsReqTotal');
        const headingNote = total?.closest('.stats-container')?.previousElementSibling?.querySelector('span');
        if (headingNote) {
            if (!headingNote.dataset.capacityFallbackDefault) {
                headingNote.dataset.capacityFallbackDefault = headingNote.textContent;
            }
            headingNote.textContent = selected === 'capacity_fallback'
                ? '（每次备用出口重试的最终结果）'
                : headingNote.dataset.capacityFallbackDefault;
        }
    }

    function populate(config, lockedKeys) {
        insertUi();
        setValue('enabled', config.antigravity_capacity_fallback_enabled, lockedKeys);
        setValue('proxyUrl', config.antigravity_capacity_fallback_proxy_url || '', lockedKeys);
        setValue('httpStatuses', csv(config.antigravity_capacity_fallback_http_statuses, '503'), lockedKeys);
        setValue('errorStatuses', csv(config.antigravity_capacity_fallback_error_statuses, 'UNAVAILABLE'), lockedKeys);
        setValue('reasons', csv(config.antigravity_capacity_fallback_reasons, 'MODEL_CAPACITY_EXHAUSTED'), lockedKeys);
        setValue('models', csv(config.antigravity_capacity_fallback_models, '*'), lockedKeys);
        setValue('maxAttempts', config.antigravity_capacity_fallback_max_attempts || 1, lockedKeys);
        setValue('connectTimeout', config.antigravity_capacity_fallback_connect_timeout_seconds || 5, lockedKeys);
        setValue('requestTimeout', config.antigravity_capacity_fallback_request_timeout_seconds || 900, lockedKeys);
        setValue('statsEnabled', config.antigravity_capacity_fallback_stats_enabled !== false, lockedKeys);
        setValue('routeName', config.antigravity_capacity_fallback_route_name || 'capacity-egress', lockedKeys);
    }

    function collect() {
        const get = id => document.getElementById(id)?.value.trim() || '';
        const enabled = Boolean(document.getElementById(fields.enabled[0])?.checked);
        const proxyUrl = get(fields.proxyUrl[0]);
        if (enabled && !proxyUrl) throw new Error('启用容量回退时必须填写备用代理地址');
        validateProxyUrl(proxyUrl);
        const routeName = get(fields.routeName[0]) || 'capacity-egress';
        if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/.test(routeName)) {
            throw new Error('统计路由名称只能包含字母、数字、点、下划线或横线');
        }
        return {
            antigravity_capacity_fallback_enabled: enabled,
            antigravity_capacity_fallback_proxy_url: proxyUrl,
            antigravity_capacity_fallback_http_statuses: get(fields.httpStatuses[0]) || '503',
            antigravity_capacity_fallback_error_statuses: get(fields.errorStatuses[0]) || 'UNAVAILABLE',
            antigravity_capacity_fallback_reasons: get(fields.reasons[0]) || 'MODEL_CAPACITY_EXHAUSTED',
            antigravity_capacity_fallback_models: get(fields.models[0]) || '*',
            antigravity_capacity_fallback_max_attempts: parseNumber(fields.maxAttempts[0], 1, 3, '最大尝试次数'),
            antigravity_capacity_fallback_connect_timeout_seconds: parseNumber(fields.connectTimeout[0], 0.1, 60, '连接超时'),
            antigravity_capacity_fallback_request_timeout_seconds: parseNumber(fields.requestTimeout[0], 1, 3600, '请求超时'),
            antigravity_capacity_fallback_stats_enabled: Boolean(document.getElementById(fields.statsEnabled[0])?.checked),
            antigravity_capacity_fallback_route_name: routeName
        };
    }

    insertUi();
    window.CapacityFallbackUI = { populate, collect };
})();
