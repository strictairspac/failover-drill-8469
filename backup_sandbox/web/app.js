/* 备份恢复演练沙盘前端（原生 JS，无依赖、无构建） */
const $ = (sel) => document.querySelector(sel);

function toLocalInput(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T` +
         `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
function toUtcIso(localValue) {
  if (!localValue) return '';
  return new Date(localValue).toISOString();
}
function fmt(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
         `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g,
    (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
function toast(msg, isErr = false) {
  const el = document.createElement('div');
  el.className = 'msg' + (isErr ? ' err' : '');
  el.textContent = msg;
  $('#toast').appendChild(el);
  setTimeout(() => el.remove(), 4000);
}
async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error?.message || `HTTP ${res.status}`);
  return data;
}

const STATUS_META = {
  queued:      ['排队中', 'info'],
  running:     ['执行中', 'warn'],
  interrupted: ['已中断·可恢复', 'warn'],
  partial:     ['部分恢复（遇缺段）', 'warn'],
  completed:   ['完成', 'ok'],
  failed:      ['失败', 'bad'],
};

async function renderBackups() {
  const backups = await api('/api/backups');
  const tbody = $('#table-backups tbody');
  tbody.innerHTML = backups.map((b) => {
    const states = [];
    if (b.pending_delete) states.push('<span class="tag warn">待删·等演练结束</span>');
    if (b.expired) states.push('<span class="tag bad">已过期</span>');
    if (!b.expired && !b.pending_delete) states.push('<span class="tag ok">可用</span>');
    const parent = b.parent_id
      ? `#${b.parent_id}` : '<span class="muted-text">—</span>';
    return `<tr>
      <td>${b.id}</td>
      <td>${esc(b.name)}</td>
      <td><span class="tag ${b.kind}">${b.kind === 'full' ? '全量' : '增量'}</span></td>
      <td>${fmt(b.taken_at)}</td>
      <td>${b.expires_at ? fmt(b.expires_at) : '<span class="muted-text">不过期</span>'}</td>
      <td>${parent}</td>
      <td>${b.size_kb}</td>
      <td>${states.join(' ')}</td>
      <td>${b.in_use ? `<span class="tag info">${b.in_use} 场</span>` : '<span class="muted-text">0</span>'}</td>
    </tr>`;
  }).join('');
  // 父依赖下拉（仅全量不能选自己当父；增量的父由后端校验）
  const sel = $('#bk-parent');
  const cur = sel.value;
  sel.innerHTML = '<option value="">—</option>' + backups
    .map((b) => `<option value="${b.id}">#${b.id} ${esc(b.name)} (${fmt(b.taken_at)})</option>`)
    .join('');
  sel.value = cur;
}

function gapText(d) {
  if (!d.gap_kind) return '<span class="muted-text">无</span>';
  const map = {
    missing_full: '缺全量：目标时点之前没有任何全量备份，无法起链',
    deleted: '备份已被清理',
    expired: '备份已过期，演练受理时即拒绝入链',
  };
  const who = d.gap_backup_name ? `（#${d.gap_backup_id} ${esc(d.gap_backup_name)}）` : '';
  return `<span class="gap">${esc(map[d.gap_kind] || d.gap_kind)}${who}<br>${esc(d.gap_detail || '')}</span>`;
}

async function renderDrills() {
  const drills = await api('/api/drills');
  const tbody = $('#table-drills tbody');
  tbody.innerHTML = drills.map((d) => {
    const [label, cls] = STATUS_META[d.status] || [d.status, 'muted'];
    const chain = d.steps.map((s) =>
      `<span class="${s.status}">${s.status === 'done' ? '✓' : '○'}#${s.backup_id ?? '?'} ${esc(s.backup_name || '已清理')}</span>`
    ).join(' → ') || '<span class="muted-text">空链</span>';
    const canRetry = ['interrupted', 'failed', 'partial'].includes(d.status);
    return `<tr>
      <td>${d.id}</td>
      <td>${esc(d.label)}</td>
      <td>${fmt(d.target_time)}</td>
      <td><span class="tag ${cls}">${label}</span></td>
      <td>${d.recovered_time
        ? `<strong>${fmt(d.recovered_time)}</strong>` +
          (d.short_of_target ? '<br><span class="gap">早于目标时点：链已截短（段可能已被清理）</span>' : '')
        : '<span class="muted-text">—</span>'}</td>
      <td>${gapText(d)}</td>
      <td>${d.reservation_count}</td>
      <td class="chain">${chain}</td>
      <td>${canRetry ? `<button class="small" onclick="retryDrill(${d.id})">重试/续跑</button>` : ''}</td>
    </tr>`;
  }).join('');
}

async function renderEvents() {
  const events = await api('/api/events');
  $('#table-events tbody').innerHTML = events.slice(0, 50).map((e) =>
    `<tr><td>${fmt(e.created_at)}</td><td>${esc(e.action)}</td><td>${esc(e.actor)}</td>
     <td class="chain">${esc(typeof e.detail === 'string' ? e.detail : JSON.stringify(e.detail))}</td></tr>`
  ).join('');
}

async function refresh() {
  try {
    await Promise.all([renderBackups(), renderDrills(), renderEvents()]);
  } catch (e) {
    toast('加载失败：' + e.message, true);
  }
}

window.retryDrill = async (id) => {
  try {
    await api(`/api/drills/${id}/retry`, { method: 'POST', body: {} });
    toast(`演练 #${id} 已按原链原进度续跑，不重复占用备份`);
    setTimeout(refresh, 100);
  } catch (e) { toast(e.message, true); }
};

$('#form-backup').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const f = new FormData(ev.target);
  const body = {
    name: f.get('name').trim(),
    kind: f.get('kind'),
    taken_at: toUtcIso(f.get('taken_at')),
    expires_at: toUtcIso(f.get('expires_at')) || null,
    parent_id: f.get('parent_id') ? Number(f.get('parent_id')) : null,
    size_kb: Number(f.get('size_kb') || 0),
  };
  try {
    await api('/api/backups', { method: 'POST', body });
    toast('备份已登记');
    ev.target.reset();
    refresh();
  } catch (e) { toast(e.message, true); }
});

$('#form-drill').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const f = new FormData(ev.target);
  try {
    const d = await api('/api/drills', {
      method: 'POST',
      body: { label: f.get('label').trim(), target_time: toUtcIso(f.get('target_time')) },
    });
    const gap = d.gap_kind ? `，首个缺段：${d.gap_kind}` : '，恢复链已受理';
    toast(`演练 #${d.id} 已发起${gap}`);
    ev.target.reset();
    setTimeout(refresh, 100);
  } catch (e) { toast(e.message, true); }
});

$('#btn-cleanup').addEventListener('click', async () => {
  try {
    const r = await api('/api/cleanup', { method: 'POST', body: {} });
    toast(`清理完成：删除 ${r.deleted_backup_ids.length} 个，在用延后 ${r.deferred_backup_ids.length} 个`);
    refresh();
  } catch (e) { toast(e.message, true); }
});
$('#btn-refresh').addEventListener('click', refresh);

setInterval(refresh, 2000);
refresh();
