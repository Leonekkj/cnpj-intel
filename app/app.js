// ─── CNPJ Intel Dashboard ───────────────────────────────────────

const API_BASE = location.hostname === "localhost" ? "http://localhost:8000" : "";
const TOKEN = new URLSearchParams(location.search).get("token") || localStorage.getItem("cnpj_token") || "";
if (TOKEN) localStorage.setItem("cnpj_token", TOKEN);
const H = TOKEN ? { "Authorization": "Bearer " + TOKEN } : {};

async function apiFetch(path, opts = {}) {
  try {
    const r = await fetch(API_BASE + path, { headers: { ...H, ...(opts.headers || {}) }, ...opts });
    if (!r.ok) return { _err: r.status, _body: await r.json().catch(() => ({})) };
    return await r.json();
  } catch (e) { return { _err: 0 }; }
}

// ─── Icons ──────────────────────────────────────────────────────
const ICONS = {
  trend:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>',
  users:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M16 20v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 20v-2a4 4 0 0 0-3-3.9M16 3.1a4 4 0 0 1 0 7.8"/></svg>',
  bolt:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>',
  target:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg>',
  heart:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M20.8 4.6a5.5 5.5 0 0 0-7.8 0L12 5.7l-1.1-1.1a5.5 5.5 0 0 0-7.8 7.8l1.1 1.1L12 21.2l7.8-7.8 1.1-1.1a5.5 5.5 0 0 0 0-7.8z"/></svg>',
  check:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M22 11.1V12a10 10 0 1 1-5.9-9.1"/><polyline points="22 4 12 14.1 9 11.1"/></svg>',
  phone:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M22 16.9v3a2 2 0 0 1-2.2 2 19.8 19.8 0 0 1-8.6-3.1 19.5 19.5 0 0 1-6-6 19.8 19.8 0 0 1-3.1-8.7A2 2 0 0 1 4.1 2h3a2 2 0 0 1 2 1.7 12.8 12.8 0 0 0 .7 2.8 2 2 0 0 1-.5 2.1L8 9.9a16 16 0 0 0 6 6l1.3-1.3a2 2 0 0 1 2.1-.5 12.8 12.8 0 0 0 2.8.7A2 2 0 0 1 22 16.9z"/></svg>',
  mail:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z"/><polyline points="22 6 12 13 2 6"/></svg>',
  globe:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="10"/><path d="M2 12h20M12 2a15 15 0 0 1 0 20M12 2a15 15 0 0 0 0 20"/></svg>',
  insta:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="2" y="2" width="20" height="20" rx="5"/><path d="M16 11.4a4 4 0 1 1-8 1.1 4 4 0 0 1 8-1.1z"/><line x1="17.5" y1="6.5" x2="17.5" y2="6.5"/></svg>',
  building: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M3 21h18M5 21V7l7-4 7 4v14M9 9h.01M9 12h.01M9 15h.01M15 9h.01M15 12h.01M15 15h.01"/></svg>',
  dollar:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
  download: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>',
  filter:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polygon points="22 3 2 3 10 12.5 10 19 14 21 14 12.5 22 3"/></svg>',
  lock:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="3" y="11" width="18" height="11" rx="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>',
  expand:   '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="9 18 15 12 9 6"/></svg>',
  more:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="1.4"/><circle cx="12" cy="5" r="1.4"/><circle cx="12" cy="19" r="1.4"/></svg>',
  bookmark: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M19 21l-7-5-7 5V5a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z"/></svg>',
  sparkles: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 3v4M12 17v4M3 12h4M17 12h4M5.6 5.6l2.8 2.8M15.6 15.6l2.8 2.8M5.6 18.4l2.8-2.8M15.6 8.4l2.8-2.8"/></svg>',
  up:       '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M7 14l5-5 5 5z"/></svg>',
  down:     '<svg viewBox="0 0 24 24" fill="currentColor"><path d="M7 10l5 5 5-5z"/></svg>',
  copy:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>',
  key:      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M21 2l-2 2m-7.6 7.6a5 5 0 1 1-7.1 7.1 5 5 0 0 1 7.1-7.1zm0 0L15.5 8.4m0 0 3 3L22 8l-3-3m-3.5 3.5L19 4"/></svg>',
  trash:    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polyline points="3 6 5 6 21 6"/><path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/></svg>',
  plus:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><line x1="12" y1="5" x2="12" y2="19"/><line x1="5" y1="12" x2="19" y2="12"/></svg>',
  wand:     '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M15 4V2M15 16v-2M8 9h2M20 9h2M17.8 11.8 19 13M15 9h.01M17.8 6.2 19 5M3 21l9-9"/></svg>',
};

// ─── State ──────────────────────────────────────────────────────
const state = {
  tab: "dashboard",
  density: "normal",
  radius: "soft",
  plan: "pro",
  planInfo: null,
  statsData: null,
  atividadeData: null,
  dados: [],
  totalDados: 0,
  loading: false,
  categories: [],
  departamentos: [],
  tokens: [],
  tokensLoading: false,
  filters: { q: "", uf: "", porte: "", categoria: "", departamento: "", tel: false, email: false, site: false, insta: false, abertura_de: "", abertura_ate: "" },
  sort: { key: null, dir: "asc" },
  page: 1,
  perPage: 15,
  selected: new Set(),
  expanded: new Set(),
  expandedData: {},
};

// ─── Dashboard mock data (awaiting dedicated backend endpoints) ──
const DASH_MOCK = {
  activity: Array.from({ length: 30 }, (_, i) => ({
    day: i + 1,
    coletadas: Math.round(150 + 40 * Math.sin(i * 0.4) + (i * 2) + Math.random() * 15),
    enriquecidas: Math.round(100 + 30 * Math.sin(i * 0.4 + 1) + (i * 1.5) + Math.random() * 10),
  })),
  insights: [
    { tone: "ac", ico: "trend",  title: "Agente de coleta ativo",    sub: "Enriquecendo CNPJs em tempo real — telefones, e-mails e sites.", time: "agora" },
    { tone: "in", ico: "users",  title: "Base crescendo",            sub: "Novos CNPJs sendo adicionados a cada ciclo de processamento.", time: "há pouco" },
    { tone: "wa", ico: "bolt",   title: "Alta taxa de telefones",    sub: "~75% dos CNPJs processados têm telefone confirmado.", time: "hoje" },
    { tone: "pu", ico: "target", title: "Cobertura nacional",        sub: "Empresas de todos os estados brasileiros indexadas na base.", time: "este mês" },
    { tone: "pk", ico: "heart",  title: "Qualidade de dados",        sub: "Deduplicação e limpeza automática de URLs inválidas.", time: "este mês" },
  ],
  sparks: {
    coletadas:    [120, 145, 128, 162, 171, 158, 143, 179, 195, 183, 201, 188, 176, 199],
    enriquecidas: [80,  98,  85,  108, 114, 105, 95,  119, 130, 122, 134, 125, 117, 132],
    contatos:     [60,  72,  58,  78,  82,  76,  68,  86,  94,  88,  96,  90,  84,  95],
    export:       [20,  35,  22,  28,  41,  38,  30,  45,  52,  48,  56,  50,  42,  58],
  },
  porteBreak: [
    { label: "MEI",          value: 987, color: "in" },
    { label: "Micro Empresa",value: 743, color: "" },
    { label: "EPP",          value: 312, color: "pu" },
    { label: "Demais",       value: 129, color: "wa" },
  ],
  setorBreak: [
    { label: "Comércio",     value: 823, color: "" },
    { label: "Serviços",     value: 651, color: "in" },
    { label: "Alimentação",  value: 398, color: "wa" },
    { label: "Saúde",        value: 287, color: "pk" },
    { label: "Tecnologia",   value: 196, color: "pu" },
  ],
  ranking: [
    { uf: "SP", count: 847 }, { uf: "MG", count: 512 }, { uf: "RJ", count: 398 },
    { uf: "RS", count: 276 }, { uf: "PR", count: 241 }, { uf: "BA", count: 198 },
  ],
};

// ─── Helpers ────────────────────────────────────────────────────
const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];
const fmt = n => (n || 0).toLocaleString("pt-BR");
const fmtK = n => n >= 1e6 ? (n / 1e6).toFixed(1) + "M" : n >= 1e3 ? (n / 1e3).toFixed(0) + "K" : String(n || 0);
const fmtCNPJ = c => c && c.length >= 14 ? c.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5") : (c || "");
const initials = s => (s || "?").split(" ").filter(Boolean).slice(0, 2).map(w => w[0]).join("").toUpperCase();
const fmtDate = s => {
  if (!s) return "—";
  if (/^\d{2}\//.test(s)) return s;
  const p = (s || "").split("-");
  return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : "—";
};
function timeAgo(iso) {
  if (!iso) return "—";
  const diff = (Date.now() - new Date(iso)) / 1000;
  if (diff < 60)    return "agora";
  if (diff < 3600)  return `há ${Math.round(diff / 60)}min`;
  if (diff < 86400) return `há ${Math.round(diff / 3600)}h`;
  return `há ${Math.round(diff / 86400)}d`;
}
function debounce(fn, ms) { let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); }; }


function porteBadge(p) {
  if (!p) return '<span class="badge de">—</span>';
  if (p === "MEI")              return '<span class="badge mei">MEI</span>';
  if (p.includes("MICRO"))      return '<span class="badge me">ME</span>';
  if (p.includes("PEQUENO"))    return '<span class="badge epp">EPP</span>';
  return '<span class="badge de">MÉDIO+</span>';
}

function planoBadge(plano, nomePlano) {
  const label = nomePlano || plano || "—";
  if (plano === "admin") return `<span class="badge mei">${label}</span>`;
  if (plano === "pro")   return `<span class="badge epp">${label}</span>`;
  if (plano === "basico") return `<span class="badge me">${label}</span>`;
  return `<span class="badge de">${label}</span>`;
}

// ─── API functions ───────────────────────────────────────────────
async function loadPlan() {
  const data = await apiFetch("/api/meu-plano");
  if (!data || data._err) return;
  state.planInfo = data;
  state.plan = data.plano;
  updateSidebar();
}

function _patchMetric(id, newVal, changed) {
  const el = $(`#${id}`);
  if (!el) return;
  el.textContent = newVal;
  if (changed) {
    el.classList.remove("metric-val-updated");
    void el.offsetWidth; // force reflow to restart animation
    el.classList.add("metric-val-updated");
    el.addEventListener("animationend", () => el.classList.remove("metric-val-updated"), { once: true });
  }
}

function _patchBreakdowns() {
  const stats = state.statsData || {};
  const _PD = {
    "MEI":                      { label: "MEI",    color: "in" },
    "MICRO EMPRESA":            { label: "ME",     color: ""   },
    "EMPRESA DE PEQUENO PORTE": { label: "EPP",    color: "pu" },
    "DEMAIS":                   { label: "Médio+", color: "wa" },
  };
  const barRow = (b, pct) =>
    `<div class="bar-row"><div class="bar-label">${b.label}</div><div class="bar-track"><div class="bar-fill ${b.color}" style="width:${pct}%"></div></div><div class="bar-val">${fmt(b.value)}</div></div>`;
  const rankRow = (r, i, pct) =>
    `<div class="rank-row"><div class="rank-num">${String(i+1).padStart(2,"0")}</div><div class="rank-name">${r.uf}</div><div class="rank-bar"><div class="rank-bar-fill" style="width:${pct}%"></div></div><div class="rank-val">${fmt(r.count)}</div></div>`;

  const porteBreak = (stats.por_porte || []).filter(p => p.porte)
    .map(p => { const d = _PD[p.porte] || { label: p.porte, color: "" }; return { label: d.label, value: p.n, color: d.color }; });
  const porteTotal = porteBreak.reduce((s, x) => s + x.value, 0) || 1;
  const bkP = $("#bk-porte-rows");
  if (bkP) bkP.innerHTML = porteBreak.map(b => barRow(b, (b.value / porteTotal * 100).toFixed(0))).join("");

  const setorBreak = (state.departamentos || [])
    .map(g => ({ label: g.setor, value: (g.departamentos || []).reduce((s, d) => s + (d.n || 0), 0), color: "" }))
    .sort((a, b) => b.value - a.value).slice(0, 5);
  const setorTotal = setorBreak.reduce((s, x) => s + x.value, 0) || 1;
  const bkS = $("#bk-setor-rows");
  if (bkS) bkS.innerHTML = setorBreak.map(b => barRow(b, (b.value / setorTotal * 100).toFixed(0))).join("");

  const ranking = (stats.por_uf || []).map(r => ({ uf: r.uf, count: r.n })).slice(0, 6);
  const rankTotal = ranking[0]?.count || 1;
  const bkU = $("#map-ranks");
  if (bkU) bkU.innerHTML = ranking.map((r, i) => rankRow(r, i, (r.count / rankTotal * 100).toFixed(0))).join("");
}

async function loadStats() {
  const data = await apiFetch("/api/stats");
  if (!data || data._err) return;

  const prev = state.statsData;
  state.statsData = data;

  const navTotal = $("#nav-total");
  if (navTotal) navTotal.textContent = fmtK(data.total);
  const agSub = $("#agent-sub");
  if (agSub && data.progresso_agente !== undefined)
    agSub.textContent = `Posição ${fmt(data.progresso_agente)}`;

  if (state.tab !== "dashboard") return;

  // Targeted patch when metric elements are already in the DOM (avoid full re-render)
  if (prev && $("#mv-total")) {
    _patchMetric("mv-total", fmt(data.total),        prev.total        !== data.total);
    _patchMetric("mv-tel",   fmt(data.com_telefone), prev.com_telefone !== data.com_telefone);
    _patchMetric("mv-email", fmt(data.com_email),    prev.com_email    !== data.com_email);
    _patchBreakdowns();
  } else {
    render();
  }
}

async function loadAtividade() {
  const data = await apiFetch("/api/atividade");
  if (!data || data._err) return;
  state.atividadeData = data;
  if (state.tab === "dashboard") render();
}

async function loadEmpresas() {
  state.loading = true;
  render();
  const f = state.filters;
  const params = new URLSearchParams({ pagina: state.page, por_pagina: state.perPage, com_contato: "true" });
  if (f.q)          params.set("q", f.q);
  if (f.uf)         params.set("uf", f.uf);
  if (f.porte)      params.set("porte", f.porte);
  if (f.departamento) params.set("departamento", f.departamento);
  else if (f.categoria) params.set("categoria", f.categoria);
  if (f.tel)        params.set("com_telefone", "true");
  if (f.email)      params.set("com_email", "true");
  if (f.site)       params.set("com_site", "true");
  if (f.insta)      params.set("com_instagram", "true");
  if (f.abertura_de)  params.set("abertura_de", f.abertura_de);
  if (f.abertura_ate) params.set("abertura_ate", f.abertura_ate);
  const data = await apiFetch(`/api/empresas?${params}`);
  if (data && !data._err) {
    state.dados = data.dados || [];
    state.totalDados = data.total || 0;
  } else {
    state.dados = [];
    state.totalDados = 0;
  }
  state.loading = false;
  render();
}

async function loadDetail(cnpj) {
  if (state.expandedData[cnpj] === "LOADING" || (state.expandedData[cnpj] && !state.expandedData[cnpj]._notfound)) return;
  state.expandedData[cnpj] = "LOADING"; // mark as loading
  render();
  const data = await apiFetch(`/api/empresa/${cnpj}`);
  state.expandedData[cnpj] = (data && !data._err) ? data : { _notfound: true };
  render();
  loadPlan();
}

async function loadCategories() {
  const FALLBACK_CATS = ["Alimentação","Saúde","Beleza","Tecnologia","Educação","Serviços","Comércio","Construção","Transporte","Agro"];
  const [cats, deptos] = await Promise.all([
    apiFetch("/api/categorias"),
    apiFetch("/api/departamentos"),
  ]);
  if (cats && !cats._err && Array.isArray(cats) && cats.length > 0) {
    state.categories = cats.map(d => d.categoria || d).filter(Boolean);
  } else {
    state.categories = FALLBACK_CATS;
  }
  if (deptos && !deptos._err && Array.isArray(deptos)) {
    state.departamentos = deptos;
    if (state.tab === "dashboard" && $("#bk-setor-rows")) _patchBreakdowns();
  }
}

async function exportCSV() {
  if (state.plan === "free") {
    alert("Exportação disponível nos planos Básico e Pro.");
    return;
  }
  const f = state.filters;
  const params = new URLSearchParams();
  if (f.q)          params.set("q", f.q);
  if (f.uf)         params.set("uf", f.uf);
  if (f.porte)      params.set("porte", f.porte);
  if (f.departamento) params.set("departamento", f.departamento);
  else if (f.categoria) params.set("categoria", f.categoria);
  if (f.tel)        params.set("com_telefone", "true");
  if (f.email)      params.set("com_email", "true");
  if (f.site)       params.set("com_site", "true");
  if (f.insta)      params.set("com_instagram", "true");
  if (f.abertura_de)  params.set("abertura_de", f.abertura_de);
  if (f.abertura_ate) params.set("abertura_ate", f.abertura_ate);
  const url = `${API_BASE}/api/export?${params}`;
  const a = document.createElement("a");
  a.href = url;
  a.setAttribute("download", `cnpj_intel_${new Date().toISOString().slice(0, 10)}.csv`);
  const headers = TOKEN ? { "Authorization": `Bearer ${TOKEN}` } : {};
  try {
    const r = await fetch(url, { headers });
    if (!r.ok) { alert("Erro ao exportar. Verifique seu plano."); return; }
    const blob = await r.blob();
    const objUrl = URL.createObjectURL(blob);
    a.href = objUrl;
    document.body.appendChild(a);
    a.click();
    setTimeout(() => { URL.revokeObjectURL(objUrl); a.remove(); }, 1000);
  } catch (e) { alert("Erro ao exportar CSV."); }
}

async function loadTokens() {
  state.tokensLoading = true;
  render();
  const data = await apiFetch("/api/admin/tokens");
  state.tokens = Array.isArray(data) ? data : [];
  state.tokensLoading = false;
  render();
}

async function criarToken(tokenVal, plano) {
  const params = new URLSearchParams({ token: tokenVal, plano });
  const data = await apiFetch(`/api/admin/tokens?${params}`, { method: "POST" });
  if (data && !data._err) {
    await loadTokens();
    return data;
  }
  alert("Erro ao criar token. Verifique os dados.");
  return null;
}

async function deletarToken(token) {
  if (!confirm(`Remover token "${token}"?`)) return;
  await apiFetch(`/api/admin/tokens/${encodeURIComponent(token)}`, { method: "DELETE" });
  await loadTokens();
}

// ─── Sidebar update ──────────────────────────────────────────────
function updateSidebar() {
  const info = state.planInfo;
  if (!info) return;

  const elName  = $("#plan-name");
  const elBadge = $("#plan-badge");
  const elBar   = $("#plan-meter-bar");
  const elStats = $("#plan-stats");
  const elUser  = $("#user-name");
  const elSub   = $("#user-sub");
  const elAvatar = $("#user-avatar");

  const nomePlano = info.nome_plano || info.plano || "Pro";
  if (elName)  elName.textContent  = `Plano ${nomePlano}`;
  if (elBadge) elBadge.textContent = "Ativo";

  let pct = 0, statsText = "";
  if (info.limite_dia) {
    pct = Math.min(100, (info.cnpjs_hoje / info.limite_dia) * 100);
    statsText = `${fmt(info.cnpjs_hoje)} / ${fmt(info.limite_dia)} hoje`;
    if (elBar) {
      elBar.style.width = pct + "%";
      elBar.style.background = pct >= 90
        ? "linear-gradient(90deg, var(--danger) 0%, oklch(0.85 0.17 25) 100%)"
        : pct >= 70
        ? "linear-gradient(90deg, var(--warn) 0%, oklch(0.90 0.14 75) 100%)"
        : "";
    }
  } else {
    statsText = `${fmt(info.cnpjs_hoje)} / ilimitado hoje`;
    if (elBar) { elBar.style.width = "0%"; elBar.style.background = ""; }
  }
  if (elStats) elStats.innerHTML = `<span class="mono">${statsText}</span>`;

  if (elUser)  elUser.textContent  = nomePlano;
  if (elSub)   elSub.textContent   = info.plano;
  if (elAvatar) elAvatar.textContent = nomePlano.slice(0, 2).toUpperCase();

  // Show admin section
  const adminSection  = $("#admin-section");
  const navClientes   = $("#nav-clientes");
  if (info.plano === "admin") {
    if (adminSection)  adminSection.style.display = "";
    if (navClientes)   navClientes.style.display  = "";
  }
}

// ─── Routing ────────────────────────────────────────────────────
const PAGE_TITLES = {
  dashboard: { t: "Visão geral",   s: "Painel de controle e insights em tempo real" },
  empresas:  { t: "Empresas",      s: "Todas as empresas enriquecidas na sua base" },
  busca:     { t: "Busca avançada",s: "Filtros combinados para prospecção precisa" },
  listas:    { t: "Minhas listas", s: "Leads salvos e campanhas em andamento" },
  exportar:  { t: "Exportar CSV",  s: "Baixe listas filtradas em formato planilha" },
  api:       { t: "API & Webhooks",s: "Integre CNPJ Intel com seu CRM" },
  clientes:  { t: "Clientes",      s: "Gerenciar tokens e planos" },
};

function showTab(t) {
  state.tab = t;
  state.selected.clear();
  $$(".nav-item").forEach(n => n.classList.toggle("active", n.dataset.tab === t));
  const info = PAGE_TITLES[t] || PAGE_TITLES.dashboard;
  const crumb = $("#crumb-current");
  if (crumb) crumb.textContent = info.t;

  if (t === "empresas" || t === "busca") {
    loadEmpresas();
  } else if (t === "clientes") {
    render();
    loadTokens();
  } else {
    render();
  }
}

// ─── Render ─────────────────────────────────────────────────────
function render() {
  const c = $("#content");
  if (!c) return;
  const t = state.tab;
  if      (t === "dashboard") c.innerHTML = viewDashboard();
  else if (t === "empresas")  c.innerHTML = viewEmpresas();
  else if (t === "busca")     c.innerHTML = viewBusca();
  else if (t === "listas")    c.innerHTML = viewListas();
  else if (t === "exportar")  c.innerHTML = viewExport();
  else if (t === "api")       c.innerHTML = viewAPI();
  else if (t === "clientes")  c.innerHTML = viewClientes();
  wireContent();
}

// ─── Dashboard ──────────────────────────────────────────────────
function sparkline(data, color = "var(--accent)") {
  const w = 120, h = 30, max = Math.max(...data), min = Math.min(...data);
  const pts = data.map((v, i) => [
    (i / (data.length - 1)) * w,
    h - ((v - min) / (max - min || 1)) * h,
  ]);
  const d = pts.map((p, i) => (i === 0 ? "M" : "L") + p[0].toFixed(1) + "," + p[1].toFixed(1)).join(" ");
  const area = `M0,${h} ` + pts.map(p => `L${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ") + ` L${w},${h} Z`;
  const id = "sg_" + Math.random().toString(36).slice(2, 8);
  return `<svg class="spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
    <defs><linearGradient id="${id}" x1="0" y1="0" x2="0" y2="1">
      <stop offset="0%" stop-color="${color}" stop-opacity="0.22"/>
      <stop offset="100%" stop-color="${color}" stop-opacity="0"/>
    </linearGradient></defs>
    <path d="${area}" fill="url(#${id})"/>
    <path d="${d}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>
  </svg>`;
}

function viewDashboard() {
  const stats = state.statsData || { total: 0, com_telefone: 0, com_email: 0 };
  const activity = state.atividadeData || DASH_MOCK.activity;
  const { insights } = DASH_MOCK;
  const spark14 = activity.slice(-14);
  const sparks = {
    coletadas:    spark14.map(d => d.coletadas),
    enriquecidas: spark14.map(d => d.enriquecidas),
    contatos:     spark14.map(d => d.com_email || 0),
    export:       spark14.map(() => 0),
  };
  function pctDelta(arr) {
    const mid = Math.floor(arr.length / 2);
    const prev = arr.slice(0, mid).reduce((s, x) => s + x, 0);
    const curr = arr.slice(mid).reduce((s, x) => s + x, 0);
    if (!prev && !curr) return { val: "—", up: true };
    if (arr.filter(x => x > 0).length < 3) return { val: "—", up: true };
    const p = (curr - prev) / prev * 100;
    return { val: (p >= 0 ? "+" : "") + p.toFixed(1) + "%", up: p >= 0 };
  }
  const dTotal = pctDelta(sparks.coletadas);
  const dTel   = pctDelta(sparks.contatos);
  const dEmail = pctDelta(sparks.enriquecidas);

  const _PORTE_DISPLAY = {
    "MEI":                      { label: "MEI",    color: "in" },
    "MICRO EMPRESA":            { label: "ME",     color: ""   },
    "EMPRESA DE PEQUENO PORTE": { label: "EPP",    color: "pu" },
    "DEMAIS":                   { label: "Médio+", color: "wa" },
  };
  const porteBreak = (stats.por_porte || [])
    .filter(p => p.porte)
    .map(p => {
      const d = _PORTE_DISPLAY[p.porte] || { label: p.porte, color: "" };
      return { label: d.label, value: p.n, color: d.color };
    });

  const setorBreak = (state.departamentos || [])
    .map(g => ({ label: g.setor, value: (g.departamentos || []).reduce((s, d) => s + (d.n || 0), 0), color: "" }))
    .sort((a, b) => b.value - a.value)
    .slice(0, 5);

  const ranking = (stats.por_uf || [])
    .map(r => ({ uf: r.uf, count: r.n }))
    .slice(0, 6);

  const metric = (lbl, val, delta, up, spark, ico, color, id) => `
    <div class="metric">
      <div class="metric-head">
        <div class="metric-ico ${color}">${ICONS[ico]}</div>
        <div class="metric-lbl">${lbl}</div>
      </div>
      <div class="metric-val"${id ? ` id="${id}"` : ""}>${val}</div>
      <div class="metric-foot">
        <span class="delta ${up ? "up" : "down"}">${up ? ICONS.up : ICONS.down}${delta}</span>
        ${spark}
      </div>
    </div>`;

  const metrics = `
    <div class="metrics">
      ${metric("Total de CNPJs",  fmt(stats.total),         dTotal.val, dTotal.up, sparkline(sparks.coletadas,    "oklch(0.72 0.14 160)"), "building", "ac", "mv-total")}
      ${metric("Com telefone",    fmt(stats.com_telefone),  dTel.val,   dTel.up,   sparkline(sparks.contatos,     "oklch(0.74 0.13 240)"), "phone",    "in", "mv-tel")}
      ${metric("Com e-mail",      fmt(stats.com_email),     dEmail.val, dEmail.up, sparkline(sparks.enriquecidas, "oklch(0.80 0.14 75)"),  "mail",     "wa", "mv-email")}
      ${metric("Exports no mês",  "—",                      "—",      true,  sparkline(sparks.export,       "oklch(0.72 0.14 295)"), "download", "pu")}
    </div>`;

  // Activity chart
  const chart = (() => {
    const W = 720, H = 220, PL = 36, PB = 28, PT = 14, PR = 14;
    const iW = W - PL - PR, iH = H - PB - PT;
    const maxV = Math.max(...activity.map(d => Math.max(d.coletadas, d.enriquecidas))) || 1;
    const sx = i  => PL + (i / (activity.length - 1)) * iW;
    const sy = v  => PT + iH - (v / maxV) * iH;
    const path = key => {
      const pts = activity.map((d, i) => [sx(i), sy(d[key])]);
      return {
        line: pts.map((p, i) => (i ? "L" : "M") + p[0].toFixed(1) + "," + p[1].toFixed(1)).join(" "),
        area: `M${pts[0][0].toFixed(1)},${H - PB} ` + pts.map(p => `L${p[0].toFixed(1)},${p[1].toFixed(1)}`).join(" ") + ` L${pts[pts.length-1][0].toFixed(1)},${H-PB} Z`,
        pts,
      };
    };
    const col = path("coletadas");
    const enr = path("enriquecidas");
    const grid = [0, 0.25, 0.5, 0.75, 1].map(p => {
      const y = PT + iH * (1 - p);
      return `<line x1="${PL}" x2="${W-PR}" y1="${y}" y2="${y}" stroke="var(--border-soft)" stroke-dasharray="${p===0?"":"2 3"}"/>
        <text x="${PL-6}" y="${y+3}" fill="var(--text-dim)" font-size="9.5" font-family="var(--mono)" text-anchor="end">${fmtK(Math.round(maxV*p))}</text>`;
    }).join("");
    const xlabels = [0, 7, 14, 21, 29].map(i => {
      const raw = activity[i]?.data;
      const label = raw ? raw.slice(8,10) + "/" + raw.slice(5,7) : `d${i+1}`;
      return `<text x="${sx(i)}" y="${H-PB+16}" fill="var(--text-dim)" font-size="9.5" text-anchor="middle" font-family="var(--mono)">${label}</text>`;
    }).join("");
    const dots = col.pts.map((p, i) =>
      `<circle class="cd" data-i="${i}" data-col="${activity[i].coletadas}" data-enr="${activity[i].enriquecidas}" data-date="${activity[i].data || ''}" cx="${p[0]}" cy="${p[1]}" r="8" fill="transparent"/>`
    ).join("");
    return `<div class="panel chart-panel">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:2px">
        <div>
          <div class="panel-title">Atividade dos últimos 30 dias</div>
          <div class="panel-sub">Coletas e enriquecimentos diários</div>
        </div>
        <div class="chart-legend">
          <span><span class="leg-sw leg-ac"></span>Coletadas</span>
          <span><span class="leg-sw leg-in"></span>Enriquecidas</span>
        </div>
      </div>
      <div class="chart-body" id="chart-body">
        <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet">
          <defs>
            <linearGradient id="g-col" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="oklch(0.72 0.14 160)" stop-opacity="0.25"/>
              <stop offset="100%" stop-color="oklch(0.72 0.14 160)" stop-opacity="0"/>
            </linearGradient>
            <linearGradient id="g-enr" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="oklch(0.74 0.13 240)" stop-opacity="0.2"/>
              <stop offset="100%" stop-color="oklch(0.74 0.13 240)" stop-opacity="0"/>
            </linearGradient>
          </defs>
          ${grid}${xlabels}
          <path d="${col.area}" fill="url(#g-col)"/>
          <path d="${col.line}" stroke="oklch(0.72 0.14 160)" stroke-width="1.8" fill="none" stroke-linejoin="round"/>
          <path d="${enr.area}" fill="url(#g-enr)"/>
          <path d="${enr.line}" stroke="oklch(0.74 0.13 240)" stroke-width="1.8" fill="none" stroke-linejoin="round" stroke-dasharray="3 3"/>
          ${dots}
        </svg>
        <div class="chart-tip" id="chart-tip"></div>
      </div>
    </div>`;
  })();

  const insightsHtml = `
    <div class="panel">
      <div class="panel-head">
        <div>
          <div class="panel-title">Insights do agente</div>
          <div class="panel-sub">Sinais e oportunidades detectados</div>
        </div>
      </div>
      <div class="insights">
        ${insights.map(i => `
          <div class="insight">
            <div class="insight-ico ${i.tone}">${ICONS[i.ico]}</div>
            <div class="insight-txt">
              <div class="insight-title">${i.title}</div>
              <div class="insight-sub">${i.sub}</div>
              <div class="insight-time">${i.time}</div>
            </div>
          </div>`).join("")}
      </div>
    </div>`;

  const porteTotal = porteBreak.reduce((s, x) => s + x.value, 0) || 1;
  const setorTotal = setorBreak.reduce((s, x) => s + x.value, 0) || 1;
  const rankTotal  = ranking[0]?.count || 1;

  const breakdown = `
    <div class="breakdown">
      <div class="panel break-card">
        <div class="break-title">Distribuição por porte</div>
        <div id="bk-porte-rows">${porteBreak.map(b => `
          <div class="bar-row">
            <div class="bar-label">${b.label}</div>
            <div class="bar-track"><div class="bar-fill ${b.color}" style="width:${(b.value/porteTotal*100).toFixed(0)}%"></div></div>
            <div class="bar-val">${fmt(b.value)}</div>
          </div>`).join("")}</div>
      </div>
      <div class="panel break-card">
        <div class="break-title">Setores mais representados</div>
        <div id="bk-setor-rows">${setorBreak.map(b => `
          <div class="bar-row">
            <div class="bar-label">${b.label}</div>
            <div class="bar-track"><div class="bar-fill ${b.color}" style="width:${(b.value/setorTotal*100).toFixed(0)}%"></div></div>
            <div class="bar-val">${fmt(b.value)}</div>
          </div>`).join("")}</div>
      </div>
    </div>`;

  const mapCard = `
    <div class="panel map-card" style="margin-bottom:20px">
      <div style="display:flex;justify-content:space-between;align-items:center">
        <div>
          <div class="panel-title">Top estados</div>
          <div class="panel-sub">Ranking por volume de empresas coletadas</div>
        </div>
        <button class="btn btn-ghost" onclick="exportCSV()">${ICONS.download}Exportar</button>
      </div>
      <div class="map-ranks" id="map-ranks">
        ${ranking.map((r, i) => `
          <div class="rank-row">
            <div class="rank-num">${String(i+1).padStart(2,"0")}</div>
            <div class="rank-name">${r.uf}</div>
            <div class="rank-bar"><div class="rank-bar-fill" style="width:${(r.count/rankTotal*100).toFixed(0)}%"></div></div>
            <div class="rank-val">${fmt(r.count)}</div>
          </div>`).join("")}
      </div>
    </div>`;

  return `
    <div class="page-head">
      <div>
        <div class="page-title">Visão geral</div>
        <div class="page-sub">Painel de controle e insights em tempo real</div>
      </div>
      <div style="display:flex;gap:8px">
        <button class="btn btn-accent" onclick="exportCSV()">${ICONS.download}Exportar</button>
      </div>
    </div>
    ${metrics}
    <div class="dash-grid">${chart}${insightsHtml}</div>
    ${breakdown}
    ${mapCard}`;
}

// ─── Empresas view ───────────────────────────────────────────────
function viewEmpresas() {
  return `
    <div class="page-head">
      <div>
        <div class="page-title">Empresas</div>
        <div class="page-sub">Base enriquecida — <span class="mono" style="color:var(--text-muted)">${fmt(state.totalDados)}</span> resultados</div>
      </div>
      <div style="display:flex;gap:8px">
        <button class="btn btn-accent" onclick="exportCSV()">${ICONS.download}Exportar CSV</button>
      </div>
    </div>
    ${filterBar(false)}
    <div class="bulk-bar" id="bulk-bar">
      <span class="bulk-count"><strong>${state.selected.size}</strong> selecionadas</span>
      <button class="btn btn-ghost" style="font-size:12px" onclick="clearSelection()">Limpar seleção</button>
      <div class="bulk-actions">
        <button class="btn">${ICONS.bookmark}Salvar em lista</button>
        <button class="btn">${ICONS.mail}Campanha</button>
        <button class="btn btn-accent" onclick="exportCSV()">${ICONS.download}Exportar</button>
      </div>
    </div>
    ${tableCard()}`;
}

function viewBusca() {
  return `
    <div class="page-head">
      <div>
        <div class="page-title">Busca avançada</div>
        <div class="page-sub">Combinações precisas por setor, porte, data e localização — <span class="mono" style="color:var(--text-muted)">${fmt(state.totalDados)}</span> resultados</div>
      </div>
      <div style="display:flex;gap:8px">
        <button class="btn btn-accent" onclick="exportCSV()">${ICONS.download}Exportar CSV</button>
      </div>
    </div>
    ${filterBar(true)}
    <div class="bulk-bar" id="bulk-bar">
      <span class="bulk-count"><strong>${state.selected.size}</strong> selecionadas</span>
      <button class="btn btn-ghost" style="font-size:12px" onclick="clearSelection()">Limpar seleção</button>
      <div class="bulk-actions">
        <button class="btn btn-accent" onclick="exportCSV()">${ICONS.download}Exportar</button>
      </div>
    </div>
    ${tableCard()}`;
}

// ─── Filter bar ─────────────────────────────────────────────────
function filterBar(showDates = false) {
  const f = state.filters;
  const ufs = ["AC","AL","AM","AP","BA","CE","DF","ES","GO","MA","MG","MS","MT","PA","PB","PE","PI","PR","RJ","RN","RO","RR","RS","SC","SE","SP","TO"];
  const cats = state.categories.length ? state.categories
    : ["Alimentação","Saúde","Beleza","Tecnologia","Educação","Serviços","Comércio","Construção","Transporte","Agro"];

  return `
    <div class="filter-bar">
      <div class="filter-search">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/></svg>
        <input type="text" placeholder="Empresa, CNPJ, cidade, sócio…" value="${f.q}" id="filter-q" oninput="debouncedQ(this.value)">
      </div>
      <div class="chip-sep"></div>
      <div class="chip select">
        <select onchange="updateFilter('uf', this.value)">
          <option value="">Estado</option>
          ${ufs.map(u => `<option ${f.uf===u?"selected":""}>${u}</option>`).join("")}
        </select>
      </div>
      <div class="chip select">
        <select onchange="updateFilter('porte', this.value)">
          <option value="">Porte</option>
          <option value="MEI" ${f.porte==="MEI"?"selected":""}>MEI</option>
          <option value="MICRO EMPRESA" ${f.porte==="MICRO EMPRESA"?"selected":""}>ME</option>
          <option value="EMPRESA DE PEQUENO PORTE" ${f.porte==="EMPRESA DE PEQUENO PORTE"?"selected":""}>EPP</option>
          <option value="DEMAIS" ${f.porte==="DEMAIS"?"selected":""}>Médio+</option>
        </select>
      </div>
      <div class="chip select">
        <select onchange="onSetorChange(this.value)">
          <option value="">Setor</option>
          ${cats.map(c => `<option ${f.categoria===c?"selected":""}>${c}</option>`).join("")}
        </select>
      </div>
      ${(() => {
        const setorSel = f.categoria;
        const deptoGroup = state.departamentos.find(g => g.setor === setorSel);
        const deptos = deptoGroup ? deptoGroup.departamentos : [];
        if (!setorSel || deptos.length === 0) return "";
        return `<div class="chip select">
          <select onchange="updateFilter('departamento', this.value)">
            <option value="">Departamento</option>
            ${deptos.map(d => `<option ${f.departamento===d.departamento?"selected":""}>${d.departamento}</option>`).join("")}
          </select>
        </div>`;
      })()}
      <div class="chip-sep"></div>
      <button class="chip ${f.tel?"on":""}"   onclick="toggleF('tel')">${ICONS.phone}Com telefone</button>
      <button class="chip ${f.email?"on in":""}" onclick="toggleF('email')">${ICONS.mail}Com e-mail</button>
      <button class="chip ${f.site?"on pu":""}"  onclick="toggleF('site')">${ICONS.globe}Com site</button>
      <button class="chip ${f.insta?"on wa":""}" onclick="toggleF('insta')">${ICONS.insta}Instagram</button>
      ${showDates ? `
      <div class="chip-sep"></div>
      <div class="chip select" style="gap:6px">
        <span style="font-size:10.5px;color:var(--text-dim)">Abertura de</span>
        <input type="date" value="${f.abertura_de}" onchange="updateFilter('abertura_de',this.value)" style="border:none;background:transparent;color:inherit;font-size:12px;cursor:pointer;outline:none">
      </div>
      <div class="chip select" style="gap:6px">
        <span style="font-size:10.5px;color:var(--text-dim)">até</span>
        <input type="date" value="${f.abertura_ate}" onchange="updateFilter('abertura_ate',this.value)" style="border:none;background:transparent;color:inherit;font-size:12px;cursor:pointer;outline:none">
      </div>` : ""}
      <div style="flex:1"></div>
      <button class="btn btn-ghost" onclick="clearFilters()">Limpar</button>
    </div>`;
}

// ─── Table ───────────────────────────────────────────────────────
function tableCard() {
  if (state.loading) {
    return `<div class="panel"><div class="empty-state"><div class="big">Carregando…</div></div></div>`;
  }

  let rows = [...state.dados];
  if (state.sort.key) {
    const k = state.sort.key, dir = state.sort.dir === "asc" ? 1 : -1;
    rows.sort((a, b) => String(a[k] || "").localeCompare(String(b[k] || ""), "pt-BR") * dir);
  }

  const total = state.totalDados;
  const pages = Math.max(1, Math.ceil(total / state.perPage));
  const start = (state.page - 1) * state.perPage;

  const pageRowIds = rows.map(r => r.cnpj);
  const allSel = pageRowIds.length > 0 && pageRowIds.every(id => state.selected.has(id));
  const someSel = pageRowIds.some(id => state.selected.has(id));

  const sortTh = (key, label, extra = "") => {
    const isSorted = state.sort.key === key;
    const cls = isSorted ? (state.sort.dir === "asc" ? "sort-asc" : "sort-desc") : "";
    return `<th class="sortable ${cls}" ${extra} onclick="sortBy('${key}')">
      <span class="th-inner">${label}<span class="sort-ico">${ICONS.up}${ICONS.down}</span></span>
    </th>`;
  };

  const idsJson = JSON.stringify(pageRowIds).replace(/"/g, "&quot;");

  return `
  <div class="panel density-${state.density}">
    <div class="table-wrap">
      <table class="data">
        <thead>
          <tr>
            <th style="width:30px;padding-left:16px">
              <input type="checkbox" class="checkbox ${someSel && !allSel ? "indet" : ""}" ${allSel ? "checked" : ""} onchange="toggleAllPage(${idsJson})">
            </th>
            ${sortTh("razao_social", "Empresa / Setor")}
            ${sortTh("cnpj", "CNPJ")}
            ${sortTh("porte", "Porte")}
            ${sortTh("municipio", "Localização")}
            ${sortTh("abertura", "Abertura")}
            <th>Contato</th>
            ${sortTh("socio_principal", "Sócio principal")}
            <th style="width:90px;text-align:right;padding-right:16px"></th>
          </tr>
        </thead>
        <tbody>
          ${rows.length === 0
            ? `<tr><td colspan="9"><div class="empty-state"><div class="big">Nenhum resultado encontrado</div>Ajuste os filtros e tente novamente.</div></td></tr>`
            : rows.map(row).join("")}
        </tbody>
      </table>
    </div>
    <div class="pager">
      <span>Mostrando <span class="mono" style="color:var(--text-muted)">${start+1}–${Math.min(start+state.perPage, total)}</span> de <span class="mono" style="color:var(--text-muted)">${fmt(total)}</span></span>
      <div class="pager-btns">${pagerBtns(pages)}</div>
    </div>
  </div>`;
}

function pagerBtns(pages) {
  const cur = state.page;
  let out = `<button class="page-btn" ${cur <= 1 ? "disabled" : ""} onclick="goPage(${cur-1})">‹</button>`;
  const push = p => out += `<button class="page-btn ${p===cur?"active":""}" onclick="goPage(${p})">${p}</button>`;
  const el   = () => out += `<span class="page-ellipsis">…</span>`;
  const s = Math.max(1, cur-2), e = Math.min(pages, cur+2);
  if (s > 1) { push(1); if (s > 2) el(); }
  for (let p = s; p <= e; p++) push(p);
  if (e < pages) { if (e < pages-1) el(); push(pages); }
  out += `<button class="page-btn" ${cur >= pages ? "disabled" : ""} onclick="goPage(${cur+1})">›</button>`;
  return out;
}

function row(d) {
  const selected = state.selected.has(d.cnpj);
  const expanded = state.expanded.has(d.cnpj);
  const displayName = d.nome_fantasia || d.razao_social || "—";
  const isFree = state.plan === "free" || state.plan === "basico";
  const telCel = d.telefone
    ? `<span class="contact-pill ac${isFree ? " masked" : ""}">${ICONS.phone}${d.telefone}</span>`
    : `<span class="contact-em">—</span>`;
  const emCel = d.email
    ? `<span class="contact-pill in${isFree ? " masked" : ""}">${ICONS.mail}${d.email.length > 22 ? d.email.slice(0,22)+"…" : d.email}</span>`
    : "";
  return `
    <tr class="${selected ? "selected" : ""} ${expanded ? "expanded" : ""}">
      <td style="padding-left:16px"><input type="checkbox" class="checkbox" ${selected?"checked":""} onchange="toggleSelect('${d.cnpj}')"></td>
      <td>
        <div class="co-cell">
          <div class="co-avatar">${initials(displayName)}</div>
          <div class="co-main">
            <div class="co-name">${displayName}</div>
            <div class="co-cnae">${d.cnae || ""}</div>
          </div>
        </div>
      </td>
      <td class="mono-cell">${fmtCNPJ(d.cnpj)}</td>
      <td>${porteBadge(d.porte)}</td>
      <td class="city-cell"><span class="c-name">${d.municipio || "—"}</span><span class="c-uf">${d.uf || ""}</span></td>
      <td class="mono-cell">${fmtDate(d.abertura)}</td>
      <td><div style="display:flex;gap:4px;flex-wrap:wrap">${telCel}${emCel}</div></td>
      <td style="color:var(--text-soft);font-size:12px">${d.socio_principal ? `<span class="${isFree ? "masked" : ""}" style="color:var(--text-soft)">${d.socio_principal}</span>` : "—"}</td>
      <td style="text-align:right;padding-right:16px">
        <div class="row-actions">
          <button class="row-btn" title="Mais ações">${ICONS.more}</button>
          <button class="row-btn expand" title="Expandir" onclick="toggleExpand('${d.cnpj}')">${ICONS.expand}</button>
        </div>
      </td>
    </tr>
    ${expanded ? detailRow(d.cnpj, d) : ""}`;
}

function detailRow(cnpj, baseData) {
  const det = state.expandedData[cnpj];

  if (det === undefined || det === null || det === "LOADING") {
    return `<tr class="detail-row"><td colspan="9"><div class="detail-loading">Carregando detalhes…</div></td></tr>`;
  }
  if (det._notfound) {
    return `<tr class="detail-row"><td colspan="9"><div class="detail-loading">Detalhes não disponíveis.</div></td></tr>`;
  }

  const d = { ...baseData, ...det };
  const isFree = state.plan === "free" || state.plan === "basico";
  const telLink = d.telefone ? `https://wa.me/55${d.telefone.replace(/\D/g,"")}` : null;
  const siteUrl = d.site ? (d.site.startsWith("http") ? d.site : "https://" + d.site) : null;

  const telVal   = d.telefone  ? `<span style="color:var(--accent-hi)">${d.telefone}</span>`   : `<span class="contact-em">Não encontrado</span>`;
  const emailVal = d.email     ? `<span style="color:var(--info)">${d.email}</span>`             : `<span class="contact-em">Não encontrado</span>`;
  const siteVal  = d.site      ? `<span style="color:var(--purple)">${d.site}</span>`            : `<span class="contact-em">—</span>`;
  const instaVal = d.instagram ? `<span style="color:var(--pink)">${d.instagram}</span>`         : `<span class="contact-em">—</span>`;

  return `
  <tr class="detail-row">
    <td colspan="9">
      <div class="detail-inner">
        <div class="detail-col">
          <h4>Informações cadastrais</h4>
          <div class="detail-bar">
            <span class="kv">Razão social <strong>${d.razao_social || "—"}</strong></span>
            <span class="sep"></span>
            <span class="kv">Situação <strong style="color:var(--accent-hi)">${d.situacao || "—"}</strong></span>
            <span class="sep"></span>
            <span class="kv">Atualizado <strong>${timeAgo(d.atualizado_em)}</strong></span>
          </div>
          <div class="detail-field"><div class="k">CNAE principal</div><div class="v">${d.cnae || "—"}</div></div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
            <div class="detail-field"><div class="k">Abertura</div><div class="v mono">${fmtDate(d.abertura)}</div></div>
            <div class="detail-field"><div class="k">Cidade / UF</div><div class="v">${d.municipio || "—"} — ${d.uf || "—"}</div></div>
          </div>
        </div>
        <div class="detail-col">
          <h4>Contatos</h4>
          <div class="detail-field"><div class="k">Telefone</div><div class="v">${telVal}</div></div>
          <div class="detail-field"><div class="k">E-mail</div><div class="v" style="word-break:break-all">${emailVal}</div></div>
          <div class="detail-field"><div class="k">Site</div><div class="v">${siteVal}</div></div>
          <div class="detail-field"><div class="k">Instagram</div><div class="v">${instaVal}</div></div>
        </div>
        <div class="detail-col">
          <h4>Quadro societário</h4>
          <div class="detail-field"><div class="k">Sócio principal</div><div class="v">${d.socio_principal || "—"}</div></div>
          <div class="detail-actions">
            ${telLink  ? `<a href="${telLink}" target="_blank" class="btn">${ICONS.phone}WhatsApp</a>` : ""}
            ${d.email  ? `<a href="mailto:${d.email}" class="btn">${ICONS.mail}E-mail</a>` : ""}
            ${siteUrl  ? `<a href="${siteUrl}" target="_blank" class="btn">${ICONS.globe}Site</a>` : ""}
            <button class="btn" onclick="navigator.clipboard.writeText('${fmtCNPJ(d.cnpj)}')">${ICONS.copy}Copiar CNPJ</button>
          </div>
        </div>
      </div>
    </td>
  </tr>`;
}

// ─── Other views ─────────────────────────────────────────────────
function viewListas() {
  const listas = [
    { nome: "Restaurantes SP — Campanha Q2", cnt: 142, cor: "ac", upd: "Em breve" },
    { nome: "Clínicas odonto premium",       cnt: 87,  cor: "in", upd: "Em breve" },
    { nome: "Academias MG/RJ",               cnt: 56,  cor: "wa", upd: "Em breve" },
    { nome: "Agências de marketing",          cnt: 214, cor: "pu", upd: "Em breve" },
  ];
  return `
    <div class="page-head">
      <div><div class="page-title">Minhas listas</div><div class="page-sub">Leads salvos organizados por campanha</div></div>
      <button class="btn btn-accent">${ICONS.plus}Nova lista</button>
    </div>
    <div class="panel" style="padding:32px;text-align:center;color:var(--text-dim);margin-bottom:20px">
      <div style="font-size:14px;color:var(--text-muted);margin-bottom:8px">Listas em breve</div>
      Salve leads da tabela de empresas e organize-os em listas personalizadas.
    </div>
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:14px;opacity:0.4;pointer-events:none">
      ${listas.map(l => `
        <div class="panel" style="padding:18px">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px">
            <div class="insight-ico ${l.cor}">${ICONS.bookmark}</div>
          </div>
          <div style="font-weight:600;margin-bottom:4px">${l.nome}</div>
          <div style="font-size:11.5px;color:var(--text-dim)">${l.cnt} empresas · ${l.upd}</div>
        </div>`).join("")}
    </div>`;
}

function viewExport() {
  const canExport = state.planInfo && state.planInfo.export;
  return `
    <div class="page-head">
      <div><div class="page-title">Exportar CSV</div><div class="page-sub">Baixe listas filtradas no formato planilha</div></div>
    </div>
    <div class="panel" style="padding:40px;text-align:center">
      ${canExport ? `
        <div style="font-size:14px;font-weight:600;margin-bottom:8px">Exportar base atual</div>
        <div style="color:var(--text-dim);margin-bottom:20px">Use os filtros nas páginas <strong>Empresas</strong> ou <strong>Busca avançada</strong> e clique em "Exportar CSV".</div>
        <button class="btn btn-accent" onclick="exportCSV()">${ICONS.download}Exportar CSV agora</button>
      ` : `
        <div style="font-size:14px;font-weight:600;margin-bottom:8px;color:var(--text-muted)">Exportação não disponível</div>
        <div style="color:var(--text-dim)">Faça upgrade para o plano Básico ou Pro para exportar listas em CSV.</div>
      `}
    </div>`;
}

function viewAPI() {
  const canUseAPI = state.planInfo && state.planInfo.api;
  return `
    <div class="page-head"><div><div class="page-title">API & Webhooks</div><div class="page-sub">Integre CNPJ Intel com seu CRM ou sistema</div></div></div>
    <div class="panel" style="padding:32px">
      ${canUseAPI ? `
        <div style="margin-bottom:16px">
          <div class="panel-title" style="margin-bottom:8px">Seu token de API</div>
          <div style="display:flex;gap:10px;align-items:center">
            <code style="font-family:var(--mono);font-size:12px;background:var(--surface);padding:8px 14px;border-radius:var(--r);border:1px solid var(--border);flex:1;word-break:break-all">${TOKEN || "—"}</code>
            <button class="btn" onclick="navigator.clipboard.writeText('${TOKEN}')">${ICONS.copy}Copiar</button>
          </div>
        </div>
        <div style="color:var(--text-dim);font-size:12.5px;line-height:1.7">
          <strong style="color:var(--text)">Autenticação:</strong> <code style="font-family:var(--mono)">Authorization: Bearer &lt;token&gt;</code><br>
          <strong style="color:var(--text)">Endpoint principal:</strong> <code style="font-family:var(--mono)">GET /api/empresas?q=&uf=&porte=&pagina=1&por_pagina=50</code><br>
          <strong style="color:var(--text)">Documentação:</strong> <a href="/docs" target="_blank" style="color:var(--accent-hi)">/docs</a>
        </div>
      ` : `
        <div style="text-align:center;color:var(--text-dim);padding:20px">
          <div style="font-size:14px;color:var(--text-muted);margin-bottom:8px">Disponível no plano Pro</div>
          Acesso via API com chave Bearer — integre com seu CRM ou sistema.
        </div>
      `}
    </div>`;
}

function viewClientes() {
  const isAdmin = state.planInfo && state.planInfo.plano === "admin";
  if (!isAdmin) {
    return `<div class="page-head"><div><div class="page-title">Clientes</div></div></div>
      <div class="panel" style="padding:40px;text-align:center;color:var(--text-dim)">Acesso restrito a administradores.</div>`;
  }

  const tokenList = state.tokensLoading
    ? `<div class="empty-state"><div class="big">Carregando…</div></div>`
    : state.tokens.length === 0
    ? `<div class="empty-state"><div class="big">Nenhum token criado ainda</div></div>`
    : state.tokens.map(t => {
        const limiteText = t.limite_dia ? fmt(t.limite_dia) : "∞";
        const ativo = t.ativo === true || t.ativo === 1;
        return `<div class="token-row">
          <div>
            <div class="token-val">${t.token}</div>
            <div style="font-size:11px;color:var(--text-dim);margin-top:2px">${planoBadge(t.plano, t.nome_plano)} criado ${timeAgo(t.criado_em)}</div>
          </div>
          <div style="font-size:12px;color:var(--text-muted)">${fmt(t.cnpjs_hoje)} / ${limiteText} hoje</div>
          <div>
            <span style="font-size:10px;padding:2px 8px;border-radius:20px;background:${ativo ? "var(--accent-soft)" : "var(--surface)"};color:${ativo ? "var(--accent-hi)" : "var(--text-dim)"}">
              ${ativo ? "Ativo" : "Inativo"}
            </span>
          </div>
          <div>
            <button class="btn" style="font-size:11px;padding:5px 10px" onclick="navigator.clipboard.writeText('${location.origin}?token=${t.token}').then(() => alert('Link copiado!'))">${ICONS.copy}Copiar link</button>
          </div>
          <div>
            <button class="row-btn" title="Remover" onclick="deletarToken('${t.token}')">${ICONS.trash}</button>
          </div>
        </div>`;
      }).join("");

  return `
    <div class="page-head">
      <div><div class="page-title">Clientes</div><div class="page-sub">Gerenciar tokens e planos</div></div>
    </div>
    <div class="panel" style="padding:18px 20px;margin-bottom:16px">
      <div class="panel-title" style="margin-bottom:14px">Criar novo token</div>
      <div class="admin-form">
        <div class="field-group">
          <label class="field-label">Token / nome do cliente</label>
          <input type="text" id="new-token" class="field-input" placeholder="ex: cliente_joao_abc123">
        </div>
        <div class="field-group">
          <label class="field-label">Plano</label>
          <select id="new-plano" class="field-input">
            <option value="free">Free (10 CNPJs/dia)</option>
            <option value="basico">Básico (500 CNPJs/dia)</option>
            <option value="pro" selected>Pro (ilimitado)</option>
          </select>
        </div>
        <button class="btn btn-accent" onclick="handleCriarToken()">${ICONS.plus}Criar</button>
      </div>
      <div id="token-result" style="margin-top:12px;display:none"></div>
    </div>
    <div class="panel" style="padding:18px 20px">
      <div class="panel-title" style="margin-bottom:14px">Tokens ativos — ${state.tokens.length}</div>
      ${tokenList}
    </div>`;
}

// ─── Interactions ────────────────────────────────────────────────
const debouncedQ = debounce(v => { state.filters.q = v; state.page = 1; loadEmpresas(); }, 400);

function updateFilter(k, v) {
  state.filters[k] = v;
  state.page = 1;
  loadEmpresas();
}
function onSetorChange(v) {
  state.filters.categoria = v;
  state.filters.departamento = "";
  state.page = 1;
  loadEmpresas();
}
function toggleF(k) {
  state.filters[k] = !state.filters[k];
  state.page = 1;
  loadEmpresas();
}
function clearFilters() {
  state.filters = { q:"", uf:"", porte:"", categoria:"", departamento:"", tel:false, email:false, site:false, insta:false, abertura_de:"", abertura_ate:"" };
  state.page = 1;
  loadEmpresas();
}
function sortBy(k) {
  if (state.sort.key === k) state.sort.dir = state.sort.dir === "asc" ? "desc" : "asc";
  else { state.sort.key = k; state.sort.dir = "asc"; }
  render();
}
function goPage(p) {
  state.page = p;
  loadEmpresas();
}
function toggleSelect(id) {
  if (state.selected.has(id)) state.selected.delete(id);
  else state.selected.add(id);
  updateBulkBar();
  render();
}
function toggleAllPage(ids) {
  const allSelected = ids.every(id => state.selected.has(id));
  if (allSelected) ids.forEach(id => state.selected.delete(id));
  else ids.forEach(id => state.selected.add(id));
  updateBulkBar();
  render();
}
function clearSelection() { state.selected.clear(); updateBulkBar(); render(); }

async function toggleExpand(cnpj) {
  if (state.expanded.has(cnpj)) {
    state.expanded.delete(cnpj);
    render();
  } else {
    state.expanded.add(cnpj);
    render();
    await loadDetail(cnpj);
  }
}

function updateBulkBar() {
  const bar = $("#bulk-bar");
  if (!bar) return;
  if (state.selected.size > 0) {
    bar.classList.add("show");
    const c = bar.querySelector(".bulk-count strong");
    if (c) c.textContent = state.selected.size;
  } else {
    bar.classList.remove("show");
  }
}

async function handleCriarToken() {
  const tokenVal = $("#new-token")?.value?.trim();
  const plano    = $("#new-plano")?.value;
  if (!tokenVal) { alert("Informe o token/nome do cliente."); return; }
  const result = await criarToken(tokenVal, plano);
  if (result) {
    const link = `${location.origin}?token=${result.token}`;
    const el = $("#token-result");
    if (el) {
      el.style.display = "block";
      el.innerHTML = `
        <div style="background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:10px 12px;font-size:12px">
          <div style="color:var(--accent-hi);margin-bottom:6px;font-weight:500">Token criado com sucesso!</div>
          <div style="display:flex;gap:8px;align-items:center">
            <code style="font-family:var(--mono);flex:1;word-break:break-all;font-size:11.5px">${link}</code>
            <button class="btn" onclick="navigator.clipboard.writeText('${link}')">${ICONS.copy}Copiar</button>
          </div>
        </div>`;
    }
    if ($("#new-token")) $("#new-token").value = "";
  }
}

// ─── Chart tooltip ───────────────────────────────────────────────
function wireContent() {
  const body = $("#chart-body");
  if (body) {
    const tip = $("#chart-tip");
    $$(".cd", body).forEach(el => {
      el.addEventListener("mouseenter", () => {
        const i = +el.dataset.i;
        const raw = el.dataset.date;
        const dateLabel = raw ? raw.slice(8,10) + "/" + raw.slice(5,7) : `Dia ${i+1}`;
        const cx = +el.getAttribute("cx"), cy = +el.getAttribute("cy");
        const sbox = body.querySelector("svg").getBoundingClientRect();
        const sx = (cx / 720) * sbox.width;
        const sy = (cy / 220) * sbox.height;
        tip.style.left = Math.min(sbox.width - 160, sx - 70) + "px";
        tip.style.top  = sy + "px";
        tip.innerHTML = `<div class="tip-lbl">${dateLabel}</div>
          <div class="tip-row"><span style="color:var(--accent)">● Coletadas</span><span>${fmt(+el.dataset.col)}</span></div>
          <div class="tip-row"><span style="color:var(--info)">● Enriquecidas</span><span>${fmt(+el.dataset.enr)}</span></div>`;
        tip.classList.add("show");
      });
      el.addEventListener("mouseleave", () => tip.classList.remove("show"));
    });
  }
  updateBulkBar();

  // Global search → redirect to empresas tab
  const gs = $("#global-search");
  if (gs) {
    gs.addEventListener("keydown", e => {
      if (e.key === "Enter" && gs.value.trim()) {
        state.filters.q = gs.value.trim();
        state.page = 1;
        showTab("empresas");
      }
    });
  }
}

// ─── Tweaks ──────────────────────────────────────────────────────
function openTweaks()  { $("#tweaks-panel")?.classList.add("open"); }
function closeTweaks() { $("#tweaks-panel")?.classList.remove("open"); }

function applyTweaks() {
  document.body.classList.remove("radius-sharp", "radius-soft", "radius-round");
  document.body.classList.add(`radius-${state.radius}`);
}

function initTweaksUI() {
  const dens = $("#tw-density");
  if (dens) $$("button", dens).forEach(b => {
    b.classList.toggle("on", b.dataset.val === state.density);
    b.onclick = () => {
      state.density = b.dataset.val;
      $$("button", dens).forEach(x => x.classList.toggle("on", x === b));
      render();
    };
  });
  const rad = $("#tw-radius");
  if (rad) $$("button", rad).forEach(b => {
    b.classList.toggle("on", b.dataset.val === state.radius);
    b.onclick = () => {
      state.radius = b.dataset.val;
      $$("button", rad).forEach(x => x.classList.toggle("on", x === b));
      applyTweaks();
    };
  });
}

// ─── Init ────────────────────────────────────────────────────────
async function init() {
  $$(".nav-item").forEach(n => { if (n.dataset.tab) n.onclick = () => showTab(n.dataset.tab); });

  applyTweaks();
  initTweaksUI();

  // Load core data
  await Promise.all([loadPlan(), loadStats(), loadCategories(), loadAtividade()]);

  // Initial view
  render();
  showTab("dashboard");

  // Auto-refresh
  setInterval(loadStats,      10000);
  setInterval(loadPlan,       15000);
  setInterval(loadCategories, 60000);
}

document.addEventListener("DOMContentLoaded", init);
