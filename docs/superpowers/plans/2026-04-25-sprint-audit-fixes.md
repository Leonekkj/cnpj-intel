# Sprint Audit Fixes — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix all 15 bugs and missing features found in the full Playwright audit, delivered by Monday April 27 2026 at 10:00.

**Architecture:** Fixes are grouped by cost — quick 1-liners first, then UX patches, then features that require backend + frontend coordination. All tasks are independent; order matters only for scheduling, not for correctness.

**Tech Stack:** Python / FastAPI / SQLite+PostgreSQL (`database.py`, `api.py`) · Vanilla JS (`app/app.js`) · CSS (`app/styles.css`)

**Deadline schedule:**
| Phase | When | Hours |
|-------|------|-------|
| Phase 1 — Critical 1-liners | Sábado tarde | ~1h |
| Phase 2 — Security + UX patches | Sábado | ~3h |
| Phase 3 — Server-side sort + Error feedback | Sábado/Domingo | ~4h |
| Phase 4 — Login + Export tracking + Insights | Domingo | ~8h |
| Phase 5 — Listas MVP | Domingo/Segunda | ~6h |

**Verification:** No automated test suite. Each task verifies with `curl` + manual browser check at `http://localhost:8000?token=test`.

---

## Phase 1 — Critical 1-liners (Sábado — ~1h total)

---

### Task 1: Add `socio_principal` to text search

**Why:** The filter bar placeholder says "sócio…" but the SQL never searches that column. Searching by partner name silently returns zero results.

**Files:**
- Modify: `database.py:1035-1038`

- [ ] **Open `database.py`, find the `if q:` block inside `buscar_empresas` (~line 1035)**

Current code:
```python
if q:
    filtros.append(f"(razao_social {LIKE} {PH} OR nome_fantasia {LIKE} {PH} OR cnpj LIKE {PH} OR municipio {LIKE} {PH})")
    like = f"%{q}%"
    params.extend([like, like, like, like])
```

- [ ] **Replace with:**
```python
if q:
    filtros.append(
        f"(razao_social {LIKE} {PH} OR nome_fantasia {LIKE} {PH} "
        f"OR cnpj LIKE {PH} OR municipio {LIKE} {PH} OR socio_principal {LIKE} {PH})"
    )
    like = f"%{q}%"
    params.extend([like, like, like, like, like])
```

- [ ] **Verify with curl:**
```bash
curl -s "http://localhost:8000/api/empresas?q=JOAO&por_pagina=5" \
  -H "Authorization: Bearer test" | python -m json.tool | grep socio_principal
```
Expected: results where `socio_principal` contains "JOAO".

- [ ] **Commit:**
```bash
git add database.py
git commit -m "fix: include socio_principal in text search"
```

---

### Task 2: Fix `perPage` default (14 → 15)

**Why:** 14 is an arbitrary number that causes the last row to clip. 15 is a round number that fits cleanly.

**Files:**
- Modify: `app/app.js:65`

- [ ] **Change `perPage: 14` to `perPage: 15` in the `state` object at the top of `app/app.js`:**
```js
perPage: 15,
```

- [ ] **Verify:** Open `http://localhost:8000?token=test` → Empresas tab → count visible rows (should be 15).

- [ ] **Commit:**
```bash
git add app/app.js
git commit -m "fix: change default perPage from 14 to 15"
```

---

### Task 3: Fix `vacuum()` connection leak on exception

**Why:** `vacuum()` creates a raw psycopg2 connection outside the pool. If `VACUUM ANALYZE` throws, the connection is never closed — silent leak in production.

**Files:**
- Modify: `database.py:1531-1543`

- [ ] **Find `def vacuum(self)` in `database.py` (~line 1531) and replace its body:**
```python
def vacuum(self):
    """Executa VACUUM ANALYZE no Postgres para liberar espaço após DELETE em massa."""
    if not USE_POSTGRES:
        return
    conn = psycopg2.connect(DATABASE_URL)
    conn.set_isolation_level(0)  # AUTOCOMMIT obrigatório — VACUUM não pode rodar em transação
    try:
        cur = conn.cursor()
        cur.execute("VACUUM ANALYZE empresas")
        cur.close()
    finally:
        conn.close()
```

- [ ] **Commit:**
```bash
git add database.py
git commit -m "fix: ensure vacuum() closes connection on exception"
```

---

### Task 4: Fix duplicate sparkline data (contatos = enriquecidas)

**Why:** The dashboard shows 4 metrics; "Com telefone" and "Com e-mail" both use `d.enriquecidas` (companies with phone per day) — identical curves. This requires also updating `atividade_diaria` in the DB to return a real `com_email` count per day.

**Files:**
- Modify: `database.py:1467-1496` (method `atividade_diaria`)
- Modify: `app/app.js:467-473`

- [ ] **In `database.py`, update the Postgres branch of `atividade_diaria` to add `com_email`:**
```python
if USE_POSTGRES:
    sql = """
        SELECT DATE(atualizado_em::timestamp)::text AS d,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE telefone IS NOT NULL AND telefone != '') AS com_tel,
               COUNT(*) FILTER (WHERE email IS NOT NULL AND email != '') AS com_email
        FROM empresas
        WHERE atualizado_em >= %s
        GROUP BY 1 ORDER BY 1
    """
```

- [ ] **Update the SQLite branch:**
```python
else:
    sql = """
        SELECT strftime('%Y-%m-%d', atualizado_em) AS d,
               COUNT(*) AS total,
               SUM(CASE WHEN telefone IS NOT NULL AND telefone != '' THEN 1 ELSE 0 END) AS com_tel,
               SUM(CASE WHEN email IS NOT NULL AND email != '' THEN 1 ELSE 0 END) AS com_email
        FROM empresas
        WHERE atualizado_em >= ?
        GROUP BY 1 ORDER BY 1
    """
```

- [ ] **Update the return statement at the bottom of `atividade_diaria`:**
```python
cur.execute(sql, (cutoff,))
rows = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}
return [
    {
        "data":        d,
        "coletadas":   rows.get(d, (0, 0, 0))[0],
        "enriquecidas": rows.get(d, (0, 0, 0))[1],
        "com_email":   rows.get(d, (0, 0, 0))[2],
    }
    for d in dates
]
```

- [ ] **In `app/app.js`, update the `sparks` object inside `viewDashboard()` (~line 467):**
```js
const sparks = {
  coletadas:    spark14.map(d => d.coletadas    || 0),
  enriquecidas: spark14.map(d => d.enriquecidas || 0),
  contatos:     spark14.map(d => d.enriquecidas || 0),  // com telefone trend
  email:        spark14.map(d => d.com_email    || 0),  // com email trend (real data)
  export:       spark14.map(() => 0),
};
```

- [ ] **Update the sparkline call for "Com e-mail" metric (line ~526):**

Find:
```js
${metric("Com e-mail", fmt(stats.com_email), dEmail.val, dEmail.up, sparkline(sparks.enriquecidas, ...
```
Replace `sparks.enriquecidas` with `sparks.email`:
```js
${metric("Com e-mail", fmt(stats.com_email), dEmail.val, dEmail.up, sparkline(sparks.email, "oklch(0.80 0.14 75)"), "mail", "wa", "mv-email")}
```

- [ ] **Update `dEmail` delta calculation (~line 485):**
```js
const dEmail = pctDelta(sparks.email);
```

- [ ] **Verify:** Open Dashboard, hover over the "Com telefone" and "Com e-mail" sparklines — they should now show different curves.

- [ ] **Commit:**
```bash
git add database.py app/app.js
git commit -m "fix: separate com_email sparkline from com_telefone — add real email/day to atividade_diaria"
```

---

### Task 5: Fix expand/collapse race condition

**Why:** Clicking expand twice while data is loading triggers two parallel fetches for the same CNPJ because `null` is falsy and bypasses the early-return guard.

**Files:**
- Modify: `app/app.js:264-270` and `app/app.js:906-912`

- [ ] **At the top of `app/app.js`, after the `state` object definition, add a module-level sentinel:**
```js
const LOADING = Symbol('loading');
```

- [ ] **Replace the `loadDetail` function (~line 264):**
```js
async function loadDetail(cnpj) {
  if (state.expandedData[cnpj] === LOADING || state.expandedData[cnpj]) return;
  state.expandedData[cnpj] = LOADING;
  render();
  const data = await apiFetch(`/api/empresa/${cnpj}`);
  state.expandedData[cnpj] = (data && !data._err) ? data : { _notfound: true };
  render();
}
```

- [ ] **Update `detailRow` to handle the LOADING sentinel (~line 908):**

Find:
```js
if (det === undefined || det === null) {
  return `<tr class="detail-row"><td colspan="9"><div class="detail-loading">Carregando detalhes…</div></td></tr>`;
}
```
Replace with:
```js
if (det === undefined || det === null || det === LOADING) {
  return `<tr class="detail-row"><td colspan="9"><div class="detail-loading">Carregando detalhes…</div></td></tr>`;
}
```

- [ ] **Verify:** Open Empresas tab, rapidly click expand/collapse on the same row 3 times quickly. Check Network tab in DevTools — there should be only one request to `/api/empresa/{cnpj}`.

- [ ] **Commit:**
```bash
git add app/app.js
git commit -m "fix: prevent parallel fetches on rapid expand/collapse using LOADING sentinel"
```

---

## Phase 2 — Security + UX Patches (Sábado — ~3h)

---

### Task 6: Fix XSS risk in admin Clientes panel

**Why:** Token values, CNPJ strings, and URLs are injected raw into `onclick="..."` inline attributes. A token containing `'` or `)` breaks the JS; a malicious value could execute code in the admin's browser.

**Files:**
- Modify: `app/app.js` — `viewClientes()` (~line 1046) and `detailRow()` (~line 953) and `wireContent()` (~line 1193)

- [ ] **In `viewClientes()`, find the "Copiar link" button (line ~1061) and remove the inline onclick:**

Old:
```js
<button class="btn" style="font-size:11px;padding:5px 10px" onclick="navigator.clipboard.writeText('${location.origin}?token=${t.token}').then(() => alert('Link copiado!'))">${ICONS.copy}Copiar link</button>
```
New (use data attribute):
```js
<button class="btn copy-link-btn" data-token="${encodeURIComponent(t.token)}" style="font-size:11px;padding:5px 10px">${ICONS.copy}Copiar link</button>
```

- [ ] **In `viewClientes()`, find the "Remover" button (line ~1064) and remove the inline onclick:**

Old:
```js
<button class="row-btn" title="Remover" onclick="deletarToken('${t.token}')">${ICONS.trash}</button>
```
New:
```js
<button class="row-btn delete-token-btn" title="Remover" data-token="${encodeURIComponent(t.token)}">${ICONS.trash}</button>
```

- [ ] **In `detailRow()`, find the "Copiar CNPJ" button (line ~953) and remove the inline onclick:**

Old:
```js
<button class="btn" onclick="navigator.clipboard.writeText('${fmtCNPJ(d.cnpj)}')">${ICONS.copy}Copiar CNPJ</button>
```
New:
```js
<button class="btn copy-cnpj-btn" data-cnpj="${fmtCNPJ(d.cnpj)}">${ICONS.copy}Copiar CNPJ</button>
```

- [ ] **In `wireContent()` (~line 1193), add event delegation for all three new classes:**
```js
function wireContent() {
  // ... existing chart tooltip code ...

  // XSS-safe handlers
  document.querySelectorAll('.copy-link-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const tok = decodeURIComponent(btn.dataset.token);
      navigator.clipboard.writeText(`${location.origin}?token=${tok}`)
        .then(() => alert('Link copiado!'));
    });
  });

  document.querySelectorAll('.delete-token-btn').forEach(btn => {
    btn.addEventListener('click', () => deletarToken(decodeURIComponent(btn.dataset.token)));
  });

  document.querySelectorAll('.copy-cnpj-btn').forEach(btn => {
    btn.addEventListener('click', () => navigator.clipboard.writeText(btn.dataset.cnpj));
  });

  // ... rest of existing wireContent ...
}
```

- [ ] **Verify:** Go to Clientes tab, create a token named `test'token`. Verify the "Copiar link" button works without breaking. Open DevTools console — no errors.

- [ ] **Commit:**
```bash
git add app/app.js
git commit -m "fix: remove inline onclick from token/CNPJ buttons — XSS prevention"
```

---

### Task 7: Fix invisible date filters leaking into Empresas tab

**Why:** Date inputs only appear in Busca avançada's filter bar, but the filter state is shared. If a user sets dates in Busca and switches to Empresas, the date filter silently narrows results with no UI indication.

**Files:**
- Modify: `app/app.js` — `showTab()` (~line 410)

- [ ] **In `showTab()`, add a date-filter clear when switching away from Busca:**
```js
function showTab(t) {
  // Date filters are only visible in Busca avançada.
  // Clear them when leaving so they don't silently affect Empresas.
  if (state.tab === 'busca' && t !== 'busca') {
    state.filters.abertura_de = '';
    state.filters.abertura_ate = '';
  }

  state.tab = t;
  state.selected.clear();
  // ... rest of existing showTab body unchanged ...
}
```

- [ ] **Verify:** 
  1. Go to Busca avançada, set date range 2020–2022.
  2. Switch to Empresas tab — results should now reflect no date filter (count should match total database size, not the narrow date range).
  3. Go back to Busca — date inputs should be reset.

- [ ] **Commit:**
```bash
git add app/app.js
git commit -m "fix: clear date filters when leaving Busca avançada to prevent silent filtering in Empresas"
```

---

### Task 8: Add error toast feedback for API failures

**Why:** When API calls fail (401, 429, 500, network error), the app shows an empty table with zero explanation. Users have no idea what went wrong.

**Files:**
- Modify: `app/styles.css`
- Modify: `app/app.js` — `apiFetch()` (~line 8)

- [ ] **Add toast styles to `app/styles.css` (append at the end of the file):**
```css
/* ─── Error toasts ────────────────────────────────────────────────────────── */
.toast {
  position: fixed;
  bottom: 24px;
  right: 24px;
  padding: 11px 18px;
  border-radius: var(--r);
  font-size: 13px;
  font-weight: 500;
  z-index: 9999;
  background: var(--surface);
  border: 1px solid var(--border);
  box-shadow: 0 4px 24px rgba(0,0,0,0.25);
  transition: opacity 0.3s;
  max-width: 340px;
  line-height: 1.4;
}
.toast.toast-error { border-color: var(--danger, #e55); color: var(--danger, #e55); }
.toast.toast-warn  { border-color: var(--warn, oklch(0.75 0.15 75)); color: var(--warn, oklch(0.75 0.15 75)); }
.toast.toast-info  { border-color: var(--accent); color: var(--accent-hi); }
.toast.fade-out    { opacity: 0; pointer-events: none; }
```

- [ ] **Add `showToast` function to `app/app.js` just after the `debounce` helper (~line 131):**
```js
function showToast(msg, type = 'error', ms = 4000) {
  const t = document.createElement('div');
  t.className = `toast toast-${type}`;
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => {
    t.classList.add('fade-out');
    setTimeout(() => t.remove(), 350);
  }, ms);
}
```

- [ ] **Replace the `apiFetch` function (~line 8) to call `showToast` on errors:**
```js
async function apiFetch(path, opts = {}) {
  try {
    const r = await fetch(API_BASE + path, { headers: { ...H, ...(opts.headers || {}) }, ...opts });
    if (!r.ok) {
      const body = await r.json().catch(() => ({}));
      if (r.status === 401) showToast('Token inválido ou não configurado.');
      else if (r.status === 403) showToast('Acesso não permitido no seu plano.');
      else if (r.status === 429) showToast(body.detail || 'Limite diário atingido. Tente amanhã.', 'warn');
      else if (r.status >= 500) showToast('Erro no servidor. Tente novamente em alguns segundos.');
      return { _err: r.status, _body: body };
    }
    return await r.json();
  } catch (e) {
    showToast('Sem conexão com o servidor.');
    return { _err: 0 };
  }
}
```

- [ ] **Verify:** 
  1. Change TOKEN to `invalid_token_xyz` in the browser URL and reload.
  2. Expected: A toast appears saying "Token inválido ou não configurado."
  3. Restore valid token.

- [ ] **Commit:**
```bash
git add app/app.js app/styles.css
git commit -m "feat: add error toast feedback for API failures (401, 403, 429, 500, network)"
```

---

## Phase 3 — Sort + Shortcut (Sábado/Domingo — ~4h)

---

### Task 9: Implement ⌘K keyboard shortcut

**Why:** The topbar shows `<kbd>⌘K</kbd>` next to the search box, implying it focuses the search. Currently it does nothing — decorative only.

**Files:**
- Modify: `app/app.js` — `init()` (~line 1262)

- [ ] **Inside `init()`, before `document.addEventListener('DOMContentLoaded', init)`, add the keyboard listener:**
```js
async function init() {
  // ⌘K / Ctrl+K → focus global search
  document.addEventListener('keydown', e => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
      e.preventDefault();
      const gs = document.querySelector('#global-search');
      if (gs) { gs.focus(); gs.select(); }
    }
  });

  // ... rest of existing init body unchanged ...
}
```

- [ ] **Verify:** Press `Ctrl+K` (Windows) or `⌘K` (Mac) — the global search input should receive focus and its text selected.

- [ ] **Commit:**
```bash
git add app/app.js
git commit -m "feat: implement Ctrl+K / Cmd+K shortcut to focus global search"
```

---

### Task 10: Server-side sorting for `/api/empresas`

**Why:** Clicking a column header currently sorts only the 15 rows on the current page in JS memory. Navigating to page 2 silently resets the sort. This is the most confusing UX issue in the app.

**Files:**
- Modify: `database.py` — `buscar_empresas()` (~line 1026)
- Modify: `api.py` — `listar_empresas()` (~line 193)
- Modify: `app/app.js` — `sortBy()` (~line 1122), `loadEmpresas()` (~line 236), `tableCard()` (~line 793)

**Step 1 — database.py:**

- [ ] **Add `sort_key` and `sort_dir` parameters to `buscar_empresas` signature (~line 1026):**
```python
def buscar_empresas(self, q="", uf="", porte="", cnae="", categoria="", departamento="",
                    abertura_de="", abertura_ate="",
                    com_email=False, com_instagram=False,
                    com_telefone=False, com_site=False,
                    com_contato=False,
                    sort_key="atualizado_em", sort_dir="desc",
                    pagina=1, por_pagina=50) -> dict:
```

- [ ] **Inside `buscar_empresas`, just before building the SELECT, add sort validation and apply it:**
```python
# Whitelist to prevent SQL injection
_ALLOWED_SORT = {
    "razao_social", "cnpj", "porte", "municipio",
    "abertura", "socio_principal", "atualizado_em",
}
sk = sort_key if sort_key in _ALLOWED_SORT else "atualizado_em"
sd = "ASC" if sort_dir == "asc" else "DESC"

# Replace the existing SELECT query:
cur.execute(
    f"SELECT * FROM empresas WHERE {where} ORDER BY {sk} {sd} LIMIT {PH} OFFSET {PH}",
    params + [por_pagina, offset]
)
```

**Step 2 — api.py:**

- [ ] **Add `sort_key` and `sort_dir` query params to `listar_empresas` (~line 207):**
```python
sort_key:      str  = Query("atualizado_em", description="Campo de ordenação"),
sort_dir:      str  = Query("desc", description="asc | desc"),
```

- [ ] **Pass them to `db.buscar_empresas` (~line 232):**
```python
resultado = db.buscar_empresas(
    q=q, uf=uf, porte=porte, cnae=cnae, categoria=categoria, departamento=departamento,
    abertura_de=abertura_de, abertura_ate=abertura_ate,
    com_email=com_email, com_instagram=com_instagram,
    com_telefone=com_telefone, com_site=com_site,
    com_contato=com_contato,
    sort_key=sort_key, sort_dir=sort_dir,
    pagina=pagina, por_pagina=por_pagina,
)
```

**Step 3 — app/app.js:**

- [ ] **Change `sortBy` to trigger a server-side fetch instead of client-side render (~line 1122):**
```js
function sortBy(k) {
  if (state.sort.key === k) state.sort.dir = state.sort.dir === "asc" ? "desc" : "asc";
  else { state.sort.key = k; state.sort.dir = "asc"; }
  state.page = 1;
  loadEmpresas();  // server fetch instead of render()
}
```

- [ ] **In `loadEmpresas()`, add sort params to the URL (~line 239, after existing params):**
```js
if (state.sort.key) {
  params.set("sort_key", state.sort.key);
  params.set("sort_dir", state.sort.dir);
}
```

- [ ] **In `tableCard()`, remove the client-side sort block (~line 798):**

Remove these lines:
```js
let rows = [...state.dados];
if (state.sort.key) {
  const k = state.sort.key, dir = state.sort.dir === "asc" ? 1 : -1;
  rows.sort((a, b) => String(a[k] || "").localeCompare(String(b[k] || ""), "pt-BR") * dir);
}
```
Replace with:
```js
const rows = [...state.dados];
```

- [ ] **Verify:**
```bash
curl -s "http://localhost:8000/api/empresas?sort_key=razao_social&sort_dir=asc&por_pagina=3" \
  -H "Authorization: Bearer test" | python -m json.tool | python -c "import sys,json; d=json.load(sys.stdin); [print(r['razao_social']) for r in d['dados']]"
```
Expected: company names in alphabetical order.

Also verify in browser: sort by "Empresa" column, go to page 2 — sort should persist.

- [ ] **Commit:**
```bash
git add database.py api.py app/app.js
git commit -m "feat: server-side sorting for /api/empresas — sort persists across pagination"
```

---

## Phase 4 — Login + Export + Insights (Domingo — ~8h)

---

### Task 11: Add active filter summary to Export tab

**Why:** The Export tab says "use filters from Empresas/Busca" but shows no summary of what's currently selected. Users don't know what they're about to export.

**Files:**
- Modify: `app/app.js` — `viewExport()` (~line 990)

- [ ] **Replace `viewExport()` with a version that includes a live filter summary:**
```js
function viewExport() {
  const canExport = state.planInfo && state.planInfo.export;
  const f = state.filters;
  const active = [
    f.q          && `Pesquisa: "${f.q}"`,
    f.uf         && `Estado: ${f.uf}`,
    f.porte      && `Porte: ${f.porte}`,
    f.departamento ? `Departamento: ${f.departamento}` : f.categoria && `Setor: ${f.categoria}`,
    f.tel        && 'Com telefone',
    f.email      && 'Com e-mail',
    f.site       && 'Com site',
    f.insta      && 'Com Instagram',
    f.abertura_de  && `Abertura de: ${f.abertura_de}`,
    f.abertura_ate && `até: ${f.abertura_ate}`,
  ].filter(Boolean);

  const filterSummary = active.length > 0
    ? `<div style="margin-bottom:16px;padding:10px 14px;background:var(--surface-2,var(--surface));border-radius:var(--r);font-size:12.5px;color:var(--text-dim)">
        <span style="color:var(--text);font-weight:500">Filtros ativos:</span> ${active.join(' · ')}
       </div>`
    : `<div style="margin-bottom:16px;font-size:12.5px;color:var(--text-dim)">
        Nenhum filtro ativo — exportará <strong>todos</strong> os CNPJs disponíveis.
        Configure filtros em <strong>Empresas</strong> ou <strong>Busca avançada</strong> primeiro.
       </div>`;

  return `
    <div class="page-head">
      <div><div class="page-title">Exportar CSV</div><div class="page-sub">Baixe listas filtradas no formato planilha</div></div>
    </div>
    <div class="panel" style="padding:32px 40px">
      ${canExport ? `
        <div style="font-size:14px;font-weight:600;margin-bottom:12px">Exportar lista atual</div>
        ${filterSummary}
        <div style="display:flex;gap:10px;align-items:center">
          <button class="btn btn-accent" onclick="exportCSV()">${ICONS.download}Exportar CSV agora</button>
          <span style="font-size:12px;color:var(--text-dim)">
            ${fmt(state.totalDados)} resultados · ${state.planInfo?.plano === 'basico' ? 'máx 500 linhas' : 'máx 5.000 linhas'}
          </span>
        </div>
      ` : `
        <div style="text-align:center;color:var(--text-dim);padding:20px">
          <div style="font-size:14px;color:var(--text-muted);margin-bottom:8px">Exportação não disponível</div>
          Faça upgrade para o plano Básico ou Pro para exportar listas em CSV.
        </div>
      `}
    </div>`;
}
```

- [ ] **Verify:** Go to Busca avançada, set UF=SP and toggle "Com e-mail". Then go to Exportar tab — summary should show "Estado: SP · Com e-mail".

- [ ] **Commit:**
```bash
git add app/app.js
git commit -m "feat: show active filter summary in Export tab before downloading"
```

---

### Task 12: Login/onboarding screen for tokenless users

**Why:** Users without a token in URL or localStorage see a blank loading screen with no explanation. They have no path to enter their token.

**Files:**
- Modify: `app/app.js` — `init()` and add `renderLogin()` + `handleLogin()`

- [ ] **Add `renderLogin()` function just before `init()` (~line 1262):**
```js
function renderLogin() {
  const c = document.querySelector('#content');
  if (!c) return;
  // Hide sidebar and topbar search — not useful without a session
  document.querySelector('.sidebar')?.style.setProperty('display', 'none');
  document.querySelector('.topbar-search')?.style.setProperty('display', 'none');

  c.innerHTML = `
    <div style="display:flex;align-items:center;justify-content:center;height:90vh">
      <div class="panel" style="padding:48px 40px;max-width:400px;width:100%;text-align:center">
        <div class="brand-mark" style="width:40px;height:40px;margin:0 auto 20px;color:var(--accent)">
          <svg viewBox="0 0 24 24" fill="none"><path d="M4 8L12 4L20 8V16L12 20L4 16V8Z" stroke="currentColor" stroke-width="1.5"/><path d="M12 12L20 8M12 12V20M12 12L4 8" stroke="currentColor" stroke-width="1.5"/></svg>
        </div>
        <div style="font-size:22px;font-weight:700;margin-bottom:6px">CNPJ Intel</div>
        <div style="color:var(--text-dim);margin-bottom:28px;font-size:13px">Insira seu token de acesso para continuar</div>
        <input
          type="text"
          id="token-input"
          class="field-input"
          placeholder="Seu token de acesso…"
          style="width:100%;margin-bottom:12px;box-sizing:border-box"
          onkeydown="if(event.key==='Enter') handleLogin()"
        >
        <button class="btn btn-accent" style="width:100%;justify-content:center" onclick="handleLogin()">
          Entrar
        </button>
        <div id="login-error" style="color:var(--danger,#e55);font-size:12px;margin-top:10px;min-height:18px"></div>
      </div>
    </div>`;
}

async function handleLogin() {
  const input = document.querySelector('#token-input')?.value?.trim();
  const errEl = document.querySelector('#login-error');
  if (!input) { if (errEl) errEl.textContent = 'Informe o token de acesso.'; return; }
  if (errEl) errEl.textContent = '';

  const r = await fetch(`${API_BASE}/api/meu-plano`, {
    headers: { Authorization: `Bearer ${input}` }
  }).catch(() => null);

  if (r && r.ok) {
    localStorage.setItem('cnpj_token', input);
    location.href = `${location.pathname}?token=${encodeURIComponent(input)}`;
  } else {
    if (errEl) errEl.textContent = 'Token inválido. Verifique com o administrador.';
  }
}
```

- [ ] **Modify `init()` to check for token before loading the app:**

Find the start of `async function init()` and add the guard as the first statement:
```js
async function init() {
  if (!TOKEN) {
    renderLogin();
    return;
  }

  $$(".nav-item").forEach(n => { if (n.dataset.tab) n.onclick = () => showTab(n.dataset.tab); });
  // ... rest of existing init body unchanged ...
}
```

- [ ] **Verify:** Open `http://localhost:8000` (without `?token=`). Should show the login panel. Enter `test` → should redirect to `?token=test` and load the full app.

- [ ] **Commit:**
```bash
git add app/app.js
git commit -m "feat: add login screen for users without token — no more blank loading state"
```

---

### Task 13: Export event tracking — fix "Exports no mês" metric

**Why:** The "Exports no mês" card always shows "—" because there's no backend tracking of exports. This task adds an `export_log` table, wires it into the export endpoint, and surfaces the count in `/api/stats`.

**Files:**
- Modify: `database.py` — add `criar_tabela_export_log()`, `registrar_export()`, and update `estatisticas()`
- Modify: `api.py` — call `registrar_export` in `/api/export` and create the table on startup
- Modify: `app/app.js` — update Export metric card and delta calculation

**Step 1 — database.py:**

- [ ] **Add `criar_tabela_export_log()` to the `Database` class (add after `criar_tabela_tokens()`):**
```python
def criar_tabela_export_log(self):
    with _conn() as conn:
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS export_log (
                    id          SERIAL PRIMARY KEY,
                    token       TEXT NOT NULL,
                    exportado_em TEXT NOT NULL,
                    total       INTEGER NOT NULL DEFAULT 0
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_export_log_token_data ON export_log(token, exportado_em)")
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS export_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    token       TEXT NOT NULL,
                    exportado_em TEXT NOT NULL,
                    total       INTEGER NOT NULL DEFAULT 0
                )
            """)
        conn.commit()
```

- [ ] **Add `registrar_export()` to the `Database` class:**
```python
def registrar_export(self, token: str, total: int):
    agora = datetime.utcnow().isoformat()
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"INSERT INTO export_log (token, exportado_em, total) VALUES ({PH},{PH},{PH})",
            (token, agora, total)
        )
        conn.commit()
```

- [ ] **Update `estatisticas()` to include this month's export count for the calling token. Since `estatisticas()` doesn't receive a token, add a separate method:**
```python
def exports_no_mes(self, token: str) -> int:
    """Retorna o total de CNPJs exportados no mês corrente pelo token."""
    from datetime import date
    inicio = date.today().replace(day=1).isoformat()
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COALESCE(SUM(total), 0) FROM export_log WHERE token = {PH} AND exportado_em >= {PH}",
            (token, inicio)
        )
        return int(cur.fetchone()[0])
```

**Step 2 — api.py:**

- [ ] **Call `db.criar_tabela_export_log()` inside `_run_db_fast()` (~line 35):**
```python
def _run_db_fast():
    db.criar_tabelas()
    db.criar_tabela_tokens()
    db.criar_tabela_export_log()   # ← add this line
    for _t in ...
```

- [ ] **In `/api/export`, call `registrar_export` after building `dados` (~line 354):**
```python
dados = resultado.get("dados", [])
if not dados:
    raise HTTPException(status_code=404, detail="Nenhum resultado para exportar")

db.registrar_export(info["token"], len(dados))  # ← add this line
```

- [ ] **In `/api/stats`, add `exports_mes` to the response:**
```python
@app.get("/api/stats")
def estatisticas(info: dict = Depends(get_token_info)):
    agora = _time.time()
    if _stats_cache["data"] and (agora - _stats_cache["ts"]) < _STATS_TTL:
        cached = dict(_stats_cache["data"])
        cached["exports_mes"] = db.exports_no_mes(info["token"])  # not cached — per-token
        return cached
    data = db.estatisticas()
    _stats_cache["data"] = data
    _stats_cache["ts"] = agora
    data = dict(data)
    data["exports_mes"] = db.exports_no_mes(info["token"])
    return data
```

**Step 3 — app/app.js:**

- [ ] **In `viewDashboard()`, update the "Exports no mês" metric (~line 527):**
```js
${metric("Exports no mês", fmt(stats.exports_mes || 0), "—", true, sparkline(sparks.export, "oklch(0.72 0.14 295)"), "download", "pu")}
```

- [ ] **Add `exports_mes` to the live-patch in `loadStats()` (~line 220):**
```js
_patchMetric("mv-total", fmt(data.total),         prev.total         !== data.total);
_patchMetric("mv-tel",   fmt(data.com_telefone),  prev.com_telefone  !== data.com_telefone);
_patchMetric("mv-email", fmt(data.com_email),     prev.com_email     !== data.com_email);
// Remove "mv-export" if element exists (currently no id on 4th metric — add one)
```

Add `id="mv-export"` to the 4th metric call:
```js
${metric("Exports no mês", fmt(stats.exports_mes || 0), "—", true, sparkline(sparks.export, "oklch(0.72 0.14 295)"), "download", "pu", "mv-export")}
```

And in the patch block:
```js
_patchMetric("mv-export", fmt(data.exports_mes || 0), (prev.exports_mes || 0) !== (data.exports_mes || 0));
```

- [ ] **Verify:**
  1. Go to Empresas tab → click "Exportar CSV".
  2. Go to Dashboard. The "Exports no mês" card should show `1` (or increment if you export again).

- [ ] **Commit:**
```bash
git add database.py api.py app/app.js
git commit -m "feat: track CSV exports — exports_no_mes metric now shows real count"
```

---

### Task 14: Dynamic insights panel

**Why:** The 5 insight cards in the Dashboard are permanently hardcoded mock text. They never reflect the actual state of the database.

**Files:**
- Modify: `database.py` — add `gerar_insights()`
- Modify: `api.py` — add `/api/insights` endpoint, add to the atividade cache block
- Modify: `app/app.js` — `loadAtividade()` / add `loadInsights()`, update `viewDashboard()`

**Step 1 — database.py:**

- [ ] **Add `gerar_insights()` to the `Database` class:**
```python
def gerar_insights(self) -> list:
    """Gera insights dinâmicos a partir dos dados reais do banco."""
    insights = []
    with _conn() as conn:
        cur = conn.cursor()

        # Total com e sem e-mail
        cur.execute("SELECT COUNT(*) FROM empresas")
        total = cur.fetchone()[0] or 1
        cur.execute("SELECT COUNT(*) FROM empresas WHERE email IS NOT NULL AND email != ''")
        com_email = cur.fetchone()[0]
        pct_email = round(com_email / total * 100)
        insights.append({
            "tone": "ac", "ico": "trend",
            "title": "Cobertura de e-mail",
            "sub": f"{pct_email}% dos CNPJs têm e-mail confirmado ({com_email:,} de {total:,}).",
            "time": "agora",
        })

        # Setor com mais empresas
        cur.execute("""
            SELECT categoria_padrao, COUNT(*) as n FROM empresas
            WHERE telefone IS NOT NULL AND telefone != ''
              AND categoria_padrao IS NOT NULL AND categoria_padrao NOT IN ('', 'Outros')
            GROUP BY categoria_padrao ORDER BY n DESC LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            insights.append({
                "tone": "in", "ico": "users",
                "title": f"Maior setor: {row[0]}",
                "sub": f"{row[1]:,} empresas com contato nesse setor.",
                "time": "hoje",
            })

        # Estado com mais empresas
        cur.execute("""
            SELECT uf, COUNT(*) as n FROM empresas
            WHERE telefone IS NOT NULL AND telefone != ''
              AND uf IS NOT NULL AND uf != ''
            GROUP BY uf ORDER BY n DESC LIMIT 1
        """)
        row = cur.fetchone()
        if row:
            insights.append({
                "tone": "wa", "ico": "bolt",
                "title": f"Estado dominante: {row[0]}",
                "sub": f"{row[1]:,} empresas com telefone em {row[0]}.",
                "time": "hoje",
            })

        # Sem e-mail (oportunidade)
        sem_email = total - com_email
        if sem_email > 0:
            insights.append({
                "tone": "pu", "ico": "target",
                "title": "Oportunidade de e-mail",
                "sub": f"{sem_email:,} empresas ainda sem e-mail — podem ser enriquecidas.",
                "time": "este mês",
            })

        # Cobertura de Instagram
        cur.execute("SELECT COUNT(*) FROM empresas WHERE instagram IS NOT NULL AND instagram != ''")
        com_insta = cur.fetchone()[0]
        pct_insta = round(com_insta / total * 100)
        insights.append({
            "tone": "pk", "ico": "heart",
            "title": "Cobertura de Instagram",
            "sub": f"{pct_insta}% dos CNPJs têm Instagram — {com_insta:,} perfis coletados.",
            "time": "este mês",
        })

    return insights
```

**Step 2 — api.py:**

- [ ] **Add `/api/insights` endpoint after `/api/atividade`:**
```python
_insights_cache = {"data": None, "ts": 0}
_INSIGHTS_TTL = 300  # 5 minutes

@app.get("/api/insights")
def insights(info: dict = Depends(get_token_info)):
    agora = _time.time()
    if _insights_cache["data"] and (agora - _insights_cache["ts"]) < _INSIGHTS_TTL:
        return _insights_cache["data"]
    data = db.gerar_insights()
    _insights_cache["data"] = data
    _insights_cache["ts"] = agora
    return data
```

**Step 3 — app/app.js:**

- [ ] **Add `state.insightsData` to the `state` object:**
```js
insightsData: null,
```

- [ ] **Add `loadInsights()` function after `loadAtividade()`:**
```js
async function loadInsights() {
  const data = await apiFetch("/api/insights");
  if (data && !data._err && Array.isArray(data)) {
    state.insightsData = data;
    if (state.tab === "dashboard") render();
  }
}
```

- [ ] **In `init()`, add `loadInsights()` to the initial Promise.all call:**
```js
await Promise.all([loadPlan(), loadStats(), loadCategories(), loadAtividade(), loadInsights()]);
```

- [ ] **Add `setInterval(loadInsights, 300000)` in `init()` alongside the other intervals:**
```js
setInterval(loadInsights, 300000);  // refresh insights every 5 min
```

- [ ] **In `viewDashboard()`, replace `const { insights } = DASH_MOCK` with real data:**

Find:
```js
const activity = state.atividadeData || DASH_MOCK.activity;
const { insights } = DASH_MOCK;
```
Replace with:
```js
const activity = state.atividadeData || DASH_MOCK.activity;
const insights = state.insightsData || DASH_MOCK.insights;
```

- [ ] **Verify:**
  1. Open Dashboard — the insight cards should now show real numbers (e.g. "Cobertura de e-mail: 100% dos CNPJs têm e-mail confirmado...").
  2. Check `/api/insights` directly: `curl -s http://localhost:8000/api/insights -H "Authorization: Bearer test"`

- [ ] **Commit:**
```bash
git add database.py api.py app/app.js
git commit -m "feat: dynamic insights panel — 5 real data-driven insight cards replace static mock"
```

---

## Phase 5 — Listas MVP (Domingo/Segunda — ~6h)

---

### Task 15: Implement Listas (saved lead lists)

**Why:** The "Minhas Listas" tab, "Salvar em lista" bulk button, and "Nova lista" button are all placeholder with no backend or frontend implementation.

**MVP scope:** Create list → Add selected companies → View lists with count → View companies in a list → Delete list.

**Files:**
- Modify: `database.py` — `criar_tabelas()` + new CRUD methods
- Modify: `api.py` — 5 new endpoints
- Modify: `app/app.js` — `viewListas()`, `state`, bulk action handler

**Step 1 — database.py: Tables + CRUD:**

- [ ] **Add the two new tables inside `criar_tabelas()`, at the end of the method (before `conn.commit()`):**
```python
cur.execute("""
    CREATE TABLE IF NOT EXISTS listas (
        id        INTEGER PRIMARY KEY,
        token     TEXT NOT NULL,
        nome      TEXT NOT NULL,
        cor       TEXT NOT NULL DEFAULT 'ac',
        criado_em TEXT NOT NULL
    )
""")
cur.execute("""
    CREATE TABLE IF NOT EXISTS lista_empresas (
        lista_id     INTEGER NOT NULL,
        cnpj         TEXT NOT NULL,
        adicionado_em TEXT NOT NULL,
        PRIMARY KEY (lista_id, cnpj)
    )
""")
if USE_POSTGRES:
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lista_token ON listas(token)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lista_emp_lista ON lista_empresas(lista_id)")
else:
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lista_token ON listas(token)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_lista_emp_lista ON lista_empresas(lista_id)")
```

- [ ] **Add list CRUD methods to the `Database` class:**
```python
def criar_lista(self, token: str, nome: str, cor: str = "ac") -> dict:
    agora = datetime.utcnow().isoformat()
    with _conn() as conn:
        cur = conn.cursor()
        if USE_POSTGRES:
            cur.execute(
                "INSERT INTO listas (token,nome,cor,criado_em) VALUES (%s,%s,%s,%s) RETURNING id",
                (token, nome, cor, agora)
            )
            lista_id = cur.fetchone()[0]
        else:
            cur.execute(
                "INSERT INTO listas (token,nome,cor,criado_em) VALUES (?,?,?,?)",
                (token, nome, cor, agora)
            )
            lista_id = cur.lastrowid
        conn.commit()
    return {"id": lista_id, "nome": nome, "cor": cor, "criado_em": agora, "total": 0}

def listar_listas(self, token: str) -> list:
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT l.id, l.nome, l.cor, l.criado_em, COUNT(le.cnpj) as total
            FROM listas l
            LEFT JOIN lista_empresas le ON le.lista_id = l.id
            WHERE l.token = {PH}
            GROUP BY l.id ORDER BY l.criado_em DESC
        """, (token,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

def excluir_lista(self, lista_id: int, token: str) -> bool:
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM lista_empresas WHERE lista_id = {PH}", (lista_id,))
        cur.execute(f"DELETE FROM listas WHERE id = {PH} AND token = {PH}", (lista_id, token))
        conn.commit()
        return cur.rowcount > 0

def adicionar_a_lista(self, lista_id: int, cnpjs: list, token: str) -> int:
    """Adds CNPJs to a list. Returns number added. Verifies token owns the list."""
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT id FROM listas WHERE id = {PH} AND token = {PH}", (lista_id, token))
        if not cur.fetchone():
            return 0
        agora = datetime.utcnow().isoformat()
        if USE_POSTGRES:
            cur.executemany(
                "INSERT INTO lista_empresas (lista_id,cnpj,adicionado_em) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                [(lista_id, c, agora) for c in cnpjs]
            )
        else:
            cur.executemany(
                "INSERT OR IGNORE INTO lista_empresas (lista_id,cnpj,adicionado_em) VALUES (?,?,?)",
                [(lista_id, c, agora) for c in cnpjs]
            )
        conn.commit()
        return cur.rowcount

def empresas_da_lista(self, lista_id: int, token: str, pagina=1, por_pagina=50) -> dict:
    """Returns paginated companies in a list, verifying token ownership."""
    with _conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT id FROM listas WHERE id = {PH} AND token = {PH}", (lista_id, token))
        if not cur.fetchone():
            return {"total": 0, "dados": []}
        cur.execute(f"SELECT COUNT(*) FROM lista_empresas WHERE lista_id = {PH}", (lista_id,))
        total = cur.fetchone()[0]
        offset = (pagina - 1) * por_pagina
        cur.execute(f"""
            SELECT e.* FROM empresas e
            JOIN lista_empresas le ON le.cnpj = e.cnpj
            WHERE le.lista_id = {PH}
            ORDER BY le.adicionado_em DESC
            LIMIT {PH} OFFSET {PH}
        """, (lista_id, por_pagina, offset))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    return {"total": total, "pagina": pagina, "por_pagina": por_pagina, "dados": rows}
```

**Step 2 — api.py: 5 new endpoints:**

- [ ] **Add the 5 Listas endpoints after `/api/export`:**
```python
# ─── Listas ────────────────────────────────────────────────────────────────────

@app.post("/api/listas")
def criar_lista(
    nome: str = Query(..., description="Nome da lista"),
    cor:  str = Query("ac", description="Cor: ac | in | wa | pu | pk"),
    info: dict = Depends(get_token_info),
):
    if not nome.strip():
        raise HTTPException(status_code=400, detail="Nome da lista não pode ser vazio")
    return db.criar_lista(info["token"], nome.strip(), cor)


@app.get("/api/listas")
def listar_listas(info: dict = Depends(get_token_info)):
    return db.listar_listas(info["token"])


@app.delete("/api/listas/{lista_id}")
def excluir_lista(lista_id: int, info: dict = Depends(get_token_info)):
    ok = db.excluir_lista(lista_id, info["token"])
    if not ok:
        raise HTTPException(status_code=404, detail="Lista não encontrada")
    return {"status": "excluida"}


@app.post("/api/listas/{lista_id}/empresas")
def adicionar_empresas_lista(
    lista_id: int,
    payload: dict,
    info: dict = Depends(get_token_info),
):
    cnpjs = payload.get("cnpjs", [])
    if not isinstance(cnpjs, list) or not cnpjs:
        raise HTTPException(status_code=400, detail="cnpjs deve ser lista não-vazia")
    added = db.adicionar_a_lista(lista_id, cnpjs, info["token"])
    return {"status": "ok", "adicionados": added}


@app.get("/api/listas/{lista_id}/empresas")
def empresas_lista(
    lista_id: int,
    pagina:    int = Query(1, ge=1),
    por_pagina: int = Query(50, le=200),
    info: dict = Depends(get_token_info),
):
    return db.empresas_da_lista(lista_id, info["token"], pagina, por_pagina)
```

**Step 3 — app/app.js: Listas tab + bulk action:**

- [ ] **Add `listas` and `listasLoading` to `state`:**
```js
listas: [],
listasLoading: false,
listaAtiva: null,  // id of the list currently being viewed, or null for list-of-lists
```

- [ ] **Add data-loading functions after `loadTokens()`:**
```js
async function loadListas() {
  state.listasLoading = true;
  render();
  const data = await apiFetch("/api/listas");
  state.listas = Array.isArray(data) ? data : [];
  state.listasLoading = false;
  render();
}

async function criarLista(nome, cor = "ac") {
  const params = new URLSearchParams({ nome, cor });
  const data = await apiFetch(`/api/listas?${params}`, { method: "POST" });
  if (data && !data._err) { await loadListas(); return data; }
  return null;
}

async function excluirLista(id) {
  if (!confirm("Excluir esta lista?")) return;
  await apiFetch(`/api/listas/${id}`, { method: "DELETE" });
  if (state.listaAtiva === id) state.listaAtiva = null;
  await loadListas();
}

async function salvarEmLista(listaId, cnpjs) {
  const data = await apiFetch(`/api/listas/${listaId}/empresas`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ cnpjs: [...cnpjs] }),
  });
  if (data && !data._err) {
    showToast(`${data.adicionados} empresa(s) adicionada(s) à lista.`, 'info');
    state.selected.clear();
    updateBulkBar();
    render();
  }
}
```

- [ ] **Replace `viewListas()` with a real implementation:**
```js
function viewListas() {
  if (state.listasLoading) {
    return `<div class="page-head"><div><div class="page-title">Minhas listas</div></div></div>
      <div class="panel"><div class="empty-state"><div class="big">Carregando…</div></div></div>`;
  }

  const COR_CLASSES = { ac:"ac", in:"in", wa:"wa", pu:"pu", pk:"pk" };
  const listaCards = state.listas.length === 0
    ? `<div class="empty-state" style="padding:40px">
        <div class="big">Nenhuma lista ainda</div>
        Selecione empresas na tabela e clique em "Salvar em lista".
       </div>`
    : state.listas.map(l => `
        <div class="panel" style="padding:18px;cursor:pointer" onclick="verLista(${l.id})">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:12px">
            <div class="insight-ico ${COR_CLASSES[l.cor] || 'ac'}">${ICONS.bookmark}</div>
            <button class="row-btn" title="Excluir lista" onclick="event.stopPropagation();excluirLista(${l.id})">${ICONS.trash}</button>
          </div>
          <div style="font-weight:600;margin-bottom:4px">${l.nome}</div>
          <div style="font-size:11.5px;color:var(--text-dim)">${fmt(l.total)} empresa(s) · ${timeAgo(l.criado_em)}</div>
        </div>`
    ).join("");

  return `
    <div class="page-head">
      <div><div class="page-title">Minhas listas</div><div class="page-sub">Leads salvos organizados por campanha</div></div>
      <button class="btn btn-accent" onclick="promptNovaLista()">${ICONS.plus}Nova lista</button>
    </div>
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:14px">
      ${listaCards}
    </div>`;
}

function promptNovaLista() {
  const nome = prompt("Nome da nova lista:");
  if (!nome?.trim()) return;
  criarLista(nome.trim());
}

async function verLista(id) {
  state.listaAtiva = id;
  state.listasLoading = true;
  render();
  const data = await apiFetch(`/api/listas/${id}/empresas?por_pagina=50`);
  if (data && !data._err) {
    state.dados = data.dados || [];
    state.totalDados = data.total || 0;
  }
  state.listasLoading = false;
  // Temporarily switch to empresas view within the listas tab
  const c = document.querySelector('#content');
  if (c) {
    const lista = state.listas.find(l => l.id === id);
    c.innerHTML = `
      <div class="page-head">
        <div>
          <div style="font-size:12px;color:var(--text-dim);cursor:pointer" onclick="state.listaAtiva=null;showTab('listas')">← Voltar às listas</div>
          <div class="page-title">${lista?.nome || 'Lista'}</div>
          <div class="page-sub">${fmt(state.totalDados)} empresas nesta lista</div>
        </div>
      </div>
      ${tableCard()}`;
    wireContent();
  }
}
```

- [ ] **Add "Salvar em lista" modal handler to the bulk action — replace the non-functional bulk button:**

In `viewEmpresas()`, update the "Salvar em lista" button (line ~696):
```js
<button class="btn" onclick="handleSalvarEmLista()">${ICONS.bookmark}Salvar em lista</button>
```

Add `handleSalvarEmLista()`:
```js
async function handleSalvarEmLista() {
  if (state.selected.size === 0) { showToast('Selecione empresas primeiro.', 'warn'); return; }
  if (state.listas.length === 0) {
    const nome = prompt("Nenhuma lista ainda. Nome para a nova lista:");
    if (!nome?.trim()) return;
    const lista = await criarLista(nome.trim());
    if (lista) await salvarEmLista(lista.id, state.selected);
    return;
  }
  const opcoes = state.listas.map((l, i) => `${i + 1}. ${l.nome} (${l.total} empresas)`).join('\n');
  const escolha = prompt(`Escolha a lista (número):\n${opcoes}\n\nOu 0 para criar nova lista:`);
  if (escolha === null) return;
  const idx = parseInt(escolha, 10);
  if (idx === 0) {
    const nome = prompt("Nome da nova lista:");
    if (!nome?.trim()) return;
    const lista = await criarLista(nome.trim());
    if (lista) await salvarEmLista(lista.id, state.selected);
  } else if (idx >= 1 && idx <= state.listas.length) {
    await salvarEmLista(state.listas[idx - 1].id, state.selected);
  }
}
```

- [ ] **In `showTab()`, load listas when switching to the tab:**
```js
} else if (t === "listas") {
  render();
  loadListas();
```

- [ ] **Verify:**
  1. Go to Empresas, select 3 companies, click "Salvar em lista", create a new list named "Teste".
  2. Go to Minhas Listas — the list card should appear with "3 empresas".
  3. Click the list card — table shows the 3 selected companies.
  4. Delete the list — it disappears.
  5. Check via API: `curl -s http://localhost:8000/api/listas -H "Authorization: Bearer test"`

- [ ] **Commit:**
```bash
git add database.py api.py app/app.js
git commit -m "feat: implement Listas MVP — create, add companies, view, delete saved lead lists"
```

---

## Final Checklist — Segunda até 10h

- [ ] Run full Playwright audit: `node playwright_audit/audit.mjs` — expect 0 FAILs
- [ ] Restart server clean and verify health: `curl http://localhost:8000/health`
- [ ] Check all 7 tabs load without JS console errors
- [ ] Verify export tracking: export CSV → Dashboard "Exports no mês" increments
- [ ] Verify login screen: open `http://localhost:8000` without token → login panel appears
- [ ] Verify `⌘K` / `Ctrl+K` focuses search from any tab
- [ ] Run git log to confirm all 15 commits present: `git log --oneline -20`
- [ ] If deployed to Railway: `git push origin main` and verify production health endpoint

---

## Self-Review

**Spec coverage check:**
1. ✅ socio_principal search — Task 1
2. ✅ perPage fix — Task 2
3. ✅ vacuum() leak — Task 3
4. ✅ sparkline duplicate — Task 4
5. ✅ expand race condition — Task 5
6. ✅ XSS in Clientes — Task 6
7. ✅ Date filter isolation — Task 7
8. ✅ Error toasts — Task 8
9. ✅ ⌘K shortcut — Task 9
10. ✅ Server-side sort — Task 10
11. ✅ Export filter summary — Task 11
12. ✅ Login/onboarding screen — Task 12
13. ✅ Export tracking — Task 13
14. ✅ Dynamic insights — Task 14
15. ✅ Listas MVP — Task 15

**Placeholder scan:** None found — all steps contain real code.

**Type consistency:** All function names, method signatures, and state keys are consistent across tasks that reference each other (e.g. `state.insightsData` added in Task 14 state definition and used in viewDashboard).
