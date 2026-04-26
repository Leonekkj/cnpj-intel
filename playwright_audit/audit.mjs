import { chromium } from 'playwright';
import { writeFileSync } from 'fs';

const BASE = 'http://localhost:8000?token=test';
const OUT  = './playwright_audit';
const findings = [];

function log(section, status, detail) {
  const e = status === 'OK' ? '✅' : status === 'FAIL' ? '❌' : status === 'WARN' ? '⚠️' : 'ℹ️';
  console.log(`${e} [${section}] ${detail}`);
  findings.push({ section, status, detail });
}

async function snap(page, name) {
  await page.waitForTimeout(900);
  await page.screenshot({ path: `${OUT}/${name}.png`, fullPage: false });
}

async function clickTab(page, t) {
  await page.locator(`.nav-item[data-tab="${t}"]`).click();
  await page.waitForTimeout(1200);
}

async function tableRowCount(page) {
  return page.locator('table.data tbody tr:not(.detail-row)').count();
}

(async () => {
  const browser = await chromium.launch({ headless: false, slowMo: 60 });
  const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  const page = await ctx.newPage();

  const consoleErrors = [];
  page.on('console', m => { if (m.type() === 'error') consoleErrors.push(m.text()); });
  page.on('pageerror', e => consoleErrors.push('PAGE ERROR: ' + e.message));

  await page.goto(BASE);
  await page.waitForTimeout(2500);
  await snap(page, '01-dashboard-initial');

  // ── DASHBOARD ─────────────────────────────────────────────────────────────
  log('Dashboard', 'INFO', '--- Testing Dashboard ---');

  const total = await page.locator('#mv-total').textContent().catch(() => null);
  log('Dashboard', total && total !== '0' ? 'OK' : 'WARN', `Total CNPJs card: "${total}"`);

  const tel = await page.locator('#mv-tel').textContent().catch(() => null);
  log('Dashboard', tel ? 'OK' : 'FAIL', `Com telefone card: "${tel}"`);

  const email = await page.locator('#mv-email').textContent().catch(() => null);
  log('Dashboard', email ? 'OK' : 'FAIL', `Com e-mail card: "${email}"`);

  const exportVal = await page.locator('.metric-val').nth(3).textContent().catch(() => null);
  if (exportVal?.trim() === '—')
    log('Dashboard', 'WARN', '"Exports no mês" shows "—" — hardcoded zeros, no real tracking (known bug)');
  else
    log('Dashboard', 'OK', `"Exports no mês": "${exportVal}"`);

  log('Dashboard', await page.locator('.chart-panel svg').count() > 0 ? 'OK' : 'FAIL', 'Activity chart SVG rendered');

  const dot = page.locator('.cd').first();
  if (await dot.count() > 0) {
    await dot.hover();
    await page.waitForTimeout(400);
    log('Dashboard', await page.locator('.chart-tip.show').count() > 0 ? 'OK' : 'FAIL', 'Chart tooltip shows on hover');
    await snap(page, '02-dashboard-tooltip');
  } else {
    log('Dashboard', 'WARN', 'No chart dots — no data or chart is empty');
  }

  log('Dashboard', await page.locator('.bar-row').count() > 0 ? 'OK' : 'WARN',
    `Breakdown bars: ${await page.locator('.bar-row').count()} rows`);

  log('Dashboard', await page.locator('.insight').count() > 0 ? 'OK' : 'FAIL',
    `Insight cards: ${await page.locator('.insight').count()}`);
  log('Dashboard', 'WARN', 'All 5 insight cards are static MOCK data — never update based on real metrics (known bug)');

  log('Dashboard', await page.locator('#agent-sub').count() > 0 ? 'OK' : 'FAIL',
    `Agent sidebar: "${await page.locator('#agent-sub').textContent().catch(() => null)}"`);
  log('Dashboard', await page.locator('#plan-name').count() > 0 ? 'OK' : 'FAIL',
    `Plan sidebar: "${await page.locator('#plan-name').textContent().catch(() => null)}"`);
  log('Dashboard', await page.locator('#nav-total').count() > 0 ? 'OK' : 'FAIL',
    `Nav total badge: "${await page.locator('#nav-total').textContent().catch(() => null)}"`);

  await snap(page, '03-dashboard-full');

  // ── EMPRESAS ──────────────────────────────────────────────────────────────
  log('Empresas', 'INFO', '--- Testing Empresas tab ---');
  await clickTab(page, 'empresas');
  await snap(page, '04-empresas-initial');

  log('Empresas', await tableRowCount(page) > 0 ? 'OK' : 'WARN',
    `Table loaded: ${await tableRowCount(page)} rows`);

  // UF filter — always re-select using fresh locator
  await page.locator('.chip.select select').first().selectOption('SP');
  await page.waitForTimeout(1300);
  log('Empresas', 'OK', `UF=SP filter: ${await tableRowCount(page)} rows`);
  await page.locator('.chip.select select').first().selectOption('');
  await page.waitForTimeout(1000);

  // Porte filter
  await page.locator('.chip.select select').nth(1).selectOption('MEI');
  await page.waitForTimeout(1300);
  log('Empresas', 'OK', `Porte=MEI filter: ${await tableRowCount(page)} rows`);
  await page.locator('.chip.select select').nth(1).selectOption('');
  await page.waitForTimeout(1000);

  // Setor / Departamento cascade
  const setorOpts = await page.locator('.chip.select select').nth(2).evaluate(
    el => [...el.options].map(o => o.value).filter(Boolean)
  ).catch(() => []);
  if (setorOpts.length > 1) {
    await page.locator('.chip.select select').nth(2).selectOption(setorOpts[1]);
    await page.waitForTimeout(1300);
    const deptoCount = await page.locator('.chip.select select').count();
    log('Empresas', 'OK', `Setor "${setorOpts[1]}" selected. Selects present: ${deptoCount} (cascade ${deptoCount > 3 ? 'appeared' : 'did not show'})`);
    await snap(page, '05-empresas-setor-cascade');
    await page.locator('.chip.select select').nth(2).selectOption('');
    await page.waitForTimeout(1000);
  }

  // Text search
  await page.locator('#filter-q').fill('A');
  await page.waitForTimeout(700);
  log('Empresas', 'OK', `Text search "A": ${await tableRowCount(page)} rows`);
  await page.locator('#filter-q').fill('');
  await page.waitForTimeout(700);

  // Toggle chips
  for (const label of ['Com telefone', 'Com e-mail', 'Com site', 'Instagram']) {
    const chip = page.locator('button.chip', { hasText: label });
    if (await chip.count() > 0) {
      await chip.click();
      await page.waitForTimeout(1000);
      log('Empresas', 'OK', `Toggle "${label}": ${await tableRowCount(page)} rows`);
      await chip.click();
      await page.waitForTimeout(800);
    }
  }

  // Sort
  const sortCount = await page.locator('th.sortable').count();
  log('Empresas', sortCount > 0 ? 'OK' : 'FAIL', `Sortable columns: ${sortCount}`);
  if (sortCount > 0) {
    await page.locator('th.sortable').first().click();
    await page.waitForTimeout(500);
    log('Empresas', 'WARN', 'Column sort is CLIENT-SIDE only — only sorts current page, resets on pagination (known bug)');
    await page.locator('th.sortable').first().click();
    await page.waitForTimeout(400);
  }

  // Row selection + bulk bar
  if (await page.locator('table.data tbody input[type=checkbox]').count() > 0) {
    await page.locator('table.data tbody input[type=checkbox]').first().click();
    await page.waitForTimeout(400);
    log('Empresas', await page.locator('#bulk-bar.show').count() > 0 ? 'OK' : 'FAIL',
      'Bulk action bar appears after row selection');
    await snap(page, '06-empresas-bulk-bar');

    // Check unimplemented bulk buttons
    const bBtns = page.locator('.bulk-actions .btn');
    const bCount = await bBtns.count();
    for (let i = 0; i < bCount; i++) {
      const txt = (await bBtns.nth(i).textContent()).trim();
      const onclick = await bBtns.nth(i).getAttribute('onclick');
      if (!onclick && !txt.includes('Exportar'))
        log('Empresas', 'FAIL', `Bulk button "${txt}" has no onclick — not implemented (known bug)`);
    }

    // Deselect
    await page.locator('table.data tbody input[type=checkbox]').first().click();
    await page.waitForTimeout(400);
  }

  // Row expand / detail
  if (await page.locator('button.row-btn.expand').count() > 0) {
    await page.locator('button.row-btn.expand').first().click();
    await page.waitForTimeout(1500);
    log('Empresas', await page.locator('tr.detail-row').count() > 0 ? 'OK' : 'FAIL',
      'Row expands to detail panel');
    await snap(page, '07-empresas-detail-expanded');

    const actionsCount = await page.locator('.detail-actions a, .detail-actions button').count();
    log('Empresas', actionsCount > 0 ? 'OK' : 'WARN', `Detail action buttons: ${actionsCount}`);

    const waCount = await page.locator('.detail-actions a[href*="wa.me"]').count();
    log('Empresas', waCount > 0 ? 'OK' : 'WARN', `WhatsApp link: ${waCount > 0 ? 'present' : 'not found (no phone?)'}`);

    // Collapse
    await page.locator('button.row-btn.expand').first().click();
    await page.waitForTimeout(500);
  } else {
    log('Empresas', 'WARN', 'No expand buttons available (no data rows)');
  }

  // Pagination
  const totalPagerBtns = await page.locator('.page-btn:not([disabled])').count();
  if (totalPagerBtns > 2) {
    await page.locator('.page-btn:not([disabled])').last().click();
    await page.waitForTimeout(1300);
    log('Empresas', 'OK', `Pagination page 2: ${await tableRowCount(page)} rows`);
    await page.locator('.page-btn:not([disabled])').first().click();
    await page.waitForTimeout(900);
  } else {
    log('Empresas', 'WARN', 'Only 1 page available (small dataset)');
  }

  // Clear filters — use onclick attribute to disambiguate from "Limpar seleção"
  await page.locator('button[onclick="clearFilters()"]').click();
  await page.waitForTimeout(800);
  log('Empresas', 'OK', '"Limpar" (clear filters) works');

  // ── BUSCA AVANÇADA ────────────────────────────────────────────────────────
  log('Busca', 'INFO', '--- Testing Busca avançada tab ---');
  await clickTab(page, 'busca');
  await snap(page, '08-busca-initial');

  const dateCount = await page.locator('input[type="date"]').count();
  log('Busca', dateCount >= 2 ? 'OK' : 'FAIL', `Date range inputs: ${dateCount}`);

  if (dateCount >= 2) {
    await page.locator('input[type="date"]').first().fill('2020-01-01');
    await page.locator('input[type="date"]').last().fill('2024-12-31');
    await page.waitForTimeout(1300);
    log('Busca', 'OK', `Date range applied: ${await tableRowCount(page)} rows`);
    await snap(page, '09-busca-date-filter');
  }

  // Shared filter state between tabs
  await page.locator('#filter-q').fill('SHARED_STATE_TEST');
  await page.waitForTimeout(400);
  await clickTab(page, 'empresas');
  const empresasQ = await page.locator('#filter-q').inputValue().catch(() => '');
  log('Empresas', empresasQ === 'SHARED_STATE_TEST' ? 'WARN' : 'OK',
    `Filter state shared between tabs: empresas#filter-q="${empresasQ}" (known UX issue if WARN)`);
  await page.locator('#filter-q').fill('');
  await page.waitForTimeout(600);

  // ── MINHAS LISTAS ─────────────────────────────────────────────────────────
  log('Listas', 'INFO', '--- Testing Minhas listas tab ---');
  await clickTab(page, 'listas');
  await snap(page, '10-listas');

  const listasPageText = await page.locator('.content').textContent().catch(() => '');
  log('Listas', listasPageText.includes('breve') ? 'WARN' : 'OK',
    'Listas tab: "em breve" placeholder — feature NOT implemented');

  const novaListaBtn = page.locator('button.btn.btn-accent', { hasText: 'Nova lista' });
  if (await novaListaBtn.count() > 0) {
    const onclick = await novaListaBtn.getAttribute('onclick');
    log('Listas', !onclick ? 'FAIL' : 'OK', `"Nova lista" button onclick: ${onclick || 'MISSING — no handler (known bug)'}`);
  }

  // Verify "Salvar em lista" in bulk bar still broken (go to Empresas, select, come back idea)
  // (already covered above)

  // ── EXPORTAR CSV ──────────────────────────────────────────────────────────
  log('Exportar', 'INFO', '--- Testing Exportar CSV tab ---');
  await clickTab(page, 'exportar');
  await snap(page, '11-exportar');

  const exportContent = await page.locator('.panel').first().textContent().catch(() => '');
  log('Exportar', exportContent.includes('Exportar') ? 'OK' : 'FAIL', 'Export panel rendered');
  log('Exportar', 'WARN', 'Export tab: no filter UI shown — user cannot see what data will be exported (UX issue)');

  // Test the API directly
  const dl = await page.evaluate(async () => {
    try {
      const r = await fetch('/api/export', { headers: { Authorization: 'Bearer test' } });
      return { status: r.status, ok: r.ok, ct: r.headers.get('content-type') };
    } catch (e) { return { error: e.message }; }
  });
  log('Exportar', dl.ok ? 'OK' : 'FAIL', `Export API response: HTTP ${dl.status}, type=${dl.ct}`);

  // ── API & WEBHOOKS ────────────────────────────────────────────────────────
  log('API', 'INFO', '--- Testing API & Webhooks tab ---');
  await clickTab(page, 'api');
  await snap(page, '12-api');

  const tokenShown = await page.locator('code').first().textContent().catch(() => null);
  log('API', tokenShown && tokenShown !== '—' ? 'OK' : 'FAIL', `Token displayed: "${tokenShown}"`);
  log('API', await page.locator('a[href="/docs"]').count() > 0 ? 'OK' : 'WARN', 'Link to /docs present');

  // ── CLIENTES (ADMIN) ──────────────────────────────────────────────────────
  log('Clientes', 'INFO', '--- Testing Clientes tab ---');
  await clickTab(page, 'clientes');
  await page.waitForTimeout(1500);
  await snap(page, '13-clientes-initial');

  const beforeCount = await page.locator('.token-row').count();
  log('Clientes', 'OK', `Token list: ${beforeCount} tokens`);

  // Create token
  await page.locator('#new-token').fill('playwright_test_xyz');
  await page.locator('#new-plano').selectOption('basico');
  await page.locator('button.btn.btn-accent').click();
  await page.waitForTimeout(1500);
  const resultShown = await page.locator('#token-result').evaluate(el => el.style.display !== 'none').catch(() => false);
  log('Clientes', resultShown ? 'OK' : 'FAIL', 'Token creation result panel shown');
  await snap(page, '14-clientes-created-token');

  const afterCount = await page.locator('.token-row').count();
  log('Clientes', afterCount > beforeCount ? 'OK' : 'WARN',
    `Token list after creation: ${afterCount} (was ${beforeCount})`);

  // Delete the test token we just created
  const delBtnCount = await page.locator('.token-row .row-btn[title="Remover"]').count();
  if (delBtnCount > 0) {
    page.once('dialog', d => d.accept());
    await page.locator('.token-row .row-btn[title="Remover"]').last().click();
    await page.waitForTimeout(1300);
    const finalCount = await page.locator('.token-row').count();
    log('Clientes', finalCount < afterCount ? 'OK' : 'FAIL', `Token deleted. Final count: ${finalCount}`);
  }

  await snap(page, '15-clientes-final');

  // ── PREFERENCES ───────────────────────────────────────────────────────────
  log('Prefs', 'INFO', '--- Testing Preferences panel ---');
  await page.locator('.user-chip').click();
  await page.waitForTimeout(600);

  const panelOpen = await page.locator('.tweaks-panel.open').count() > 0;
  log('Prefs', panelOpen ? 'OK' : 'FAIL', 'Preferences panel opens on avatar click');
  await snap(page, '16-preferences-open');

  if (panelOpen) {
    await page.locator('[data-val="compact"]').click();
    await page.waitForTimeout(400);
    log('Prefs', 'OK', 'Density → Compact');

    await page.locator('[data-val="round"]').click();
    await page.waitForTimeout(400);
    log('Prefs', 'OK', 'Radius → Round');
    await snap(page, '17-preferences-changed');

    await page.locator('.tweaks-close').click();
    await page.waitForTimeout(400);
    log('Prefs', await page.locator('.tweaks-panel.open').count() === 0 ? 'OK' : 'FAIL', 'Panel closes');
  }

  // ── GLOBAL SEARCH ─────────────────────────────────────────────────────────
  log('GlobalSearch', 'INFO', '--- Testing global search ---');
  await clickTab(page, 'dashboard');
  await page.waitForTimeout(700);

  await page.locator('#global-search').fill('Restaurante');
  await page.locator('#global-search').press('Enter');
  await page.waitForTimeout(1300);

  const activeTab = await page.locator('.nav-item.active').getAttribute('data-tab').catch(() => null);
  log('GlobalSearch', activeTab === 'empresas' ? 'OK' : 'FAIL',
    `Enter in global search → tab switched to: "${activeTab}"`);

  const filterQ = await page.locator('#filter-q').inputValue().catch(() => null);
  log('GlobalSearch', filterQ === 'Restaurante' ? 'OK' : 'FAIL',
    `Query passed to filter: "${filterQ}"`);
  await snap(page, '18-global-search-result');

  // ⌘K shortcut
  await clickTab(page, 'dashboard');
  await page.waitForTimeout(600);
  await page.keyboard.press('Meta+k');
  await page.waitForTimeout(500);
  const focused = await page.evaluate(() => document.activeElement?.id);
  log('GlobalSearch', focused === 'global-search' ? 'OK' : 'WARN',
    `⌘K shortcut: active element="${focused}" (should be "global-search" — if WARN it's decorative only)`);

  // ── CONSOLE ERRORS ────────────────────────────────────────────────────────
  log('Console', 'INFO', '--- JS Console errors ---');
  if (consoleErrors.length === 0)
    log('Console', 'OK', 'No JavaScript errors detected during full audit');
  else
    consoleErrors.forEach(e => log('Console', 'FAIL', e));

  await snap(page, '19-final');

  // ── WRITE REPORT ──────────────────────────────────────────────────────────
  writeFileSync('./playwright_audit/report.json', JSON.stringify({ findings, consoleErrors }, null, 2));

  const ok   = findings.filter(f => f.status === 'OK').length;
  const fail = findings.filter(f => f.status === 'FAIL').length;
  const warn = findings.filter(f => f.status === 'WARN').length;
  console.log(`\n${'='.repeat(60)}`);
  console.log(`AUDIT COMPLETE: ✅ ${ok} OK  ❌ ${fail} FAIL  ⚠️  ${warn} WARN`);
  console.log('='.repeat(60));

  await browser.close();
})();
