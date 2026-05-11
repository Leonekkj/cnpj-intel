import { chromium } from 'playwright';
import { existsSync, mkdirSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── CONFIG ────────────────────────────────────────────────────────────────────
const APP_URL       = 'http://localhost:8000?token=demo123';
const AUTH_FILE     = path.join(__dirname, 'auth.json');
const RECORDINGS    = path.join(__dirname, '..', 'recordings');

// Adjust these if you need more/less time per scene
const T = {
  scene1:     20_000,   // 0:00–0:20  black screen intro
  scene2:     30_000,   // 0:20–0:50  filter selection
  scene3:     25_000,   // 0:50–1:15  results + scroll
  scene4:     30_000,   // 1:15–1:45  export CSV
  scene5:     25_000,   // 1:45–2:10  whatsapp click
  scene6:     20_000,   // 2:10–2:30  CTA + fade to black
};

const GANCHO_TEXT =
  'Você passa horas procurando o telefone de uma empresa no Google... ' +
  'ou paga caro por uma lista desatualizada. Dá pra fazer diferente.';

// ── HELPERS ───────────────────────────────────────────────────────────────────
const sleep = ms => new Promise(r => setTimeout(r, ms));

if (!existsSync(RECORDINGS)) mkdirSync(RECORDINGS, { recursive: true });

// ── MAIN ──────────────────────────────────────────────────────────────────────
(async () => {
  const hasAuth = existsSync(AUTH_FILE);

  const browser = await chromium.launch({
    headless: false,
    slowMo: 0,
    args: ['--window-size=1920,1080', '--window-position=0,0'],
  });

  const context = await browser.newContext({
    storageState: hasAuth ? AUTH_FILE : undefined,
    viewport: { width: 1920, height: 1080 },
    recordVideo: {
      dir: RECORDINGS,
      size: { width: 1920, height: 1080 },
    },
  });

  const page = await context.newPage();

  // ── FIRST-RUN AUTH ─────────────────────────────────────────────────────────
  if (!hasAuth) {
    console.log('\n🔐 First run: log in with Google in the browser, then press Resume in the Playwright inspector.\n');
    await page.goto(APP_URL);
    await page.pause();
    await context.storageState({ path: AUTH_FILE });
    console.log('✅ Auth saved to', AUTH_FILE);
  }

  const start = Date.now();
  const elapsed = () => `${((Date.now() - start) / 1000).toFixed(1)}s`;

  // ── SCENE 1: GANCHO (0–20s) ───────────────────────────────────────────────
  console.log(`\n🎬 Scene 1: GANCHO  [${elapsed()}]`);

  await page.goto('about:blank');
  await page.evaluate((text) => {
    document.body.style.cssText =
      'margin:0;padding:0;background:#000;display:flex;align-items:center;' +
      'justify-content:center;height:100vh;overflow:hidden;';

    const p = document.createElement('p');
    p.style.cssText =
      'color:#ffffff;font-size:54px;font-family:system-ui,-apple-system,BlinkMacSystemFont,sans-serif;' +
      'text-align:center;max-width:78%;line-height:1.55;font-weight:300;letter-spacing:-0.5px;';
    document.body.appendChild(p);

    const words = text.split(' ');
    const msPerWord = 17500 / words.length; // leaves ~2.5s of full text visible
    let i = 0;
    const iv = setInterval(() => {
      if (i < words.length) {
        p.textContent += (i > 0 ? ' ' : '') + words[i++];
      } else {
        clearInterval(iv);
      }
    }, msPerWord);
  }, GANCHO_TEXT);

  await sleep(T.scene1);

  // ── SCENE 2: FILTRO (20–50s) ──────────────────────────────────────────────
  console.log(`\n🎬 Scene 2: FILTRO  [${elapsed()}]`);

  await page.goto(APP_URL);
  await page.waitForLoadState('networkidle').catch(() => {});
  await sleep(2500);

  // Navigate to Empresas tab if it exists
  const empresasTab = page.locator('.nav-item[data-tab="empresas"]');
  if (await empresasTab.count() > 0) {
    await empresasTab.click();
    await sleep(1500);
  }

  await page.waitForSelector('table.data tbody tr', { timeout: 8000 }).catch(() => {});
  await sleep(1000);

  // Select UF = SP
  const ufSelect = page.locator('.chip.select select').first();
  await ufSelect.hover();
  await sleep(600);
  await ufSelect.selectOption('SP');
  await sleep(4000);

  // Select Setor/CNAE ≈ restaurante (3rd select, if present)
  const setorSelect = page.locator('.chip.select select').nth(2);
  if (await setorSelect.count() > 0) {
    await setorSelect.hover();
    await sleep(600);
    const matched = await setorSelect
      .selectOption({ label: /aliment|restaur|bar|cafe|lanchon|bares/i })
      .catch(async () => {
        // fallback: pick first non-empty option
        const opts = await setorSelect.locator('option').all();
        for (const opt of opts) {
          const val = await opt.getAttribute('value');
          if (val && val.trim() !== '') {
            await setorSelect.selectOption(val);
            break;
          }
        }
        return null;
      });
    void matched;
    await sleep(4000);
  }

  // Select Porte = MICRO EMPRESA (2nd select)
  const porteSelect = page.locator('.chip.select select').nth(1);
  await porteSelect.hover();
  await sleep(600);
  await porteSelect
    .selectOption({ label: /micro empresa/i })
    .catch(async () => {
      const opts = await porteSelect.locator('option').all();
      if (opts.length > 1) await porteSelect.selectOption({ index: 1 });
    });
  await sleep(4000);

  // Let results settle
  await page.waitForLoadState('networkidle').catch(() => {});
  await sleep(8000); // pad remaining scene 2 time

  // ── SCENE 3: RESULTADO (50s–1:15) ─────────────────────────────────────────
  console.log(`\n🎬 Scene 3: RESULTADO  [${elapsed()}]`);

  await page.waitForSelector('table.data tbody tr:not(.detail-row)', { timeout: 8000 }).catch(() => {});
  await sleep(3500);

  // Slow, deliberate scroll showing phone numbers
  for (let i = 0; i < 6; i++) {
    await page.mouse.wheel(0, 190);
    await sleep(2500);
  }

  // Scroll back to top
  await page.keyboard.press('Home');
  await sleep(3000);

  // ── SCENE 4: EXPORTAR (1:15–1:45) ─────────────────────────────────────────
  console.log(`\n🎬 Scene 4: EXPORTAR  [${elapsed()}]`);

  const exportBtn = page
    .getByRole('button', { name: /export/i })
    .first()
    .or(page.locator('button.btn-accent').first());

  await exportBtn.hover();
  await sleep(800);

  // Start download — use Promise.all to avoid timing issues with download event
  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: 8000 }).catch(() => null),
    exportBtn.click(),
  ]);

  if (download) {
    console.log(`  📥 Download triggered: ${download.suggestedFilename()}`);
  }

  await sleep(6000);  // show browser download bar
  await sleep(22000); // hold on the page

  // ── SCENE 5: WHATSAPP (1:45–2:10) ─────────────────────────────────────────
  console.log(`\n🎬 Scene 5: WHATSAPP  [${elapsed()}]`);

  const waLink = page.locator('a[href^="https://wa.me"]').first();

  if (await waLink.count() > 0) {
    await waLink.hover();
    await sleep(600);

    const [waPage] = await Promise.all([
      context.waitForEvent('page'),
      waLink.click(),
    ]);

    await waPage.waitForLoadState('domcontentloaded').catch(() => {});
    await sleep(9000);
    await waPage.close().catch(() => {});
  } else {
    console.log('  ⚠️  No wa.me link found — skipping WhatsApp click');
    await sleep(9000);
  }

  await sleep(14000); // hold for rest of scene 5

  // ── SCENE 6: CTA (2:10–2:30) ──────────────────────────────────────────────
  console.log(`\n🎬 Scene 6: CTA  [${elapsed()}]`);

  await page.bringToFront();
  await sleep(3000);

  // Highlight empty search input
  await page.click('#filter-q').catch(() => {});
  await sleep(5000);

  // Fade to black — ready for logo/CTA overlay in editing
  await page.evaluate(() => {
    const overlay = document.createElement('div');
    overlay.style.cssText =
      'position:fixed;inset:0;background:#000;opacity:0;z-index:2147483647;' +
      'transition:opacity 3s ease;pointer-events:none;';
    document.body.appendChild(overlay);
    // Double rAF ensures the transition actually fires
    requestAnimationFrame(() =>
      requestAnimationFrame(() => { overlay.style.opacity = '1'; })
    );
  });

  await sleep(10000); // 3s fade + 7s on black

  // ── DONE ──────────────────────────────────────────────────────────────────
  console.log(`\n✅ All scenes done  [${elapsed()}]`);
  console.log('Closing context...');
  await context.close();
  await browser.close();

  console.log(`\n📹 Video saved in: ${RECORDINGS}/`);
  console.log('   Open the folder and pick the most recently modified .webm file.\n');
})().catch(err => {
  console.error('\n❌ Recording failed:', err);
  process.exit(1);
});
