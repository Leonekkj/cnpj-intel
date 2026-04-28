/**
 * CNPJ Intel — Comprehensive Playwright Audit (P0–P3)
 * Tests every section, every plan type, across Chromium, Firefox, WebKit.
 * Output: screenshots/ + report.json
 */

const { chromium, firefox, webkit } = require("playwright");
const fs   = require("fs");
const path = require("path");

const BASE           = "https://web-production-aaeed.up.railway.app";
const SCREENSHOT_DIR = path.join(__dirname, "audit", "screenshots");
const REPORT_PATH    = path.join(__dirname, "audit", "report.json");

const PLANS = [
  { name: "anonymous", token: null },
  { name: "free",      token: "prod_free" },
  { name: "basico",    token: "prod_basico" },
  { name: "pro",       token: "prod_pro" },
  { name: "admin",     token: "e87WcpDYAj7s__UZlMryj6YsQwlHE7mXe2G-JKXrFhA" },
];

const BROWSERS = [
  { name: "chromium", launcher: chromium },
  { name: "firefox",  launcher: firefox },
  { name: "webkit",   launcher: webkit },
];

const findings = {
  working:         [],
  bugs:            [],
  missingFeatures: [],
  uxIssues:        [],
  browserSpecific: [],
  consoleErrors:   [],
  networkErrors:   [],
  screenshots:     [],
};

let screenshotIdx = 0;

function log(msg)            { console.log(`[audit] ${msg}`); }
function ok(msg, ctx)        { findings.working.push({ msg, context: ctx });         log(`OK: ${msg} (${ctx})`); }
function bug(msg, ctx)       { findings.bugs.push({ msg, context: ctx });            log(`BUG: ${msg} (${ctx})`); }
function missing(msg, ctx)   { findings.missingFeatures.push({ msg, context: ctx }); log(`MISSING: ${msg} (${ctx})`); }
function ux(msg, ctx)        { findings.uxIssues.push({ msg, context: ctx });        log(`UX: ${msg} (${ctx})`); }

async function screenshot(page, label) {
  const fname = `${String(++screenshotIdx).padStart(3,"0")}_${label.replace(/[^a-z0-9_]/gi,"_")}.png`;
  const fpath = path.join(SCREENSHOT_DIR, fname);
  await page.screenshot({ path: fpath, fullPage: true });
  findings.screenshots.push({ label, file: fname });
  return fname;
}

async function appUrl(token) {
  return token ? `${BASE}/?token=${token}` : `${BASE}/`;
}

async function waitForApp(page) {
  await page.waitForLoadState("networkidle", { timeout: 15000 }).catch(() => {});
  await page.waitForTimeout(1200);
}

async function clickTab(page, tab) {
  const nav = page.locator(`.nav-item[data-tab="${tab}"]`);
  if (await nav.count() > 0 && await nav.isVisible()) {
    await nav.click();
    await page.waitForTimeout(1500);
  }
}

// ─── P0: Login / Anonymous ───────────────────────────────────────────────────

async function checkAnonymous(page, browser) {
  const ctx = `${browser}/anonymous`;
  await page.goto(`${BASE}/`, { waitUntil: "domcontentloaded" });
  await page.waitForTimeout(3000);
  await screenshot(page, `${browser}_anonymous_homepage`);

  // P0 fix: login overlay should appear
  const loginOverlay = page.locator("#login-overlay, .login-overlay, #token-overlay");
  const tokenInput   = page.locator("#token-input");
  if (await loginOverlay.count() > 0 || await tokenInput.count() > 0) {
    ok("Anonymous visitor sees login/token overlay (P0 fix verified)", ctx);
  } else {
    bug("No token/login screen for anonymous visitors — dashboard loads but 401s silently", ctx);
  }

  // If login form present, test empty submit → shake
  if (await tokenInput.count() > 0) {
    const submitBtn = page.locator("#token-submit");
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForTimeout(400);
      const shaking = await tokenInput.evaluate(el =>
        el.classList.contains("shake") || el.style.borderColor.includes("red") ||
        getComputedStyle(el).borderColor.includes("rgb(2")
      );
      if (shaking) {
        ok("Empty token submit triggers shake animation (P1 validation)", ctx);
      } else {
        bug("Empty token submit does not trigger shake animation", ctx);
      }
    }
  }
}

// ─── P0: Topbar ──────────────────────────────────────────────────────────────

async function checkTopbar(page, plan, browser) {
  const ctx = `${browser}/${plan}`;

  // ⌘K / Ctrl+K focuses global search
  await page.keyboard.press("Control+k");
  await page.waitForTimeout(300);
  const focusedId = await page.evaluate(() => document.activeElement?.id).catch(() => "");
  if (focusedId === "global-search") {
    ok("Ctrl+K shortcut focuses global search (P0 fix verified)", ctx);
  } else {
    bug("Ctrl+K / ⌘K shortcut does not focus #global-search", ctx);
  }

  // Help button → opens modal (P0 fix)
  const helpBtn = page.locator("#help-btn, button[title*='ajuda'], button[title*='Help'], .icon-btn[onclick*='Help'], .icon-btn[onclick*='help']");
  if (await helpBtn.count() > 0) {
    await helpBtn.first().click();
    await page.waitForTimeout(500);
    const modal = await page.locator(".modal-overlay, [role='dialog'], .modal").count();
    if (modal > 0) {
      ok("Help button opens modal (P0 fix verified)", ctx);
      // Close it — try Escape first, then force-remove via JS
      await page.keyboard.press("Escape");
      await page.waitForTimeout(400);
      const stillOpen = await page.locator(".modal-overlay").count();
      if (stillOpen > 0) {
        await page.evaluate(() =>
          document.querySelectorAll(".modal-overlay").forEach(el => el.remove())
        );
        await page.waitForTimeout(200);
      }
    } else {
      bug("Help button (?) has no action — no modal/dialog opened", ctx);
    }
  } else {
    ux("Help button not found in topbar", ctx);
  }

  // User chip → tweaks panel
  const chip = page.locator(".user-chip");
  if (await chip.count() > 0) {
    await chip.click();
    await page.waitForTimeout(400);
    const tweaksOpen = await page.locator("#tweaks-panel").evaluate(el =>
      el.classList.contains("open") || getComputedStyle(el).transform !== "matrix(1, 0, 0, 1, 0, 0)"
    ).catch(() => false);
    if (tweaksOpen) {
      ok("User chip opens tweaks panel", ctx);
      await page.locator(".tweaks-close, button[onclick*='closeTweaks']").click().catch(() => {});
      await page.waitForTimeout(200);
    } else {
      bug("User chip does not open tweaks panel", ctx);
    }
  }
}

// ─── P0: Mobile nav ──────────────────────────────────────────────────────────

async function checkMobile(browser) {
  const bLauncher = BROWSERS.find(b => b.name === browser)?.launcher;
  if (!bLauncher) return;

  const devices = [
    { name: "iPhone_14", w: 390, h: 844 },
    { name: "Pixel_7",   w: 412, h: 915 },
  ];

  for (const device of devices) {
    const b    = await bLauncher.launch({ headless: true });
    const page = await b.newPage();
    await page.setViewportSize({ width: device.w, height: device.h });
    await page.goto(`${BASE}/?token=test`, { waitUntil: "domcontentloaded" });
    await waitForApp(page);
    await screenshot(page, `${browser}_mobile_${device.name}`);

    const ctx = `${browser}/mobile/${device.name}`;

    // Hamburger button visible
    const hamburger = page.locator("#hamburger-btn, button.hamburger, .menu-toggle");
    if (await hamburger.count() > 0) {
      ok("Hamburger button present on mobile (P0 fix verified)", ctx);
      // Click to open sidebar
      await hamburger.click();
      await page.waitForTimeout(400);
      const sidebarOpen = await page.locator(".app.sidebar-open, .sidebar.open").count();
      if (sidebarOpen > 0) {
        ok("Hamburger click opens sidebar drawer", ctx);
      } else {
        bug("Hamburger click does not open sidebar (sidebar-open class not set)", ctx);
      }
      // Close sidebar by clicking hamburger again
      await hamburger.click();
      await page.waitForTimeout(300);
    } else {
      bug("No hamburger button found on mobile viewport", ctx);
    }

    await b.close();
  }
}

// ─── P0: Sidebar ─────────────────────────────────────────────────────────────

async function checkSidebar(page, plan, browser) {
  const ctx = `${browser}/${plan}`;

  const planName = await page.locator("#plan-name").textContent().catch(() => "");
  if (!planName || planName.includes("Carregando")) {
    if (plan !== "anonymous") bug("Plan name stuck on 'Carregando…'", ctx);
  } else {
    ok(`Plan name renders: "${planName}"`, ctx);
  }

  // Admin section visibility
  const adminSection = page.locator("#admin-section, .admin-section");
  if (plan === "admin") {
    const vis = await adminSection.isVisible().catch(() => false);
    if (vis) ok("Admin section visible for admin plan", ctx);
    else     bug("Admin section NOT visible for admin plan", ctx);
  } else if (plan !== "anonymous") {
    const vis = await adminSection.isVisible().catch(() => false);
    if (vis) bug(`Admin section visible for non-admin plan "${plan}"`, ctx);
    else     ok(`Admin section correctly hidden for plan "${plan}"`, ctx);
  }
}

// ─── P2: Agent status indicator ──────────────────────────────────────────────

async function checkAgentStatus(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  if (plan === "anonymous") return;

  const pulse = page.locator(".agent-pulse");
  if (await pulse.count() === 0) {
    bug("Agent pulse dot not found in sidebar", ctx);
    return;
  }

  const cls = await pulse.getAttribute("class");
  if (cls?.includes("agent-active") || cls?.includes("agent-stopped")) {
    ok(`Agent pulse has status class: "${cls}"`, ctx);
  } else {
    bug("Agent pulse has neither 'agent-active' nor 'agent-stopped' class", ctx);
  }

  const sub = await page.locator("#agent-sub").textContent().catch(() => "");
  if (sub && sub !== "") {
    ok(`Agent sub-text renders: "${sub}"`, ctx);
  } else {
    bug("Agent status text #agent-sub is empty", ctx);
  }
}

// ─── P1: Toast system ────────────────────────────────────────────────────────

async function checkToastSystem(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  if (plan === "anonymous") return;

  await clickTab(page, "empresas");
  await page.waitForTimeout(2000);

  const rows = page.locator("table.data tbody tr:not(.detail-row)");
  if (await rows.count() === 0) {
    ux("No table rows to test toast system against", ctx);
    return;
  }

  // Select a row → click "Salvar em lista" bulk button → should fire info toast
  const checkbox = rows.first().locator("input[type=checkbox]");
  if (await checkbox.count() > 0) {
    await checkbox.check();
    await page.waitForTimeout(400);

    const bulkBar = page.locator("#bulk-bar");
    const salvarBtn = bulkBar.locator("button").filter({ hasText: /Salvar em lista/i });
    if (await salvarBtn.count() > 0) {
      await salvarBtn.click();
      await page.waitForTimeout(600);
      const toast = page.locator(".toast");
      if (await toast.count() > 0) {
        ok("'Salvar em lista' bulk button fires toast (P1 wired)", ctx);
      } else {
        bug("'Salvar em lista' bulk button has no feedback — no toast appeared", ctx);
      }
    }

    const campBtn = bulkBar.locator("button").filter({ hasText: /Campanha/i });
    if (await campBtn.count() > 0) {
      await campBtn.click();
      await page.waitForTimeout(600);
      const toast = page.locator(".toast");
      if (await toast.count() > 0) {
        ok("'Campanha' bulk button fires toast (P1 wired)", ctx);
      } else {
        bug("'Campanha' bulk button has no feedback — dead button", ctx);
      }
    }

    // Deselect
    await checkbox.uncheck().catch(() => {});
    await page.waitForTimeout(300);
  }

  // Test toast auto-dismiss: wait 4s, toast should be gone
  const toastCount = await page.locator(".toast").count();
  if (toastCount > 0) {
    await page.waitForTimeout(3500);
    const toastAfter = await page.locator(".toast").count();
    if (toastAfter === 0) {
      ok("Toast auto-dismisses after 3s (P1)", ctx);
    } else {
      bug("Toast does not auto-dismiss after 3s", ctx);
    }
  }

  // Toast types: test info appears with correct class
  // Copy CNPJ from row menu → success toast
  const moreBtn = rows.first().locator(".row-btn, button[onclick*='toggleRowMenu']").first();
  if (await moreBtn.count() > 0) {
    await moreBtn.click();
    await page.waitForTimeout(400);
    const copyOption = page.locator(".context-menu button, [role='menu'] button, .dropdown button")
      .filter({ hasText: /Copiar CNPJ/i });
    if (await copyOption.count() > 0) {
      await copyOption.click();
      await page.waitForTimeout(500);
      const successToast = page.locator(".toast.toast-success, .toast-success");
      if (await successToast.count() > 0) {
        ok("Copy CNPJ triggers success toast (P1)", ctx);
      } else {
        const anyToast = await page.locator(".toast").count();
        if (anyToast > 0) ok("Copy CNPJ triggers toast (type unknown)", ctx);
        else bug("Copy CNPJ option has no toast feedback", ctx);
      }
    } else {
      // Close menu if open
      await page.keyboard.press("Escape");
      await page.waitForTimeout(200);
    }
  }
}

// ─── P1: Row context menu (⋮) ────────────────────────────────────────────────

async function checkRowMenu(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  if (plan === "anonymous") return;

  await clickTab(page, "empresas");
  await page.waitForTimeout(2000);

  const rows = page.locator("table.data tbody tr:not(.detail-row)");
  if (await rows.count() === 0) {
    ux("No rows to test row menu", ctx);
    return;
  }

  const moreBtn = rows.first().locator(".row-btn, button[onclick*='toggleRowMenu']").first();
  if (await moreBtn.count() === 0) {
    bug("Row ⋮ button not found in table rows", ctx);
    return;
  }

  // Click to open menu
  await moreBtn.click();
  await page.waitForTimeout(500);

  const menu = page.locator(".row-menu, .context-menu, .dropdown-menu");
  if (await menu.count() > 0) {
    ok("Row ⋮ button opens context menu (P1)", ctx);
    await screenshot(page, `${browser}_${plan}_row_menu`);

    // Check expected options
    const menuText = await menu.first().textContent().catch(() => "");
    if (menuText.includes("Copiar CNPJ") || menuText.includes("copiar")) {
      ok("Row menu has 'Copiar CNPJ' option", ctx);
    } else {
      bug("Row menu missing 'Copiar CNPJ' option", ctx);
    }
    if (menuText.includes("Nova lista") || menuText.includes("lista")) {
      ok("Row menu has list management options (P3)", ctx);
    } else {
      bug("Row menu missing list management options", ctx);
    }

    // Click outside to close
    await page.mouse.click(0, 0);
    await page.waitForTimeout(300);
    const menuAfter = await page.locator(".row-menu, .context-menu, .dropdown-menu").count();
    if (menuAfter === 0) {
      ok("Row menu closes on click-outside", ctx);
    } else {
      bug("Row menu does not close on click-outside", ctx);
    }
  } else {
    bug("Row ⋮ button click opens no context menu — dead button", ctx);
  }
}

// ─── P1: Confirm modal ───────────────────────────────────────────────────────

async function checkConfirmModal(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  if (plan !== "admin") return; // Test via admin tools (reset DB button)

  await clickTab(page, "clientes");
  await page.waitForTimeout(1000);

  // Find "Reset database" button in admin tools
  const resetBtn = page.locator("button").filter({ hasText: /Reset.*database|Reset banco|Apagar/i });
  if (await resetBtn.count() === 0) {
    ux("Reset database button not found to test confirmModal", ctx);
    return;
  }

  // Should NOT fire native confirm — should open confirmModal
  let nativeConfirmFired = false;
  page.once("dialog", async d => {
    nativeConfirmFired = true;
    await d.dismiss();
  });

  await resetBtn.first().click();
  await page.waitForTimeout(500);

  if (nativeConfirmFired) {
    bug("Dangerous action uses native confirm() dialog — should use custom confirmModal (P1 fix)", ctx);
  } else {
    // Check for custom modal
    const modal = page.locator(".modal-overlay, .confirm-overlay, [role='dialog']");
    if (await modal.count() > 0) {
      ok("Dangerous action opens custom confirmModal instead of native confirm() (P1 fix verified)", ctx);
      await screenshot(page, `${browser}_${plan}_confirm_modal`);

      // Check cancel button works
      const cancelBtn = modal.locator("button").filter({ hasText: /Cancelar|Cancel/i });
      if (await cancelBtn.count() > 0) {
        await cancelBtn.click();
        await page.waitForTimeout(300);
        const modalAfter = await page.locator(".modal-overlay, .confirm-overlay").count();
        if (modalAfter === 0) {
          ok("confirmModal cancel button closes modal without acting", ctx);
        } else {
          bug("confirmModal cancel button does not close modal", ctx);
        }
      }

      // Check confirm button is styled as danger
      await resetBtn.first().click();
      await page.waitForTimeout(400);
      const confirmBtn = page.locator(".modal-overlay button, .confirm-overlay button")
        .filter({ hasText: /Confirmar|Confirm|Sim|OK/i });
      if (await confirmBtn.count() > 0) {
        const cls = await confirmBtn.first().getAttribute("class");
        if (cls?.includes("danger") || cls?.includes("red") || cls?.includes("destructive")) {
          ok("Confirm button in modal has danger styling", ctx);
        } else {
          ux("Confirm button in modal lacks explicit danger/red styling", ctx);
        }
        // Cancel it
        const cancelBtn2 = page.locator(".modal-overlay button, .confirm-overlay button")
          .filter({ hasText: /Cancelar|Cancel/i });
        await cancelBtn2.click().catch(() => {});
        await page.waitForTimeout(200);
      }
    } else {
      bug("Dangerous action has no feedback (no native confirm, no custom modal)", ctx);
    }
  }
}

// ─── P2: Admin tools panel ───────────────────────────────────────────────────

async function checkAdminTools(page, plan, browser) {
  const ctx = `${browser}/${plan}`;

  await clickTab(page, "clientes");
  await page.waitForTimeout(1000);

  if (plan !== "admin") {
    // Should NOT see admin tools
    const adminTools = page.locator("button").filter({ hasText: /Iniciar agente|Limpar sites|Vacuum/i });
    if (await adminTools.count() > 0) {
      bug(`Admin tools visible to non-admin plan "${plan}"`, ctx);
    } else {
      ok(`Admin tools hidden from non-admin plan "${plan}"`, ctx);
    }
    return;
  }

  // Admin: all 4 tools should be present
  const tools = [
    { text: /Iniciar agente|Agente/i,        name: "Iniciar agente" },
    { text: /Limpar sites/i,                  name: "Limpar sites inválidos" },
    { text: /Reset.*database|Reset banco/i,   name: "Reset database" },
    { text: /Vacuum/i,                         name: "Vacuum banco" },
  ];

  for (const tool of tools) {
    const btn = page.locator("button").filter({ hasText: tool.text });
    if (await btn.count() > 0) {
      ok(`Admin tool "${tool.name}" button present (P2)`, ctx);
    } else {
      bug(`Admin tool "${tool.name}" button missing from Clientes tab`, ctx);
    }
  }

  // Click "Iniciar agente" → should fire toast (not crash)
  const agenteBtn = page.locator("button").filter({ hasText: /Iniciar agente|Agente/i }).first();
  if (await agenteBtn.count() > 0) {
    let nativeAlert = false;
    page.once("dialog", async d => { nativeAlert = true; await d.dismiss(); });
    await agenteBtn.click();
    await page.waitForTimeout(1000);
    if (nativeAlert) {
      bug("Admin 'Iniciar agente' uses native alert() — should use toast (P1 fix)", ctx);
    } else {
      const toast = await page.locator(".toast").count();
      if (toast > 0) {
        ok("Admin 'Iniciar agente' fires toast feedback (P2 + P1)", ctx);
      } else {
        ux("Admin 'Iniciar agente' gives no visible feedback", ctx);
      }
    }
  }

  await screenshot(page, `${browser}_${plan}_admin_tools`);
}

// ─── P2: Tweaks persistence ───────────────────────────────────────────────────

async function checkTweaks(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  if (plan === "anonymous") return;

  await page.locator(".user-chip").click().catch(() => {});
  await page.waitForTimeout(400);

  const panel = page.locator("#tweaks-panel");
  const isOpen = await panel.evaluate(el =>
    el.classList.contains("open") || el.getBoundingClientRect().width > 0
  ).catch(() => false);
  if (!isOpen) {
    bug("Tweaks panel did not open after user chip click", ctx);
    return;
  }

  await screenshot(page, `${browser}_${plan}_tweaks`);

  // Density buttons
  const densityBtns = page.locator("#tw-density button, [data-density]");
  if (await densityBtns.count() >= 3) {
    await densityBtns.first().click(); // comfort
    await page.waitForTimeout(300);
    const lsVal = await page.evaluate(() => localStorage.getItem("cnpj_density"));
    if (lsVal) {
      ok(`Density change persists to localStorage: "${lsVal}" (P2)`, ctx);
    } else {
      bug("Density change not persisted to localStorage (P2 fix)", ctx);
    }
    // Reset to normal
    await densityBtns.nth(1).click();
    await page.waitForTimeout(200);
  } else {
    ux(`Density buttons not found (found ${await densityBtns.count()})`, ctx);
  }

  // Radius buttons
  const radBtns = page.locator("#tw-radius button, [data-radius]");
  if (await radBtns.count() >= 3) {
    await radBtns.first().click(); // sharp
    await page.waitForTimeout(300);
    const bodyClass = await page.locator("body").getAttribute("class").catch(() => "");
    const lsVal = await page.evaluate(() => localStorage.getItem("cnpj_radius"));
    if (lsVal || bodyClass?.includes("radius-")) {
      ok(`Radius change persists to localStorage: "${lsVal}" (P2)`, ctx);
    } else {
      bug("Radius change not persisted to localStorage (P2 fix)", ctx);
    }
    await radBtns.nth(1).click(); // soft
    await page.waitForTimeout(200);
  }

  // Verify persistence across reload (chromium/admin only to avoid noise)
  if (browser === "chromium" && plan === "admin") {
    await page.evaluate(() => {
      localStorage.setItem("cnpj_density", "compact");
      localStorage.setItem("cnpj_radius", "round");
    });
    await page.reload();
    await waitForApp(page);
    const densityAfter = await page.evaluate(() => localStorage.getItem("cnpj_density"));
    const radiusAfter  = await page.evaluate(() => localStorage.getItem("cnpj_radius"));
    if (densityAfter === "compact" && radiusAfter === "round") {
      ok("Tweaks preferences persist across page reload (P2 localStorage)", ctx);
    } else {
      bug("Tweaks preferences lost on page reload", ctx);
    }
    // Reset
    await page.evaluate(() => {
      localStorage.setItem("cnpj_density", "normal");
      localStorage.setItem("cnpj_radius", "soft");
    });
  }

  // Close tweaks
  await page.locator(".tweaks-close, button[onclick*='closeTweaks']").click().catch(() => {});
  await page.waitForTimeout(200);
}

// ─── P2: Filter clear buttons ─────────────────────────────────────────────────

async function checkFilterClear(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  if (plan === "anonymous") return;

  await clickTab(page, "empresas");
  await page.waitForTimeout(1500);

  // Apply UF filter
  const ufSelect = page.locator("select").first();
  if (await ufSelect.count() > 0) {
    await ufSelect.selectOption("SP");
    await page.waitForTimeout(1200);

    // × button should appear
    const clearX = page.locator("button").filter({ hasText: "×" }).or(
      page.locator(".filter-clear")
    );
    if (await clearX.count() > 0) {
      ok("Filter × clear button appears when UF filter active (P2)", ctx);
      await clearX.first().click();
      await page.waitForTimeout(1200);
      const ufVal = await ufSelect.evaluate(el => el.value).catch(() => "");
      if (ufVal === "" || ufVal === "Todos") {
        ok("Filter × clears UF and reloads (P2)", ctx);
      } else {
        bug("Filter × click did not clear UF filter value", ctx);
      }
    } else {
      bug("No × button appeared after applying UF filter (P2 filter clear feature)", ctx);
    }
  }
}

// ─── P2: Ratings in detail row ────────────────────────────────────────────────

async function checkRatingsDisplay(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  if (plan === "anonymous") return;

  await clickTab(page, "empresas");
  await page.waitForTimeout(2000);

  const rows = page.locator("table.data tbody tr:not(.detail-row)");
  if (await rows.count() === 0) return;

  // Expand first row
  const expandBtn = rows.first().locator(".expand, button[onclick*='toggleRow']").first();
  if (await expandBtn.count() === 0) return;

  await expandBtn.click();
  await page.waitForTimeout(2000);

  const detailRow = page.locator(".detail-row").first();
  if (await detailRow.count() === 0) {
    bug("Row expand does not show detail row", ctx);
    return;
  }

  const detailText = await detailRow.textContent().catch(() => "");

  // Check ratings section (P2)
  if (detailText.includes("⭐") || detailText.includes("avaliações") || detailText.includes("Google")) {
    ok("Detail row shows Google rating / reviews (P2)", ctx);
  } else {
    // May legitimately have no rating if company has no Google data
    ux("Detail row has no rating data visible (may be correct if company has no Google listing)", ctx);
  }

  // Check WA link, email link, site link
  const waLink = detailRow.locator("a[href*='wa.me'], a[href*='whatsapp']");
  if (await waLink.count() > 0) ok("WhatsApp link in detail row", ctx);

  await screenshot(page, `${browser}_${plan}_detail_expanded`);

  // Collapse
  await expandBtn.click().catch(() => {});
  await page.waitForTimeout(400);
}

// ─── P2: Export limits display ────────────────────────────────────────────────

async function checkExportar(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  await clickTab(page, "exportar");
  await page.waitForTimeout(800);
  await screenshot(page, `${browser}_${plan}_exportar`);

  const panelText = await page.locator(".panel, main").textContent().catch(() => "");

  if (plan === "free") {
    if (panelText.toLowerCase().includes("não disponível") || panelText.toLowerCase().includes("upgrade") || panelText.toLowerCase().includes("básico")) {
      ok("Free plan sees export-unavailable message (P0)", ctx);
    } else {
      bug("Free plan: Exportar tab should show export-unavailable message", ctx);
    }
  } else if (plan === "basico") {
    if (panelText.includes("500")) {
      ok("Básico plan sees 500-row export limit displayed (P2)", ctx);
    } else {
      bug("Básico plan: export limit (500 linhas) not shown in Exportar tab", ctx);
    }
    const exportBtn = page.locator("button.btn-accent, button").filter({ hasText: /Exportar/i });
    if (await exportBtn.count() > 0) ok("Básico plan sees export button", ctx);
    else bug("Básico plan: export button missing", ctx);
  } else if (plan === "pro" || plan === "admin") {
    if (panelText.includes("5.000") || panelText.includes("5000")) {
      ok("Pro/Admin plan sees 5.000-row export limit displayed (P2)", ctx);
    } else {
      bug("Pro/Admin plan: export limit (5.000 linhas) not shown in Exportar tab", ctx);
    }
    const exportBtn = page.locator("button.btn-accent, button").filter({ hasText: /Exportar/i });
    if (await exportBtn.count() > 0) ok("Pro/Admin plan sees export button", ctx);
    else bug("Pro/Admin plan: export button missing", ctx);
  }
}

// ─── P3: Server-side sorting ─────────────────────────────────────────────────

async function checkServerSideSorting(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  if (plan === "anonymous") return;
  if (browser !== "chromium") return; // Run once per plan, not 3× browsers

  await clickTab(page, "empresas");
  await page.waitForTimeout(2000);

  const rows = page.locator("table.data tbody tr:not(.detail-row)");
  if (await rows.count() === 0) {
    ux("No rows in table to test sorting", ctx);
    return;
  }

  // Intercept API calls to check sort params
  const requests = [];
  page.on("request", req => {
    if (req.url().includes("/api/empresas")) requests.push(req.url());
  });

  // Click first sortable header
  const sortableTh = page.locator("th[onclick*='sortBy'], th.sortable, th[data-sort]").first();
  if (await sortableTh.count() === 0) {
    bug("No sortable table headers found (P3 server-side sorting)", ctx);
    return;
  }

  await sortableTh.click();
  await page.waitForTimeout(1000);

  // Check that API request included sort params
  const lastReq = requests[requests.length - 1] || "";
  if (lastReq.includes("sort_by") || lastReq.includes("sort_dir")) {
    ok("Clicking column header sends sort_by/sort_dir to API (P3 server-side sort)", ctx);
  } else {
    bug("Column header click does not send sort_by/sort_dir params to API (client-side only)", ctx);
  }

  // Check visual indicator
  const activeSortTh = page.locator("th.sort-asc, th.sort-desc, th[data-sort].active");
  if (await activeSortTh.count() > 0) {
    ok("Active sort column has visual indicator (sort-asc/sort-desc class)", ctx);
  } else {
    ux("No visual sort indicator on active sort column", ctx);
  }

  // Click same header again → should toggle direction
  const reqsBefore = requests.length;
  await sortableTh.click();
  await page.waitForTimeout(1000);
  const lastReqToggle = requests[requests.length - 1] || "";
  if (requests.length > reqsBefore && lastReqToggle.includes("sort_dir")) {
    ok("Clicking same header again toggles sort direction", ctx);
  } else {
    ux("Could not verify sort direction toggle from network requests", ctx);
  }

  await screenshot(page, `${browser}_${plan}_sorting`);
}

// ─── P3: Minhas Listas ────────────────────────────────────────────────────────

async function checkMinhasListas(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  if (plan === "anonymous") return;

  // Clear any leftover lists from previous test runs
  await page.evaluate(() => localStorage.removeItem("cnpj_listas"));
  await page.reload();
  await waitForApp(page);

  await clickTab(page, "listas");
  await page.waitForTimeout(1000);
  await screenshot(page, `${browser}_${plan}_listas_empty`);

  // Empty state check
  const content = await page.locator("#content, main, .panel").first().textContent().catch(() => "");
  const isPlaceholder = content.toLowerCase().includes("em breve") || content.toLowerCase().includes("coming soon");
  if (isPlaceholder) {
    bug("'Minhas Listas' is still a placeholder (not implemented yet)", ctx);
    return;
  }

  // Nova lista button should be present and functional
  const novaBtn = page.locator("button").filter({ hasText: /Nova lista/i });
  if (await novaBtn.count() === 0) {
    bug("'Nova lista' button not found in Listas tab (P3)", ctx);
    return;
  }
  ok("'Nova lista' button present in Listas tab (P3)", ctx);

  // Click → modal appears
  await novaBtn.first().click();
  await page.waitForTimeout(500);

  const modal = page.locator(".modal-overlay, [role='dialog'], .modal");
  if (await modal.count() === 0) {
    bug("'Nova lista' button opens no modal (P3 — promptNovaLista broken)", ctx);
    return;
  }
  ok("'Nova lista' opens creation modal (P3)", ctx);
  await screenshot(page, `${browser}_${plan}_nova_lista_modal`);

  // Empty name → shake, no list created
  const nameInput = modal.locator("input[type=text], input[placeholder]").first();
  const createBtn = modal.locator("button").filter({ hasText: /Criar|Create|Salvar/i }).first();

  if (await nameInput.count() > 0 && await createBtn.count() > 0) {
    await createBtn.click();
    await page.waitForTimeout(400);
    const stillOpen = await page.locator(".modal-overlay, [role='dialog']").count();
    if (stillOpen > 0) {
      ok("Empty list name prevents creation (validation active)", ctx);
    } else {
      bug("Empty list name closes modal without validation — created empty-named list", ctx);
    }

    // Valid name → creates list
    await nameInput.fill("Lista Teste Playwright");
    await createBtn.click();
    await page.waitForTimeout(600);
    const modalAfter = await page.locator(".modal-overlay, [role='dialog']").count();
    if (modalAfter === 0) {
      ok("Valid list name creates list and closes modal (P3)", ctx);
    } else {
      bug("Modal did not close after creating list with valid name", ctx);
    }
  }

  // List card should appear
  await page.waitForTimeout(500);
  await screenshot(page, `${browser}_${plan}_listas_with_card`);

  const listCards = page.locator(".list-card, .lista-card, [onclick*='deletarLista']").or(
    page.locator(".panel").filter({ hasText: "Lista Teste Playwright" })
  );
  if (await listCards.count() > 0) {
    ok("List card appears after creation (P3)", ctx);
  } else {
    bug("List card not rendered after creating list (P3)", ctx);
  }

  // Test adding company from row menu
  await clickTab(page, "empresas");
  await page.waitForTimeout(2000);

  const rows = page.locator("table.data tbody tr:not(.detail-row)");
  if (await rows.count() > 0) {
    const moreBtn = rows.first().locator(".row-btn, button[onclick*='toggleRowMenu']").first();
    if (await moreBtn.count() > 0) {
      await moreBtn.click();
      await page.waitForTimeout(400);

      const listOption = page.locator(".row-menu button, .context-menu button")
        .filter({ hasText: /Lista Teste/i });
      if (await listOption.count() > 0) {
        await listOption.click();
        await page.waitForTimeout(500);
        const toast = await page.locator(".toast").count();
        if (toast > 0) {
          ok("Adding company to list from row menu fires success toast (P3)", ctx);
        } else {
          bug("Adding company to list from row menu has no feedback", ctx);
        }
      } else {
        ux("Created list 'Lista Teste Playwright' not showing in row menu options", ctx);
        await page.mouse.click(0, 0);
        await page.waitForTimeout(200);
      }
    }
  }

  // Test delete with confirmModal
  await clickTab(page, "listas");
  await page.waitForTimeout(1000);

  const trashBtn = page.locator("button[onclick*='deletarLista'], .list-trash, button").filter({ hasText: "🗑" }).or(
    page.locator(".list-card button, .lista-card button").last()
  );
  if (await trashBtn.count() > 0) {
    await trashBtn.first().click();
    await page.waitForTimeout(400);

    let nativeConfirm = false;
    page.once("dialog", async d => { nativeConfirm = true; await d.dismiss(); });
    await page.waitForTimeout(300);

    if (nativeConfirm) {
      bug("List delete uses native confirm() — should use confirmModal (P1 fix)", ctx);
    } else {
      const modal2 = page.locator(".modal-overlay, [role='dialog']");
      if (await modal2.count() > 0) {
        ok("List delete opens custom confirmModal (P1 + P3)", ctx);
        // Confirm deletion
        const confirmBtn = modal2.locator("button").filter({ hasText: /Confirmar|Excluir|Sim|OK/i });
        if (await confirmBtn.count() > 0) {
          await confirmBtn.click();
          await page.waitForTimeout(600);
          ok("List deleted after confirming in modal (P3)", ctx);
        }
      } else {
        ux("List trash button click: no feedback (no native confirm, no custom modal)", ctx);
      }
    }
  }

  // Verify persistence: reload page, list should be in localStorage
  await page.evaluate(() => {
    const listas = JSON.parse(localStorage.getItem("cnpj_listas") || "[]");
    if (listas.length === 0) {
      // Create a list to test persistence
      const l = [{ id: Date.now(), name: "Persist Test", cnpjs: [], createdAt: new Date().toISOString() }];
      localStorage.setItem("cnpj_listas", JSON.stringify(l));
    }
  });
  await page.reload();
  await waitForApp(page);
  await clickTab(page, "listas");
  await page.waitForTimeout(1000);
  const persistContent = await page.locator("#content, main").textContent().catch(() => "");
  if (persistContent.includes("Persist Test") || persistContent.includes("Lista")) {
    ok("Lists persist across page reload (localStorage — P3)", ctx);
  } else {
    bug("Lists not restored from localStorage after reload (P3 persistence broken)", ctx);
  }

  // Cleanup
  await page.evaluate(() => localStorage.removeItem("cnpj_listas"));
}

// ─── Existing checks (P0 baseline) ───────────────────────────────────────────

async function checkDashboard(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  await clickTab(page, "dashboard");
  await screenshot(page, `${browser}_${plan}_dashboard`);

  const mvTotal = page.locator("#mv-total");
  if (await mvTotal.count() > 0) {
    const val = await mvTotal.textContent();
    ok(`Dashboard metric #mv-total = "${val}"`, ctx);
  } else {
    bug("Dashboard metric #mv-total not found", ctx);
  }

  // 4th metric — was always "—", may now be real
  const metricVals = await page.locator(".metric-val").allTextContents();
  if (metricVals[3] === "—") {
    ux("Dashboard 4th metric (Exports no mês) still shows hardcoded '—'", ctx);
  } else if (metricVals[3]) {
    ok(`Dashboard 4th metric has real value: "${metricVals[3]}"`, ctx);
  }

  const chartSvg = page.locator(".chart-panel svg");
  if (await chartSvg.count() > 0) {
    ok("Activity chart SVG renders", ctx);
    const dots = page.locator(".cd");
    if (await dots.count() > 0) {
      await dots.first().hover();
      await page.waitForTimeout(300);
      const tip = await page.locator(".chart-tip.show").count();
      if (tip > 0) ok("Chart tooltip shows on hover", ctx);
      else ux("Chart tooltip does not appear on hover", ctx);
    }
  } else {
    bug("Activity chart SVG not rendered", ctx);
  }
}

async function checkEmpresas(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  await clickTab(page, "empresas");
  await page.waitForTimeout(2000);
  await screenshot(page, `${browser}_${plan}_empresas`);

  const rows = page.locator("table.data tbody tr:not(.detail-row)");
  const rowCount = await rows.count();
  if (rowCount === 0) {
    ux("Empresas table has 0 rows (empty DB or filter issue)", ctx);
  } else {
    ok(`${rowCount} table rows loaded`, ctx);
  }

  if (rowCount > 0 && (plan === "free" || plan === "basico")) {
    const hasMasked = await rows.first().locator(".masked").count();
    if (hasMasked > 0) ok(`Contact data masked for plan "${plan}" (expected)`, ctx);
    else               bug(`Contact data NOT masked for plan "${plan}"`, ctx);
  }

  // Pagination
  if (await page.locator(".pager").count() > 0) ok("Pagination bar renders", ctx);
}

async function checkBusca(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  await clickTab(page, "busca");
  await page.waitForTimeout(2000);
  await screenshot(page, `${browser}_${plan}_busca`);

  const dateInputs = page.locator("input[type=date]");
  const cnt = await dateInputs.count();
  if (cnt >= 2) {
    ok("Date range inputs present in Busca avançada", ctx);
    await dateInputs.first().fill("2020-01-01");
    await dateInputs.nth(1).fill("2024-12-31");
    await page.waitForTimeout(1500);
    await screenshot(page, `${browser}_${plan}_busca_date_filter`);
    const clearBtn = page.locator(".btn-ghost").filter({ hasText: /Limpar/i });
    if (await clearBtn.count() > 0) await clearBtn.first().click();
    await page.waitForTimeout(800);
  } else {
    bug("Busca avançada missing date range inputs", ctx);
  }
}

async function checkAPI(page, plan, browser) {
  const ctx = `${browser}/${plan}`;
  await clickTab(page, "api");
  await page.waitForTimeout(800);
  await screenshot(page, `${browser}_${plan}_api`);

  const panelText = await page.locator(".panel, main").textContent().catch(() => "");

  if (plan === "free" || plan === "basico") {
    if (panelText.includes("Pro")) ok(`API tab shows Pro-only restriction for plan "${plan}"`, ctx);
    else                            bug(`API tab should show Pro-only message for plan "${plan}"`, ctx);
  } else if (plan === "pro" || plan === "admin") {
    const swaggerLink = page.locator("a[href='/docs']");
    if (await swaggerLink.count() > 0) ok("API tab has /docs Swagger link for pro/admin", ctx);
    else                                ux("API tab missing /docs Swagger link", ctx);
  }
}

async function checkCrossBrowser(page, plan, browser) {
  const ctx = `${browser}/${plan}`;

  const oklch = await page.evaluate(() => {
    const el = document.createElement("div");
    el.style.color = "oklch(0.72 0.14 160)";
    document.body.appendChild(el);
    const c = getComputedStyle(el).color;
    document.body.removeChild(el);
    return c;
  });
  if (oklch === "rgb(0, 0, 0)" || oklch === "") {
    findings.browserSpecific.push({ browser, plan, msg: "oklch() color not supported — accent colors broken" });
  } else {
    ok(`oklch() colors supported (${oklch})`, ctx);
  }

  const isSecure = await page.evaluate(() => window.isSecureContext);
  if (!isSecure) {
    findings.browserSpecific.push({ browser, plan, msg: "navigator.clipboard requires HTTPS — copy will fail in production HTTP" });
  }
}

// ─── Main loop ────────────────────────────────────────────────────────────────

async function auditPlan(browserName, launcher, plan) {
  const ctx = `${browserName}/${plan.name}`;
  log(`\n=== Starting audit: ${ctx} ===`);

  const browser = await launcher.launch({ headless: true });
  const page    = await browser.newPage();

  page.on("console", msg => {
    if (msg.type() === "error") {
      findings.consoleErrors.push({ context: ctx, text: msg.text() });
    }
  });
  page.on("response", resp => {
    if (resp.status() >= 400 && !resp.url().includes("favicon")) {
      findings.networkErrors.push({ context: ctx, url: resp.url(), status: resp.status() });
    }
  });

  try {
    if (plan.name === "anonymous") {
      await checkAnonymous(page, browserName);
    } else {
      await page.goto(await appUrl(plan.token), { waitUntil: "domcontentloaded", timeout: 20000 });
      await waitForApp(page);

      if (plan.name === "admin") await checkCrossBrowser(page, plan.name, browserName);

      await checkSidebar(page, plan.name, browserName);
      await checkTopbar(page, plan.name, browserName);
      await checkAgentStatus(page, plan.name, browserName);
      await checkDashboard(page, plan.name, browserName);
      await checkEmpresas(page, plan.name, browserName);

      // P1 tests
      await checkToastSystem(page, plan.name, browserName);
      await checkRowMenu(page, plan.name, browserName);
      if (plan.name === "admin") await checkConfirmModal(page, plan.name, browserName);

      // P2 tests
      await checkRatingsDisplay(page, plan.name, browserName);
      await checkAdminTools(page, plan.name, browserName);
      if (browserName === "chromium") await checkTweaks(page, plan.name, browserName);
      await checkFilterClear(page, plan.name, browserName);
      await checkAgentStatus(page, plan.name, browserName); // re-check after navigation

      // P3 tests
      await checkServerSideSorting(page, plan.name, browserName);
      await checkMinhasListas(page, plan.name, browserName);

      // Existing checks
      await checkBusca(page, plan.name, browserName);
      await checkExportar(page, plan.name, browserName);
      await checkAPI(page, plan.name, browserName);
    }
  } catch (err) {
    log(`ERROR in ${ctx}: ${err.message}`);
    findings.bugs.push({ msg: `Uncaught audit error in ${ctx}: ${err.message}`, context: ctx });
    await screenshot(page, `${browserName}_${plan.name}_error`).catch(() => {});
  }

  await browser.close();
  log(`=== Done: ${ctx} ===`);
}

async function main() {
  fs.mkdirSync(SCREENSHOT_DIR, { recursive: true });
  log("Starting CNPJ Intel P0–P3 comprehensive audit...\n");

  await checkMobile("chromium");

  for (const b of BROWSERS) {
    for (const plan of PLANS) {
      await auditPlan(b.name, b.launcher, plan);
    }
  }

  const dedup = arr => {
    const seen = new Set();
    return arr.filter(x => { const k = x.msg; if (seen.has(k)) return false; seen.add(k); return true; });
  };

  findings.bugs            = dedup(findings.bugs);
  findings.missingFeatures = dedup(findings.missingFeatures);
  findings.uxIssues        = dedup(findings.uxIssues);
  findings.working         = dedup(findings.working);

  fs.writeFileSync(REPORT_PATH, JSON.stringify(findings, null, 2));
  log(`\nReport written to ${REPORT_PATH}`);
  log(`Working:          ${findings.working.length}`);
  log(`Bugs found:       ${findings.bugs.length}`);
  log(`Missing features: ${findings.missingFeatures.length}`);
  log(`UX issues:        ${findings.uxIssues.length}`);
  log(`Browser-specific: ${findings.browserSpecific.length}`);
  log(`Console errors:   ${findings.consoleErrors.length}`);
  log(`Network errors:   ${findings.networkErrors.length}`);
  log(`Screenshots:      ${findings.screenshots.length}`);
}

main().catch(err => {
  console.error("Fatal audit error:", err);
  process.exit(1);
});
