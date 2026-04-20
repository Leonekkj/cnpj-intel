import os
import requests
from pathlib import Path
from playwright.sync_api import sync_playwright

DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "").rstrip("/")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")


def _headers() -> dict:
    return {"Authorization": f"Bearer {ADMIN_TOKEN}"}


def fetch_stats() -> dict:
    """Retorna stats brutas: total_empresas, com_telefone, com_email, com_site, com_instagram."""
    r = requests.get(f"{DASHBOARD_URL}/api/stats", headers=_headers(), timeout=10)
    r.raise_for_status()
    return r.json()


def fetch_cnaes() -> list[str]:
    """Retorna lista dos top CNAEs disponíveis."""
    r = requests.get(f"{DASHBOARD_URL}/api/cnaes", headers=_headers(), timeout=10)
    r.raise_for_status()
    data = r.json()
    # Endpoint retorna lista de dicts [{cnae, count}] ou lista de strings
    if data and isinstance(data[0], dict):
        return [row.get("cnae", "") for row in data]
    return list(data)


def fetch_empresas_sample(limit: int = 100) -> list[dict]:
    """Retorna amostra de empresas (com e sem contato) para calcular fill rates."""
    r = requests.get(
        f"{DASHBOARD_URL}/api/empresas",
        headers=_headers(),
        params={"limit": limit, "com_contato": "false"},
        timeout=15,
    )
    r.raise_for_status()
    return r.json().get("empresas", [])


def compute_fill_rates(empresas: list[dict]) -> dict[str, float]:
    """Calcula porcentagem de preenchimento dos campos de contato."""
    if not empresas:
        return {}
    total = len(empresas)
    fields = ["telefone", "email", "site", "instagram"]
    return {f: sum(1 for e in empresas if e.get(f, "")) / total for f in fields}


def collect_data_snapshot() -> dict:
    """Retorna snapshot completo para comparação com baseline."""
    stats = fetch_stats()
    cnaes = fetch_cnaes()
    sample = fetch_empresas_sample()
    fill_rates = compute_fill_rates(sample)
    return {
        "stats": stats,
        "cnaes": cnaes,
        "cnaes_count": len(cnaes),
        "fill_rates": fill_rates,
    }


def capture_screenshots(output_dir: str) -> dict[str, str]:
    """
    Captura screenshots do dashboard e retorna {nome: caminho}.
    Autentica injetando o token no localStorage (padrão do app/index.html).
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}

    with sync_playwright() as p:
        browser = p.chromium.launch()
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        # Injeta token antes do primeiro request para evitar redirect
        page.goto(DASHBOARD_URL)
        page.evaluate(f"localStorage.setItem('cnpj_token', '{ADMIN_TOKEN}')")

        # Dashboard principal
        page.goto(f"{DASHBOARD_URL}/")
        page.wait_for_load_state("networkidle", timeout=15000)
        # Aguarda a tabela de empresas renderizar
        try:
            page.wait_for_selector("table tbody tr", timeout=10000)
        except Exception:
            pass  # dashboard pode estar vazio em ambiente de teste
        path = str(Path(output_dir) / "dashboard.png")
        page.screenshot(path=path)
        paths["dashboard"] = path

        # Busca avançada (segunda aba)
        try:
            page.click("text=Busca Avançada")
            page.wait_for_load_state("networkidle", timeout=8000)
            path_adv = str(Path(output_dir) / "advanced_search.png")
            page.screenshot(path=path_adv)
            paths["advanced_search"] = path_adv
        except Exception:
            pass

        browser.close()

    return paths
