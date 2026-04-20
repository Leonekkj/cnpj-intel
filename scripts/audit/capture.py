import os
import json
import requests
from dataclasses import dataclass, field

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
