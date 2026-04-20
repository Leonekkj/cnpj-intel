import os
import subprocess
from datetime import date

import anthropic

from scripts.audit.detector import Anomaly

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
REPO = os.environ.get("GITHUB_REPOSITORY", "")  # "owner/repo" — injetado pelo Actions


def _build_diagnosis_prompt(anomalies: list[Anomaly]) -> str:
    descriptions = "\n".join(f"- {a.type} ({a.severity}): {a.description}" for a in anomalies)
    return f"""Você é um engenheiro sênior analisando anomalias no CNPJ Intel, um SaaS B2B de enriquecimento de dados.

Anomalias detectadas em {date.today().isoformat()}:
{descriptions}

Stack do projeto:
- Frontend: app/index.html (vanilla JS, 3 abas: Dashboard, Busca Avançada, Clientes)
- API: api.py (FastAPI) — endpoints /api/stats, /api/empresas, /api/cnaes
- Banco: database.py (PostgreSQL/SQLite via psycopg2 pool)
- Agente de scraping: agent/agent.py (asyncio + aiohttp, processa cnpjs_seed.txt)

Faça o seguinte:
1. Identifique a causa raiz mais provável percorrendo o caminho: dashboard → api.py → database.py → agent/agent.py
2. Descreva em 3-5 bullets o que provavelmente causou a anomalia
3. Proponha um patch mínimo (diff ou pseudocódigo) para corrigir

Responda em português. Seja direto e técnico."""


def diagnose_with_claude(anomalies: list[Anomaly]) -> str:
    """Invoca a Anthropic API para diagnóstico. Retorna análise como string."""
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        messages=[{"role": "user", "content": _build_diagnosis_prompt(anomalies)}],
    )
    return message.content[0].text


def create_github_issue(anomalies: list[Anomaly], diagnosis: str) -> str:
    """Cria GitHub Issue via gh CLI e retorna URL."""
    today = date.today().isoformat()
    types = ", ".join(sorted({a.type for a in anomalies}))
    title = f"[AUDIT] Anomalia detectada: {types} — {today}"

    body_lines = [
        "## Anomalias detectadas",
        "",
    ]
    for a in anomalies:
        body_lines.append(f"- **{a.type}** ({a.severity}): {a.description}")

    body_lines += [
        "",
        "## Diagnóstico (Claude)",
        "",
        diagnosis,
        "",
        "## Próximos passos",
        "",
        "- [ ] Revisar patch proposto acima",
        "- [ ] Aplicar correção em branch separada",
        "- [ ] Validar com `python scripts/dashboard_audit.py`",
        "",
        f"*Gerado automaticamente pelo audit noturno em {today}*",
    ]

    body = "\n".join(body_lines)

    result = subprocess.run(
        ["gh", "issue", "create",
         "--title", title,
         "--body", body,
         "--label", "audit,bug"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh issue create falhou: {result.stderr}")
    return result.stdout.strip()


def report(anomalies: list[Anomaly]) -> None:
    """Pipeline completo: diagnóstico → GitHub Issue."""
    print(f"[reporter] {len(anomalies)} anomalia(s) detectada(s). Invocando Claude...")
    diagnosis = diagnose_with_claude(anomalies)
    print("[reporter] Diagnóstico recebido. Criando GitHub Issue...")
    url = create_github_issue(anomalies, diagnosis)
    print(f"[reporter] Issue criado: {url}")
