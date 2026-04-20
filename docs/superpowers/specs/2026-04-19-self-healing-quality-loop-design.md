# Self-Healing Quality Loop — CNPJ Intel Dashboard

**Data:** 2026-04-19  
**Status:** Aprovado

## Contexto

O CNPJ Intel é um SaaS B2B brasileiro sem suite de testes automatizados, CI/CD ou monitoramento de qualidade de dados. Anomalias como spikes de campos nulos, perda de categorias CNAE ou regressões visuais no dashboard só são detectadas quando um usuário reclama. Este sistema cria um loop de qualidade noturno que audita o dashboard de produção (Railway), detecta anomalias automaticamente e invoca o Claude Code para rastrear a causa raiz e abrir um GitHub Issue com diagnóstico + patch proposto.

## Objetivo

1. Script noturno `scripts/dashboard_audit.py` audita o dashboard de produção via Playwright MCP.
2. Baselines armazenados em `tests/visual/baselines/` servem como referência.
3. Detector de anomalias sinaliza: spikes de nulos >10%, CNAEs ausentes, diff visual >5%.
4. Ao detectar anomalia: `claude -p` rastreia dashboard → API → banco → scraper e abre GitHub Issue.
5. Tudo orquestrado por GitHub Actions com cron diário.

## Arquitetura

```
.github/workflows/dashboard-audit.yml   ← cron 02:00 UTC
  └─► scripts/dashboard_audit.py        ← entry point / orquestrador
        ├─► scripts/audit/capture.py    ← Playwright MCP → screenshots + JSON
        ├─► scripts/audit/detector.py   ← regras de anomalia
        └─► scripts/audit/reporter.py   ← claude -p + gh issue create

tests/visual/baselines/
  dashboard.json          ← stats, field_fill_rates, cnaes_count
  dashboard.png           ← screenshot de referência
  advanced_search.png
  baseline_meta.json      ← { created_at, version, thresholds }
```

## Módulos

### capture.py
Dois modos de operação:

| Contexto | Estratégia |
|----------|-----------|
| **Dados (API)** | `requests` → `/api/stats`, `/api/cnaes`, `/api/empresas` com `ADMIN_TOKEN` — extrai JSON diretamente |
| **Screenshots (visual)** | `playwright-python` (`pip install playwright`) → abre Chrome headless, navega `/`, captura PNG |

- Playwright MCP é usado **apenas em sessões interativas** do Claude Code localmente para exploração.
- No CI (GitHub Actions), `playwright install --with-deps chromium` instala o browser headless.
- Salva: screenshots PNG em `tests/visual/baselines/` e dados em JSON temporário.
- `--update-baselines`: primeiro run cria os arquivos de referência.

### detector.py
Três regras hard-coded:

| Regra | Critério |
|-------|----------|
| Spike de nulos | `fill_rate < (baseline_fill_rate - 0.10)` para telefone/email/site |
| CNAE ausente | categoria presente no baseline ausente no run atual |
| Diff visual | `pixel_diff_pct > 0.05` via Pillow `ImageChops.difference` |

Output: `list[Anomaly]` com campos `type`, `severity` (`warning`/`critical`), `description`, `evidence`.

### reporter.py
1. Monta prompt estruturado para `claude -p`:
   - Descreve a anomalia (tipo, campo afetado, delta)
   - Instrui trace: `app/index.html → api.py → database.py → agent/agent.py`
   - Pede causa raiz + patch mínimo
2. Executa `claude -p "<prompt>"` via `subprocess.run()`, timeout 120s.
3. Chama `gh issue create` com:
   - Título: `[AUDIT] <tipo anomalia> — <data>`
   - Labels: `audit`, `bug`
   - Body: saída do Claude (root cause + diff) + evidências (screenshots inline)

### dashboard_audit.py (entry point)
```python
anomalies = capture() | detect(baselines) 
if anomalies:
    for a in anomalies:
        reporter.report(a)
    sys.exit(1)   # sinaliza falha no CI
```
Flag `--update-baselines` pula a detecção e sobrescreve os arquivos de baseline.

## GitHub Actions Workflow

```yaml
name: Dashboard Audit
on:
  schedule:
    - cron: '0 2 * * *'   # 02:00 UTC diário
  workflow_dispatch:       # trigger manual

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install pillow requests
      - run: python scripts/dashboard_audit.py
        env:
          DASHBOARD_URL: ${{ secrets.DASHBOARD_URL }}
          ADMIN_TOKEN: ${{ secrets.ADMIN_TOKEN }}
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
```

`claude -p` é chamado via `subprocess` usando o `ANTHROPIC_API_KEY` do ambiente — sem necessidade de instalar o CLI completo do Claude Code no runner.

> **Nota:** O reporter usa a Anthropic API diretamente (via `anthropic` Python SDK) no lugar de `claude -p` CLI, para simplificar a dependência no runner de CI.

## Dependências Python adicionais

```
pillow          ← diff visual
anthropic       ← invocação do Claude para diagnóstico
requests        ← chamadas à API do dashboard
playwright      ← screenshots headless (CI e local)
```

No GitHub Actions, adicionar step `playwright install --with-deps chromium` antes do audit.

## Delegação de Agentes

| Agente | Responsabilidade |
|--------|-----------------|
| `senior-backend-dev` | `capture.py`, `reporter.py`, `dashboard_audit.py`, GitHub Actions workflow |
| `qa-debug-engineer` | `detector.py`, estrutura de baselines, lógica de diff visual (Pillow) |
| `cnpj-data-engineer` | queries de saúde do banco embutidas no capture (null rates via `/api/stats`) |
| `frontend-dev-react` | seletores DOM para extração de dados renderizados do `app/index.html` |
| `orquestrador` | coordena execução paralela dos agentes acima |

## Verificação / Teste Manual

1. **Primeiro run (criar baselines):**
   ```bash
   ADMIN_TOKEN=xxx DASHBOARD_URL=https://seu-app.railway.app \
     python scripts/dashboard_audit.py --update-baselines
   ```
   Verifica: `tests/visual/baselines/dashboard.json` criado com dados reais.

2. **Run normal (detectar anomalias):**
   ```bash
   ADMIN_TOKEN=xxx DASHBOARD_URL=https://seu-app.railway.app \
     python scripts/dashboard_audit.py
   ```
   Verifica: sem anomalias → exit code 0. Com anomalias → exit code 1 + issue criado.

3. **Simular anomalia:**
   Editar `tests/visual/baselines/dashboard.json` e aumentar `fill_rate.email` para 0.99.
   Rodar o audit: deve detectar spike e acionar o reporter.

4. **GitHub Actions:**
   Verificar `.github/workflows/dashboard-audit.yml` executando via `workflow_dispatch`.
