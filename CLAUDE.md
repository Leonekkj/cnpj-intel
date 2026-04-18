# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Visão Geral

B2B SaaS brasileiro para busca e enriquecimento de dados de empresas via CNPJ.
Permite pesquisar empresas por filtros (UF, porte, CNAE, data de abertura) e
exportar listas com contatos (telefone, e-mail, Instagram, site).

## Rate Limiting & Async Patterns
- When adding delays/sleeps in concurrent code, ALWAYS place them INSIDE the semaphore block to prevent thundering herd effects
- For external APIs with rate limits (BrasilAPI, etc.), use exponential backoff and verify with actual logs before declaring fixes complete

## Before Refactoring
- Do not refactor multiple concerns in one pass; make one change, verify it works with logs/tests, then proceed
- When user reports logs/errors, ASK them to paste the actual log output rather than inferring from recent commits
- If a fix doesn't work on the second attempt, propose reverting to last working commit rather than layering more changes

## Stack

| Camada | Tecnologia |
|--------|-----------|
| API | FastAPI + Uvicorn |
| Banco (produção) | PostgreSQL (Railway) via psycopg2 pool |
| Banco (local) | SQLite (`cnpj_intel.db`) |
| Frontend | HTML/CSS/JS vanilla em `app/index.html` |
| Agente de scraping | asyncio + aiohttp + DuckDuckGo (`agent/agent.py`) |
| Deploy | Railway (Nixpacks) |

## Project Stack
- Primary language: Python (async-heavy with asyncio)
- Use `asyncio.to_thread` for wrapping blocking I/O in async contexts
- Database: avoid N+1 queries; batch pre-filter before loops
- Use Ruflo agents when available for specialized subtasks

## Comandos de Desenvolvimento

```bash
# Instalar dependências
pip install -r requirements.txt

# Rodar somente a API (sem agente, SQLite automático)
ADMIN_TOKEN=meutoken uvicorn api:app --reload --port 8000

# Rodar API + agente juntos (como no Railway)
ADMIN_TOKEN=meutoken python start.py

# Rodar o agente isolado
ADMIN_TOKEN=meutoken python agent/agent.py

# Rodar o agente em modo re-enrich
ADMIN_TOKEN=meutoken REENRICH_SEM_CONTATO=1 python agent/agent.py

# Gerar cnpjs_seed.txt a partir de arquivo da Receita Federal
python extrator.py --arquivo Estabelecimentos0.zip --uf SP,MG --limite 100000

# Docs interativos da API (Swagger)
# http://localhost:8000/docs
```

Não há suite de testes automatizados no projeto. Para verificar um endpoint
manualmente use o Swagger UI ou curl com `Authorization: Bearer <token>`.

## Arquitetura

### Fluxo principal

```
cnpjs_seed.txt
      │
      ▼
agent/agent.py          ← lê CNPJs, enriquece via BrasilAPI + Google Places + DDG + scraping
      │
      ▼
database.py (empresas)  ← SQLite local / PostgreSQL em produção
      │
      ▼
api.py (FastAPI)        ← autentica token, aplica limites de plano, serve dados
      │
      ▼
app/index.html          ← dashboard vanilla JS
```

`start.py` inicia `agent/agent.py` como subprocesso e depois faz `os.execv` para uvicorn —
o Railway monitora o processo uvicorn como processo principal.

### Detecção automática de banco (database.py:9-17)
- `DATABASE_URL` ou `DATABASE_PUBLIC_URL` no env → PostgreSQL com pool de conexões
- Sem nenhuma → SQLite local (`cnpj_intel.db`)
- `PH` = placeholder (`%s` PG / `?` SQLite); `LIKE` = `ILIKE` PG / `LIKE` SQLite

### Tabelas

**`empresas`** — dados cadastrais + contatos enriquecidos  
Campos: `cnpj (PK)`, `razao_social`, `nome_fantasia`, `porte`, `cnae`, `situacao`, `abertura`, `municipio`, `uf`, `socio_principal`, `telefone`, `email`, `instagram`, `site`, `rating_google`, `avaliacoes`, `atualizado_em`  
Índices em: `uf`, `porte`, `email`, `cnae`, `abertura`, `atualizado_em`. PostgreSQL também cria índices parciais (`WHERE != ''`) em `telefone/email/instagram/site`.

**`tokens`** — autenticação e quota  
Campos: `token (PK)`, `plano`, `cnpjs_hoje`, `data_reset`, `ativo`, `criado_em`

**`agente_progresso`** — linha única (`id=1`), campo `posicao` = offset atual do agente

## Sistema de Planos e Auth

### Planos (database.py:52-56)
| Plano | Limite/dia | Export CSV | API key |
|-------|-----------|------------|---------|
| free | 10 CNPJs | Não | Não |
| basico | 500 CNPJs | Sim (500 linhas) | Não |
| pro | ilimitado | Sim (5.000 linhas) | Sim |
| admin | ilimitado | Sim | Sim |

### Fluxo de autenticação
1. `HTTPBearer` extrai token do header `Authorization: Bearer <token>`
2. `get_token_info()` — se token == `ADMIN_TOKEN` env → perfil admin imediato
3. Caso contrário → `db.verificar_token_db()` — reseta quota diária se a data mudou
4. Se `limite_atingido` → HTTP 429

### Variáveis de ambiente
```
# Obrigatórias
ADMIN_TOKEN             — token master do administrador
DATABASE_URL            — string de conexão PostgreSQL (Railway injeta automaticamente)

# Opcionais
ALLOWED_ORIGINS         — origens CORS separadas por vírgula (padrão: *)
TOKENS                  — tokens legados separados por vírgula (migrados como "pro" no startup)
PORT                    — porta HTTP (padrão: 8000)
GOOGLE_API_KEY          — Google Places API para o agente (melhora qualidade)
REENRICH_SEM_CONTATO    — "1" para rodar agente em modo re-enrich
```

## Endpoints da API

| Método | Path | Auth | Descrição |
|--------|------|------|-----------|
| GET | `/` | — | Serve `app/index.html` |
| GET | `/health` | — | Health check |
| GET | `/api/meu-plano` | token | Info do plano atual |
| GET | `/api/empresas` | token | Busca paginada com filtros |
| GET | `/api/empresa/{cnpj}` | token | Detalhe de uma empresa |
| GET | `/api/stats` | token | Estatísticas gerais (cache 60s) |
| GET | `/api/cnaes` | token | Top 100 CNAEs para filtro |
| GET | `/api/export` | basico/pro | Download CSV |
| POST | `/api/admin/tokens` | admin | Cria token |
| GET | `/api/admin/tokens` | admin | Lista tokens |
| DELETE | `/api/admin/tokens/{token}` | admin | Remove token |
| POST | `/api/admin/agente` | admin | Inicia agente de scraping |
| POST | `/api/admin/limpar-sites` | admin | Limpa URLs de diretórios |

## Agente de Enriquecimento (`agent/agent.py`)

Script assíncrono que processa CNPJs em paralelo (asyncio + aiohttp).

### Pipeline por empresa
```
1. Seed expandido (12 cols)   — razao_social/porte/sócio vêm do extrator (Empresas+Socios)
   → pula BrasilAPI quando seed completo (caso >95%). BrasilAPI é FALLBACK só
     para seeds antigos de 9 colunas, via semáforo de 3 (dentro do rate limit real).
2. Google Places API          — telefone formatado + site + rating (requer GOOGLE_API_KEY)
3. DuckDuckGo (fallback)      — busca site quando Google Places indisponível
4. Scraping do site           — homepage + /contato + /fale-conosco em paralelo
                                extrai email (mailto, ofuscado, data-email) e Instagram
```

**Critério mínimo para aparecer no dashboard:** ter telefone.
Sem telefone → salvo no banco (para não reprocessar) mas invisível ao cliente.

### Modos de operação
| Modo | Env var | Comportamento |
|------|---------|---------------|
| Seed (padrão) | — | Processa `cnpjs_seed.txt` em ordem, retomando do offset salvo em `agente_progresso` |
| Re-enrich | `REENRICH_SEM_CONTATO=1` | Re-processa registros já no banco sem email/instagram/site |

### Concorrência adaptativa
- Via rápida: 15 workers (3 em re-enrich)
- Via lenta: 30 workers com `GOOGLE_API_KEY`, 15 sem, 3 em re-enrich
- DuckDuckGo: semáforo global de 8 conexões simultâneas + backoff exponencial
- BrasilAPI: semáforo global de 3 (fallback raro, dentro do rate limit real da API)
- Timeout por CNPJ: 30s; progresso salvo a cada ciclo, pré-filtro batch de CNPJs já processados

### Coalesce no upsert (`database.py:salvar_empresa`)
Campos de contato só são sobrescritos se o novo valor não for vazio — preserva dados existentes quando o scraping não encontra resultado numa segunda passagem.

## Quirks Importantes

**`com_contato=True` (padrão em `/api/empresas`)** → filtra `WHERE telefone != ''`. As stats exibidas ao usuário também contam só empresas com telefone, não o total bruto.

**Cache de stats** — `/api/stats` usa dict in-memory com TTL de 60s (`_stats_cache`). Invalidado manualmente ao chamar `POST /api/admin/limpar-sites`.

**Limpeza de sites de diretório** — `db.limpar_sites_diretorio()` roda no startup da API e zera o campo `site` de registros que têm URLs de dnb.com, cnpj.biz, LinkedIn, etc. salvos erroneamente pelo agente.

**Migração de tokens legados** — a env var `TOKENS` é migrada para o banco como plano "pro" no startup com `ON CONFLICT DO UPDATE`.

## Importação de CNPJs (`extrator.py`)

Ferramenta CLI offline para gerar `cnpjs_seed.txt` a partir dos arquivos brutos da Receita Federal.

Fonte: `https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9`

Três grupos de arquivos:
- `Estabelecimentos0.zip` … `9.zip` — **obrigatório**. Dados cadastrais + contatos.
- `Empresas0.zip` … `9.zip` — **opcional mas recomendado**. Adiciona `razao_social` e `porte`.
- `Socios0.zip` … `9.zip` — **opcional mas recomendado**. Adiciona `socio_principal`.

Todos: CSV com `;`, encoding latin-1, sem cabeçalho.

```bash
# Modo antigo (9 colunas — agente cai no fallback da BrasilAPI):
python extrator.py --arquivo Estabelecimentos0.zip --uf SP,MG --limite 100000

# Modo recomendado (12 colunas — agente NÃO chama BrasilAPI, ~10× mais rápido):
python extrator.py --arquivo Estabelecimentos0.zip \
    --empresas Empresas0.zip --socios Socios0.zip \
    --uf SP --limite 100000
```

CNPJ montado de 3 campos: `cnpj_basico` (8 dígitos) + `cnpj_ordem` (4) + `cnpj_dv` (2).
Situação `02` = ativa (único valor importado por padrão).
CNAEs pré-filtrados para alto valor comercial definidos em `extrator.py` (`CNAES_INTERESSE`).
Empresas/Socios são carregados em memória por `cnpj_basico` (~400MB+200MB RAM para país inteiro).

## Deploy (Railway)

1. Push para `main` — Railway detecta `Procfile` e executa `python start.py`
2. `DATABASE_URL` e `ADMIN_TOKEN` configurados nas variáveis do serviço
3. Restart automático em falha (até 10 tentativas — `railway.json`)
