"""
CNPJ Intel Agent — pipeline dual de enriquecimento.

Via rápida (telefone já vem do seed da Receita Federal):
  Seed TSV → BrasilAPI (razao_social/porte/socios) → salvar
  ~40 workers, sem DDG/scraping, ~30k CNPJs/hora

Via lenta (sem telefone no seed):
  BrasilAPI → DuckDuckGo (site) → scraping (contatos) → salvar
  ~20 workers, com rate limiting
"""

import asyncio
import aiohttp
import re
import logging
import os
import sys
import gzip
from urllib.parse import quote, urljoin, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Database, telefone_valido
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [AGENTE] %(message)s")
log = logging.getLogger(__name__)

GOOGLE_API_KEY       = os.environ.get("GOOGLE_API_KEY", "")
REENRICH_SEM_CONTATO = os.environ.get("REENRICH_SEM_CONTATO", "").lower() in ("1", "true", "yes")

# ─── Concorrência e throughput ────────────────────────────────────────────────
# Via rápida (seed com telefone): só BrasilAPI → alta concorrência
# Via lenta (sem telefone): DDG + scraping → concorrência baixa para evitar rate limit
CONCORRENCIA_RAPIDA = 3 if REENRICH_SEM_CONTATO else 40
CONCORRENCIA_LENTA  = 3 if REENRICH_SEM_CONTATO else 20

LOTE         = 3000 if REENRICH_SEM_CONTATO else 5000
PAUSA_CICLO  = 0.5
TIMEOUT_RAPIDO = 10   # BrasilAPI only
TIMEOUT_LENTO  = 20   # BrasilAPI + DDG + scraping

# Headers realistas para evitar bloqueio nos scrapers
HEADERS_BROWSER = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Handles de Instagram que não são perfis de negócio
INSTA_IGNORAR = {
    "p", "tv", "reel", "reels", "stories", "explore",
    "accounts", "sharer", "about", "help", "legal",
    "privacy", "share", "direct", "login",
}

# Domínios de e-mail que indicam ruído (não são do negócio)
EMAIL_RUIDO = [
    "sentry", "example", "noreply", "wix.com", "pixel",
    "schema", "w3.org", "wordpress", "jquery", "google",
    "facebook", "instagram", "shopify", "hotmart",
]

# Slugs de páginas de contato para tentar após homepage
PAGINAS_CONTATO = ["/contato", "/contact"]


# ─── BrasilAPI ────────────────────────────────────────────────────────────────

async def buscar_brasilapi(session, cnpj):
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    sem = _BRASIL_SEM if _BRASIL_SEM is not None else asyncio.Semaphore(4)
    rate_limited = False
    try:
        async with sem:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status == 200:
                    return await r.json()
                elif r.status == 429:
                    rate_limited = True
    except Exception:
        pass
    # sleep FORA do semáforo — não trava os demais workers durante a espera
    if rate_limited:
        log.warning("BrasilAPI rate limit — aguardando 2s...")
        await asyncio.sleep(2)
    return None


# ─── Google Places (requer GOOGLE_API_KEY) ────────────────────────────────────

# Domínios que NÃO são o site real da empresa:
# diretórios de CNPJ, redes sociais, buscadores, marketplaces, etc.
# Usada por _site_valido() (Google Places) E por buscar_site_ddg().
_DOMINIOS_FALSOS = {
    # Diretórios brasileiros de CNPJ
    "cadastroempresa", "cnpjativos", "infocnpj", "buscacnpj", "consulta-cnpj",
    "empresasdobrasil", "cnpj.info", "cnpja", "cnpjbiz",
    "cnpj.biz", "cnpj.ws", "qsa.me", "econodata", "minhareceita",
    "receitaws", "casadosdados", "sintegra", "portaldatransparencia",
    "servicos.receita", "oportunidades.com",
    # Diretórios internacionais
    "dnb.com", "d-u-n-s", "opencorporates", "bloomberg.com",
    "crunchbase", "zoominfo", "manta.com", "bizapedia",
    "companieshouse", "corpwatch",
    # Listas e guias
    "telelistas", "guiamais", "apontador", "yellowpages", "paginas.amarelas",
    "yelp.com", "tripadvisor", "foursquare", "infobel", "hotfrog",
    # Governo / jurídico
    "receitafederal", "gov.br", "jusbrasil", "escavador", "tjsp",
    # Marketplaces
    "mercadolivre", "shopee", "americanas", "magazineluiza", "ifood", "rappi",
    # Redes sociais e buscadores
    "linkedin.com", "facebook.com", "instagram.com",
    "google.com", "youtube.com", "twitter.com", "tiktok.com",
    "bing.com", "yahoo.com", "whatsapp.com",
}

def _site_valido(url: str) -> bool:
    """Retorna False se a URL for de um diretório/listagem, não do site real."""
    if not url:
        return False
    from urllib.parse import urlparse
    dominio = urlparse(url).netloc.lower().lstrip("www.")
    return not any(falso in dominio for falso in _DOMINIOS_FALSOS)


async def buscar_google_places(session, nome, cidade):
    if not GOOGLE_API_KEY:
        return {}
    try:
        # Passo 1: findplacefromtext → obter place_id
        url1 = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
        params1 = {
            "input": f"{nome} {cidade}",
            "inputtype": "textquery",
            "fields": "place_id",
            "key": GOOGLE_API_KEY,
        }
        async with session.get(url1, params=params1, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                return {}
            data1 = await r.json()
        candidates = data1.get("candidates", [])
        if not candidates:
            return {}
        place_id = candidates[0].get("place_id")
        if not place_id:
            return {}

        # Passo 2: Place Details → telefone, site, rating (formatted_phone_number só disponível aqui)
        url2 = "https://maps.googleapis.com/maps/api/place/details/json"
        params2 = {
            "place_id": place_id,
            "fields": "formatted_phone_number,website,rating,user_ratings_total",
            "key": GOOGLE_API_KEY,
        }
        async with session.get(url2, params=params2, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status != 200:
                return {}
            data2 = await r.json()
        result = data2.get("result", {})
        site_raw = result.get("website", "")
        site = site_raw if _site_valido(site_raw) else ""
        return {
            "telefone_google": result.get("formatted_phone_number", ""),
            "site_google":     site,
            "rating_google":   result.get("rating", ""),
            "avaliacoes":      result.get("user_ratings_total", ""),
        }
    except Exception:
        pass
    return {}


# ─── DuckDuckGo (fallback sem API key) ───────────────────────────────────────

def _extrair_instagram_do_html(html: str) -> str:
    """Extrai o primeiro @handle do Instagram a partir de HTML."""
    handles = re.findall(r'instagram\.com/([A-Za-z0-9_.]{2,30})(?:[/"?\s]|$)', html)
    handles = [h for h in handles if h.lower() not in INSTA_IGNORAR]
    return ("@" + handles[0]) if handles else ""


# Semáforo global de DDG — mais de 3 conexões simultâneas disparam rate limit.
# Inicializado em rodar_agente() dentro do event loop correto.
_DDG_SEM: asyncio.Semaphore | None = None

# Semáforo global de BrasilAPI — com 15 workers simultâneos, a API retorna 429.
# Limita a 4 requisições paralelas para evitar rate limit.
_BRASIL_SEM: asyncio.Semaphore | None = None


async def buscar_duckduckgo(session, query: str, tentativas: int = 1) -> str:
    """
    Busca no DuckDuckGo Lite com retry e backoff.
    Retorna string vazia se bloqueado após todas as tentativas.
    Usa _DDG_SEM para garantir no máximo 3 requisições simultâneas ao DDG,
    evitando rate limit quando há muitos workers em paralelo.
    """
    sem = _DDG_SEM if _DDG_SEM is not None else asyncio.Semaphore(3)
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
    async with sem:
        for tentativa in range(tentativas):
            try:
                if tentativa > 0:
                    await asyncio.sleep(2 + tentativa * 2)  # backoff: 4s, 6s...
                async with session.get(
                    url, headers=HEADERS_BROWSER,
                    timeout=aiohttp.ClientTimeout(total=8),
                    allow_redirects=True,
                ) as r:
                    if r.status == 200:
                        html = await r.text(errors="ignore")
                        if html:
                            return html
                    elif r.status == 429:
                        log.debug("DDG rate limit — aguardando antes de retry")
                        await asyncio.sleep(5)
            except Exception:
                pass
    return ""


async def buscar_instagram_ddg(session, nome: str, cidade: str) -> str:
    """
    Busca o perfil de Instagram da empresa no DuckDuckGo.
    Tenta duas queries: com aspas (exata) e sem (ampla).
    """
    nome_busca = nome.strip()[:50]

    # Query 1: busca exata no Instagram
    query1 = f'site:instagram.com "{nome_busca}" {cidade}'
    html = await buscar_duckduckgo(session, query1)
    if not html:
        # Query 2: sem aspas, mais ampla
        query2 = f'site:instagram.com {nome_busca} {cidade}'
        html = await buscar_duckduckgo(session, query2)
    if not html:
        return ""
    handle = _extrair_instagram_do_html(html)
    if handle:
        log.debug(f"Instagram via DDG: {handle} para '{nome_busca}'")
    return handle


async def buscar_site_ddg(session, nome: str, cidade: str) -> str:
    """
    Tenta encontrar o site oficial da empresa via DuckDuckGo
    quando o Google Places não está disponível.
    Retorna a primeira URL relevante encontrada.
    """
    query = f'"{nome}" {cidade} site oficial contato'
    html = await buscar_duckduckgo(session, query)
    if not html:
        return ""

    # Extrai URLs dos resultados do DuckDuckGo (padrão do HTML lite)
    urls = re.findall(r'class="result__url"[^>]*>\s*([^\s<]+)', html)
    if not urls:
        # Fallback: qualquer URL nos resultados
        urls = re.findall(r'href="(https?://[^"&]{10,})"', html)

    # Reutiliza _DOMINIOS_FALSOS — lista unificada com Google Places e banco.
    # Também descarta duckduckgo.com que aparece nos próprios resultados.
    _ignorar_ddg = _DOMINIOS_FALSOS | {"duckduckgo"}
    for url in urls:
        url = url.strip()
        if not url.startswith("http"):
            url = "https://" + url
        dominio = urlparse(url).netloc.lower()
        if not any(ign in dominio for ign in _ignorar_ddg):
            log.debug(f"Site via DDG: {url} para '{nome}'")
            return url
    return ""


# ─── Scraping do site ─────────────────────────────────────────────────────────

def _desofuscar_email(html: str) -> list:
    """
    Tenta extrair emails ofuscados comuns em sites brasileiros:
    - 'contato [at] empresa [dot] com'
    - 'contato(at)empresa(dot)com'
    - 'contato AT empresa DOT com'
    - data-email="contato@empresa.com" (atributos HTML)
    """
    encontrados = []
    # Padrão ofuscado com [at] ou (at)
    pat_at = re.findall(
        r'([a-zA-Z0-9._%+\-]+)\s*[\[(]at[\])]\s*([a-zA-Z0-9.\-]+)\s*[\[(]dot[\])]\s*([a-zA-Z]{2,})',
        html, re.IGNORECASE
    )
    for u, d, t in pat_at:
        encontrados.append(f"{u}@{d}.{t}")
    # Atributos data-email / data-mail
    pat_data = re.findall(r'data-(?:e?mail|email-address)["\s]*[=:]["\s]*([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})', html, re.IGNORECASE)
    encontrados += pat_data
    return encontrados


def _extrair_emails(html: str) -> list:
    """Extrai e-mails do HTML, incluindo formatos ofuscados."""
    # mailto: links (maior prioridade)
    mailtos = re.findall(r'mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})', html)
    # Padrão normal
    emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", html)
    # Ofuscados
    ofuscados = _desofuscar_email(html)
    todos = list(dict.fromkeys(mailtos + ofuscados + emails))  # prioriza mailto e ofuscados
    return [e for e in todos if not any(x in e.lower() for x in EMAIL_RUIDO)]


# Regex telefone BR — captura os formatos mais comuns em sites:
# (11) 9999-9999, (11) 99999-9999, 11 9999-9999, 11 99999-9999, tel:+5511...
_RE_TELEFONE_SITE = re.compile(
    r'(?:\+?55\s?)?'           # código do país opcional
    r'\(?\s*(\d{2})\s*\)?'    # DDD com ou sem parênteses
    r'[\s\-]?'
    r'(\d{4,5})'              # primeira parte (4 ou 5 dígitos)
    r'[\s\-]?'
    r'(\d{4})'                # última parte (4 dígitos)
)


async def _scrape_pagina(session, url: str) -> tuple[str, str, str]:
    """Scrapa uma página e retorna (email, instagram, telefone) encontrados."""
    try:
        async with session.get(
            url, headers=HEADERS_BROWSER,
            timeout=aiohttp.ClientTimeout(total=4),
            allow_redirects=True,
        ) as r:
            if r.status == 200:
                html = await r.text(errors="ignore")
                emails = _extrair_emails(html)
                email = emails[0] if emails else ""
                instagram = _extrair_instagram_do_html(html)
                telefone = ""
                tel_match = _RE_TELEFONE_SITE.search(html)
                if tel_match:
                    ddd, parte1, parte2 = tel_match.group(1), tel_match.group(2), tel_match.group(3)
                    tel_digits = ddd + parte1 + parte2
                    if len(tel_digits) in (10, 11):
                        telefone = f"({ddd}) {parte1}-{parte2}"
                return email, instagram, telefone
    except Exception:
        pass
    return "", "", ""


async def extrair_contatos_do_site(session, site_url: str) -> dict:
    """
    Scrapa homepage + páginas de contato EM PARALELO.
    Usa asyncio.gather — todas as páginas são buscadas simultaneamente,
    e pega o primeiro resultado não-vazio de cada campo (prioridade = ordem das páginas).
    """
    resultado = {"email_site": "", "instagram_site": "", "telefone_site": ""}
    if not site_url:
        return resultado

    if not site_url.startswith("http"):
        site_url = "https://" + site_url

    paginas = [site_url] + [urljoin(site_url, slug) for slug in PAGINAS_CONTATO]

    # Dispara todas as páginas em paralelo
    try:
        resultados = await asyncio.gather(
            *[_scrape_pagina(session, url) for url in paginas],
            return_exceptions=True,
        )
    except Exception:
        return resultado

    # Mantém prioridade (homepage primeiro, depois /contato, /contact, etc.)
    for r in resultados:
        if isinstance(r, Exception):
            continue
        email, instagram, telefone = r
        if email and not resultado["email_site"]:
            resultado["email_site"] = email
        if instagram and not resultado["instagram_site"]:
            resultado["instagram_site"] = instagram
        if telefone and not resultado["telefone_site"]:
            resultado["telefone_site"] = telefone
        if resultado["email_site"] and resultado["instagram_site"] and resultado["telefone_site"]:
            break

    return resultado


# ─── Pipeline principal ───────────────────────────────────────────────────────

async def _processar_rapido(session, seed_data, db):
    """
    Via rápida: CNPJ já tem telefone do seed (Receita Federal).
    Só chama BrasilAPI para razao_social/porte/socios. Pula DDG e scraping.
    """
    cnpj = re.sub(r"\D", "", seed_data["cnpj"])
    try:
        if db.cnpj_existe_recente(cnpj, dias=30):
            return None

        # Dados do seed (Receita Federal via extrator.py)
        tel_seed   = seed_data.get("telefone", "")
        email_seed = seed_data.get("email", "")
        fantasia   = seed_data.get("nome_fantasia", "")
        uf_seed    = seed_data.get("uf", "")
        cidade_seed = seed_data.get("municipio", "")
        cnae_seed  = seed_data.get("cnae", "")
        abertura_seed = seed_data.get("abertura", "")

        # BrasilAPI para dados complementares (razao_social, porte, socios)
        dados = await buscar_brasilapi(session, cnpj)

        if dados:
            if dados.get("descricao_situacao_cadastral", "").upper() != "ATIVA":
                return None
            nome   = dados.get("razao_social", "")
            porte  = dados.get("porte", "")
            socios = dados.get("qsa", [])
            socio  = socios[0].get("nome_socio", "") if socios else ""
            # BrasilAPI tem prioridade — seus dados são legíveis (vs. códigos brutos do seed)
            fantasia = fantasia or dados.get("nome_fantasia", "")
            uf_seed       = dados.get("uf", "")                    or uf_seed
            cidade_seed   = dados.get("municipio", "")             or cidade_seed
            cnae_seed     = dados.get("cnae_fiscal_descricao", "") or cnae_seed
            abertura_seed = dados.get("data_inicio_atividade", "") or abertura_seed
            email_api = dados.get("email", "")
            # Telefone da BrasilAPI como fallback
            ddd = dados.get("ddd_telefone_1", "")
            num = dados.get("telefone_1", "")
            tel_api = f"({ddd}) {num}" if ddd and num else ""
        else:
            # BrasilAPI falhou — usa só dados do seed
            nome  = fantasia
            porte = ""
            socio = ""
            email_api = ""
            tel_api = ""

        perfil = {
            "cnpj":            cnpj,
            "razao_social":    nome,
            "nome_fantasia":   fantasia,
            "porte":           porte,
            "cnae":            cnae_seed,
            "situacao":        "ATIVA",
            "abertura":        abertura_seed,
            "municipio":       cidade_seed,
            "uf":              uf_seed,
            "socio_principal": socio,
            "telefone":        tel_seed or tel_api,
            "email":           email_seed or email_api or "",
            "instagram":       "",
            "site":            "",
            "rating_google":   "",
            "avaliacoes":      "",
            "atualizado_em":   datetime.utcnow().isoformat(),
        }

        achou = telefone_valido(perfil["telefone"])
        nome_busca = fantasia or nome
        log.debug(
            f"{'✓' if achou else '·'} [RÁPIDO] {nome_busca[:30]:<30} | {uf_seed} | tel:{bool(perfil['telefone'])}"
        )
        return perfil

    except Exception as e:
        log.debug(f"Erro rápido {cnpj}: {e}")
        return None


async def _processar_lento(session, seed_data, db, forcar=False):
    """
    Via lenta: CNPJ sem telefone no seed. Pipeline completo com DDG + scraping.
    Também usado no modo REENRICH.
    """
    cnpj = re.sub(r"\D", "", seed_data["cnpj"])
    try:
        if forcar:
            registro = db.buscar_empresa_por_cnpj(cnpj)
            if not registro:
                return None
            nome       = registro.get("razao_social", "")
            fantasia   = registro.get("nome_fantasia", "")
            cidade     = registro.get("municipio", "")
            socio      = registro.get("socio_principal", "")
            tel_receita = registro.get("telefone", "")
            nome_busca = fantasia or nome
        else:
            if db.cnpj_existe_recente(cnpj, dias=30):
                return None

            dados = await buscar_brasilapi(session, cnpj)
            if not dados:
                return None
            if dados.get("descricao_situacao_cadastral", "").upper() != "ATIVA":
                return None

            nome       = dados.get("razao_social", "")
            fantasia   = seed_data.get("nome_fantasia", "") or dados.get("nome_fantasia", "")
            cidade     = dados.get("municipio", "") or seed_data.get("municipio", "")
            socios     = dados.get("qsa", [])
            socio      = socios[0].get("nome_socio", "") if socios else ""
            ddd        = dados.get("ddd_telefone_1", "")
            num        = dados.get("telefone_1", "")
            tel_receita = f"({ddd}) {num}" if ddd and num else ""
            nome_busca = fantasia or nome

        # DDG para encontrar o site oficial da empresa
        site = await buscar_site_ddg(session, nome_busca, cidade)

        # Scraping do site: extrai email, instagram e telefone
        contatos = await extrair_contatos_do_site(session, site)

        if forcar:
            porte    = registro.get("porte", "")
            cnae_str = registro.get("cnae", "")
            abertura = registro.get("abertura", "")
            uf       = registro.get("uf", "")
            email_rf = registro.get("email", "")
        else:
            porte    = dados.get("porte", "")
            cnae_str = dados.get("cnae_fiscal_descricao", "") or seed_data.get("cnae", "")
            abertura = dados.get("data_inicio_atividade", "") or seed_data.get("abertura", "")
            uf       = dados.get("uf", "") or seed_data.get("uf", "")
            email_rf = seed_data.get("email", "") or dados.get("email", "")

        perfil = {
            "cnpj":            cnpj,
            "razao_social":    nome,
            "nome_fantasia":   fantasia,
            "porte":           porte,
            "cnae":            cnae_str,
            "situacao":        "ATIVA",
            "abertura":        abertura,
            "municipio":       cidade,
            "uf":              uf,
            "socio_principal": socio,
            "telefone":        contatos.get("telefone_site", "") or tel_receita,
            "email":           contatos.get("email_site", "") or email_rf or "",
            "instagram":       contatos.get("instagram_site", ""),
            "site":            site,
            "rating_google":   "",
            "avaliacoes":      "",
            "atualizado_em":   datetime.utcnow().isoformat(),
        }

        achou = telefone_valido(perfil["telefone"])
        if not achou:
            perfil["site"] = ""

        log.debug(
            f"{'✓' if achou else '·'} [LENTO] {nome_busca[:30]:<30} | {uf} | "
            f"tel:{bool(perfil['telefone'])} site:{bool(perfil['site'])}"
        )
        return perfil

    except Exception as e:
        log.debug(f"Erro lento {cnpj}: {e}")
        return None


async def enriquecer_rapido(session, seed_data, db):
    try:
        return await asyncio.wait_for(
            _processar_rapido(session, seed_data, db),
            timeout=TIMEOUT_RAPIDO,
        )
    except (asyncio.TimeoutError, Exception):
        return None


async def enriquecer_lento(session, seed_data, db, forcar=False):
    try:
        return await asyncio.wait_for(
            _processar_lento(session, seed_data, db, forcar=forcar),
            timeout=TIMEOUT_LENTO,
        )
    except (asyncio.TimeoutError, Exception):
        return None


# ─── Loop principal ───────────────────────────────────────────────────────────

async def rodar_agente():
    global _DDG_SEM, _BRASIL_SEM
    _DDG_SEM = asyncio.Semaphore(5)       # max 5 conexões simultâneas ao DuckDuckGo
    _BRASIL_SEM = asyncio.Semaphore(4)    # max 4 conexões simultâneas à BrasilAPI (evita 429)

    db = Database()
    db.criar_tabelas()
    db.criar_tabela_progresso()

    log.info(
        f"Pipeline dual: via rápida={CONCORRENCIA_RAPIDA} workers, "
        f"via lenta={CONCORRENCIA_LENTA} workers"
    )

    if REENRICH_SEM_CONTATO:
        log.info("Modo REENRICH_SEM_CONTATO ativo")
        await rodar_reenrich(db)
    else:
        await rodar_seed(db)


async def rodar_seed(db):
    """Loop padrão: processa CNPJs do cnpjs_seed.txt com pipeline dual."""
    import time
    registros = carregar_cnpjs_seed()
    total = len(registros)

    offset = db.carregar_progresso()
    if offset > 0 and offset < total:
        log.info(f"Retomando do CNPJ {offset:,} de {total:,}")
    elif offset >= total:
        log.info("Todos processados. Reiniciando do zero.")
        offset = 0
        db.salvar_progresso(0)
    else:
        log.info(f"Iniciando do zero. {total:,} CNPJs na fila.")

    sem_rapido = asyncio.Semaphore(CONCORRENCIA_RAPIDA)
    sem_lento  = asyncio.Semaphore(CONCORRENCIA_LENTA)
    SUB_LOTE   = 500  # batch size para saves e progresso

    async def worker_rapido(session, seed_data, db):
        async with sem_rapido:
            return await enriquecer_rapido(session, seed_data, db)

    async def worker_lento(session, seed_data, db):
        async with sem_lento:
            return await enriquecer_lento(session, seed_data, db)

    connector = aiohttp.TCPConnector(limit=200, limit_per_host=30, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                inicio = offset
                fim    = min(offset + LOTE, total)
                lote   = registros[inicio:fim]

                if not lote:
                    log.info("Todos processados! Reiniciando em 60s...")
                    db.salvar_progresso(0)
                    offset = 0
                    await asyncio.sleep(60)
                    continue

                # Separa CNPJs em via rápida (tem telefone) e via lenta (sem telefone)
                rapidos = [r for r in lote if r.get("telefone")]
                lentos  = [r for r in lote if not r.get("telefone")]

                log.info(
                    f"Ciclo: {inicio:,} → {fim:,} de {total:,} ({100*inicio//total}%) | "
                    f"rápidos={len(rapidos)} lentos={len(lentos)}"
                )
                salvos_tel = 0
                salvos_total = 0
                t_inicio_ciclo = time.monotonic()

                # Processa via rápida em sub-lotes
                for sub_inicio in range(0, len(rapidos), SUB_LOTE):
                    sub = rapidos[sub_inicio:sub_inicio + SUB_LOTE]
                    try:
                        resultados = await asyncio.gather(
                            *[worker_rapido(session, r, db) for r in sub],
                            return_exceptions=True,
                        )
                        perfis = [r for r in resultados if r and not isinstance(r, Exception)]
                        if perfis:
                            db.salvar_empresas_batch(perfis)
                            salvos_total += len(perfis)
                            salvos_tel += sum(1 for p in perfis if telefone_valido(p.get("telefone", "")))
                    except Exception as e:
                        log.debug(f"Erro sub-lote rápido: {e}")

                # Processa via lenta em sub-lotes
                for sub_inicio in range(0, len(lentos), SUB_LOTE):
                    sub = lentos[sub_inicio:sub_inicio + SUB_LOTE]
                    try:
                        resultados = await asyncio.gather(
                            *[worker_lento(session, r, db) for r in sub],
                            return_exceptions=True,
                        )
                        perfis = [r for r in resultados if r and not isinstance(r, Exception)]
                        if perfis:
                            db.salvar_empresas_batch(perfis)
                            salvos_total += len(perfis)
                            salvos_tel += sum(1 for p in perfis if telefone_valido(p.get("telefone", "")))
                    except Exception as e:
                        log.debug(f"Erro sub-lote lento: {e}")

                offset = fim
                db.salvar_progresso(offset)
                pct_tel = (salvos_tel / salvos_total * 100) if salvos_total else 0
                elapsed = time.monotonic() - t_inicio_ciclo
                throughput = (len(lote) / elapsed * 3600) if elapsed > 0 else 0
                log.info(
                    f"Ciclo completo. {salvos_total} salvos ({salvos_tel} com tel = {pct_tel:.0f}%). "
                    f"Posição: {offset:,}/{total:,} | Throughput: {throughput:,.0f} CNPJs/hora"
                )
                await asyncio.sleep(PAUSA_CICLO)

            except Exception as e:
                log.error(f"Erro crítico: {e} — reiniciando em 30s")
                await asyncio.sleep(30)


async def rodar_reenrich(db):
    """
    Re-enriquece registros já salvos no banco que não têm email, instagram nem site.
    """
    total_sem_contato = db.contar_sem_contato()
    log.info(f"{total_sem_contato:,} empresas sem contato — iniciando re-enriquecimento")

    semaphore = asyncio.Semaphore(CONCORRENCIA_LENTA)
    SUB_LOTE  = CONCORRENCIA_LENTA * 10
    offset_db = 0
    total_salvos = 0

    async def worker_reenrich(session, cnpj, db):
        async with semaphore:
            return await enriquecer_lento(session, {"cnpj": cnpj}, db, forcar=True)

    connector = aiohttp.TCPConnector(limit=100, limit_per_host=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                lote = db.buscar_cnpjs_sem_contato(limite=LOTE, offset=offset_db)
                if not lote:
                    log.info(f"REENRICH completo! {total_salvos:,} atualizados. Aguardando 5min...")
                    await asyncio.sleep(300)
                    offset_db = 0
                    total_sem_contato = db.contar_sem_contato()
                    log.info(f"Novo ciclo REENRICH: {total_sem_contato:,} ainda sem contato")
                    continue

                log.info(f"REENRICH: {len(lote)} CNPJs (offset={offset_db:,}) | {total_salvos:,} salvos")
                salvos_lote = 0

                for sub_inicio in range(0, len(lote), SUB_LOTE):
                    sub = lote[sub_inicio:sub_inicio + SUB_LOTE]
                    try:
                        resultados = await asyncio.gather(
                            *[worker_reenrich(session, cnpj, db) for cnpj in sub],
                            return_exceptions=True,
                        )
                        perfis = [r for r in resultados if r and not isinstance(r, Exception)]
                        if perfis:
                            db.salvar_empresas_batch(perfis)
                            salvos_lote += len(perfis)
                    except Exception as e:
                        log.debug(f"Erro sub-lote reenrich: {e}")

                total_salvos += salvos_lote
                offset_db += len(lote)
                log.info(f"Lote REENRICH: {salvos_lote} atualizados. Total: {total_salvos:,}")
                await asyncio.sleep(PAUSA_CICLO)

            except Exception as e:
                log.error(f"Erro crítico REENRICH: {e} — reiniciando em 30s")
                await asyncio.sleep(30)


def carregar_cnpjs_seed():
    """
    Carrega o seed file. Suporta dois formatos:
    - Formato antigo: um CNPJ por linha
    - Formato TSV: cnpj\\tnome_fantasia\\tuf\\tmunicipio\\tcnae\\tabertura\\ttelefone1\\ttelefone2\\temail
    Retorna lista de dicts com os campos disponíveis.
    """
    locais = [
        "cnpjs_seed.txt.gz", "cnpjs_seed.txt",
        "../cnpjs_seed.txt.gz", "../cnpjs_seed.txt",
        "/app/cnpjs_seed.txt.gz", "/app/cnpjs_seed.txt",
    ]
    for caminho in locais:
        if not os.path.exists(caminho):
            continue
        registros = []
        com_tel = 0
        opener = gzip.open if caminho.endswith(".gz") else open
        with opener(caminho, "rt", encoding="utf-8", errors="ignore") as f:
            for linha in f:
                linha = linha.strip()
                if not linha:
                    continue
                partes = linha.split("\t")
                if len(partes) >= 9:
                    tel = partes[6] or partes[7]
                    reg = {
                        "cnpj": partes[0],
                        "nome_fantasia": partes[1],
                        "uf": partes[2],
                        "municipio": partes[3],
                        "cnae": partes[4],
                        "abertura": partes[5],
                        "telefone": tel,
                        "email": partes[8],
                    }
                    if tel:
                        com_tel += 1
                else:
                    reg = {"cnpj": partes[0]}
                registros.append(reg)
        total = len(registros)
        pct = (com_tel / total * 100) if total else 0
        log.info(f"Carregados {total:,} CNPJs de '{caminho}' | {com_tel:,} com telefone ({pct:.0f}%)")
        return registros
    log.warning("cnpjs_seed.txt não encontrado!")
    return []


if __name__ == "__main__":
    asyncio.run(rodar_agente())