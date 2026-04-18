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
import random
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
CONCORRENCIA_RAPIDA = 3 if REENRICH_SEM_CONTATO else 15
# Via lenta é I/O-bound (DDG + scraping). Sobe com GOOGLE_API_KEY (sem depender de DDG para site).
CONCORRENCIA_LENTA  = 3 if REENRICH_SEM_CONTATO else (30 if GOOGLE_API_KEY else 15)

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

async def buscar_brasilapi(session, cnpj, tentativas=3):
    global _BRASIL_429_COUNT
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    sem = _BRASIL_SEM if _BRASIL_SEM is not None else asyncio.Semaphore(4)
    for tentativa in range(tentativas):
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
            _BRASIL_429_COUNT += 1
            espera = (2 ** tentativa) + random.uniform(0, 1)
            log.debug(f"BrasilAPI rate limit — aguardando {espera:.1f}s (tentativa {tentativa+1}/{tentativas})...")
            await asyncio.sleep(espera)
        else:
            break  # erro de rede ou outro — não retenta
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

# Semáforo global de BrasilAPI — via rápida agora quase não chama (seed expandido
# traz razao_social/porte/socio). Fica baixo (3) para os fallbacks esporádicos
# e para a via lenta, que continua usando BrasilAPI.
_BRASIL_SEM: asyncio.Semaphore | None = None

# Contador agregado de 429s da BrasilAPI — logado como resumo por ciclo
# em vez de uma linha por tentativa.
_BRASIL_429_COUNT: int = 0


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
        if await asyncio.to_thread(db.cnpj_existe_recente, cnpj, 30):
            return None

        # Dados do seed (Receita Federal via extrator.py)
        tel_seed   = seed_data.get("telefone", "")
        email_seed = seed_data.get("email", "")
        fantasia   = seed_data.get("nome_fantasia", "")
        uf_seed    = seed_data.get("uf", "")
        cidade_seed = seed_data.get("municipio", "")
        cnae_seed  = seed_data.get("cnae", "")
        abertura_seed = seed_data.get("abertura", "")

        # Dados complementares: seed expandido (12 colunas) elimina BrasilAPI na via rápida.
        # Se faltar razao_social/porte no seed (seed antigo de 9 colunas), cai no fallback.
        razao_seed = seed_data.get("razao_social", "")
        porte_seed = seed_data.get("porte", "")
        socio_seed = seed_data.get("socio_principal", "")

        if razao_seed and porte_seed:
            # Caminho principal (≥95% dos CNPJs): zero chamadas à BrasilAPI
            nome   = razao_seed
            porte  = porte_seed
            socio  = socio_seed
            email_api = ""
            tel_api = ""
        else:
            # Fallback: seed antigo sem as colunas de Empresas/Socios
            dados = await buscar_brasilapi(session, cnpj)
            if dados:
                if dados.get("descricao_situacao_cadastral", "").upper() != "ATIVA":
                    return None
                nome   = dados.get("razao_social", "")
                porte  = dados.get("porte", "")
                socios = dados.get("qsa", [])
                socio  = socios[0].get("nome_socio", "") if socios else ""
                fantasia = fantasia or dados.get("nome_fantasia", "")
                uf_seed       = dados.get("uf", "")                    or uf_seed
                cidade_seed   = dados.get("municipio", "")             or cidade_seed
                cnae_seed     = dados.get("cnae_fiscal_descricao", "") or cnae_seed
                abertura_seed = dados.get("data_inicio_atividade", "") or abertura_seed
                email_api = dados.get("email", "")
                ddd = dados.get("ddd_telefone_1", "")
                num = dados.get("telefone_1", "")
                tel_api = f"({ddd}) {num}" if ddd and num else ""
            else:
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
            registro = await asyncio.to_thread(db.buscar_empresa_por_cnpj, cnpj)
            if not registro:
                return None
            nome       = registro.get("razao_social", "")
            fantasia   = registro.get("nome_fantasia", "")
            cidade     = registro.get("municipio", "")
            socio      = registro.get("socio_principal", "")
            tel_receita = registro.get("telefone", "")
            nome_busca = fantasia or nome
        else:
            if await asyncio.to_thread(db.cnpj_existe_recente, cnpj, 30):
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
    _DDG_SEM = asyncio.Semaphore(min(CONCORRENCIA_LENTA, 10))  # alinhado com workers lenta
    # BrasilAPI agora é fallback raro (seed expandido via extrator --empresas/--socios).
    # 3 é suficiente e fica dentro do rate limit real da API (~3 req/s).
    _BRASIL_SEM = asyncio.Semaphore(3)

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
    meta = carregar_cnpjs_seed()
    while not meta.get("total"):
        log.error("Seed vazio ou não encontrado. Aguardando 300s antes de nova tentativa.")
        await asyncio.sleep(300)
        meta = carregar_cnpjs_seed()

    total              = meta["total"]
    caminho            = meta["caminho"]
    com_tel_amostra    = meta.get("com_tel", 0)
    com_razao_amostra  = meta.get("com_razao", 0)
    amostra            = meta.get("amostra", 0)
    pct_tel_amostra    = (com_tel_amostra / amostra * 100) if amostra else 0
    log.info(
        f"Seed pronto: {total:,} CNPJs de '{caminho}' | amostra={amostra:,}: "
        f"{com_tel_amostra:,} com telefone ({pct_tel_amostra:.0f}%), "
        f"{com_razao_amostra:,} com razao_social"
    )
    if amostra and com_tel_amostra == 0:
        log.warning("Nenhum CNPJ com telefone na amostra. Todos provavelmente irão via lenta (BrasilAPI+DDG) — progresso será lento.")

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
    SUB_LOTE_RAPIDO = 500
    SUB_LOTE_LENTO  = 200  # menor para suavizar WAL writes no PG

    async def worker_rapido(session, seed_data, db):
        async with sem_rapido:
            return await enriquecer_rapido(session, seed_data, db)

    async def worker_lento(session, seed_data, db):
        async with sem_lento:
            return await enriquecer_lento(session, seed_data, db)

    connector = aiohttp.TCPConnector(limit=200, limit_per_host=50, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                inicio = offset
                fim    = min(offset + LOTE, total)
                # Streaming: lê apenas o lote atual do arquivo — O(LOTE) de memória.
                lote   = await asyncio.to_thread(_ler_lote_seed, caminho, inicio, LOTE)

                if not lote:
                    log.info("Todos processados! Reiniciando em 60s...")
                    db.salvar_progresso(0)
                    offset = 0
                    await asyncio.sleep(60)
                    continue

                # Pré-filtro do lote inteiro: uma query batch elimina CNPJs já processados,
                # evitando despachar workers inúteis.
                cnpjs_lote = [re.sub(r"\D", "", r["cnpj"]) for r in lote]
                recentes_lote = await asyncio.to_thread(db.filtrar_cnpjs_recentes, cnpjs_lote)
                lote_filtrado = [r for r in lote if re.sub(r"\D", "", r["cnpj"]) not in recentes_lote]
                pulados_lote = len(lote) - len(lote_filtrado)

                # Separa CNPJs em via rápida (tem telefone) e via lenta (sem telefone).
                # Seed 1-coluna: todos têm só "cnpj" → força via rápida (BrasilAPI busca tel).
                if lote_filtrado and all(len(r) == 1 for r in lote_filtrado):
                    log.info("Seed formato 1-coluna: roteando lote inteiro para via rápida (BrasilAPI)")
                    rapidos = lote_filtrado
                    lentos  = []
                else:
                    rapidos = [r for r in lote_filtrado if r.get("telefone")]
                    lentos  = [r for r in lote_filtrado if not r.get("telefone")]

                log.info(
                    f"Ciclo: {inicio:,} → {fim:,} de {total:,} ({100*inicio//total}%) | "
                    f"rápidos={len(rapidos)} lentos={len(lentos)} (pulados={pulados_lote:,})"
                )
                salvos_tel = 0
                salvos_total = 0
                t_inicio_ciclo = time.monotonic()

                global _BRASIL_429_COUNT
                _BRASIL_429_COUNT = 0

                # Processa via rápida em sub-lotes
                n_sub_rapidos = (len(rapidos) + SUB_LOTE_RAPIDO - 1) // SUB_LOTE_RAPIDO
                for sub_inicio in range(0, len(rapidos), SUB_LOTE_RAPIDO):
                    sub = rapidos[sub_inicio:sub_inicio + SUB_LOTE_RAPIDO]
                    sub_n = sub_inicio // SUB_LOTE_RAPIDO + 1
                    log.info(f"  Sub-lote rápido {sub_n}/{n_sub_rapidos}: {len(sub)} CNPJs...")
                    if not sub:
                        continue
                    try:
                        resultados = await asyncio.gather(
                            *[worker_rapido(session, r, db) for r in sub],
                            return_exceptions=True,
                        )
                        perfis = [r for r in resultados if r and not isinstance(r, Exception)]
                        if perfis:
                            await asyncio.to_thread(db.salvar_empresas_batch, perfis)
                            salvos_total += len(perfis)
                            salvos_tel += sum(1 for p in perfis if telefone_valido(p.get("telefone", "")))
                    except Exception as e:
                        log.warning(f"Erro sub-lote rápido: {e}")

                # Processa via lenta em sub-lotes (menores para suavizar WAL writes)
                n_sub_lentos = (len(lentos) + SUB_LOTE_LENTO - 1) // SUB_LOTE_LENTO
                for sub_inicio in range(0, len(lentos), SUB_LOTE_LENTO):
                    sub = lentos[sub_inicio:sub_inicio + SUB_LOTE_LENTO]
                    sub_n = sub_inicio // SUB_LOTE_LENTO + 1
                    log.info(f"  Sub-lote lento {sub_n}/{n_sub_lentos}: {len(sub)} CNPJs...")
                    if not sub:
                        continue
                    try:
                        resultados = await asyncio.gather(
                            *[worker_lento(session, r, db) for r in sub],
                            return_exceptions=True,
                        )
                        perfis = [r for r in resultados if r and not isinstance(r, Exception)]
                        if perfis:
                            await asyncio.to_thread(db.salvar_empresas_batch, perfis)
                            salvos_total += len(perfis)
                            salvos_tel += sum(1 for p in perfis if telefone_valido(p.get("telefone", "")))
                    except Exception as e:
                        log.warning(f"Erro sub-lote lento: {e}")

                offset = fim
                await asyncio.to_thread(db.salvar_progresso, offset)
                pct_tel = (salvos_tel / salvos_total * 100) if salvos_total else 0
                elapsed = time.monotonic() - t_inicio_ciclo
                throughput = (len(lote) / elapsed * 3600) if elapsed > 0 else 0
                log.info(
                    f"Ciclo completo. {salvos_total} salvos ({salvos_tel} com tel = {pct_tel:.0f}%). "
                    f"Posição: {offset:,}/{total:,} | Throughput: {throughput:,.0f} CNPJs/hora"
                )
                if _BRASIL_429_COUNT > 0:
                    log.info(f"BrasilAPI: {_BRASIL_429_COUNT} rate limits neste ciclo")
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
                            await asyncio.to_thread(db.salvar_empresas_batch, perfis)
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


# ─── Seed I/O (streaming) ────────────────────────────────────────────────────
# Antes carregávamos o seed inteiro em memória (~1.3GB para 1.3M CNPJs).
# Agora:
#   - carregar_cnpjs_seed() → retorna APENAS metadados (total, caminho, amostra)
#   - _ler_lote_seed(caminho, offset, batch_size) → lê um lote sob demanda
# Memória pico por ciclo: ~LOTE × ~1KB ≈ 5MB.

_SEED_LOCAIS = [
    "cnpjs_seed.txt.gz", "cnpjs_seed.txt",
    "../cnpjs_seed.txt.gz", "../cnpjs_seed.txt",
    "/app/cnpjs_seed.txt.gz", "/app/cnpjs_seed.txt",
]


def _encontrar_seed() -> str:
    """Retorna o primeiro caminho existente do seed, ou '' se não encontrar."""
    for caminho in _SEED_LOCAIS:
        if os.path.exists(caminho):
            return caminho
    return ""


def _abrir_seed(caminho: str):
    """Abre o seed (.gz ou texto) em modo texto com encoding tolerante."""
    opener = gzip.open if caminho.endswith(".gz") else open
    return opener(caminho, "rt", encoding="utf-8", errors="ignore")


def _parse_linha_seed(linha: str) -> dict | None:
    """
    Converte uma linha do seed em dict.
    Suporta três formatos por contagem de colunas:
    - 1 coluna:  apenas CNPJ
    - 9 colunas: cnpj, nome_fantasia, uf, municipio, cnae, abertura, tel1, tel2, email
    - 12 colunas: + razao_social, porte, socio_principal
    Retorna None se a linha for vazia.
    """
    linha = linha.strip()
    if not linha:
        return None
    partes = linha.split("\t")
    if len(partes) >= 9:
        tel = partes[6] or partes[7]
        return {
            "cnpj":            partes[0],
            "nome_fantasia":   partes[1],
            "uf":              partes[2],
            "municipio":       partes[3],
            "cnae":            partes[4],
            "abertura":        partes[5],
            "telefone":        tel,
            "email":           partes[8],
            "razao_social":    partes[9]  if len(partes) >= 10 else "",
            "porte":           partes[10] if len(partes) >= 11 else "",
            "socio_principal": partes[11] if len(partes) >= 12 else "",
        }
    return {"cnpj": partes[0]}


def _contar_linhas_seed() -> tuple[int, str]:
    """
    Conta linhas não-vazias do seed sem carregá-lo em memória.
    Retorna (total_linhas, caminho) ou (0, '') se não encontrar.
    """
    caminho = _encontrar_seed()
    if not caminho:
        return 0, ""
    total = 0
    with _abrir_seed(caminho) as f:
        for linha in f:
            if linha.strip():
                total += 1
    return total, caminho


def _ler_lote_seed(caminho: str, offset: int, batch_size: int) -> list[dict]:
    """
    Lê um lote do seed a partir de `offset` (contado em linhas não-vazias),
    com no máximo `batch_size` registros. Suporta .gz e .txt.
    Memória: O(batch_size). Não loga — chamado a cada ciclo.
    """
    if not caminho or batch_size <= 0:
        return []
    lote: list[dict] = []
    visto = 0  # linhas não-vazias já vistas
    try:
        with _abrir_seed(caminho) as f:
            for linha in f:
                if not linha.strip():
                    continue
                if visto < offset:
                    visto += 1
                    continue
                reg = _parse_linha_seed(linha)
                if reg is not None:
                    lote.append(reg)
                visto += 1
                if len(lote) >= batch_size:
                    break
    except (OSError, EOFError) as e:
        log.warning(f"Erro lendo seed '{caminho}' em offset={offset}: {e}")
    return lote


def carregar_cnpjs_seed() -> dict:
    """
    Retorna metadados do seed (sem carregar registros em memória).

    Suporta três formatos (detecção por número de colunas):
    - 1 coluna:   apenas CNPJ
    - 9 colunas:  cnpj, nome_fantasia, uf, municipio, cnae, abertura, tel1, tel2, email
    - 12 colunas: + razao_social, porte, socio_principal  (extrator com --empresas/--socios)

    Retorno:
        {
            "total":     int,   # total de linhas não-vazias
            "caminho":   str,   # caminho do arquivo encontrado
            "com_tel":   int,   # CNPJs com telefone na amostra inicial
            "com_razao": int,   # CNPJs com razao_social na amostra inicial
            "amostra":   int,   # tamanho da amostra lida
        }
    Em caso de seed ausente, retorna dict com total=0.
    """
    total, caminho = _contar_linhas_seed()
    if not caminho:
        log.warning("cnpjs_seed.txt não encontrado!")
        return {"total": 0, "caminho": "", "com_tel": 0, "com_razao": 0, "amostra": 0}

    # Amostra de até 5.000 registros no início do arquivo — o suficiente para
    # estimar % com telefone / razao_social sem carregar o seed inteiro.
    AMOSTRA_MAX = 5000
    amostra_regs = _ler_lote_seed(caminho, 0, AMOSTRA_MAX)
    amostra = len(amostra_regs)
    com_tel   = sum(1 for r in amostra_regs if r.get("telefone"))
    com_razao = sum(1 for r in amostra_regs if r.get("razao_social"))

    pct_tel   = (com_tel   / amostra * 100) if amostra else 0
    pct_razao = (com_razao / amostra * 100) if amostra else 0
    log.info(
        f"Seed '{caminho}': {total:,} CNPJs total | amostra={amostra:,}: "
        f"{com_tel:,} com telefone ({pct_tel:.0f}%), "
        f"{com_razao:,} com razao_social ({pct_razao:.0f}%)"
    )
    return {
        "total":     total,
        "caminho":   caminho,
        "com_tel":   com_tel,
        "com_razao": com_razao,
        "amostra":   amostra,
    }


if __name__ == "__main__":
    asyncio.run(rodar_agente())