"""
CNPJ Intel Agent — com persistência de progresso no PostgreSQL.
Ao reiniciar, retoma de onde parou sem reprocessar CNPJs já salvos.

Estratégia de enriquecimento (em ordem de prioridade):
  1. Google Places API (melhor qualidade — requer GOOGLE_API_KEY no Railway)
  2. DuckDuckGo search (fallback sem API key — acha Instagram e site)
  3. Scraping do site oficial (homepage + página de contato)
  4. BrasilAPI email/telefone da Receita Federal
"""

import asyncio
import aiohttp
import re
import logging
import os
import sys
from urllib.parse import quote, urljoin, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Database
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [AGENTE] %(message)s")
log = logging.getLogger(__name__)

GOOGLE_API_KEY     = os.environ.get("GOOGLE_API_KEY", "")
REENRICH_SEM_CONTATO = os.environ.get("REENRICH_SEM_CONTATO", "").lower() in ("1", "true", "yes")

# Com Google Places: REENRICH usa concorrência 10 (Google é rápido,
# DDG só é chamado como fallback dentro de cada task).
# Sem Google Places: REENRICH fica em 3 para não travar o DDG.
CONCORRENCIA = 10 if (REENRICH_SEM_CONTATO and GOOGLE_API_KEY) else \
               (3  if REENRICH_SEM_CONTATO else \
               (15 if GOOGLE_API_KEY else 5))

DELAY        = 0.3 if GOOGLE_API_KEY else (0.8 if REENRICH_SEM_CONTATO else 0.5)
LOTE         = 3000 if REENRICH_SEM_CONTATO else 5000
PAUSA_CICLO  = 5
TIMEOUT_CNPJ = 20
SALVAR_A_CADA = 50

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
PAGINAS_CONTATO = ["/contato", "/contact", "/fale-conosco", "/sobre", "/about"]


# ─── BrasilAPI ────────────────────────────────────────────────────────────────

async def buscar_brasilapi(session, cnpj):
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status == 200:
                return await r.json()
            elif r.status == 429:
                log.warning("BrasilAPI rate limit — aguardando 60s...")
                await asyncio.sleep(60)
    except Exception:
        pass
    return None


# ─── Google Places (requer GOOGLE_API_KEY) ────────────────────────────────────

async def buscar_google_places(session, nome, cidade):
    if not GOOGLE_API_KEY:
        return {}
    try:
        url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
        params = {
            "input": f"{nome} {cidade}",
            "inputtype": "textquery",
            "fields": "formatted_phone_number,website,rating,user_ratings_total",
            "key": GOOGLE_API_KEY,
        }
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as r:
            if r.status == 200:
                data = await r.json()
                candidates = data.get("candidates", [])
                if candidates:
                    p = candidates[0]
                    return {
                        "telefone_google": p.get("formatted_phone_number", ""),
                        "site_google":     p.get("website", ""),
                        "rating_google":   p.get("rating", ""),
                        "avaliacoes":      p.get("user_ratings_total", ""),
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


async def buscar_duckduckgo(session, query: str, tentativas: int = 2) -> str:
    """
    Busca no DuckDuckGo Lite com retry e backoff.
    Retorna string vazia se bloqueado após todas as tentativas.
    """
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
    for tentativa in range(tentativas):
        try:
            if tentativa > 0:
                await asyncio.sleep(2 + tentativa * 2)  # backoff: 4s, 6s...
            async with session.get(
                url, headers=HEADERS_BROWSER,
                timeout=aiohttp.ClientTimeout(total=10),
                allow_redirects=True,
            ) as r:
                if r.status == 200:
                    html = await r.text(errors="ignore")
                    # DDG retorna página de "no results" ou CAPTCHA quando bloqueado
                    if "duckduckgo" in html and len(html) > 2000:
                        return html
                elif r.status == 429:
                    log.debug(f"DDG rate limit — aguardando antes de retry")
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

    # Filtra URLs irrelevantes — redes sociais, buscadores e
    # diretórios de empresas que aparecem antes do site real
    dominios_ignorar = {
        # Redes sociais e buscadores
        "duckduckgo", "google", "facebook", "instagram", "youtube",
        "linkedin", "twitter", "tiktok", "bing", "yahoo", "whatsapp",
        # Governo / jurídico
        "receitafederal", "gov.br", "jusbrasil", "escavador", "tjsp",
        # Diretórios de CNPJ brasileiros
        "cnpj.info", "cnpja", "cnpjbiz", "cnpj.biz", "cnpj.ws",
        "empresasdobrasil", "qsa.me", "econodata", "minhareceita",
        "receitaws", "casadosdados", "sintegra", "servicos.receita",
        # Diretórios internacionais de empresas
        "dnb.com", "d-u-n-s", "opencorporates", "bloomberg.com",
        "crunchbase", "zoominfo", "manta.com", "bizapedia",
        "companieshouse", "corpwatch",
        # Listas e guias
        "telelistas", "guiamais", "apontador", "yellowpages",
        "yelp.com", "tripadvisor", "foursquare", "paginas.amarelas",
        "infobel", "hotfrog",
        # Marketplaces e e-commerce genéricos
        "mercadolivre", "shopee", "americanas", "magazineluiza",
        "ifood", "rappi",
    }
    for url in urls:
        url = url.strip()
        if not url.startswith("http"):
            url = "https://" + url
        dominio = urlparse(url).netloc.lower()
        if not any(ign in dominio for ign in dominios_ignorar):
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


async def _scrape_pagina(session, url: str) -> tuple[str, str]:
    """Scrapa uma página e retorna (email, instagram) encontrados."""
    try:
        async with session.get(
            url, headers=HEADERS_BROWSER,
            timeout=aiohttp.ClientTimeout(total=6),
            allow_redirects=True,
        ) as r:
            if r.status == 200:
                html = await r.text(errors="ignore")
                emails = _extrair_emails(html)
                email = emails[0] if emails else ""
                instagram = _extrair_instagram_do_html(html)
                return email, instagram
    except Exception:
        pass
    return "", ""


async def extrair_contatos_do_site(session, site_url: str) -> dict:
    """
    Scrapa homepage + páginas de contato EM PARALELO.
    Usa asyncio.gather — todas as páginas são buscadas simultaneamente,
    e pega o primeiro resultado não-vazio de cada campo (prioridade = ordem das páginas).
    """
    resultado = {"email_site": "", "instagram_site": ""}
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
        email, instagram = r
        if email and not resultado["email_site"]:
            resultado["email_site"] = email
        if instagram and not resultado["instagram_site"]:
            resultado["instagram_site"] = instagram
        if resultado["email_site"] and resultado["instagram_site"]:
            break

    return resultado


# ─── Pipeline principal ───────────────────────────────────────────────────────

async def _processar(session, cnpj, db, forcar=False):
    cnpj = re.sub(r"\D", "", cnpj)
    try:
        # Em modo REENRICH (forcar=True): usa dados já salvos no banco
        # para evitar chamadas desnecessárias à BrasilAPI (que tem rate limit).
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
            fantasia   = dados.get("nome_fantasia", "")
            cidade     = dados.get("municipio", "")
            socios     = dados.get("qsa", [])
            socio      = socios[0].get("nome_socio", "") if socios else ""
            ddd        = dados.get("ddd_telefone_1", "")
            num        = dados.get("telefone_1", "")
            tel_receita = f"({ddd}) {num}" if ddd and num else ""
            nome_busca = fantasia or nome

        # 1+4. Google Places e DDG Instagram são independentes → rodam em paralelo
        google_task = buscar_google_places(session, nome_busca, cidade)
        insta_task  = buscar_instagram_ddg(session, nome_busca, cidade)
        google, insta_ddg_fallback = await asyncio.gather(google_task, insta_task)

        site = google.get("site_google", "")

        # 2. Se não tem site (Google Places off ou sem resultado), tenta DuckDuckGo
        if not site:
            site = await buscar_site_ddg(session, nome_busca, cidade)

        # 3. Scraping do site (homepage + páginas de contato, em paralelo)
        contatos = await extrair_contatos_do_site(session, site)

        # 4. Fallback Instagram: já buscamos em paralelo acima, aproveita se necessário
        if not contatos["instagram_site"]:
            contatos["instagram_site"] = insta_ddg_fallback

        # No REENRICH usamos dados do banco; no fluxo normal usamos dados da BrasilAPI
        if forcar:
            porte    = registro.get("porte", "")
            cnae_str = registro.get("cnae", "")
            abertura = registro.get("abertura", "")
            uf       = registro.get("uf", "")
            email_rf = registro.get("email", "")
        else:
            porte    = dados.get("porte", "")
            cnae_str = dados.get("cnae_fiscal_descricao", "")
            abertura = dados.get("data_inicio_atividade", "")
            uf       = dados.get("uf", "")
            email_rf = dados.get("email", "")

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
            "telefone":        google.get("telefone_google", "") or tel_receita,
            "email":           contatos.get("email_site", "") or email_rf or "",
            "instagram":       contatos.get("instagram_site", ""),
            "site":            site,
            "rating_google":   str(google.get("rating_google", "")),
            "avaliacoes":      str(google.get("avaliacoes", "")),
            "atualizado_em":   datetime.utcnow().isoformat(),
        }

        achou = bool(perfil["email"] or perfil["instagram"] or perfil["site"] or
                     (perfil["telefone"] and not tel_receita))  # telefone novo (não era do banco)

        # No REENRICH, só salva (e atualiza atualizado_em) se achou algo novo.
        # Isso evita que o ORDER BY atualizado_em ASC fique descontrolado
        # e o offset pule empresas não processadas.
        if not forcar or achou:
            db.salvar_empresa(perfil)

        log.info(
            f"{'✓' if achou else '·'} {nome_busca[:35]:<35} | {uf} | "
            f"tel:{bool(perfil['telefone'])} "
            f"email:{bool(perfil['email'])} "
            f"insta:{bool(perfil['instagram'])}"
        )
        return perfil if achou else None

    except Exception as e:
        log.debug(f"Erro {cnpj}: {e}")
        return None


async def enriquecer(session, cnpj, db, forcar=False):
    try:
        return await asyncio.wait_for(_processar(session, cnpj, db, forcar=forcar), timeout=TIMEOUT_CNPJ)
    except asyncio.TimeoutError:
        return None
    except Exception:
        return None


# ─── Loop principal ───────────────────────────────────────────────────────────

async def rodar_agente():
    db = Database()
    db.criar_tabelas()
    db.criar_tabela_progresso()

    if GOOGLE_API_KEY:
        log.info(f"✅ Google Places API ativa — concorrência={CONCORRENCIA}, delay={DELAY}s")
    else:
        log.info(f"⚠️  GOOGLE_API_KEY não configurada — DuckDuckGo fallback | concorrência={CONCORRENCIA}")

    if REENRICH_SEM_CONTATO:
        log.info("🔁 Modo REENRICH_SEM_CONTATO ativo — re-processando registros sem email/instagram/site")
        await rodar_reenrich(db)
    else:
        await rodar_seed(db)


async def rodar_seed(db):
    """Loop padrão: processa CNPJs do cnpjs_seed.txt em ordem."""
    cnpjs = carregar_cnpjs_seed()
    total = len(cnpjs)

    offset = db.carregar_progresso()
    if offset > 0 and offset < total:
        log.info(f"🔄 Retomando do CNPJ {offset:,} de {total:,}")
    elif offset >= total:
        log.info("✅ Todos processados. Reiniciando do zero.")
        offset = 0
        db.salvar_progresso(0)
    else:
        log.info(f"🆕 Iniciando do zero. {total:,} CNPJs na fila.")

    semaphore = asyncio.Semaphore(CONCORRENCIA)
    SUB_LOTE  = CONCORRENCIA * 10

    async def enriquecer_paralelo(session, cnpj, db):
        async with semaphore:
            resultado = await enriquecer(session, cnpj, db)
            await asyncio.sleep(DELAY)
            return resultado

    connector = aiohttp.TCPConnector(limit=100, limit_per_host=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                inicio = offset
                fim    = min(offset + LOTE, total)
                lote   = cnpjs[inicio:fim]

                if not lote:
                    log.info("✅ Todos processados! Reiniciando em 60s...")
                    db.salvar_progresso(0)
                    offset = 0
                    await asyncio.sleep(60)
                    continue

                log.info(f"Ciclo: {inicio:,} → {fim:,} de {total:,} ({100*inicio//total}%) | concorrência={CONCORRENCIA}")
                salvos = 0

                for sub_inicio in range(0, len(lote), SUB_LOTE):
                    sub = lote[sub_inicio:sub_inicio + SUB_LOTE]
                    try:
                        resultados = await asyncio.gather(
                            *[enriquecer_paralelo(session, cnpj, db) for cnpj in sub],
                            return_exceptions=True
                        )
                        salvos += sum(1 for r in resultados if r and not isinstance(r, Exception))
                    except Exception as e:
                        log.debug(f"Erro no sub-lote: {e}")

                    db.salvar_progresso(inicio + sub_inicio + len(sub))

                offset = fim
                db.salvar_progresso(offset)
                log.info(f"Ciclo completo. {salvos} salvos. Posição: {offset:,}/{total:,}")
                await asyncio.sleep(PAUSA_CICLO)

            except Exception as e:
                log.error(f"Erro crítico: {e} — reiniciando em 30s")
                await asyncio.sleep(30)


async def rodar_reenrich(db):
    """
    Re-enriquece registros já salvos no banco que não têm email, instagram nem site.
    Ideal para usar com Google Places API para extrair o máximo de contatos dos 130k.
    """
    total_sem_contato = db.contar_sem_contato()
    log.info(f"📊 {total_sem_contato:,} empresas sem contato algum — iniciando re-enriquecimento")

    semaphore = asyncio.Semaphore(CONCORRENCIA)
    SUB_LOTE  = CONCORRENCIA * 10
    offset_db = 0
    total_salvos = 0

    async def enriquecer_forçado(session, cnpj, db):
        async with semaphore:
            resultado = await enriquecer(session, cnpj, db, forcar=True)
            await asyncio.sleep(DELAY)
            return resultado

    connector = aiohttp.TCPConnector(limit=100, limit_per_host=20, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            try:
                lote = db.buscar_cnpjs_sem_contato(limite=LOTE, offset=offset_db)
                if not lote:
                    log.info(f"✅ REENRICH completo! {total_salvos:,} registros atualizados. Aguardando 5min...")
                    await asyncio.sleep(300)
                    # Recomeça — pode ter novos sem contato
                    offset_db = 0
                    total_sem_contato = db.contar_sem_contato()
                    log.info(f"🔄 Novo ciclo REENRICH: {total_sem_contato:,} ainda sem contato")
                    continue

                log.info(f"REENRICH: processando {len(lote)} CNPJs (offset={offset_db:,}) | {total_salvos:,} salvos até agora")
                salvos_lote = 0

                for sub_inicio in range(0, len(lote), SUB_LOTE):
                    sub = lote[sub_inicio:sub_inicio + SUB_LOTE]
                    try:
                        resultados = await asyncio.gather(
                            *[enriquecer_forçado(session, cnpj, db) for cnpj in sub],
                            return_exceptions=True
                        )
                        salvos_lote += sum(1 for r in resultados if r and not isinstance(r, Exception))
                    except Exception as e:
                        log.debug(f"Erro sub-lote reenrich: {e}")

                total_salvos += salvos_lote
                # Avança pelo tamanho total do lote — os que achamos contato saem
                # da query (não têm mais email/insta vazio), os que não achamos
                # ficam no banco com atualizado_em INALTERADO (não chamamos salvar_empresa
                # pra eles), então são vistos novamente na próxima rodada mas com offset maior.
                offset_db += len(lote)
                log.info(f"Lote REENRICH: {salvos_lote} atualizados. Total: {total_salvos:,}")
                await asyncio.sleep(PAUSA_CICLO)

            except Exception as e:
                log.error(f"Erro crítico REENRICH: {e} — reiniciando em 30s")
                await asyncio.sleep(30)


def carregar_cnpjs_seed():
    locais = ["cnpjs_seed.txt", "../cnpjs_seed.txt", "/app/cnpjs_seed.txt"]
    for caminho in locais:
        if os.path.exists(caminho):
            with open(caminho, "r") as f:
                cnpjs = [l.strip() for l in f if l.strip()]
            log.info(f"Carregados {len(cnpjs):,} CNPJs de '{caminho}'")
            return cnpjs
    log.warning("cnpjs_seed.txt não encontrado!")
    return []


if __name__ == "__main__":
    asyncio.run(rodar_agente())