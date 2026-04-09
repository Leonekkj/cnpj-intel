"""
CNPJ Intel Agent — coleta e enriquece dados de empresas automaticamente.
Fontes: BrasilAPI (gratuita) + Google Places + scraping de site/Instagram.
"""

import asyncio
import aiohttp
import re
import json
import logging
import os
import time
from datetime import datetime
from typing import Optional
from database import Database

logging.basicConfig(level=logging.INFO, format="%(asctime)s [AGENTE] %(message)s")
log = logging.getLogger(__name__)

# ─── Configurações ────────────────────────────────────────────────
GOOGLE_API_KEY = "SUA_CHAVE_GOOGLE_PLACES_AQUI"   # opcional mas recomendado
HUNTER_API_KEY  = "SUA_CHAVE_HUNTER_IO_AQUI"       # opcional, acha e-mails

# CNAEs que costumam ter bom retorno comercial (adapte como quiser)
CNAES_ALVO = [
    "4711301", "4711302",  # supermercados
    "5611201", "5611203",  # restaurantes
    "8630503", "8630504",  # clínicas médicas/odontológicas
    "6201500",             # desenvolvimento de software
    "4771701",             # farmácias
    "4512901",             # concessionárias
    "9602501",             # salões de beleza
    "8011101",             # segurança privada
]

DELAY_ENTRE_REQUESTS = 1.2   # segundos — respeita rate limit da BrasilAPI
MAX_CNPJS_POR_CICLO  = 500   # limite por rodada do agente


# ─── Busca de dados na BrasilAPI (100% gratuita) ──────────────────
async def buscar_brasilapi(session: aiohttp.ClientSession, cnpj: str) -> Optional[dict]:
    """Retorna dados cadastrais completos de um CNPJ via BrasilAPI."""
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                return await r.json()
            elif r.status == 429:
                log.warning("Rate limit BrasilAPI — aguardando 30s...")
                await asyncio.sleep(30)
    except Exception as e:
        log.error(f"Erro BrasilAPI {cnpj}: {e}")
    return None


# ─── Busca telefone/site no Google Places ─────────────────────────
async def buscar_google_places(session: aiohttp.ClientSession, nome: str, cidade: str) -> dict:
    """Busca telefone, site e avaliações da empresa no Google Places."""
    if not GOOGLE_API_KEY or GOOGLE_API_KEY.startswith("SUA_"):
        return {}
    query = f"{nome} {cidade}"
    search_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id,name,formatted_phone_number,website,rating,user_ratings_total",
        "key": GOOGLE_API_KEY,
    }
    try:
        async with session.get(search_url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as r:
            data = await r.json()
            candidates = data.get("candidates", [])
            if candidates:
                place = candidates[0]
                return {
                    "telefone_google": place.get("formatted_phone_number", ""),
                    "site_google":     place.get("website", ""),
                    "rating_google":   place.get("rating", ""),
                    "avaliacoes":      place.get("user_ratings_total", ""),
                }
    except Exception as e:
        log.warning(f"Erro Google Places ({nome}): {e}")
    return {}


# ─── Extração de e-mail e Instagram do site da empresa ────────────
async def extrair_contatos_do_site(session: aiohttp.ClientSession, url: str) -> dict:
    """Raspa e-mail e perfil do Instagram diretamente do site da empresa."""
    if not url:
        return {}
    if not url.startswith("http"):
        url = "https://" + url
    resultado = {"email_site": "", "instagram_site": ""}
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; CNPJIntelBot/1.0)"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10), allow_redirects=True) as r:
            if r.status == 200:
                html = await r.text(errors="ignore")

                # e-mails
                emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", html)
                emails_validos = [e for e in emails if not any(x in e for x in ["@sentry", "@example", "@pixel", "noreply", "wix.com"])]
                if emails_validos:
                    resultado["email_site"] = emails_validos[0]

                # instagram
                insta = re.findall(r'instagram\.com/([A-Za-z0-9_.]{2,30})', html)
                insta = [i for i in insta if i not in ("p", "tv", "reel", "stories", "explore", "accounts")]
                if insta:
                    resultado["instagram_site"] = "@" + insta[0]
    except Exception:
        pass
    return resultado


# ─── Busca e-mail via Hunter.io ───────────────────────────────────
async def buscar_email_hunter(session: aiohttp.ClientSession, dominio: str) -> str:
    """Busca e-mails corporativos pelo domínio do site via Hunter.io."""
    if not HUNTER_API_KEY or HUNTER_API_KEY.startswith("SUA_") or not dominio:
        return ""
    dominio = re.sub(r"https?://", "", dominio).split("/")[0]
    url = f"https://api.hunter.io/v2/domain-search?domain={dominio}&api_key={HUNTER_API_KEY}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json()
                emails = data.get("data", {}).get("emails", [])
                if emails:
                    return emails[0].get("value", "")
    except Exception:
        pass
    return ""


# ─── Monta o perfil completo de uma empresa ───────────────────────
async def enriquecer_empresa(session: aiohttp.ClientSession, cnpj: str, db: Database) -> Optional[dict]:
    """Pipeline completo: busca + enriquece + salva um CNPJ."""
    cnpj_limpo = re.sub(r"\D", "", cnpj)

    # Pula se já está no banco e foi atualizado há menos de 30 dias
    if db.cnpj_existe_recente(cnpj_limpo, dias=30):
        return None

    dados_rf = await buscar_brasilapi(session, cnpj_limpo)
    if not dados_rf or dados_rf.get("descricao_situacao_cadastral") != "ATIVA":
        return None

    nome    = dados_rf.get("razao_social", "")
    fantasia = dados_rf.get("nome_fantasia", "")
    cidade  = dados_rf.get("municipio", "")
    uf      = dados_rf.get("uf", "")
    porte   = dados_rf.get("porte", "")
    cnae    = dados_rf.get("cnae_fiscal_descricao", "")
    socio_lista = dados_rf.get("qsa", [])
    socio_principal = socio_lista[0].get("nome_socio", "") if socio_lista else ""
    abertura = dados_rf.get("data_inicio_atividade", "")

    await asyncio.sleep(0.3)

    # Google Places
    google = await buscar_google_places(session, fantasia or nome, cidade)
    site   = google.get("site_google", "")

    # Contatos do site
    contatos_site = await extrair_contatos_do_site(session, site)

    # Hunter.io como fallback de e-mail
    email = contatos_site.get("email_site", "")
    if not email and site:
        email = await buscar_email_hunter(session, site)

    perfil = {
        "cnpj":            cnpj_limpo,
        "razao_social":    nome,
        "nome_fantasia":   fantasia,
        "porte":           porte,
        "cnae":            cnae,
        "situacao":        "ATIVA",
        "abertura":        abertura,
        "municipio":       cidade,
        "uf":              uf,
        "socio_principal": socio_principal,
        "telefone":        google.get("telefone_google", dados_rf.get("ddd_telefone_1", "")),
        "email":           email,
        "instagram":       contatos_site.get("instagram_site", ""),
        "site":            site,
        "rating_google":   google.get("rating_google", ""),
        "avaliacoes":      google.get("avaliacoes", ""),
        "atualizado_em":   datetime.utcnow().isoformat(),
    }

    db.salvar_empresa(perfil)
    log.info(f"✓ {nome[:50]} | {uf} | tel:{bool(perfil['telefone'])} email:{bool(email)} insta:{bool(perfil['instagram'])}")
    return perfil


# ─── Loop principal do agente ─────────────────────────────────────
async def rodar_agente():
    """Roda o agente em loop infinito, coletando e atualizando CNPJs."""
    db = Database()
    db.criar_tabelas()

    # Lista de CNPJs de demonstração + lógica de descoberta contínua
    cnpjs_seed = carregar_cnpjs_seed()

    log.info(f"Agente iniciado. {len(cnpjs_seed)} CNPJs na fila inicial.")

    async with aiohttp.ClientSession() as session:
        while True:
            processados = 0
            for cnpj in cnpjs_seed[:MAX_CNPJS_POR_CICLO]:
                await enriquecer_empresa(session, cnpj, db)
                processados += 1
                await asyncio.sleep(DELAY_ENTRE_REQUESTS)

            log.info(f"Ciclo completo: {processados} CNPJs processados. Aguardando 10 min...")
            await asyncio.sleep(600)   # espera 10 minutos antes do próximo ciclo


def carregar_cnpjs_seed() -> list:
    """
    Carrega lista de CNPJs para o agente processar.
    Procura o arquivo em vários locais possíveis.
    """
    locais = [
        "cnpjs_seed.txt",          # raiz do projeto (Railway)
        "../cnpjs_seed.txt",       # um nível acima
        "/app/cnpjs_seed.txt",     # Railway volume mount
    ]
    for caminho in locais:
        if os.path.exists(caminho):
            with open(caminho, "r") as f:
                cnpjs = [linha.strip() for linha in f if linha.strip()]
            log.info(f"Carregados {len(cnpjs):,} CNPJs de '{caminho}'")
            return cnpjs

    log.warning("cnpjs_seed.txt não encontrado — usando CNPJs de exemplo")
    return [
        "11222333000181", "22333444000192", "33444555000103",
        "44555666000114", "55666777000125", "66777888000136",
    ]


if __name__ == "__main__":
    asyncio.run(rodar_agente())
