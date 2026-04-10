"""
CNPJ Intel Agent — coleta e enriquece dados de empresas automaticamente.
"""

import asyncio
import aiohttp
import re
import logging
import os
import sys

# Adiciona o diretório pai ao path para encontrar database.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Database
from datetime import datetime
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [AGENTE] %(message)s")
log = logging.getLogger(__name__)

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
HUNTER_API_KEY = os.environ.get("HUNTER_API_KEY", "")

DELAY_ENTRE_REQUESTS = 0.5   # segundos entre CNPJs
MAX_CNPJS_POR_CICLO  = 2000  # CNPJs por ciclo
PAUSA_ENTRE_CICLOS   = 60    # segundos entre ciclos (1 minuto)
TIMEOUT_REQUEST      = 8     # timeout por request


async def buscar_brasilapi(session: aiohttp.ClientSession, cnpj: str) -> Optional[dict]:
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT_REQUEST)) as r:
            if r.status == 200:
                return await r.json()
            elif r.status == 429:
                log.warning("Rate limit — aguardando 60s...")
                await asyncio.sleep(60)
            elif r.status == 404:
                pass  # CNPJ não encontrado, normal
    except asyncio.TimeoutError:
        pass  # timeout, pula esse CNPJ
    except Exception as e:
        log.debug(f"Erro CNPJ {cnpj}: {e}")
    return None


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
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=6)) as r:
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


async def extrair_contatos_do_site(session, url):
    if not url:
        return {}
    if not url.startswith("http"):
        url = "https://" + url
    resultado = {"email_site": "", "instagram_site": ""}
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=6), allow_redirects=True) as r:
            if r.status == 200:
                html = await r.text(errors="ignore")
                emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", html)
                emails = [e for e in emails if not any(x in e for x in ["sentry", "example", "noreply", "wix.com"])]
                if emails:
                    resultado["email_site"] = emails[0]
                insta = re.findall(r'instagram\.com/([A-Za-z0-9_.]{2,30})', html)
                insta = [i for i in insta if i not in ("p", "tv", "reel", "stories", "explore", "accounts")]
                if insta:
                    resultado["instagram_site"] = "@" + insta[0]
    except Exception:
        pass
    return resultado


async def enriquecer_empresa(session, cnpj, db):
    cnpj = re.sub(r"\D", "", cnpj)
    try:
        if db.cnpj_existe_recente(cnpj, dias=30):
            return None

        dados = await buscar_brasilapi(session, cnpj)
        if not dados:
            return None

        if dados.get("descricao_situacao_cadastral", "").upper() != "ATIVA":
            return None

        nome     = dados.get("razao_social", "")
        fantasia = dados.get("nome_fantasia", "")
        cidade   = dados.get("municipio", "")
        socios   = dados.get("qsa", [])
        socio    = socios[0].get("nome_socio", "") if socios else ""

        google = await buscar_google_places(session, fantasia or nome, cidade)
        site   = google.get("site_google", "")
        contatos = await extrair_contatos_do_site(session, site)

        perfil = {
            "cnpj":            cnpj,
            "razao_social":    nome,
            "nome_fantasia":   fantasia,
            "porte":           dados.get("porte", ""),
            "cnae":            dados.get("cnae_fiscal_descricao", ""),
            "situacao":        "ATIVA",
            "abertura":        dados.get("data_inicio_atividade", ""),
            "municipio":       cidade,
            "uf":              dados.get("uf", ""),
            "socio_principal": socio,
            "telefone":        google.get("telefone_google", dados.get("ddd_telefone_1", "")),
            "email":           contatos.get("email_site", ""),
            "instagram":       contatos.get("instagram_site", ""),
            "site":            site,
            "rating_google":   google.get("rating_google", ""),
            "avaliacoes":      google.get("avaliacoes", ""),
            "atualizado_em":   datetime.utcnow().isoformat(),
        }

        db.salvar_empresa(perfil)
        log.info(f"✓ {nome[:40]} | {dados.get('uf','')} | tel:{bool(perfil['telefone'])} email:{bool(perfil['email'])}")
        return perfil

    except Exception as e:
        log.debug(f"Erro ao enriquecer {cnpj}: {e}")
        return None


async def rodar_agente():
    db = Database()
    db.criar_tabelas()

    cnpjs = carregar_cnpjs_seed()
    total = len(cnpjs)
    log.info(f"Agente iniciado. {total:,} CNPJs na fila.")

    processados_total = 0

    async with aiohttp.ClientSession() as session:
        while True:
            inicio = processados_total
            fim    = min(processados_total + MAX_CNPJS_POR_CICLO, total)
            lote   = cnpjs[inicio:fim]

            if not lote:
                log.info("Todos os CNPJs processados! Reiniciando do zero...")
                processados_total = 0
                await asyncio.sleep(PAUSA_ENTRE_CICLOS)
                continue

            log.info(f"Ciclo: CNPJs {inicio:,} a {fim:,} de {total:,}")
            salvos = 0

            for cnpj in lote:
                resultado = await enriquecer_empresa(session, cnpj, db)
                if resultado:
                    salvos += 1
                await asyncio.sleep(DELAY_ENTRE_REQUESTS)

            processados_total = fim
            log.info(f"Ciclo completo. {salvos} novos salvos. Total processado: {processados_total:,}/{total:,}")
            await asyncio.sleep(PAUSA_ENTRE_CICLOS)


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
