"""
CNPJ Intel Agent — com persistência de progresso no PostgreSQL.
Ao reiniciar, retoma de onde parou sem reprocessar CNPJs já salvos.
"""

import asyncio
import aiohttp
import re
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import Database
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [AGENTE] %(message)s")
log = logging.getLogger(__name__)

GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY", "")
DELAY          = 0.5
LOTE           = 2000
PAUSA_CICLO    = 30
TIMEOUT_CNPJ   = 15
SALVAR_A_CADA  = 100


async def buscar_brasilapi(session, cnpj):
    url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status == 200:
                return await r.json()
            elif r.status == 429:
                log.warning("Rate limit — aguardando 60s...")
                await asyncio.sleep(60)
    except Exception:
        pass
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


async def extrair_contatos_do_site(session, url):
    if not url:
        return {}
    if not url.startswith("http"):
        url = "https://" + url
    resultado = {"email_site": "", "instagram_site": ""}
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        async with session.get(url, headers=headers,
                               timeout=aiohttp.ClientTimeout(total=5),
                               allow_redirects=True) as r:
            if r.status == 200:
                html = await r.text(errors="ignore")
                emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", html)
                emails = [e for e in emails if not any(x in e for x in
                          ["sentry", "example", "noreply", "wix.com", "pixel", "schema"])]
                if emails:
                    resultado["email_site"] = emails[0]
                insta = re.findall(r'instagram\.com/([A-Za-z0-9_.]{2,30})', html)
                insta = [i for i in insta if i not in
                         ("p","tv","reel","stories","explore","accounts","sharer")]
                if insta:
                    resultado["instagram_site"] = "@" + insta[0]
    except Exception:
        pass
    return resultado


async def _processar(session, cnpj, db):
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

        google   = await buscar_google_places(session, fantasia or nome, cidade)
        site     = google.get("site_google", "")
        contatos = await extrair_contatos_do_site(session, site)

        ddd = dados.get("ddd_telefone_1", "")
        num = dados.get("telefone_1", "")
        tel_receita = f"({ddd}) {num}" if ddd and num else ""

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
            "telefone":        google.get("telefone_google", "") or tel_receita,
            "email":           contatos.get("email_site", "") or dados.get("email", "") or "",
            "instagram":       contatos.get("instagram_site", ""),
            "site":            site,
            "rating_google":   str(google.get("rating_google", "")),
            "avaliacoes":      str(google.get("avaliacoes", "")),
            "atualizado_em":   datetime.utcnow().isoformat(),
        }

        db.salvar_empresa(perfil)
        log.info(f"✓ {nome[:40]} | {dados.get('uf','')} | "
                 f"tel:{bool(perfil['telefone'])} "
                 f"email:{bool(perfil['email'])}")
        return perfil

    except Exception as e:
        log.debug(f"Erro {cnpj}: {e}")
        return None


async def enriquecer(session, cnpj, db):
    try:
        return await asyncio.wait_for(_processar(session, cnpj, db), timeout=TIMEOUT_CNPJ)
    except asyncio.TimeoutError:
        return None
    except Exception:
        return None


async def rodar_agente():
    db = Database()
    db.criar_tabelas()
    db.criar_tabela_progresso()

    cnpjs = carregar_cnpjs_seed()
    total = len(cnpjs)

    # Retoma de onde parou
    offset = db.carregar_progresso()
    if offset > 0 and offset < total:
        log.info(f"🔄 Retomando do CNPJ {offset:,} de {total:,}")
    elif offset >= total:
        log.info("✅ Todos processados. Reiniciando do zero.")
        offset = 0
        db.salvar_progresso(0)
    else:
        log.info(f"🆕 Iniciando do zero. {total:,} CNPJs na fila.")

    async with aiohttp.ClientSession() as session:
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

                log.info(f"Ciclo: {inicio:,} → {fim:,} de {total:,} ({100*inicio//total}%)")
                salvos = 0

                for i, cnpj in enumerate(lote):
                    try:
                        r = await enriquecer(session, cnpj, db)
                        if r:
                            salvos += 1
                        await asyncio.sleep(DELAY)

                        if (i + 1) % SALVAR_A_CADA == 0:
                            db.salvar_progresso(inicio + i + 1)

                    except Exception as e:
                        log.debug(f"Erro no loop: {e}")
                        continue

                offset = fim
                db.salvar_progresso(offset)
                log.info(f"Ciclo completo. {salvos} salvos. Posição: {offset:,}/{total:,}")
                await asyncio.sleep(PAUSA_CICLO)

            except Exception as e:
                log.error(f"Erro crítico: {e} — reiniciando em 30s")
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
