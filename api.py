"""
API REST — serve dados do banco + dashboard web.
"""

import asyncio
import os
import re
import subprocess
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
from typing import Annotated
from database import Database
import csv
import io

import logging as _logging
_log_api = _logging.getLogger("api")

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
if not ADMIN_TOKEN:
    import warnings
    warnings.warn("⚠️  ADMIN_TOKEN não configurado!")

db = Database()
_db_ready = asyncio.Event()


def _run_db_fast():
    """Operações mínimas antes de liberar a API. Deve completar em < 1s."""
    db.criar_tabelas()
    db.criar_tabela_tokens()
    db.criar_tabela_listas()
    db.criar_tabela_stats_snapshots()
    # Ensure ADMIN_TOKEN exists in tokens table so FK on listas works
    if ADMIN_TOKEN:
        db.criar_token(ADMIN_TOKEN, "admin")
    for _t in [t.strip() for t in os.environ.get("TOKENS", "").split(",") if t.strip()]:
        if _t != ADMIN_TOKEN:
            db.criar_token(_t, "pro")


def _run_db_migrations():
    """Migrações pesadas — roda em background DEPOIS da API já estar servindo."""
    n = db.migrar_telefones_invalidos()
    if n > 0:
        _log_api.info(f"🧹 {n} telefones inválidos convertidos para NULL")
    n = db.remigrar_departamentos()
    if n > 0:
        _log_api.info(f"🏷️  {n} registros reclassificados (setor + departamento)")
    n = db.migrar_categorias_faltantes()
    if n > 0:
        _log_api.info(f"🏷️  {n} categorias/departamentos preenchidos")
    n = db.migrar_municipios()
    if n > 0:
        _log_api.info(f"🏙️  {n} municípios migrados de código numérico para nome")
    n = db.migrar_cnae()
    if n > 0:
        _log_api.info(f"🏭 {n} CNAEs migrados de código numérico para descrição")
    n = db.limpar_sites_diretorio()
    if n > 0:
        _log_api.info(f"🧹 {n} sites de diretório removidos do banco")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async def _init_db():
        await asyncio.to_thread(_run_db_fast)
        _db_ready.set()
        _log_api.info("DB pronto — API totalmente operacional")
        asyncio.create_task(asyncio.to_thread(_run_db_migrations))

    asyncio.create_task(_init_db())
    yield


app = FastAPI(title="CNPJ Intel API", version="3.0", lifespan=lifespan)

ALLOWED_ORIGINS = [
    o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()
] or ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)


@app.middleware("http")
async def db_readiness_gate(request: Request, call_next):
    if not _db_ready.is_set() and request.url.path not in ("/health", "/"):
        return JSONResponse(status_code=503, content={"detail": "Service starting, DB not ready yet"})
    return await call_next(request)

_agente_proc: subprocess.Popen | None = None

security = HTTPBearer(auto_error=False)


# ─── Auth & planos ─────────────────────────────────────────────────────────────

def get_token_info(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """
    Valida o token e retorna informações do plano.
    Levanta 401 se inválido, 429 se limite diário atingido.
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Token obrigatório")

    token = credentials.credentials

    # Admin token — bypass de tudo
    if ADMIN_TOKEN and token == ADMIN_TOKEN:
        return {
            "token": token,
            "plano": "admin",
            "nome_plano": "Admin",
            "nome": "Admin",
            "cnpjs_hoje": 0,
            "limite_dia": None,
            "restante": None,
            "export": True,
            "api": True,
            "limite_atingido": False,
            "is_admin": True,
        }

    info = db.verificar_token_db(token)
    if not info:
        raise HTTPException(status_code=401, detail="Token inválido ou inativo")

    if info["limite_atingido"]:
        plano = info["nome_plano"]
        raise HTTPException(
            status_code=429,
            detail=f"Limite diário do plano {plano} atingido ({info['limite_dia']} CNPJs/dia). "
                   f"Renova amanhã ou faça upgrade.",
        )

    info["is_admin"] = False
    return info


def get_token_info_soft(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """
    Valida o token mas nunca levanta 429 por limite — retorna info com limite_atingido=True.
    Usar em endpoints de leitura que não consomem quota.
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Token obrigatório")

    token = credentials.credentials

    if ADMIN_TOKEN and token == ADMIN_TOKEN:
        return {
            "token": token,
            "plano": "admin",
            "nome_plano": "Admin",
            "nome": "Admin",
            "cnpjs_hoje": 0,
            "limite_dia": None,
            "restante": None,
            "export": True,
            "api": True,
            "limite_atingido": False,
            "is_admin": True,
        }

    info = db.verificar_token_db(token)
    if not info:
        raise HTTPException(status_code=401, detail="Token inválido ou inativo")

    info["is_admin"] = False
    return info


def require_export(info: dict = Depends(get_token_info)) -> dict:
    """Dependência que exige plano com export liberado."""
    if not info.get("export"):
        raise HTTPException(
            status_code=403,
            detail="Export de CSV não disponível no plano Gratuito. Faça upgrade para Básico ou Pro.",
        )
    return info


def require_admin(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    if not credentials or credentials.credentials != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Acesso restrito ao administrador")
    return credentials.credentials


# ─── Páginas ───────────────────────────────────────────────────────────────────

@app.get("/")
def index():
    path = Path("app/index.html")
    if path.exists():
        return FileResponse(str(path))
    return HTMLResponse("<h2>CNPJ Intel API ✓</h2>")


@app.get("/health")
def health():
    return {"status": "ok"}


# ─── Auth público (signup / login por e-mail) ──────────────────────────────────

class SignupBody(BaseModel):
    email: str
    password: str
    nome: str

class LoginBody(BaseModel):
    email: str
    password: str

@app.post("/api/signup")
def signup(body: SignupBody):
    if len(body.password) < 6:
        raise HTTPException(400, "Senha deve ter ao menos 6 caracteres")
    try:
        token = db.criar_conta_email(body.email, body.password, body.nome)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return {"token": token, "plano": "free"}

@app.post("/api/login")
def login_email(body: LoginBody):
    token = db.login_email(body.email, body.password)
    if not token:
        raise HTTPException(401, "E-mail ou senha incorretos")
    info = db.verificar_token_db(token)
    return {"token": token, "plano": info["plano"], "nome_plano": info["nome_plano"]}


# ─── Plano do usuário ──────────────────────────────────────────────────────────

@app.get("/api/meu-plano")
def meu_plano(info: dict = Depends(get_token_info_soft)):
    """Retorna informações do plano do token atual."""
    return {
        "plano":                   info["plano"],
        "nome_plano":              info["nome_plano"],
        "nome":                    info.get("nome") or "",
        "cnpjs_hoje":              info["cnpjs_hoje"],
        "limite_dia":              info["limite_dia"],
        "restante":                info["restante"],
        "export":                  info["export"],
        "api":                     info["api"],
        "limite_atingido":         info["limite_atingido"],
        "subscription_status":     info.get("subscription_status") or "free",
        "subscription_period_end": info.get("subscription_period_end"),
    }


# ─── Dados ─────────────────────────────────────────────────────────────────────

@app.get("/api/empresas")
def listar_empresas(
    q:             str  = Query(""),
    uf:            str  = Query(""),
    porte:         str  = Query(""),
    cnae:          str  = Query("", description="Filtro livre por descrição CNAE"),
    categoria:     str  = Query("", description="Filtro por macro setor (ex: Saúde e Bem-estar)"),
    departamento:  str  = Query("", description="Filtro por departamento (ex: Odontologia)"),
    abertura_de:   str  = Query("", description="Data de abertura inicial YYYY-MM-DD"),
    abertura_ate:  str  = Query("", description="Data de abertura final YYYY-MM-DD"),
    com_email:     bool = Query(False),
    com_socio:     bool = Query(False),
    com_telefone:  bool = Query(False),
    com_site:      bool = Query(False),
    com_contato:   bool = Query(False, description="Padrão: retorna todos os CNPJs"),
    pagina:        int  = Query(1, ge=1),
    por_pagina:    int  = Query(50, le=200),
    sort_by:       str  = Query("razao_social", description="Campo para ordenar"),
    sort_dir:      str  = Query("asc", description="asc ou desc"),
    info:          dict = Depends(get_token_info_soft),
):
    plano = info["plano"]

    resultado = db.buscar_empresas(
        q=q, uf=uf, porte=porte, cnae=cnae, categoria=categoria, departamento=departamento,
        abertura_de=abertura_de, abertura_ate=abertura_ate,
        com_email=com_email, com_socio=com_socio,
        com_telefone=com_telefone, com_site=com_site,
        com_contato=com_contato,
        pagina=pagina, por_pagina=por_pagina,
        sort_by=sort_by, sort_dir=sort_dir,
    )

    resultado["plano"]    = info["nome_plano"]
    resultado["restante"] = info.get("restante")
    return resultado


# Cache em memória das estatísticas — 5 COUNT(*) full-scan custam caro
import time as _time
_stats_cache = {"data": None, "ts": 0}
_STATS_TTL = 10  # segundos

@app.get("/api/stats")
def estatisticas(info: dict = Depends(get_token_info_soft)):
    agora = _time.time()
    if _stats_cache["data"] and (agora - _stats_cache["ts"]) < _STATS_TTL:
        return _stats_cache["data"]
    data = db.estatisticas()
    db.salvar_snapshot_diario(data["total"], data["com_telefone"], data["com_email"])
    ontem = db.get_snapshot_anterior()
    if ontem:
        data["ontem"] = ontem
    historico = db.get_snapshots_historico(14)
    if historico:
        data["historico"] = historico
    _stats_cache["data"] = data
    _stats_cache["ts"] = agora
    return data


_atividade_cache = {"data": None, "ts": 0}
_ATIVIDADE_TTL = 300  # 5 minutes

@app.get("/api/atividade")
def atividade(info: dict = Depends(get_token_info_soft)):
    agora = _time.time()
    if _atividade_cache["data"] and (agora - _atividade_cache["ts"]) < _ATIVIDADE_TTL:
        return _atividade_cache["data"]
    data = db.atividade_diaria(dias=30)
    _atividade_cache["data"] = data
    _atividade_cache["ts"] = agora
    return data


@app.get("/api/cnaes")
def listar_cnaes(info: dict = Depends(get_token_info_soft)):
    """Retorna os CNAEs mais frequentes para popular o filtro de nicho."""
    return db.listar_cnaes()


@app.get("/api/categorias")
def listar_categorias(info: dict = Depends(get_token_info_soft)):
    """Retorna macro-setores presentes no banco para popular o filtro de setor."""
    return db.listar_categorias()


@app.get("/api/departamentos")
def listar_departamentos(info: dict = Depends(get_token_info_soft)):
    """Retorna hierarquia macro_setor → departamentos com contagem."""
    return db.listar_departamentos()


@app.get("/api/empresa/{cnpj}")
def detalhe_empresa(cnpj: str, info: dict = Depends(get_token_info)):
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    result = db.buscar_empresa_por_cnpj(cnpj_limpo)
    if not result:
        raise HTTPException(status_code=404, detail="CNPJ não encontrado")

    # Quota: só o plano free consome ao abrir detalhe (ver+)
    # Básico consome no export, Pro é ilimitado
    if not info.get("is_admin") and info["plano"] in ("free", "basico"):
        if not db.consumir_quota_atomico(info["token"], 1, info["limite_dia"]):
            raise HTTPException(status_code=429,
                                detail=f"Limite diário do plano {info['nome_plano']} atingido "
                                       f"({info['limite_dia']} CNPJs/dia). Renova amanhã ou faça upgrade.")

    return result


# ─── Export CSV (plano Básico e Pro) ──────────────────────────────────────────

@app.get("/api/export")
def exportar_csv(
    q:             str  = Query(""),
    uf:            str  = Query(""),
    porte:         str  = Query(""),
    cnae:          str  = Query(""),
    categoria:     str  = Query(""),
    departamento:  str  = Query(""),
    abertura_de:   str  = Query(""),
    abertura_ate:  str  = Query(""),
    com_email:     bool = Query(False),
    com_socio:     bool = Query(False),
    com_telefone:  bool = Query(False),
    com_site:      bool = Query(False),
    info:          dict = Depends(require_export),
):
    """Exporta resultados como CSV. Requer plano Básico ou Pro."""
    limite_export = 500 if info["plano"] == "basico" else 5000
    resultado = db.buscar_empresas(
        q=q, uf=uf, porte=porte, cnae=cnae, categoria=categoria, departamento=departamento,
        abertura_de=abertura_de, abertura_ate=abertura_ate,
        com_email=com_email, com_socio=com_socio,
        com_telefone=com_telefone, com_site=com_site,
        pagina=1, por_pagina=limite_export,
    )

    dados = resultado.get("dados", [])
    if not dados:
        raise HTTPException(status_code=404, detail="Nenhum resultado para exportar")

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=dados[0].keys())
    writer.writeheader()
    writer.writerows(dados)
    output.seek(0)

    from datetime import date
    filename = f"cnpj_intel_{date.today()}.csv"
    return StreamingResponse(
        iter(["\ufeff" + output.getvalue()]),  # BOM para Excel
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ─── Listas ────────────────────────────────────────────────────────────────────

class CriarListaBody(BaseModel):
    nome: str = Field(..., min_length=1, max_length=120)

class RenomearListaBody(BaseModel):
    nome: str = Field(..., min_length=1, max_length=120)

class AdicionarItensBody(BaseModel):
    cnpjs: list[Annotated[str, Field(max_length=14)]] = Field(..., max_length=500)


@app.get("/api/listas")
def get_listas(token_info=Depends(get_token_info_soft)):
    token = token_info["token"]
    return db.listar_listas(token)


@app.post("/api/listas", status_code=201)
def post_criar_lista(body: CriarListaBody, token_info=Depends(get_token_info_soft)):
    nome = body.nome.strip()
    if not nome:
        raise HTTPException(status_code=400, detail="Nome não pode ser vazio")
    token = token_info["token"]
    try:
        return db.criar_lista(token, nome)
    except Exception as e:
        _log_api.error(f"criar_lista error [{type(e).__name__}]: {e}")
        if "unique" in str(e).lower():
            raise HTTPException(status_code=409, detail="Já existe uma lista com esse nome")
        raise HTTPException(status_code=500, detail=f"Erro ao criar lista: {type(e).__name__}: {str(e)[:300]}")


@app.get("/api/listas/{lista_id}")
def get_lista(lista_id: int, token_info=Depends(get_token_info_soft)):
    token = token_info["token"]
    lista = db.obter_lista(token, lista_id)
    if not lista:
        raise HTTPException(status_code=404, detail="Lista não encontrada")
    return lista


@app.put("/api/listas/{lista_id}")
def put_renomear_lista(lista_id: int, body: RenomearListaBody, token_info=Depends(get_token_info_soft)):
    nome = body.nome.strip()
    if not nome:
        raise HTTPException(status_code=400, detail="Nome não pode ser vazio")
    token = token_info["token"]
    try:
        ok = db.renomear_lista(token, lista_id, nome)
    except Exception as e:
        if "unique" in str(e).lower():
            raise HTTPException(status_code=409, detail="Já existe uma lista com esse nome")
        raise
    if not ok:
        raise HTTPException(status_code=404, detail="Lista não encontrada")
    return {"ok": True}


@app.delete("/api/listas/{lista_id}")
def delete_lista(lista_id: int, token_info=Depends(get_token_info_soft)):
    token = token_info["token"]
    ok = db.deletar_lista(token, lista_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Lista não encontrada")
    return {"ok": True}


@app.post("/api/listas/{lista_id}/itens")
def post_adicionar_itens(lista_id: int, body: AdicionarItensBody, token_info=Depends(get_token_info_soft)):
    token = token_info["token"]
    if not body.cnpjs:
        raise HTTPException(status_code=400, detail="Lista de CNPJs vazia")
    added = db.adicionar_itens_lista(token, lista_id, body.cnpjs)
    if added == 0 and not db.obter_lista(token, lista_id):
        raise HTTPException(status_code=404, detail="Lista não encontrada")
    return {"adicionados": added}


@app.delete("/api/listas/{lista_id}/itens/{cnpj}")
def delete_item_lista(lista_id: int, cnpj: str, token_info=Depends(get_token_info_soft)):
    token = token_info["token"]
    ok = db.remover_item_lista(token, lista_id, cnpj)
    if not ok:
        raise HTTPException(status_code=404, detail="Item não encontrado")
    return {"ok": True}


@app.get("/api/listas/{lista_id}/export")
def export_lista(lista_id: int, token_info=Depends(get_token_info_soft)):
    token = token_info["token"]
    plano = token_info.get("plano", "free")
    if plano not in ("basico", "pro", "admin"):
        raise HTTPException(status_code=403, detail="Plano basico ou superior necessário para exportar")

    lista = db.obter_lista(token, lista_id)
    if not lista:
        raise HTTPException(status_code=404, detail="Lista não encontrada")

    output = io.StringIO()
    if lista["itens"]:
        writer = csv.DictWriter(output, fieldnames=lista["itens"][0].keys())
        writer.writeheader()
        writer.writerows(lista["itens"])
    content = "﻿" + output.getvalue()  # UTF-8 BOM for Excel
    nome_safe = re.sub(r'[^\w\-]', '_', lista["nome"])
    return Response(
        content=content.encode("utf-8"),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="lista_{nome_safe}.csv"'}
    )


# ─── Admin ─────────────────────────────────────────────────────────────────────

@app.post("/api/admin/tokens")
def criar_token(
    token: str = Query(..., description="O token a ser criado"),
    plano: str = Query("free", description="free | basico | pro"),
    _: str = Depends(require_admin),
):
    """Cria um novo token de acesso para um cliente."""
    if plano not in ("free", "basico", "pro"):
        raise HTTPException(status_code=400, detail="Plano inválido. Use: free, basico, pro")
    return db.criar_token(token, plano)


@app.get("/api/admin/tokens")
def listar_tokens(_: str = Depends(require_admin)):
    """Lista todos os tokens e uso atual."""
    return db.listar_tokens()


@app.delete("/api/admin/tokens/{token}")
def excluir_token(token: str, _: str = Depends(require_admin)):
    """Exclui permanentemente um token de acesso."""
    db.excluir_token(token)
    return {"status": "excluido", "token": token}


@app.post("/api/admin/reset-database")
def reset_database(_: str = Depends(require_admin)):
    """Apaga todos os dados de empresas e zera o progresso do agente."""
    db.reset_completo()
    _stats_cache["data"] = None
    return {"status": "ok", "mensagem": "Base zerada com sucesso"}


@app.post("/api/admin/agente")
def iniciar_agente(_: str = Depends(require_admin)):
    global _agente_proc
    if _agente_proc is not None and _agente_proc.poll() is None:
        return {"status": "Agente já está rodando", "pid": _agente_proc.pid}
    _agente_proc = subprocess.Popen([sys.executable, "agent/agent.py"])
    return {"status": "Agente iniciado", "pid": _agente_proc.pid}


@app.post("/api/admin/limpar-sites")
def limpar_sites(_: str = Depends(require_admin)):
    """Remove URLs de diretórios/listagens salvas erroneamente no campo 'site'."""
    total = db.limpar_sites_falsos()
    # Invalida cache de stats
    _stats_cache["data"] = None
    return {"status": "ok", "registros_limpos": total}


@app.get("/api/admin/diagnostico-telefone")
def diagnostico_telefone(_: str = Depends(require_admin)):
    """Diagnóstico: verifica se telefones estão sendo salvos no banco."""
    return db.diagnostico_telefone()


@app.post("/api/admin/corrigir-mei")
async def corrigir_mei(payload: dict, _: str = Depends(require_admin)):
    """Atualiza porte para 'MEI' nos CNPJs informados (para corrigir dados históricos)."""
    cnpjs = payload.get("cnpjs", [])
    if not isinstance(cnpjs, list):
        raise HTTPException(status_code=400, detail="cnpjs deve ser uma lista")
    updated = db.corrigir_porte_mei(cnpjs)
    _stats_cache["data"] = None
    return {"status": "ok", "atualizados": updated}


@app.post("/api/admin/corrigir-mei-auto")
async def corrigir_mei_auto(_: str = Depends(require_admin)):
    """Verifica via BrasilAPI quais empresas com porte vazio são MEI
    (natureza_juridica contém 'MEI') e corrige o porte para 'MEI' no banco.
    MEI tem porte code '00' no RF (não informado), por isso fica como '' no banco."""
    import aiohttp

    candidatos = db.listar_cnpjs_por_porte("", limite=10000)
    if not candidatos:
        return {"status": "ok", "total_verificados": 0, "total_corrigidos": 0}

    sem = asyncio.Semaphore(5)
    mei_cnpjs: list[str] = []

    async def verificar(session: aiohttp.ClientSession, cnpj: str):
        url = f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}"
        async with sem:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    if r.status == 200:
                        dados = await r.json()
                        nj = str(dados.get("natureza_juridica", "")).upper()
                        if "MEI" in nj or "MICROEMPREENDEDOR" in nj:
                            mei_cnpjs.append(cnpj)
            except Exception:
                pass

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[verificar(session, c) for c in candidatos])

    corrigidos = db.corrigir_porte_mei(mei_cnpjs) if mei_cnpjs else 0
    _stats_cache["data"] = None
    return {"status": "ok", "total_verificados": len(candidatos), "total_corrigidos": corrigidos}


@app.post("/api/admin/vacuum")
def vacuum_banco(_: str = Depends(require_admin)):
    """Executa VACUUM ANALYZE no Postgres para liberar espaço após DELETE em massa."""
    db.vacuum()
    return {"status": "ok", "mensagem": "VACUUM ANALYZE executado"}


# ─── Kiwify / Pagamento ────────────────────────────────────────────────────────
# Kiwify não exige CNPJ — funciona com CPF.
# Env vars necessárias:
#   KIWIFY_CHECKOUT_BASICO  — ex: https://pay.kiwify.com.br/xxxxxxx
#   KIWIFY_CHECKOUT_PRO     — ex: https://pay.kiwify.com.br/yyyyyyy
#   KIWIFY_WEBHOOK_TOKEN    — token secreto configurado no painel Kiwify
KIWIFY_CHECKOUT_BASICO  = os.environ.get("KIWIFY_CHECKOUT_BASICO", "")
KIWIFY_CHECKOUT_PRO     = os.environ.get("KIWIFY_CHECKOUT_PRO", "")
KIWIFY_WEBHOOK_TOKEN    = os.environ.get("KIWIFY_WEBHOOK_TOKEN", "")

_KIWIFY_CHECKOUT_MAP = {
    "basico": KIWIFY_CHECKOUT_BASICO,
    "pro":    KIWIFY_CHECKOUT_PRO,
}


@app.get("/api/checkout-url")
def checkout_url(plano: str = Query(..., pattern="^(basico|pro)$"),
                 info: dict = Depends(get_token_info_soft)):
    """Retorna URL do checkout Kiwify com e-mail do usuário pré-preenchido."""
    base_url = _KIWIFY_CHECKOUT_MAP.get(plano, "")
    if not base_url:
        raise HTTPException(503, "Link de pagamento não configurado. Entre em contato com o suporte.")

    email = info.get("email", "")
    # Kiwify aceita ?email= na query string para pré-preencher o campo
    url = f"{base_url}?email={email}" if email else base_url
    return {"checkout_url": url}


@app.post("/api/webhook/kiwify")
async def webhook_kiwify(request: Request):
    """Recebe eventos da Kiwify e atualiza plano do usuário.

    Eventos tratados:
      PURCHASE_APPROVED  → ativa plano basico ou pro
      PURCHASE_CANCELED  → reverte para free
      PURCHASE_REFUNDED  → reverte para free
      SUBSCRIPTION_CANCELED → reverte para free
    """
    import json

    body_bytes = await request.body()
    try:
        payload = json.loads(body_bytes)
    except Exception:
        raise HTTPException(400, "Payload inválido")

    # Verificação do token secreto (configurado no painel Kiwify → Webhooks)
    if KIWIFY_WEBHOOK_TOKEN and payload.get("token") != KIWIFY_WEBHOOK_TOKEN:
        raise HTTPException(401, "Token inválido")

    event   = payload.get("event", "")
    data    = payload.get("data", {})
    # Kiwify envia o e-mail em data.customer.email
    customer = data.get("customer", {})
    email    = customer.get("email", "").lower().strip()

    if not email:
        return {"status": "ignored", "reason": "no email"}

    # Diferencia Básico/Pro pelo preço pago (em reais)
    amount = float(data.get("purchase", {}).get("price", {}).get("value", 0) or 0)
    plano = "pro" if amount >= 90 else "basico"

    subscription_id = data.get("subscription", {}).get("id", "")
    period_end      = data.get("subscription", {}).get("next_payment", "") or ""

    if event == "PURCHASE_APPROVED":
        db.atualizar_plano_pagarme(email, plano, subscription_id, "active", period_end)
        _stats_cache["data"] = None

    elif event in ("PURCHASE_CANCELED", "PURCHASE_REFUNDED", "SUBSCRIPTION_CANCELED"):
        db.atualizar_plano_pagarme(email, "free", subscription_id, "canceled", "")
        _stats_cache["data"] = None

    return {"status": "ok"}


@app.get("/api/billing-portal")
def billing_portal(info: dict = Depends(get_token_info_soft)):
    """Retorna URL do portal Kiwify para o cliente gerenciar a assinatura."""
    if info.get("plano") == "free":
        raise HTTPException(404, "Nenhuma assinatura ativa encontrada.")
    # Kiwify não tem portal via API — o cliente acessa diretamente pelo e-mail
    return {"portal_url": "https://kiwify.com.br/customer"}


# ─── Stats públicos (sem auth — para landing page) ────────────────────────────

@app.get("/api/public-stats")
def public_stats():
    """Estatísticas públicas para a landing page — sem autenticação."""
    stats = db.estatisticas()
    return {
        "total_empresas":     stats.get("total", 0),
        "total_com_telefone": stats.get("com_telefone", 0),
        "total_com_email":    stats.get("com_email", 0),
    }


app.mount("/app", StaticFiles(directory="app"), name="app_static")


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)