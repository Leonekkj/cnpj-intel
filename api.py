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
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse, JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
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


def _run_db_init():
    db.criar_tabelas()
    db.criar_tabela_tokens()
    n = db.migrar_telefones_invalidos()
    if n > 0:
        _log_api.info(f"🧹 {n} telefones inválidos convertidos para NULL")
    n = db.migrar_categorias_faltantes()
    if n > 0:
        _log_api.info(f"🏷️  {n} categorias recomputadas")
    n = db.migrar_municipios()
    if n > 0:
        _log_api.info(f"🏙️  {n} municípios migrados de código IBGE para nome")
    n = db.limpar_sites_diretorio()
    if n > 0:
        _log_api.info(f"🧹 {n} sites de diretório removidos do banco")
    for _t in [t.strip() for t in os.environ.get("TOKENS", "").split(",") if t.strip()]:
        if _t != ADMIN_TOKEN:
            db.criar_token(_t, "pro")


@asynccontextmanager
async def lifespan(app: FastAPI):
    async def _init_db():
        await asyncio.to_thread(_run_db_init)
        _db_ready.set()
        _log_api.info("DB pronto — API totalmente operacional")

    asyncio.create_task(_init_db())
    yield


app = FastAPI(title="CNPJ Intel API", version="3.0", lifespan=lifespan)

ALLOWED_ORIGINS = [
    o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()
] or ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "DELETE"],
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


# ─── Plano do usuário ──────────────────────────────────────────────────────────

@app.get("/api/meu-plano")
def meu_plano(info: dict = Depends(get_token_info)):
    """Retorna informações do plano do token atual."""
    return {
        "plano":      info["plano"],
        "nome_plano": info["nome_plano"],
        "cnpjs_hoje": info["cnpjs_hoje"],
        "limite_dia": info["limite_dia"],
        "restante":   info["restante"],
        "export":     info["export"],
        "api":        info["api"],
    }


# ─── Dados ─────────────────────────────────────────────────────────────────────

@app.get("/api/empresas")
def listar_empresas(
    q:             str  = Query(""),
    uf:            str  = Query(""),
    porte:         str  = Query(""),
    cnae:          str  = Query("", description="Filtro livre por descrição CNAE"),
    categoria:     str  = Query("", description="Filtro por categoria padronizada (ex: Advocacia, Saúde)"),
    abertura_de:   str  = Query("", description="Data de abertura inicial YYYY-MM-DD"),
    abertura_ate:  str  = Query("", description="Data de abertura final YYYY-MM-DD"),
    com_email:     bool = Query(False),
    com_instagram: bool = Query(False),
    com_telefone:  bool = Query(False),
    com_site:      bool = Query(False),
    com_contato:   bool = Query(False, description="Padrão: retorna todos os CNPJs"),
    pagina:        int  = Query(1, ge=1),
    por_pagina:    int  = Query(50, le=200),
    info:          dict = Depends(get_token_info),
):
    plano = info["plano"]

    # Free: limita por_pagina ao restante; quota consumida no ver+
    if plano == "free":
        restante = info.get("restante", 0)
        if info.get("limite_dia") is not None:
            por_pagina = min(por_pagina, restante) if restante > 0 else 0
            if por_pagina == 0:
                return {"total": 0, "pagina": pagina, "por_pagina": 0, "dados": [],
                        "plano": info["nome_plano"], "restante": 0}

    # Básico: limita por_pagina ao restante e consome quota na listagem
    if plano == "basico":
        restante = info.get("restante", 0)
        if info.get("limite_dia") is not None:
            por_pagina = min(por_pagina, restante) if restante > 0 else 0
            if por_pagina == 0:
                return {"total": 0, "pagina": pagina, "por_pagina": 0, "dados": [],
                        "plano": info["nome_plano"], "restante": 0}

    resultado = db.buscar_empresas(
        q=q, uf=uf, porte=porte, cnae=cnae, categoria=categoria,
        abertura_de=abertura_de, abertura_ate=abertura_ate,
        com_email=com_email, com_instagram=com_instagram,
        com_telefone=com_telefone, com_site=com_site,
        com_contato=com_contato,
        pagina=pagina, por_pagina=por_pagina,
    )

    # Básico: consome quota com base nas linhas retornadas (atômico — sem race)
    retornados = len(resultado.get("dados", []))
    if plano == "basico" and retornados > 0 and not info.get("is_admin"):
        if not db.consumir_quota_atomico(info["token"], retornados, info["limite_dia"]):
            raise HTTPException(status_code=429,
                                detail=f"Limite diário do plano {info['nome_plano']} atingido "
                                       f"({info['limite_dia']} CNPJs/dia). Renova amanhã ou faça upgrade.")

    # Free: oculta contatos na listagem (revelados via ver+)
    if plano == "free":
        for emp in resultado.get("dados", []):
            for campo in ("telefone", "email", "instagram", "site"):
                emp[campo] = ""

    resultado["plano"]    = info["nome_plano"]
    resultado["restante"] = info.get("restante")
    return resultado


# Cache em memória das estatísticas — 5 COUNT(*) full-scan custam caro
import time as _time
_stats_cache = {"data": None, "ts": 0}
_STATS_TTL = 60  # segundos

@app.get("/api/stats")
def estatisticas(info: dict = Depends(get_token_info)):
    agora = _time.time()
    if _stats_cache["data"] and (agora - _stats_cache["ts"]) < _STATS_TTL:
        return _stats_cache["data"]
    data = db.estatisticas()
    _stats_cache["data"] = data
    _stats_cache["ts"] = agora
    return data


@app.get("/api/cnaes")
def listar_cnaes(info: dict = Depends(get_token_info)):
    """Retorna os CNAEs mais frequentes para popular o filtro de nicho."""
    return db.listar_cnaes()


@app.get("/api/categorias")
def listar_categorias(info: dict = Depends(get_token_info)):
    """Retorna macro-setores presentes no banco para popular o filtro de setor."""
    return db.listar_categorias()


@app.get("/api/empresa/{cnpj}")
def detalhe_empresa(cnpj: str, info: dict = Depends(get_token_info)):
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    result = db.buscar_empresa_por_cnpj(cnpj_limpo)
    if not result:
        raise HTTPException(status_code=404, detail="CNPJ não encontrado")

    # Quota: só o plano free consome ao abrir detalhe (ver+)
    # Básico consome no export, Pro é ilimitado
    if not info.get("is_admin") and info["plano"] == "free":
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
    abertura_de:   str  = Query(""),
    abertura_ate:  str  = Query(""),
    com_email:     bool = Query(False),
    com_instagram: bool = Query(False),
    com_telefone:  bool = Query(False),
    com_site:      bool = Query(False),
    info:          dict = Depends(require_export),
):
    """Exporta resultados como CSV. Requer plano Básico ou Pro."""
    limite_export = 500 if info["plano"] == "basico" else 5000
    resultado = db.buscar_empresas(
        q=q, uf=uf, porte=porte, cnae=cnae, categoria=categoria,
        abertura_de=abertura_de, abertura_ate=abertura_ate,
        com_email=com_email, com_instagram=com_instagram,
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


@app.post("/api/admin/vacuum")
def vacuum_banco(_: str = Depends(require_admin)):
    """Executa VACUUM ANALYZE no Postgres para liberar espaço após DELETE em massa."""
    db.vacuum()
    return {"status": "ok", "mensagem": "VACUUM ANALYZE executado"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)