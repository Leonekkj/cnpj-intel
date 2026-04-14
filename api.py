"""
API REST — serve dados do banco + dashboard web.
"""

import os
import re
import subprocess
import sys
from pathlib import Path
from fastapi import FastAPI, Query, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database import Database
import csv
import io

app = FastAPI(title="CNPJ Intel API", version="3.0")

ALLOWED_ORIGINS = [
    o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()
] or ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
if not ADMIN_TOKEN:
    import warnings
    warnings.warn("⚠️  ADMIN_TOKEN não configurado!")

db = Database()
db.criar_tabelas()
db.criar_tabela_tokens()

# Migra tokens da variável de ambiente TOKENS para o banco como plano "pro"
# (compatibilidade com tokens já em uso)
_tokens_legados = os.environ.get("TOKENS", "")
for _t in [t.strip() for t in _tokens_legados.split(",") if t.strip()]:
    if _t != ADMIN_TOKEN:
        db.criar_token(_t, "pro")  # tokens antigos viram pro por padrão

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
    cnae:          str  = Query("", description="Filtro por CNAE ou palavra-chave do setor"),
    abertura_de:   str  = Query("", description="Data de abertura inicial YYYY-MM-DD"),
    abertura_ate:  str  = Query("", description="Data de abertura final YYYY-MM-DD"),
    com_email:     bool = Query(False),
    com_instagram: bool = Query(False),
    com_telefone:  bool = Query(False),
    com_site:      bool = Query(False),
    com_contato:   bool = Query(True,  description="Padrão: só empresas com pelo menos 1 contato"),
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
        q=q, uf=uf, porte=porte, cnae=cnae,
        abertura_de=abertura_de, abertura_ate=abertura_ate,
        com_email=com_email, com_instagram=com_instagram,
        com_telefone=com_telefone, com_site=com_site,
        com_contato=com_contato,
        pagina=pagina, por_pagina=por_pagina,
    )

    # Básico: consome quota com base nas linhas retornadas
    retornados = len(resultado.get("dados", []))
    if plano == "basico" and retornados > 0 and not info.get("is_admin"):
        db.consumir_quota(info["token"], retornados)

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


@app.get("/api/empresa/{cnpj}")
def detalhe_empresa(cnpj: str, info: dict = Depends(get_token_info)):
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    result = db.buscar_empresa_por_cnpj(cnpj_limpo)
    if not result:
        raise HTTPException(status_code=404, detail="CNPJ não encontrado")

    # Quota: só o plano free consome ao abrir detalhe (ver+)
    # Básico consome no export, Pro é ilimitado
    if not info.get("is_admin") and info["plano"] == "free":
        db.consumir_quota(info["token"], 1)

    return result


# ─── Export CSV (plano Básico e Pro) ──────────────────────────────────────────

@app.get("/api/export")
def exportar_csv(
    q:             str  = Query(""),
    uf:            str  = Query(""),
    porte:         str  = Query(""),
    cnae:          str  = Query(""),
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
        q=q, uf=uf, porte=porte, cnae=cnae,
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


@app.post("/api/admin/agente")
def iniciar_agente(_: str = Depends(require_admin)):
    subprocess.Popen([sys.executable, "agent/agent.py"])
    return {"status": "Agente iniciado"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)