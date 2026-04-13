"""
API REST — serve dados do banco + dashboard web.
"""

import os
import re
import subprocess
import sys
from pathlib import Path
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database import Database

app = FastAPI(title="CNPJ Intel API", version="2.0")

ALLOWED_ORIGINS = [
    o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()
] or ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST"],
    allow_headers=["Authorization", "Content-Type"],
)

db = Database()
db.criar_tabelas()

_tokens_env = os.environ.get("TOKENS", "")
if not _tokens_env:
    import warnings
    warnings.warn("⚠️  Variável de ambiente TOKENS não configurada! Use tokens seguros em produção.")
TOKENS_VALIDOS = set(t.strip() for t in _tokens_env.split(",") if t.strip()) or {"demo123"}

security = HTTPBearer(auto_error=False)

def verificar_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials or credentials.credentials not in TOKENS_VALIDOS:
        raise HTTPException(status_code=401, detail="Token inválido")
    return credentials.credentials


@app.get("/")
def index():
    path = Path("app/index.html")
    if path.exists():
        return FileResponse(str(path))
    return HTMLResponse("<h2>CNPJ Intel API ✓</h2>")

@app.get("/health")
def health():
    return {"status": "ok"}


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
    pagina:        int  = Query(1, ge=1),
    por_pagina:    int  = Query(50, le=200),
    token:         str  = Depends(verificar_token),
):
    return db.buscar_empresas(
        q=q, uf=uf, porte=porte, cnae=cnae,
        abertura_de=abertura_de, abertura_ate=abertura_ate,
        com_email=com_email, com_instagram=com_instagram,
        com_telefone=com_telefone, com_site=com_site,
        pagina=pagina, por_pagina=por_pagina,
    )


@app.get("/api/stats")
def estatisticas(token: str = Depends(verificar_token)):
    return db.estatisticas()


@app.get("/api/cnaes")
def listar_cnaes(token: str = Depends(verificar_token)):
    """Retorna os CNAEs mais frequentes para popular o filtro de nicho."""
    return db.listar_cnaes()


@app.get("/api/empresa/{cnpj}")
def detalhe_empresa(cnpj: str, token: str = Depends(verificar_token)):
    cnpj_limpo = re.sub(r"\D", "", cnpj)
    result = db.buscar_empresa_por_cnpj(cnpj_limpo)
    if not result:
        raise HTTPException(status_code=404, detail="CNPJ não encontrado")
    return result


@app.post("/api/admin/agente")
def iniciar_agente(token: str = Depends(verificar_token)):
    if token != os.environ.get("ADMIN_TOKEN", "admin456"):
        raise HTTPException(status_code=403, detail="Apenas admins")
    subprocess.Popen([sys.executable, "agent/agent.py"])
    return {"status": "Agente iniciado"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
