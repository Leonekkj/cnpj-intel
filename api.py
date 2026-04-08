"""
API REST — serve dados do banco + dashboard web.
Rode com: uvicorn api:app --host 0.0.0.0 --port 8000
"""

import os
import subprocess
import sys
from pathlib import Path
from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from database import Database

app = FastAPI(title="CNPJ Intel API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

db = Database()
db.criar_tabelas()

# ─── Autenticação por token ───────────────────────────────────────
# No Railway, defina a variável de ambiente:
# TOKENS=token_cliente1,token_cliente2,token_cliente3
# Para liberar acesso a um novo cliente, adicione o token dele aqui.
TOKENS_VALIDOS = set(
    t.strip() for t in os.environ.get("TOKENS", "demo123,admin456").split(",") if t.strip()
)

security = HTTPBearer(auto_error=False)

def verificar_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if not credentials or credentials.credentials not in TOKENS_VALIDOS:
        raise HTTPException(status_code=401, detail="Token inválido")
    return credentials.credentials


# ─── Endpoints públicos ───────────────────────────────────────────

@app.get("/")
def index():
    path = Path("app/index.html")
    if path.exists():
        return FileResponse(str(path))
    return HTMLResponse("<h2>CNPJ Intel API ✓</h2>")

@app.get("/health")
def health():
    return {"status": "ok"}


# ─── Endpoints protegidos ─────────────────────────────────────────

@app.get("/api/empresas")
def listar_empresas(
    q:             str  = Query(""),
    uf:            str  = Query(""),
    porte:         str  = Query(""),
    com_email:     bool = Query(False),
    com_instagram: bool = Query(False),
    pagina:        int  = Query(1, ge=1),
    por_pagina:    int  = Query(50, le=200),
    token:         str  = Depends(verificar_token),
):
    return db.buscar_empresas(
        q=q, uf=uf, porte=porte,
        com_email=com_email,
        com_instagram=com_instagram,
        pagina=pagina,
        por_pagina=por_pagina,
    )


@app.get("/api/stats")
def estatisticas(token: str = Depends(verificar_token)):
    return db.estatisticas()


@app.get("/api/empresa/{cnpj}")
def detalhe_empresa(cnpj: str, token: str = Depends(verificar_token)):
    result = db.buscar_empresas(q=cnpj, por_pagina=1)
    dados = result.get("dados", [])
    if not dados:
        raise HTTPException(status_code=404, detail="CNPJ não encontrado")
    return dados[0]


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
