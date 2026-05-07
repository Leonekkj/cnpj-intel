"""
start.py — inicia a API e o agente ao mesmo tempo no Railway.
"""
import subprocess
import sys
import os
import time

print("Iniciando CNPJ Intel...")

# ── Seed download (opcional) ─────────────────────────────────────────────────
# Se SEED_URL estiver definido e o seed local não existir (ou for um ponteiro
# Git LFS), baixa o arquivo real antes de iniciar o agente.
_SEED_PATH = "cnpjs_seed.txt.gz"
_SEED_URL  = os.environ.get("SEED_URL", "")

def _is_lfs_pointer(path: str) -> bool:
    try:
        with open(path, "rb") as f:
            return f.read(2) != b"\x1f\x8b"
    except OSError:
        return False

if _SEED_URL:
    needs_download = not os.path.exists(_SEED_PATH) or _is_lfs_pointer(_SEED_PATH)
    if needs_download:
        print(f"SEED_URL definido — baixando seed de {_SEED_URL} ...")
        import urllib.request
        tmp = _SEED_PATH + ".tmp"
        try:
            urllib.request.urlretrieve(_SEED_URL, tmp)
            os.replace(tmp, _SEED_PATH)
            size_mb = os.path.getsize(_SEED_PATH) / 1_048_576
            print(f"Seed baixado com sucesso ({size_mb:.0f} MB).")
        except Exception as e:
            print(f"AVISO: Falha ao baixar seed: {e}. Agente iniciará sem seed.")
            if os.path.exists(tmp):
                os.remove(tmp)
    else:
        print("Seed local já presente e válido — download ignorado.")
elif os.path.exists(_SEED_PATH) and _is_lfs_pointer(_SEED_PATH):
    print(
        "AVISO: cnpjs_seed.txt.gz é um ponteiro Git LFS (arquivo real ausente). "
        "Defina a variável SEED_URL no Railway para baixar o seed automaticamente. "
        "O agente aguardará até o seed estar disponível."
    )

# Inicia o agente em background
agente = subprocess.Popen([sys.executable, "agent/agent.py"])
print(f"Agente iniciado (PID {agente.pid})")

time.sleep(2)

# Inicia a API (processo principal — Railway monitora este)
port = os.environ.get("PORT", "8000")
print(f"Iniciando API na porta {port}...")
os.execv(sys.executable, [
    sys.executable, "-m", "uvicorn",
    "api:app",
    "--host", "0.0.0.0",
    "--port", port
])
