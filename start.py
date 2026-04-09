"""
start.py — inicia a API e o agente ao mesmo tempo no Railway.
"""
import subprocess
import sys
import os
import time

print("Iniciando CNPJ Intel...")

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
