"""
Gera os dicts de lookup data/rf_municipios.py e data/cnae_descricoes.py
a partir dos arquivos públicos da Receita Federal.

Uso:
  python gerar_lookups.py --municipios Municipios.zip [--cnae CNAE0.zip]

Se --cnae não for fornecido, busca as descrições via BrasilAPI (requer internet).
"""

import argparse
import io
import json
import os
import urllib.request
import zipfile

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def _iter_linhas_municipios(path: str):
    """Itera linhas do arquivo de municípios (zip ou CSV extraído)."""
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as z:
            for fname in z.namelist():
                with z.open(fname) as f:
                    yield from io.TextIOWrapper(f, encoding="latin-1")
    else:
        with open(path, encoding="latin-1") as f:
            yield from f


def gerar_rf_municipios(path: str) -> dict[str, str]:
    """Lê Municipios.zip ou CSV extraído da RF e retorna {codigo: nome}."""
    municipios = {}
    for line in _iter_linhas_municipios(path):
        parts = line.strip().split(";")
        if len(parts) < 2:
            continue
        codigo = parts[0].strip().strip('"')
        nome = parts[1].strip().strip('"').upper()
        if codigo and nome:
            municipios[codigo] = nome
    return municipios


def gerar_cnae_de_zip(zip_path: str) -> dict[str, str]:
    """Lê CNAE*.zip da RF e retorna {codigo: descricao}."""
    cnae = {}
    with zipfile.ZipFile(zip_path) as z:
        for fname in z.namelist():
            with z.open(fname) as f:
                for line in io.TextIOWrapper(f, encoding="latin-1"):
                    parts = line.strip().split(";")
                    if len(parts) < 2:
                        continue
                    codigo = parts[0].strip().strip('"')
                    desc = parts[1].strip().strip('"')
                    if codigo.isdigit() and desc:
                        cnae[codigo] = desc
    return cnae


def gerar_cnae_de_api() -> dict[str, str]:
    """Busca todos os CNAEs via API IBGE e retorna {codigo: descricao}."""
    print("Buscando CNAEs via IBGE API...")
    url = "https://servicodados.ibge.gov.br/api/v2/cnae/subclasses"
    with urllib.request.urlopen(url, timeout=30) as resp:
        raw = resp.read()
        # IBGE API retorna Latin-1; tenta UTF-8 primeiro, faz fallback para Latin-1
        try:
            data = json.loads(raw.decode("utf-8"))
        except UnicodeDecodeError:
            data = json.loads(raw.decode("latin-1"))
    cnae = {}
    for item in data:
        codigo = str(item.get("id", "")).replace(".", "").replace("-", "").replace("/", "")
        desc = item.get("descricao", "").strip()
        if codigo and desc:
            cnae[codigo] = desc
    print(f"  {len(cnae)} subclasses CNAE obtidas.")
    return cnae


def salvar_dict(path: str, nome_var: str, data: dict[str, str]) -> None:
    linhas = [f"{nome_var} = {{"]
    for k, v in sorted(data.items()):
        v_esc = v.replace("\\", "\\\\").replace('"', '\\"')
        linhas.append(f'    "{k}": "{v_esc}",')
    linhas.append("}")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(linhas) + "\n")
    print(f"  Salvo: {path} ({len(data)} entradas)")


def main():
    parser = argparse.ArgumentParser(description="Gera lookup tables para CNPJ Intel")
    parser.add_argument("--municipios", help="Caminho para Municipios.zip da RF")
    parser.add_argument("--cnae", help="Caminho para CNAE*.zip da RF (opcional)")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    # Municípios
    if args.municipios:
        print(f"Processando municípios de: {args.municipios}")
        muns = gerar_rf_municipios(args.municipios)
        salvar_dict(
            os.path.join(DATA_DIR, "rf_municipios.py"),
            "RF_MUNICIPIOS",
            muns,
        )
    else:
        print("AVISO: --municipios não fornecido. Baixe Municipios.zip da RF e rode novamente.")

    # CNAE
    cnae_path = os.path.join(DATA_DIR, "cnae_descricoes.py")
    if args.cnae:
        print(f"Processando CNAEs de: {args.cnae}")
        cnae = gerar_cnae_de_zip(args.cnae)
    else:
        try:
            cnae = gerar_cnae_de_api()
        except Exception as e:
            print(f"Falha ao buscar CNAEs da API: {e}")
            cnae = {}

    if cnae:
        salvar_dict(cnae_path, "CNAE_DESCRICOES", cnae)

    print("Pronto.")


if __name__ == "__main__":
    main()
