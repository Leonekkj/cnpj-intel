"""
Extrator de CNPJs da base oficial da Receita Federal.
Lê o arquivo de Estabelecimentos (.csv descompactado) e gera o cnpjs_seed.txt
filtrado por estado, porte e situação cadastral.

Como usar:
1. Baixe UM arquivo de: https://arquivos.receitafederal.gov.br/index.php/s/YggdBLfdninEJX9
   Exemplo: Estabelecimentos0.zip
2. Extraia o .zip — vai gerar um arquivo sem extensão ou .csv
3. Rode: python extrator.py --arquivo Estabelecimentos0 --uf MG
"""

import csv
import argparse
import os
import zipfile
import sys

# ─── Colunas do arquivo de Estabelecimentos da Receita Federal ────
# Ordem oficial do layout (sem cabeçalho no arquivo)
COLUNAS = [
    "cnpj_basico",        # 0  — primeiros 8 dígitos
    "cnpj_ordem",         # 1  — 4 dígitos do meio
    "cnpj_dv",            # 2  — 2 dígitos verificadores
    "identificador",      # 3  — 1=matriz, 2=filial
    "nome_fantasia",      # 4
    "situacao",           # 5  — 01=nula,02=ativa,03=suspensa,04=inapta,08=baixada
    "data_situacao",      # 6
    "motivo_situacao",    # 7
    "nome_cidade_ext",    # 8
    "pais",               # 9
    "data_inicio",        # 10
    "cnae_principal",     # 11
    "cnae_secundario",    # 12
    "tipo_logradouro",    # 13
    "logradouro",         # 14
    "numero",             # 15
    "complemento",        # 16
    "bairro",             # 17
    "cep",                # 18
    "uf",                 # 19
    "municipio",          # 20
    "ddd1",               # 21
    "telefone1",          # 22
    "ddd2",               # 23
    "telefone2",          # 24
    "ddd_fax",            # 25
    "fax",                # 26
    "email",              # 27
    "situacao_especial",  # 28
    "data_situacao_esp",  # 29
]

# CNAEs de alto valor comercial (adapte como quiser)
CNAES_INTERESSE = {
    # Alimentação
    "5611201", "5611202", "5611203", "5612100",
    # Saúde
    "8630503", "8630504", "8630506", "8621601",
    # Tecnologia
    "6201500", "6202300", "6203100",
    # Comércio varejista
    "4771701", "4772500", "4781400", "4712100",
    # Beleza e estética
    "9602501", "9602502",
    # Educação
    "8511200", "8512100", "8513900",
    # Construção
    "4120400", "4330404",
    # Serviços financeiros
    "6422100", "6491300",
}


def montar_cnpj(basico, ordem, dv):
    """Junta os 3 campos em CNPJ de 14 dígitos."""
    return f"{basico.zfill(8)}{ordem.zfill(4)}{dv.zfill(2)}"


def extrair_zip(caminho_zip):
    """Extrai o zip e retorna o caminho do arquivo interno."""
    print(f"Extraindo {caminho_zip}...")
    pasta = os.path.dirname(caminho_zip)
    with zipfile.ZipFile(caminho_zip, 'r') as z:
        nomes = z.namelist()
        print(f"  Arquivos no zip: {nomes}")
        z.extractall(pasta)
        # retorna o primeiro arquivo que não é pasta
        for nome in nomes:
            caminho = os.path.join(pasta, nome)
            if os.path.isfile(caminho):
                return caminho
    return None


def _formatar_telefone(ddd: str, numero: str) -> str:
    """Formata DDD+número da Receita em '(DD) XXXXX-XXXX' ou '(DD) XXXX-XXXX'."""
    ddd = ddd.strip()
    numero = numero.strip()
    if not ddd or not numero:
        return ""
    if len(numero) == 9:
        return f"({ddd}) {numero[:5]}-{numero[5:]}"
    elif len(numero) == 8:
        return f"({ddd}) {numero[:4]}-{numero[4:]}"
    elif len(numero) >= 7:
        return f"({ddd}) {numero}"
    return ""


def extrair_cnpjs(
    arquivo,
    ufs=None,
    cnaes=None,
    apenas_ativas=True,
    apenas_matriz=False,
    limite=None,
    saida="cnpjs_seed.txt",
):
    """
    Lê o arquivo de Estabelecimentos e filtra os CNPJs.
    Gera TSV com dados cadastrais e de contato (telefone, email) da Receita.

    Parâmetros:
        arquivo      — caminho do arquivo da Receita (zip ou csv)
        ufs          — lista de UFs ex: ['SP','MG'] ou None para todos
        cnaes        — set de CNAEs ou None para todos
        apenas_ativas — só situação 02 (ativa)
        apenas_matriz — só matrizes (identificador == 1)
        limite        — máximo de CNPJs a extrair (None = sem limite)
        saida         — nome do arquivo de saída
    """
    # Descompacta se for zip
    if arquivo.endswith(".zip"):
        arquivo = extrair_zip(arquivo)
        if not arquivo:
            print("Erro: não encontrou arquivo dentro do zip.")
            sys.exit(1)

    if not os.path.exists(arquivo):
        print(f"Arquivo não encontrado: {arquivo}")
        sys.exit(1)

    tamanho_mb = os.path.getsize(arquivo) / 1024 / 1024
    print(f"\nArquivo: {arquivo} ({tamanho_mb:.0f} MB)")
    print(f"Filtros: UFs={ufs or 'TODOS'} | Ativas={apenas_ativas} | Limite={limite or 'SEM LIMITE'}")
    print(f"Formato de saída: TSV com dados de contato (telefone, email)")
    print("Processando... (pode demorar alguns minutos para arquivos grandes)\n")

    encontrados = 0
    com_telefone = 0
    com_email = 0
    processados = 0
    ignorados   = 0

    with open(arquivo, encoding="latin-1", errors="ignore") as f_in, \
         open(saida, "w", encoding="utf-8") as f_out:

        reader = csv.reader(f_in, delimiter=";", quotechar='"')

        for linha in reader:
            processados += 1

            # Mostra progresso a cada 500k linhas
            if processados % 500_000 == 0:
                print(f"  {processados:,} linhas processadas | {encontrados:,} CNPJs | {com_telefone:,} com tel...")

            if len(linha) < 20:
                ignorados += 1
                continue

            try:
                situacao   = linha[5].strip()
                uf         = linha[19].strip().upper()
                cnae       = linha[11].strip()
                identif    = linha[3].strip()
                cnpj_base  = linha[0].strip()
                cnpj_ordem = linha[1].strip()
                cnpj_dv    = linha[2].strip()
            except IndexError:
                ignorados += 1
                continue

            # Filtro: situação ativa (02)
            if apenas_ativas and situacao != "02":
                continue

            # Filtro: UF
            if ufs and uf not in ufs:
                continue

            # Filtro: apenas matriz
            if apenas_matriz and identif != "1":
                continue

            # Filtro: CNAE de interesse
            if cnaes and cnae not in cnaes:
                continue

            cnpj = montar_cnpj(cnpj_base, cnpj_ordem, cnpj_dv)

            # Extrai dados de contato se disponíveis (colunas 21-27)
            nome_fantasia = ""
            municipio     = ""
            data_inicio   = ""
            telefone1     = ""
            telefone2     = ""
            email         = ""

            if len(linha) >= 28:
                nome_fantasia = linha[4].strip()
                municipio     = linha[20].strip()
                raw_data      = linha[10].strip()
                if len(raw_data) == 8 and raw_data.isdigit():
                    data_inicio = f"{raw_data[:4]}-{raw_data[4:6]}-{raw_data[6:]}"
                else:
                    data_inicio = raw_data

                ddd1  = linha[21].strip()
                num1  = linha[22].strip()
                ddd2  = linha[23].strip()
                num2  = linha[24].strip()

                telefone1 = _formatar_telefone(ddd1, num1)
                telefone2 = _formatar_telefone(ddd2, num2)
                email     = linha[27].strip().lower()

            # TSV: cnpj\tnome_fantasia\tuf\tmunicipio\tcnae\tabertura\ttelefone1\ttelefone2\temail
            campos = [cnpj, nome_fantasia, uf, municipio, cnae, data_inicio,
                      telefone1, telefone2, email]
            f_out.write("\t".join(campos) + "\n")

            encontrados += 1
            if telefone1:
                com_telefone += 1
            if email:
                com_email += 1

            if limite and encontrados >= limite:
                print(f"\nLimite de {limite:,} CNPJs atingido.")
                break

    pct_tel = (com_telefone / encontrados * 100) if encontrados else 0
    pct_email = (com_email / encontrados * 100) if encontrados else 0

    print(f"\n{'='*50}")
    print(f"Concluído!")
    print(f"  Linhas processadas : {processados:,}")
    print(f"  CNPJs extraídos    : {encontrados:,}")
    print(f"  Com telefone       : {com_telefone:,} ({pct_tel:.1f}%)")
    print(f"  Com email          : {com_email:,} ({pct_email:.1f}%)")
    print(f"  Arquivo gerado     : {saida}")
    print(f"{'='*50}")
    print(f"\nAgora rode: python agent/agent.py")


# ─── CLI ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extrai CNPJs da base da Receita Federal"
    )
    parser.add_argument(
        "--arquivo", required=True,
        help="Caminho do arquivo da Receita (ex: Estabelecimentos0.zip ou Estabelecimentos0)"
    )
    parser.add_argument(
        "--uf", default="",
        help="Estados separados por vírgula ex: SP,MG,RJ (vazio = todos)"
    )
    parser.add_argument(
        "--limite", type=int, default=50000,
        help="Máximo de CNPJs a extrair (padrão: 50000)"
    )
    parser.add_argument(
        "--todos-cnaes", action="store_true",
        help="Inclui todos os CNAEs (padrão: só CNAEs de alto valor)"
    )
    parser.add_argument(
        "--so-matriz", action="store_true",
        help="Extrai só estabelecimentos matriz"
    )
    parser.add_argument(
        "--saida", default="cnpjs_seed.txt",
        help="Nome do arquivo de saída (padrão: cnpjs_seed.txt)"
    )

    args = parser.parse_args()

    ufs_filtro   = [u.strip().upper() for u in args.uf.split(",") if u.strip()] or None
    cnaes_filtro = None if args.todos_cnaes else CNAES_INTERESSE

    extrair_cnpjs(
        arquivo=args.arquivo,
        ufs=ufs_filtro,
        cnaes=cnaes_filtro,
        apenas_ativas=True,
        apenas_matriz=args.so_matriz,
        limite=args.limite,
        saida=args.saida,
    )
