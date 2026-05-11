"""Insere empresas de teste no SQLite local para desenvolvimento."""
import sqlite3
from datetime import datetime

DB = "cnpj_intel.db"
NOW = datetime.now().isoformat()

empresas = [
    # score 100 — tudo preenchido
    ("11111111000191", "TECH SOLUTIONS LTDA", "Tech Solutions", "MICRO EMPRESA", "62.01-5-01 - Desenvolvimento de programas de computador sob encomenda",
     "ATIVA", "2015-03-10", "São Paulo", "SP", "JOAO DA SILVA",
     "11999990001", "contato@techsolutions.com.br", "@techsolutions", "techsolutions.com.br", 4.8, 320),

    # score 85 — sem instagram
    ("22222222000182", "CLINICA BEM ESTAR LTDA", "Clínica Bem Estar", "MICRO EMPRESA", "86.30-5-04 - Atividades de fisioterapia",
     "ATIVA", "2018-06-22", "Belo Horizonte", "MG", "MARIA SOUZA",
     "31988880002", "agendamento@bemestarclinica.com", None, "bemestarclinica.com.br", 4.5, 180),

    # score 65 — telefone + email + site, sem instagram nem rating
    ("33333333000173", "PADARIA E CONFEITARIA BOA FORMA", "Padaria Boa Forma", "MEI", "10.91-1-02 - Fabricação de produtos de padaria e confeitaria",
     "ATIVA", "2020-01-15", "Curitiba", "PR", "PEDRO OLIVEIRA",
     "41977770003", "padariaboaforma@gmail.com", None, "padariaboaforma.com.br", None, 0),

    # score 50 — só telefone + email
    ("44444444000164", "CONSULTORIA FISCAL RAPIDA ME", None, "MICRO EMPRESA", "69.20-6-01 - Atividades de contabilidade",
     "ATIVA", "2019-09-30", "Porto Alegre", "RS", "ANA COSTA",
     "51966660004", "ana@fiscalrapida.com.br", None, None, None, 0),

    # score 40 — só telefone + instagram
    ("55555555000155", "BARBEARIA DO REI", "Barbearia do Rei", "MEI", "96.02-5-01 - Cabeleireiros, manicure e pedicure",
     "ATIVA", "2021-04-05", "Salvador", "BA", "CARLOS SANTOS",
     "71955550005", None, "@barbeariadorei", None, None, 0),

    # score 25 — só telefone
    ("66666666000146", "TRANSPORTE RAPIDO ME", None, "MICRO EMPRESA", "49.30-2-02 - Transporte rodoviário de carga",
     "ATIVA", "2017-07-20", "Fortaleza", "CE", "LUCAS FERREIRA",
     "85944440006", None, None, None, None, 0),

    # score 0 — nenhum contato (invisível no dashboard com com_contato=true)
    ("77777777000137", "HOLDING FANTASMA SA", None, "MEDIO", "64.62-0-00 - Holdings de instituições não-financeiras",
     "ATIVA", "2010-11-11", "Rio de Janeiro", "RJ", "FULANO TAL",
     None, None, None, None, None, 0),
]

conn = sqlite3.connect(DB)
cur = conn.cursor()

for e in empresas:
    cur.execute("""
        INSERT OR REPLACE INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal,
             telefone, email, instagram, site, rating_google, avaliacoes, atualizado_em)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (*e, NOW))

conn.commit()
conn.close()

print(f"✓ {len(empresas)} empresas de teste inseridas em {DB}")
print("  Scores esperados:")
print("  11111111000191 → 100 (verde)")
print("  22222222000182 →  85 (verde)")
print("  33333333000173 →  65 (amarelo)")
print("  44444444000164 →  50 (amarelo)")
print("  55555555000155 →  40 (amarelo)")
print("  66666666000146 →  25 (vermelho)")
print("  77777777000137 →   0 (oculto com filtro padrão)")
