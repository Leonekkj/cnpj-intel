"""
Banco de dados — PostgreSQL (produção) ou SQLite (local).
Inclui persistência de progresso do agente.
"""

import os
import secrets
import unicodedata
from datetime import datetime, timedelta, date as date_type

DATABASE_URL = (
    os.environ.get("DATABASE_URL") or
    os.environ.get("DATABASE_PUBLIC_URL") or
    ""
)

print(f"DATABASE_URL detectado: {DATABASE_URL[:40] if DATABASE_URL else 'VAZIO - usando SQLite'}")

USE_POSTGRES = DATABASE_URL.startswith("postgresql") or DATABASE_URL.startswith("postgres")

if USE_POSTGRES:
    import time
    import psycopg2
    import threading
    from psycopg2 import pool as _pg_pool
    from contextlib import contextmanager

    _POOL = None
    _POOL_LOCK = threading.Lock()

    def _get_pool():
        global _POOL
        if _POOL is None:
            with _POOL_LOCK:
                if _POOL is None:
                    print("Conectando ao PostgreSQL com pool...")
                    attempt = 0
                    while True:
                        try:
                            _POOL = _pg_pool.ThreadedConnectionPool(
                                minconn=1, maxconn=20, dsn=DATABASE_URL,
                                keepalives=1,
                                keepalives_idle=30,
                                keepalives_interval=10,
                                keepalives_count=3,
                            )
                            break
                        except psycopg2.OperationalError as e:
                            wait = min(2 ** attempt, 30)
                            attempt += 1
                            if attempt <= 15:
                                label = f"tentativa {attempt}"
                            else:
                                label = f"tentativa {attempt} (DB ainda indisponível, aguardando)"
                            print(f"PostgreSQL indisponível ({label}), retry em {wait}s: {e}")
                            time.sleep(wait)
        return _POOL

    @contextmanager
    def _conn():
        pool = _get_pool()
        c = pool.getconn()
        try:
            # Testa se a conexão ainda está viva; reconecta se necessário
            try:
                c.cursor().execute("SELECT 1")
            except Exception:
                try:
                    pool.putconn(c, close=True)
                except Exception:
                    pass
                c = psycopg2.connect(DATABASE_URL)
            yield c
        finally:
            try:
                pool.putconn(c)
            except Exception:
                try:
                    c.close()
                except Exception:
                    pass
else:
    import sqlite3
    print("Usando SQLite local")
    def _conn():
        conn = sqlite3.connect("cnpj_intel.db")
        conn.row_factory = sqlite3.Row
        return conn

PH = "%s" if USE_POSTGRES else "?"
LIKE = "ILIKE" if USE_POSTGRES else "LIKE"


# ─── Utilitários de validação ─────────────────────────────────────────────────

def _norm(text: str) -> str:
    """Lowercase + remove diacríticos. 'Contábil' → 'contabil'."""
    return unicodedata.normalize("NFD", text.lower()).encode("ascii", "ignore").decode()


_TELEFONES_INVALIDOS = {"", "n/a", "none", "null", "nan", "-", "0", "00000000"}

def telefone_valido(tel) -> bool:
    """Retorna True apenas para strings com conteúdo telefônico real."""
    if not tel:
        return False
    return tel.strip().lower() not in _TELEFONES_INVALIDOS


# ─── Mapeamento CNAE → Departamento + Macro Setor ────────────────────────────
# Método primário: prefixo de 3 dígitos do código CNAE → (departamento, macro_setor).
# Cobre ~95% dos casos (seeds com código numérico de 7 dígitos da Receita Federal).
_CNAE_GRUPOS = {
    # Agro e Agronegócio (01-03, 75)
    "011": ("Agricultura e Cultivos",           "Agro e Agronegócio"),
    "012": ("Agricultura e Cultivos",           "Agro e Agronegócio"),
    "013": ("Pecuária",                         "Agro e Agronegócio"),
    "014": ("Pecuária",                         "Agro e Agronegócio"),
    "015": ("Pecuária",                         "Agro e Agronegócio"),
    "016": ("Agricultura e Cultivos",           "Agro e Agronegócio"),
    "021": ("Silvicultura e Floresta",          "Agro e Agronegócio"),
    "022": ("Silvicultura e Floresta",          "Agro e Agronegócio"),
    "023": ("Silvicultura e Floresta",          "Agro e Agronegócio"),
    "024": ("Silvicultura e Floresta",          "Agro e Agronegócio"),
    "031": ("Pesca e Aquicultura",              "Agro e Agronegócio"),
    "032": ("Pesca e Aquicultura",              "Agro e Agronegócio"),
    "750": ("Veterinária",                      "Agro e Agronegócio"),
    # Indústria e Produção — Mineração e Extração (05-09)
    "051": ("Mineração e Extração",             "Indústria e Produção"),
    "052": ("Mineração e Extração",             "Indústria e Produção"),
    "061": ("Mineração e Extração",             "Indústria e Produção"),
    "062": ("Mineração e Extração",             "Indústria e Produção"),
    "071": ("Mineração e Extração",             "Indústria e Produção"),
    "072": ("Mineração e Extração",             "Indústria e Produção"),
    "081": ("Mineração e Extração",             "Indústria e Produção"),
    "089": ("Mineração e Extração",             "Indústria e Produção"),
    "091": ("Mineração e Extração",             "Indústria e Produção"),
    "099": ("Mineração e Extração",             "Indústria e Produção"),
    # Indústria e Produção — Alimentícia (10-12)
    "101": ("Indústria Alimentícia",            "Indústria e Produção"),
    "102": ("Indústria Alimentícia",            "Indústria e Produção"),
    "103": ("Indústria Alimentícia",            "Indústria e Produção"),
    "104": ("Indústria Alimentícia",            "Indústria e Produção"),
    "105": ("Indústria Alimentícia",            "Indústria e Produção"),
    "106": ("Indústria Alimentícia",            "Indústria e Produção"),
    "107": ("Indústria Alimentícia",            "Indústria e Produção"),
    "108": ("Indústria Alimentícia",            "Indústria e Produção"),
    "109": ("Indústria Alimentícia",            "Indústria e Produção"),
    "110": ("Indústria Alimentícia",            "Indústria e Produção"),
    "111": ("Indústria Alimentícia",            "Indústria e Produção"),
    "112": ("Indústria Alimentícia",            "Indústria e Produção"),
    "120": ("Indústria Alimentícia",            "Indústria e Produção"),
    # Indústria e Produção — Têxtil e Vestuário (13-15)
    "131": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    "132": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    "133": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    "134": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    "135": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    "139": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    "141": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    "142": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    "151": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    "152": ("Indústria Têxtil e Vestuário",     "Indústria e Produção"),
    # Indústria e Produção — Papel, Madeira e Móveis (16-18, 31)
    "161": ("Papel, Madeira e Móveis",          "Indústria e Produção"),
    "162": ("Papel, Madeira e Móveis",          "Indústria e Produção"),
    "170": ("Papel, Madeira e Móveis",          "Indústria e Produção"),
    "171": ("Papel, Madeira e Móveis",          "Indústria e Produção"),
    "172": ("Papel, Madeira e Móveis",          "Indústria e Produção"),
    "181": ("Papel, Madeira e Móveis",          "Indústria e Produção"),
    "182": ("Papel, Madeira e Móveis",          "Indústria e Produção"),
    "310": ("Papel, Madeira e Móveis",          "Indústria e Produção"),
    # Indústria e Produção — Química e Farmacêutica (19-21)
    "191": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "192": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "201": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "202": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "203": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "204": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "205": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "206": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "211": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "212": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    # Indústria e Produção — Borracha e Plástico (22) → mesma cadeia petroquímica
    "221": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    "222": ("Indústria Química e Farmacêutica", "Indústria e Produção"),
    # Indústria e Produção — Minerais Não-Metálicos (23)
    "231": ("Metalurgia e Siderurgia",          "Indústria e Produção"),  # vidro
    "232": ("Mineração e Extração",             "Indústria e Produção"),  # cimento
    "233": ("Mineração e Extração",             "Indústria e Produção"),  # cerâmica
    "234": ("Mineração e Extração",             "Indústria e Produção"),  # gesso/cal
    "235": ("Mineração e Extração",             "Indústria e Produção"),  # pedra
    "239": ("Mineração e Extração",             "Indústria e Produção"),  # outros minerais
    # Indústria e Produção — Bens de Consumo e Lazer (32 sem 325)
    "321": ("Bens de Consumo e Lazer",          "Indústria e Produção"),  # joalheria
    "322": ("Bens de Consumo e Lazer",          "Indústria e Produção"),  # instrumentos musicais
    "323": ("Bens de Consumo e Lazer",          "Indústria e Produção"),  # artefatos esporte/pesca
    "324": ("Bens de Consumo e Lazer",          "Indústria e Produção"),  # brinquedos
    "325": ("Eletrônica e Equipamentos",        "Indústria e Produção"),  # instrumentos médicos
    "329": ("Bens de Consumo e Lazer",          "Indústria e Produção"),  # produtos diversos
    # Indústria e Produção — Manutenção e Instalação (33)
    "331": ("Reparação e Manutenção",           "Serviços Locais"),       # manutenção de máquinas
    "332": ("Serviços Especializados de Construção", "Construção e Infraestrutura"),  # instalação
    # Indústria e Produção — Metalurgia (24-25)
    "241": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "242": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "243": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "244": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "245": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "246": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "251": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "252": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "253": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "254": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "255": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    "259": ("Metalurgia e Siderurgia",          "Indústria e Produção"),
    # Indústria e Produção — Eletrônica e Equipamentos (26-28)
    "261": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "262": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "263": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "264": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "265": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "266": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "267": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "268": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "271": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "272": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "273": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "274": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "275": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "279": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "281": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "282": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "283": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "284": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    "289": ("Eletrônica e Equipamentos",        "Indústria e Produção"),
    # Indústria e Produção — Automotivo (29-30)
    "291": ("Automotivo",                       "Indústria e Produção"),
    "292": ("Automotivo",                       "Indústria e Produção"),
    "293": ("Automotivo",                       "Indústria e Produção"),
    "301": ("Automotivo",                       "Indústria e Produção"),
    "302": ("Automotivo",                       "Indústria e Produção"),
    "303": ("Automotivo",                       "Indústria e Produção"),
    "304": ("Automotivo",                       "Indústria e Produção"),
    "309": ("Automotivo",                       "Indústria e Produção"),
    # Energia e Utilities (35-39)
    "351": ("Energia Elétrica e Gás",           "Energia e Utilities"),
    "352": ("Energia Elétrica e Gás",           "Energia e Utilities"),
    "353": ("Energia Elétrica e Gás",           "Energia e Utilities"),
    "360": ("Água e Saneamento",                "Energia e Utilities"),
    "370": ("Água e Saneamento",                "Energia e Utilities"),
    "381": ("Gestão de Resíduos",               "Energia e Utilities"),
    "382": ("Gestão de Resíduos",               "Energia e Utilities"),
    "383": ("Gestão de Resíduos",               "Energia e Utilities"),
    "390": ("Gestão de Resíduos",               "Energia e Utilities"),
    # Construção e Infraestrutura (41-43)
    "411": ("Construção Civil e Edifícios",          "Construção e Infraestrutura"),
    "412": ("Construção Civil e Edifícios",          "Construção e Infraestrutura"),
    "421": ("Obras de Infraestrutura",               "Construção e Infraestrutura"),
    "422": ("Obras de Infraestrutura",               "Construção e Infraestrutura"),
    "429": ("Obras de Infraestrutura",               "Construção e Infraestrutura"),
    "431": ("Serviços Especializados de Construção", "Construção e Infraestrutura"),
    "432": ("Serviços Especializados de Construção", "Construção e Infraestrutura"),
    "433": ("Serviços Especializados de Construção", "Construção e Infraestrutura"),
    "439": ("Serviços Especializados de Construção", "Construção e Infraestrutura"),
    # Comércio Atacado e Varejo (45-47)
    "451": ("Comércio de Veículos e Peças",          "Comércio Atacado e Varejo"),
    "452": ("Comércio de Veículos e Peças",          "Comércio Atacado e Varejo"),
    "453": ("Comércio de Veículos e Peças",          "Comércio Atacado e Varejo"),
    "454": ("Comércio de Veículos e Peças",          "Comércio Atacado e Varejo"),
    "461": ("Atacado de Alimentos e Bebidas",        "Comércio Atacado e Varejo"),
    "462": ("Atacado de Alimentos e Bebidas",        "Comércio Atacado e Varejo"),
    "463": ("Atacado de Materiais de Construção",    "Comércio Atacado e Varejo"),
    "464": ("Atacado de Outros Produtos",            "Comércio Atacado e Varejo"),
    "465": ("Atacado de Outros Produtos",            "Comércio Atacado e Varejo"),
    "466": ("Atacado de Saúde e Higiene",            "Comércio Atacado e Varejo"),
    "467": ("Atacado de Outros Produtos",            "Comércio Atacado e Varejo"),
    "469": ("Atacado de Outros Produtos",            "Comércio Atacado e Varejo"),
    "471": ("Varejo de Alimentos e Supermercados",   "Comércio Atacado e Varejo"),
    "472": ("Varejo de Alimentos e Supermercados",   "Comércio Atacado e Varejo"),
    "473": ("Comércio de Veículos e Peças",          "Comércio Atacado e Varejo"),
    "474": ("Varejo de Casa e Decoração",            "Comércio Atacado e Varejo"),
    "475": ("Varejo de Casa e Decoração",            "Comércio Atacado e Varejo"),
    "476": ("Varejo de Eletrônicos e Informática",   "Comércio Atacado e Varejo"),
    "477": ("Varejo de Saúde e Farmácias",           "Comércio Atacado e Varejo"),
    "478": ("Varejo de Vestuário e Calçados",        "Comércio Atacado e Varejo"),
    "479": ("Outros Comércios Varejistas",           "Comércio Atacado e Varejo"),
    # Logística e Transporte (49-53)
    "491": ("Transporte Rodoviário de Cargas",       "Logística e Transporte"),
    "492": ("Transporte Rodoviário de Passageiros",  "Logística e Transporte"),
    "493": ("Transporte Rodoviário de Passageiros",  "Logística e Transporte"),
    "494": ("Transporte Rodoviário de Cargas",       "Logística e Transporte"),
    "495": ("Transporte Rodoviário de Cargas",       "Logística e Transporte"),
    "501": ("Transporte Aquaviário",                 "Logística e Transporte"),
    "502": ("Transporte Aquaviário",                 "Logística e Transporte"),
    "503": ("Transporte Aquaviário",                 "Logística e Transporte"),
    "504": ("Transporte Aquaviário",                 "Logística e Transporte"),
    "511": ("Transporte Aéreo",                      "Logística e Transporte"),
    "512": ("Transporte Aéreo",                      "Logística e Transporte"),
    "521": ("Armazenagem e Logística",               "Logística e Transporte"),
    "522": ("Armazenagem e Logística",               "Logística e Transporte"),
    "531": ("Correio e Entregas",                    "Logística e Transporte"),
    "532": ("Correio e Entregas",                    "Logística e Transporte"),
    # Alimentação e Hospitalidade (55-56)
    "551": ("Hotéis e Pousadas",                     "Alimentação e Hospitalidade"),
    "552": ("Hostels e Hospedagem Alternativa",      "Alimentação e Hospitalidade"),
    "559": ("Hotéis e Pousadas",                     "Alimentação e Hospitalidade"),
    "561": ("Restaurantes e Lanchonetes",            "Alimentação e Hospitalidade"),
    "562": ("Delivery e Catering",                   "Alimentação e Hospitalidade"),
    "563": ("Bares e Entretenimento Noturno",        "Alimentação e Hospitalidade"),
    # Tecnologia e Marketing (58-63, 73-74)
    "581": ("Mídia e Conteúdo",                      "Tecnologia e Marketing"),
    "582": ("Mídia e Conteúdo",                      "Tecnologia e Marketing"),
    "591": ("Criação e Design",                      "Tecnologia e Marketing"),
    "592": ("Mídia e Conteúdo",                      "Tecnologia e Marketing"),
    "601": ("Mídia e Conteúdo",                      "Tecnologia e Marketing"),
    "602": ("Mídia e Conteúdo",                      "Tecnologia e Marketing"),
    "611": ("Telecomunicações",                      "Tecnologia e Marketing"),
    "612": ("Telecomunicações",                      "Tecnologia e Marketing"),
    "613": ("Telecomunicações",                      "Tecnologia e Marketing"),
    "619": ("Telecomunicações",                      "Tecnologia e Marketing"),
    "620": ("Desenvolvimento de Software",           "Tecnologia e Marketing"),
    "621": ("Desenvolvimento de Software",           "Tecnologia e Marketing"),
    "622": ("Desenvolvimento de Software",           "Tecnologia e Marketing"),
    "631": ("Infraestrutura e Cloud",                "Tecnologia e Marketing"),
    "639": ("Pesquisa e Dados",                      "Tecnologia e Marketing"),
    "731": ("Marketing e Publicidade",               "Tecnologia e Marketing"),
    "732": ("Pesquisa e Dados",                      "Tecnologia e Marketing"),
    "741": ("Criação e Design",                      "Tecnologia e Marketing"),
    "742": ("Criação e Design",                      "Tecnologia e Marketing"),
    "743": ("Pesquisa e Dados",                      "Tecnologia e Marketing"),
    # Financeiro (64-66)
    "641": ("Bancos e Crédito",                      "Financeiro"),
    "642": ("Bancos e Crédito",                      "Financeiro"),
    "643": ("Bancos e Crédito",                      "Financeiro"),
    "649": ("Correspondentes e Serviços Financeiros","Financeiro"),
    "651": ("Seguros e Previdência",                 "Financeiro"),
    "652": ("Seguros e Previdência",                 "Financeiro"),
    "661": ("Investimentos e Mercado de Capitais",   "Financeiro"),
    "662": ("Seguros e Previdência",                 "Financeiro"),
    "663": ("Investimentos e Mercado de Capitais",   "Financeiro"),
    # Imobiliário (68)
    "681": ("Incorporação Imobiliária",              "Imobiliário"),
    "682": ("Locação e Gestão de Imóveis",           "Imobiliário"),
    # Jurídico (69)
    "691": ("Advogados e Escritórios Jurídicos",     "Jurídico"),
    "692": ("Contabilidade e Auditoria",             "Jurídico"),
    "693": ("Cartórios e Registros",                 "Jurídico"),
    # Serviços Profissionais (70-72, 749)
    "701": ("Consultoria Empresarial e Gestão",      "Serviços Profissionais"),
    "702": ("Consultoria Empresarial e Gestão",      "Serviços Profissionais"),
    "711": ("Arquitetura e Engenharia",              "Serviços Profissionais"),
    "712": ("Arquitetura e Engenharia",              "Serviços Profissionais"),
    "713": ("Arquitetura e Engenharia",              "Serviços Profissionais"),
    "721": ("Pesquisa e Desenvolvimento",            "Serviços Profissionais"),
    "722": ("Pesquisa e Desenvolvimento",            "Serviços Profissionais"),
    "749": ("Consultoria Empresarial e Gestão",       "Serviços Profissionais"),
    # Serviços Profissionais — RH (78)
    "781": ("Recursos Humanos",                      "Serviços Profissionais"),
    "782": ("Recursos Humanos",                      "Serviços Profissionais"),
    "783": ("Recursos Humanos",                      "Serviços Profissionais"),
    # Serviços Locais (77, 79-82)
    "771": ("Serviços de Apoio Administrativo",      "Serviços Locais"),
    "772": ("Reparação e Manutenção",                "Serviços Locais"),
    "773": ("Serviços de Apoio Administrativo",      "Serviços Locais"),
    "774": ("Serviços de Apoio Administrativo",      "Serviços Locais"),
    "775": ("Serviços de Apoio Administrativo",      "Serviços Locais"),
    "791": ("Agências de Viagem e Turismo",          "Serviços Locais"),
    "799": ("Agências de Viagem e Turismo",          "Serviços Locais"),
    "801": ("Segurança Privada",                     "Serviços Locais"),
    "802": ("Segurança Privada",                     "Serviços Locais"),
    "803": ("Segurança Privada",                     "Serviços Locais"),
    "811": ("Limpeza e Higienização",                "Serviços Locais"),
    "812": ("Limpeza e Higienização",                "Serviços Locais"),
    "813": ("Limpeza e Higienização",                "Serviços Locais"),
    "821": ("Serviços de Apoio Administrativo",      "Serviços Locais"),
    "822": ("Serviços de Apoio Administrativo",      "Serviços Locais"),
    "823": ("Serviços de Apoio Administrativo",      "Serviços Locais"),
    "829": ("Serviços de Apoio Administrativo",      "Serviços Locais"),
    # Setor Público e Associações (84, 94)
    "841": ("Administração Pública",                 "Setor Público e Associações"),
    "842": ("Administração Pública",                 "Setor Público e Associações"),
    "843": ("Administração Pública",                 "Setor Público e Associações"),
    "941": ("Associações e Sindicatos",              "Setor Público e Associações"),
    "942": ("Associações e Sindicatos",              "Setor Público e Associações"),
    "943": ("ONGs e Fundações",                      "Setor Público e Associações"),
    "944": ("Organizações Religiosas",               "Setor Público e Associações"),
    "949": ("ONGs e Fundações",                      "Setor Público e Associações"),
    # Educação e Treinamento (85)
    "851": ("Ensino Básico",                         "Educação e Treinamento"),
    "852": ("Ensino Básico",                         "Educação e Treinamento"),
    "853": ("Ensino Básico",                         "Educação e Treinamento"),
    "854": ("Ensino Superior",                       "Educação e Treinamento"),
    "855": ("Ensino Profissionalizante e Cursos",    "Educação e Treinamento"),
    "856": ("Treinamento Corporativo",               "Educação e Treinamento"),
    "859": ("Cursos Livres e Idiomas",               "Educação e Treinamento"),
    # Saúde e Bem-estar (86-88)
    "861": ("Hospitais e Clínicas Gerais",           "Saúde e Bem-estar"),
    "862": ("Hospitais e Clínicas Gerais",           "Saúde e Bem-estar"),
    "863": ("Consultórios Médicos",                  "Saúde e Bem-estar"),
    "864": ("Laboratórios e Diagnóstico",            "Saúde e Bem-estar"),
    "865": ("Fisioterapia e Reabilitação",           "Saúde e Bem-estar"),
    "869": ("Consultórios Médicos",                  "Saúde e Bem-estar"),
    "871": ("Assistência Social",                    "Saúde e Bem-estar"),
    "872": ("Saúde Mental",                          "Saúde e Bem-estar"),
    "873": ("Assistência Social",                    "Saúde e Bem-estar"),
    "879": ("Assistência Social",                    "Saúde e Bem-estar"),
    "881": ("Assistência Social",                    "Saúde e Bem-estar"),
    "889": ("Assistência Social",                    "Saúde e Bem-estar"),
    # Serviços Locais — Arte, Esportes e Entretenimento (90-93)
    "900": ("Arte e Cultura",                        "Serviços Locais"),
    "910": ("Arte e Cultura",                        "Serviços Locais"),
    "911": ("Arte e Cultura",                        "Serviços Locais"),
    "912": ("Arte e Cultura",                        "Serviços Locais"),
    "920": ("Apostas e Loteria",                     "Serviços Locais"),
    "931": ("Academias e Esportes",                  "Serviços Locais"),
    "932": ("Entretenimento e Lazer",                "Serviços Locais"),
    "933": ("Academias e Esportes",                  "Serviços Locais"),
    "939": ("Entretenimento e Lazer",                "Serviços Locais"),
    # Serviços Locais — Reparação e Serviços Pessoais (95-96)
    "951": ("Reparação e Manutenção",                "Serviços Locais"),
    "952": ("Reparação e Manutenção",                "Serviços Locais"),
    "960": ("Beleza e Estética",                     "Serviços Locais"),
    "961": ("Beleza e Estética",                     "Serviços Locais"),
    "962": ("Beleza e Estética",                     "Serviços Locais"),
    "963": ("Beleza e Estética",                     "Serviços Locais"),
    "969": ("Beleza e Estética",                     "Serviços Locais"),
}

# Método fallback: padrões de raiz textual para CNAEs salvos como descrição.
# Ordem importa: mais específicos primeiro para evitar falso-positivo.
# Cada entrada: (padrão_raiz_lowercase, (departamento, macro_setor))
_DEPARTAMENTOS_TEXTO = [
    # Jurídico
    ("advoca",         ("Advogados e Escritórios Jurídicos",  "Jurídico")),
    ("juridic",        ("Advogados e Escritórios Jurídicos",  "Jurídico")),
    ("cartório",       ("Cartórios e Registros",              "Jurídico")),
    ("cartorio",       ("Cartórios e Registros",              "Jurídico")),
    ("contab",         ("Contabilidade e Auditoria",          "Jurídico")),
    ("auditoria",      ("Contabilidade e Auditoria",          "Jurídico")),
    ("escrit",         ("Contabilidade e Auditoria",          "Jurídico")),
    # Saúde e Bem-estar
    ("hospital",       ("Hospitais e Clínicas Gerais",        "Saúde e Bem-estar")),
    ("clinica",        ("Hospitais e Clínicas Gerais",        "Saúde e Bem-estar")),
    ("odontol",        ("Odontologia",                        "Saúde e Bem-estar")),
    ("dentar",         ("Odontologia",                        "Saúde e Bem-estar")),
    ("laborat",        ("Laboratórios e Diagnóstico",         "Saúde e Bem-estar")),
    ("fisioter",       ("Fisioterapia e Reabilitação",        "Saúde e Bem-estar")),
    ("psicolog",       ("Saúde Mental",                       "Saúde e Bem-estar")),
    ("medic",          ("Consultórios Médicos",               "Saúde e Bem-estar")),
    ("assist social",  ("Assistência Social",                 "Saúde e Bem-estar")),
    # Varejo de Saúde e Farmácias (47) — antes de "saude" genérico
    ("farmac",         ("Varejo de Saúde e Farmácias",        "Comércio Atacado e Varejo")),
    ("drogari",        ("Varejo de Saúde e Farmácias",        "Comércio Atacado e Varejo")),
    # Alimentação e Hospitalidade
    ("restaur",        ("Restaurantes e Lanchonetes",         "Alimentação e Hospitalidade")),
    ("lanchon",        ("Restaurantes e Lanchonetes",         "Alimentação e Hospitalidade")),
    ("padaria",        ("Restaurantes e Lanchonetes",         "Alimentação e Hospitalidade")),
    ("pizzar",         ("Restaurantes e Lanchonetes",         "Alimentação e Hospitalidade")),
    ("churrascar",     ("Restaurantes e Lanchonetes",         "Alimentação e Hospitalidade")),
    ("buffet",         ("Delivery e Catering",                "Alimentação e Hospitalidade")),
    ("catering",       ("Delivery e Catering",                "Alimentação e Hospitalidade")),
    ("hotel",          ("Hotéis e Pousadas",                  "Alimentação e Hospitalidade")),
    ("pousada",        ("Hostels e Hospedagem Alternativa",   "Alimentação e Hospitalidade")),
    ("alojament",      ("Hostels e Hospedagem Alternativa",   "Alimentação e Hospitalidade")),
    # Tecnologia e Marketing
    ("softwar",        ("Desenvolvimento de Software",        "Tecnologia e Marketing")),
    ("sistem",         ("Desenvolvimento de Software",        "Tecnologia e Marketing")),
    ("tecnolog",       ("Infraestrutura e Cloud",             "Tecnologia e Marketing")),
    ("internet",       ("Telecomunicações",                   "Tecnologia e Marketing")),
    ("telecom",        ("Telecomunicações",                   "Tecnologia e Marketing")),
    ("publicidad",     ("Marketing e Publicidade",            "Tecnologia e Marketing")),
    ("marketing",      ("Marketing e Publicidade",            "Tecnologia e Marketing")),
    ("design",         ("Criação e Design",                   "Tecnologia e Marketing")),
    ("fotograf",       ("Criação e Design",                   "Tecnologia e Marketing")),
    ("audiovisual",    ("Criação e Design",                   "Tecnologia e Marketing")),
    # Educação e Treinamento
    ("escola",         ("Ensino Básico",                      "Educação e Treinamento")),
    ("colegio",        ("Ensino Básico",                      "Educação e Treinamento")),
    ("faculdad",       ("Ensino Superior",                    "Educação e Treinamento")),
    ("universid",      ("Ensino Superior",                    "Educação e Treinamento")),
    ("curso",          ("Cursos Livres e Idiomas",            "Educação e Treinamento")),
    ("treinam",        ("Treinamento Corporativo",            "Educação e Treinamento")),
    ("idioma",         ("Cursos Livres e Idiomas",            "Educação e Treinamento")),
    # Construção e Infraestrutura
    ("constru",        ("Construção Civil e Edifícios",       "Construção e Infraestrutura")),
    ("incorpor",       ("Construção Civil e Edifícios",       "Construção e Infraestrutura")),
    ("reform",         ("Serviços Especializados de Construção","Construção e Infraestrutura")),
    ("instalac",       ("Serviços Especializados de Construção","Construção e Infraestrutura")),
    ("eletricist",     ("Serviços Especializados de Construção","Construção e Infraestrutura")),
    ("encanador",      ("Serviços Especializados de Construção","Construção e Infraestrutura")),
    # Imobiliário
    ("imobiliar",      ("Locação e Gestão de Imóveis",        "Imobiliário")),
    ("imovel",         ("Locação e Gestão de Imóveis",        "Imobiliário")),
    ("locac",          ("Locação e Gestão de Imóveis",        "Imobiliário")),
    # Financeiro
    ("banco",          ("Bancos e Crédito",                   "Financeiro")),
    ("financ",         ("Bancos e Crédito",                   "Financeiro")),
    ("seguro",         ("Seguros e Previdência",              "Financeiro")),
    ("corretora",      ("Investimentos e Mercado de Capitais","Financeiro")),
    ("previdenc",      ("Seguros e Previdência",              "Financeiro")),
    # Logística e Transporte
    ("transpor",       ("Transporte Rodoviário de Cargas",    "Logística e Transporte")),
    ("logistic",       ("Armazenagem e Logística",            "Logística e Transporte")),
    ("frete",          ("Transporte Rodoviário de Cargas",    "Logística e Transporte")),
    ("entrega",        ("Correio e Entregas",                 "Logística e Transporte")),
    ("armazen",        ("Armazenagem e Logística",            "Logística e Transporte")),
    # Serviços Profissionais
    ("engenhari",      ("Arquitetura e Engenharia",           "Serviços Profissionais")),
    ("arquitet",       ("Arquitetura e Engenharia",           "Serviços Profissionais")),
    ("consultori",     ("Consultoria Empresarial e Gestão",   "Serviços Profissionais")),
    ("recrutam",       ("Recursos Humanos",                   "Serviços Profissionais")),
    ("selecao",        ("Recursos Humanos",                   "Serviços Profissionais")),
    # Serviços Locais
    ("beleza",         ("Beleza e Estética",                  "Serviços Locais")),
    ("estetica",       ("Beleza e Estética",                  "Serviços Locais")),
    ("salao",          ("Beleza e Estética",                  "Serviços Locais")),
    ("barbearia",      ("Beleza e Estética",                  "Serviços Locais")),
    ("academia",       ("Academias e Esportes",               "Serviços Locais")),
    ("limpeza",        ("Limpeza e Higienização",             "Serviços Locais")),
    ("vigilanc",       ("Segurança Privada",                  "Serviços Locais")),
    ("seguranca",      ("Segurança Privada",                  "Serviços Locais")),
    ("viagem",         ("Agências de Viagem e Turismo",       "Serviços Locais")),
    ("turism",         ("Agências de Viagem e Turismo",       "Serviços Locais")),
    ("manutenc",       ("Reparação e Manutenção",             "Serviços Locais")),
    ("reparo",         ("Reparação e Manutenção",             "Serviços Locais")),
    # Agro e Agronegócio
    ("agricol",        ("Agricultura e Cultivos",             "Agro e Agronegócio")),
    ("agropecuar",     ("Pecuária",                          "Agro e Agronegócio")),
    ("pecuar",         ("Pecuária",                          "Agro e Agronegócio")),
    ("veterinar",      ("Veterinária",                       "Agro e Agronegócio")),
    ("pet shop",       ("Veterinária",                       "Agro e Agronegócio")),
    ("pesca",          ("Pesca e Aquicultura",               "Agro e Agronegócio")),
    # Setor Público e Associações
    ("associac",       ("Associações e Sindicatos",           "Setor Público e Associações")),
    ("sindicat",       ("Associações e Sindicatos",           "Setor Público e Associações")),
    ("fundacao",       ("ONGs e Fundações",                   "Setor Público e Associações")),
    ("igreja",         ("Organizações Religiosas",            "Setor Público e Associações")),
    ("templo",         ("Organizações Religiosas",            "Setor Público e Associações")),
    # Comércio Atacado e Varejo (genérico — deve vir por último)
    ("supermercad",    ("Varejo de Alimentos e Supermercados","Comércio Atacado e Varejo")),
    ("comercio",       ("Outros Comércios Varejistas",        "Comércio Atacado e Varejo")),
    ("atacado",        ("Atacado de Outros Produtos",         "Comércio Atacado e Varejo")),
]


def cnae_para_departamento(cnae: str) -> tuple:
    """
    Retorna (departamento, macro_setor) a partir do código CNAE.
    Método primário: prefixo de 3 dígitos → _CNAE_GRUPOS.
    Fallback: padrões textuais normalizados → _DEPARTAMENTOS_TEXTO.
    """
    cnae = (cnae or "").strip()
    if not cnae:
        return ("Outros", "Outros")
    if len(cnae) >= 3 and cnae[:3].isdigit():
        resultado = _CNAE_GRUPOS.get(cnae[:3])
        if resultado:
            return resultado
        return ("Outros", "Outros")
    cnae_norm = _norm(cnae)
    for padrao, resultado in _DEPARTAMENTOS_TEXTO:
        if padrao in cnae_norm:
            return resultado
    return ("Outros", "Outros")


def cnae_para_categoria(cnae: str) -> str:
    """Retorna o macro-setor a partir do código CNAE. Wrapper de cnae_para_departamento."""
    return cnae_para_departamento(cnae)[1]


# Conjunto de macro-setores válidos da versão atual.
# Usado por migrar_categorias_faltantes() para pular registros já corretos.
_MACRO_SETORES_VALIDOS = frozenset({
    "Agro e Agronegócio", "Indústria e Produção", "Energia e Utilities",
    "Construção e Infraestrutura", "Comércio Atacado e Varejo", "Logística e Transporte",
    "Alimentação e Hospitalidade", "Tecnologia e Marketing", "Financeiro",
    "Imobiliário", "Jurídico", "Serviços Profissionais", "Serviços Locais",
    "Educação e Treinamento", "Saúde e Bem-estar", "Setor Público e Associações",
    "Outros",
})


class Database:

    # ─── Planos ────────────────────────────────────────────────────
    PLANOS = {
        "free":   {"limite_dia": 10,  "export": False, "api": False, "nome": "Gratuito"},
        "basico": {"limite_dia": 500, "export": True,  "api": False, "nome": "Básico"},
        "pro":    {"limite_dia": None,"export": True,  "api": True,  "nome": "Pro"},
    }

    def criar_tabela_tokens(self):
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    token      TEXT PRIMARY KEY,
                    plano      TEXT NOT NULL DEFAULT 'free',
                    cnpjs_hoje INTEGER NOT NULL DEFAULT 0,
                    data_reset TEXT,
                    ativo      BOOLEAN NOT NULL DEFAULT TRUE,
                    criado_em  TEXT
                )
            """)
            conn.commit()
        # Migrate: add email/password/nome columns if missing
        self._migrar_colunas_auth()

    def _migrar_colunas_auth(self):
        with _conn() as conn:
            cur = conn.cursor()
            if USE_POSTGRES:
                cur.execute("ALTER TABLE tokens ADD COLUMN IF NOT EXISTS email TEXT UNIQUE")
                cur.execute("ALTER TABLE tokens ADD COLUMN IF NOT EXISTS password_hash TEXT")
                cur.execute("ALTER TABLE tokens ADD COLUMN IF NOT EXISTS nome TEXT")
            else:
                for col, defn in [("email", "TEXT UNIQUE"), ("password_hash", "TEXT"), ("nome", "TEXT")]:
                    try:
                        cur.execute(f"ALTER TABLE tokens ADD COLUMN {col} {defn}")
                    except Exception:
                        conn.rollback()
            conn.commit()

    def criar_conta_email(self, email: str, password: str, nome: str) -> str:
        from passlib.hash import bcrypt as _bcrypt
        email = email.strip().lower()
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT token FROM tokens WHERE email = {PH}", (email,))
            if cur.fetchone():
                raise ValueError("E-mail já cadastrado")
        token = secrets.token_urlsafe(32)
        pw_hash = _bcrypt.hash(password.encode()[:72].decode("utf-8", errors="ignore"))
        agora = datetime.utcnow().isoformat()
        hoje  = str(date_type.today())
        with _conn() as conn:
            cur = conn.cursor()
            if USE_POSTGRES:
                cur.execute("""
                    INSERT INTO tokens (token, plano, cnpjs_hoje, data_reset, ativo, criado_em, email, password_hash, nome)
                    VALUES (%s, 'free', 0, %s, TRUE, %s, %s, %s, %s)
                """, (token, hoje, agora, email, pw_hash, nome))
            else:
                cur.execute("""
                    INSERT INTO tokens (token, plano, cnpjs_hoje, data_reset, ativo, criado_em, email, password_hash, nome)
                    VALUES (?, 'free', 0, ?, 1, ?, ?, ?, ?)
                """, (token, hoje, agora, email, pw_hash, nome))
            conn.commit()
        return token

    def login_email(self, email: str, password: str) -> str | None:
        from passlib.hash import bcrypt as _bcrypt
        email = email.strip().lower()
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT token, password_hash, ativo FROM tokens WHERE email = {PH}", (email,))
            row = cur.fetchone()
        if not row:
            return None
        token, pw_hash, ativo = row[0], row[1], row[2]
        if not ativo or not pw_hash:
            return None
        if not _bcrypt.verify(password.encode()[:72].decode("utf-8", errors="ignore"), pw_hash):
            return None
        return token

    def criar_token(self, token: str, plano: str = "free") -> dict:
        """Cria um novo token com plano especificado."""
        agora = datetime.utcnow().isoformat()
        hoje  = str(date_type.today())
        with _conn() as conn:
            cur = conn.cursor()
            if USE_POSTGRES:
                cur.execute("""
                    INSERT INTO tokens (token, plano, cnpjs_hoje, data_reset, ativo, criado_em)
                    VALUES (%s, %s, 0, %s, TRUE, %s)
                    ON CONFLICT (token) DO UPDATE SET plano = EXCLUDED.plano, ativo = TRUE
                """, (token, plano, hoje, agora))
            else:
                cur.execute("""
                    INSERT OR REPLACE INTO tokens (token, plano, cnpjs_hoje, data_reset, ativo, criado_em)
                    VALUES (?, ?, 0, ?, 1, ?)
                """, (token, plano, hoje, agora))
            conn.commit()
        return {"token": token, "plano": plano}

    def verificar_token_db(self, token: str) -> dict | None:
        """
        Verifica token no banco. Retorna dict com plano e uso, ou None se inválido.
        Reseta o contador diário automaticamente quando a data muda.
        Não incrementa — use consumir_quota() para isso.
        """
        hoje = str(date_type.today())
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT plano, cnpjs_hoje, data_reset, ativo FROM tokens WHERE token = {PH}", (token,))
            row = cur.fetchone()
            if not row:
                return None

            plano, cnpjs_hoje, data_reset, ativo = row[0], row[1], row[2], row[3]

            if not ativo:
                return None

            # Reset diário automático
            if data_reset != hoje:
                cur.execute(
                    f"UPDATE tokens SET cnpjs_hoje = 0, data_reset = {PH} WHERE token = {PH}",
                    (hoje, token)
                )
                conn.commit()
                cnpjs_hoje = 0

            info_plano = self.PLANOS.get(plano, self.PLANOS["free"])
            limite     = info_plano["limite_dia"]

            return {
                "token":       token,
                "plano":       plano,
                "nome_plano":  info_plano["nome"],
                "cnpjs_hoje":  cnpjs_hoje,
                "limite_dia":  limite,
                "restante":    (limite - cnpjs_hoje) if limite is not None else None,
                "export":      info_plano["export"],
                "api":         info_plano["api"],
                "limite_atingido": limite is not None and cnpjs_hoje >= limite,
            }

    def consumir_quota_atomico(self, token: str, quantidade: int, limite) -> bool:
        """Incrementa o contador diário apenas se ainda dentro do limite.

        Retorna True se consumido, False se o limite já foi atingido.
        A verificação e o incremento ocorrem na mesma operação SQL — sem race condition.
        """
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"UPDATE tokens SET cnpjs_hoje = cnpjs_hoje + {PH} "
                f"WHERE token = {PH} AND ({PH} IS NULL OR cnpjs_hoje + {PH} <= {PH})",
                (quantidade, token, limite, quantidade, limite),
            )
            conn.commit()
            return cur.rowcount > 0

    def listar_tokens(self) -> list:
        """Lista todos os tokens (painel admin)."""
        hoje = str(date_type.today())
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT token, plano, cnpjs_hoje, data_reset, ativo, criado_em
                FROM tokens ORDER BY criado_em DESC
            """)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            for r in rows:
                info = self.PLANOS.get(r["plano"], self.PLANOS["free"])
                r["nome_plano"] = info["nome"]
                r["limite_dia"] = info["limite_dia"]
                # Reset contador se for de outro dia
                if r["data_reset"] != hoje:
                    r["cnpjs_hoje"] = 0
            return rows

    def excluir_token(self, token: str):
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"DELETE FROM tokens WHERE token = {PH}", (token,))
            conn.commit()

    def criar_tabelas(self):
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS empresas (
                    cnpj             TEXT PRIMARY KEY,
                    razao_social     TEXT,
                    nome_fantasia    TEXT,
                    porte            TEXT,
                    cnae             TEXT,
                    situacao         TEXT,
                    abertura         TEXT,
                    municipio        TEXT,
                    uf               TEXT,
                    socio_principal  TEXT,
                    telefone         TEXT,
                    email            TEXT,
                    instagram        TEXT,
                    site             TEXT,
                    rating_google    TEXT,
                    avaliacoes       TEXT,
                    atualizado_em    TEXT,
                    categoria_padrao TEXT,
                    qualidade_contato TEXT DEFAULT 'media',
                    departamento     TEXT
                )
            """)
            # Adiciona colunas em bancos já existentes (idempotente)
            if USE_POSTGRES:
                cur.execute("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS categoria_padrao TEXT")
                cur.execute("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS qualidade_contato TEXT DEFAULT 'media'")
                cur.execute("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS departamento TEXT")
            else:
                try:
                    cur.execute("ALTER TABLE empresas ADD COLUMN categoria_padrao TEXT")
                except Exception:
                    conn.rollback()
                try:
                    cur.execute("ALTER TABLE empresas ADD COLUMN qualidade_contato TEXT DEFAULT 'media'")
                except Exception:
                    conn.rollback()
                try:
                    cur.execute("ALTER TABLE empresas ADD COLUMN departamento TEXT")
                except Exception:
                    conn.rollback()

            for idx in ["uf", "porte", "email", "cnae", "abertura", "atualizado_em", "categoria_padrao", "departamento"]:
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{idx} ON empresas({idx})")

            # Índices parciais para os filtros de "tem contato" — só PostgreSQL
            if USE_POSTGRES:
                for col in ("telefone", "email", "instagram", "site"):
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_tem_{col} "
                        f"ON empresas({col}) WHERE {col} IS NOT NULL AND {col} != ''"
                    )
            conn.commit()

    def criar_tabela_progresso(self):
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agente_progresso (
                    id      INTEGER PRIMARY KEY,
                    posicao INTEGER NOT NULL DEFAULT 0,
                    updated TEXT
                )
            """)
            # Só insere linha inicial se ainda não existir — preserva progresso salvo
            if USE_POSTGRES:
                cur.execute("""
                    INSERT INTO agente_progresso (id, posicao, updated)
                    VALUES (1, 0, NOW()::TEXT)
                    ON CONFLICT (id) DO NOTHING
                """)
            else:
                cur.execute("""
                    INSERT OR IGNORE INTO agente_progresso (id, posicao, updated)
                    VALUES (1, 0, datetime('now'))
                """)
            conn.commit()

    def reset_completo(self):
        """Apaga todas as empresas e zera o progresso do agente."""
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM empresas")
            cur.execute("DELETE FROM agente_progresso")
            if USE_POSTGRES:
                cur.execute("""
                    INSERT INTO agente_progresso (id, posicao, updated)
                    VALUES (1, 0, NOW()::TEXT)
                    ON CONFLICT (id) DO UPDATE SET posicao = 0, updated = NOW()::TEXT
                """)
            else:
                cur.execute("""
                    INSERT OR REPLACE INTO agente_progresso (id, posicao, updated)
                    VALUES (1, 0, datetime('now'))
                """)
            conn.commit()

    def salvar_progresso(self, offset: int):
        with _conn() as conn:
            cur = conn.cursor()
            if USE_POSTGRES:
                cur.execute(
                    "UPDATE agente_progresso SET posicao = %s, updated = NOW()::TEXT WHERE id = 1",
                    (offset,)
                )
            else:
                cur.execute(
                    "UPDATE agente_progresso SET posicao = ?, updated = datetime('now') WHERE id = 1",
                    (offset,)
                )
            conn.commit()

    def carregar_progresso(self) -> int:
        try:
            with _conn() as conn:
                cur = conn.cursor()
                cur.execute("SELECT posicao FROM agente_progresso WHERE id = 1")
                row = cur.fetchone()
                if row:
                    return int(row[0])
        except Exception as e:
            print(f"Erro ao carregar progresso: {e}")
        return 0

    def salvar_empresa(self, perfil: dict):
        # Campos de contato: só atualiza se o novo valor NÃO for vazio.
        # Isso evita que o REENRICH apague dados existentes quando a nova
        # passagem não encontra o contato (ex: telefone que já estava salvo).
        _coalesce_pg = "CASE WHEN EXCLUDED.{f} != '' THEN EXCLUDED.{f} ELSE empresas.{f} END"
        _coalesce_sq = "CASE WHEN excluded.{f} != '' THEN excluded.{f} ELSE empresas.{f} END"

        contatos = ("telefone", "email", "instagram", "site", "rating_google", "avaliacoes", "socio_principal")

        def _sets_pg():
            fixos = ["razao_social","nome_fantasia","porte","cnae","situacao",
                     "municipio","uf","atualizado_em","categoria_padrao",
                     "qualidade_contato","departamento"]
            partes = [f"{f}=EXCLUDED.{f}" for f in fixos]
            partes += [f"{f}={_coalesce_pg.format(f=f)}" for f in contatos]
            return ", ".join(partes)

        def _sets_sq():
            fixos = ["razao_social","nome_fantasia","porte","cnae","situacao",
                     "municipio","uf","atualizado_em","categoria_padrao",
                     "qualidade_contato","departamento"]
            partes = [f"{f}=excluded.{f}" for f in fixos]
            partes += [f"{f}={_coalesce_sq.format(f=f)}" for f in contatos]
            return ", ".join(partes)

        # Deriva categoria_padrao e departamento a partir do CNAE
        cnae_val = perfil.get("cnae", "")
        depto, cat = cnae_para_departamento(cnae_val)
        if perfil.get("categoria_padrao"):
            cat = perfil["categoria_padrao"]
        if perfil.get("departamento"):
            depto = perfil["departamento"]
        qualidade = perfil.get("qualidade_contato", "media")

        sql_pg = f"""
            INSERT INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em, categoria_padrao,
             qualidade_contato, departamento)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (cnpj) DO UPDATE SET {_sets_pg()}
        """
        sql_sq = f"""
            INSERT INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em, categoria_padrao,
             qualidade_contato, departamento)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cnpj) DO UPDATE SET {_sets_sq()}
        """
        valores = (
            perfil.get("cnpj"), perfil.get("razao_social"), perfil.get("nome_fantasia"),
            perfil.get("porte"), perfil.get("cnae"), perfil.get("situacao"),
            perfil.get("abertura"), perfil.get("municipio"), perfil.get("uf"),
            perfil.get("socio_principal"), perfil.get("telefone",""), perfil.get("email",""),
            perfil.get("instagram",""), perfil.get("site",""), perfil.get("rating_google",""),
            perfil.get("avaliacoes",""), perfil.get("atualizado_em"), cat, qualidade, depto,
        )
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(sql_pg if USE_POSTGRES else sql_sq, valores)
            conn.commit()

    def salvar_empresas_batch(self, perfis: list):
        """
        Salva múltiplas empresas em uma única transação.
        Usa executemany para reduzir overhead de conexão/commit.
        """
        if not perfis:
            return

        _coalesce_pg = "CASE WHEN EXCLUDED.{f} != '' THEN EXCLUDED.{f} ELSE empresas.{f} END"
        _coalesce_sq = "CASE WHEN excluded.{f} != '' THEN excluded.{f} ELSE empresas.{f} END"
        contatos = ("telefone", "email", "instagram", "site", "rating_google", "avaliacoes", "socio_principal")
        fixos = ["razao_social","nome_fantasia","porte","cnae","situacao",
                 "municipio","uf","atualizado_em","categoria_padrao",
                 "qualidade_contato","departamento"]

        sets_pg = ", ".join(
            [f"{f}=EXCLUDED.{f}" for f in fixos] +
            [f"{f}={_coalesce_pg.format(f=f)}" for f in contatos]
        )
        sets_sq = ", ".join(
            [f"{f}=excluded.{f}" for f in fixos] +
            [f"{f}={_coalesce_sq.format(f=f)}" for f in contatos]
        )

        sql_pg = f"""
            INSERT INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em, categoria_padrao,
             qualidade_contato, departamento)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (cnpj) DO UPDATE SET {sets_pg}
        """
        sql_sq = f"""
            INSERT INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em, categoria_padrao,
             qualidade_contato, departamento)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cnpj) DO UPDATE SET {sets_sq}
        """

        rows = []
        for perfil in perfis:
            cnae_val = perfil.get("cnae", "")
            depto, cat = cnae_para_departamento(cnae_val)
            if perfil.get("categoria_padrao"):
                cat = perfil["categoria_padrao"]
            if perfil.get("departamento"):
                depto = perfil["departamento"]
            qualidade = perfil.get("qualidade_contato", "media")
            rows.append((
                perfil.get("cnpj"), perfil.get("razao_social"), perfil.get("nome_fantasia"),
                perfil.get("porte"), perfil.get("cnae"), perfil.get("situacao"),
                perfil.get("abertura"), perfil.get("municipio"), perfil.get("uf"),
                perfil.get("socio_principal"), perfil.get("telefone", ""), perfil.get("email", ""),
                perfil.get("instagram", ""), perfil.get("site", ""), perfil.get("rating_google", ""),
                perfil.get("avaliacoes", ""), perfil.get("atualizado_em"), cat, qualidade, depto,
            ))

        sql = sql_pg if USE_POSTGRES else sql_sq
        with _conn() as conn:
            cur = conn.cursor()
            cur.executemany(sql, rows)
            conn.commit()

    def buscar_telefone_salvo(self, cnpj: str) -> str:
        """Retorna o telefone salvo no banco para o CNPJ, ou '' se vazio/ausente."""
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT telefone FROM empresas WHERE cnpj = {PH}", (cnpj,))
            row = cur.fetchone()
            if row and row[0]:
                return str(row[0])
        return ""

    def cnpj_existe_recente(self, cnpj: str, dias: int = 30) -> bool:
        """
        Retorna True se o CNPJ já foi processado recentemente e deve ser pulado.
        TTL adaptativo:
        - Empresa COM telefone → pula por `dias` (padrão 30 dias)
        - Empresa SEM telefone → pula por apenas 3 dias (retenta mais cedo)
        """
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT atualizado_em, telefone FROM empresas WHERE cnpj = {PH}", (cnpj,))
            row = cur.fetchone()
            if not row or not row[0]:
                return False
            has_phone = telefone_valido(row[1])
            ttl = dias if has_phone else 3
            limite = (datetime.utcnow() - timedelta(days=ttl)).isoformat()
            return row[0] > limite

    def filtrar_cnpjs_recentes(self, cnpjs: list, dias: int = 30) -> set:
        """
        Retorna o SET de CNPJs que já foram processados recentemente e devem ser pulados.
        Usa uma única query IN(...) em vez de N queries individuais.
        TTL adaptativo: com telefone → `dias` dias, sem telefone → 3 dias.
        """
        if not cnpjs:
            return set()
        limite_com_tel = (datetime.utcnow() - timedelta(days=dias)).isoformat()
        limite_sem_tel = (datetime.utcnow() - timedelta(days=3)).isoformat()
        placeholders = ",".join([PH] * len(cnpjs))
        sql = f"SELECT cnpj, atualizado_em, telefone FROM empresas WHERE cnpj IN ({placeholders})"
        recentes = set()
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(sql, tuple(cnpjs))
            for row in cur.fetchall():
                cnpj_db, atualizado, tel = row[0], row[1], row[2]
                if not atualizado:
                    continue
                limite = limite_com_tel if telefone_valido(tel) else limite_sem_tel
                if atualizado > limite:
                    recentes.add(cnpj_db)
        return recentes

    def buscar_empresas(self, q="", uf="", porte="", cnae="", categoria="", departamento="",
                        abertura_de="", abertura_ate="",
                        com_email=False, com_instagram=False,
                        com_telefone=False, com_site=False,
                        com_contato=False,
                        pagina=1, por_pagina=50,
                        sort_by="razao_social", sort_dir="asc") -> dict:
        filtros = ["1=1"]
        params = []

        if q:
            filtros.append(
                f"(razao_social {LIKE} {PH} OR nome_fantasia {LIKE} {PH} "
                f"OR cnpj LIKE {PH} OR municipio {LIKE} {PH} OR socio_principal {LIKE} {PH})"
            )
            like = f"%{q}%"
            params.extend([like, like, like, like, like])
        if uf:
            filtros.append(f"uf = {PH}")
            params.append(uf.upper())
        if porte:
            filtros.append(f"porte {LIKE} {PH}")
            params.append(f"%{porte}%")
        if departamento:
            filtros.append(f"departamento = {PH}")
            params.append(departamento)
        elif categoria:
            filtros.append(f"categoria_padrao = {PH}")
            params.append(categoria)
        elif cnae:
            # Filtro direto por descrição CNAE (busca textual)
            filtros.append(f"cnae {LIKE} {PH}")
            params.append(f"%{cnae}%")
        if abertura_de:
            filtros.append(f"abertura >= {PH}")
            params.append(abertura_de)
        if abertura_ate:
            filtros.append(f"abertura <= {PH}")
            params.append(abertura_ate)
        if com_email:
            filtros.append("email IS NOT NULL AND email != ''")
        if com_instagram:
            filtros.append("instagram IS NOT NULL AND instagram != ''")
        _tel_valido_sql = (
            "telefone IS NOT NULL AND TRIM(telefone) != '' "
            "AND LOWER(telefone) NOT IN ('n/a','none','null','nan','-')"
        )
        if com_telefone:
            filtros.append(_tel_valido_sql)
        if com_site:
            filtros.append("site IS NOT NULL AND site != ''")
        if com_contato:
            # Critério mínimo: telefone válido obrigatório.
            filtros.append(_tel_valido_sql)

        where = " AND ".join(filtros)
        offset = (pagina - 1) * por_pagina

        _ALLOWED_SORT = {"razao_social", "cnpj", "porte", "municipio", "abertura", "atualizado_em"}
        if sort_by not in _ALLOWED_SORT:
            sort_by = "razao_social"
        direction = "ASC" if sort_dir.lower() != "desc" else "DESC"

        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM empresas WHERE {where}", params)
            total = cur.fetchone()[0]
            cur.execute(
                f"SELECT * FROM empresas WHERE {where} ORDER BY {sort_by} {direction} LIMIT {PH} OFFSET {PH}",
                params + [por_pagina, offset]
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        return {"total": total, "pagina": pagina, "por_pagina": por_pagina, "dados": rows}

    def buscar_empresa_por_cnpj(self, cnpj: str) -> dict | None:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT * FROM empresas WHERE cnpj = {PH}", (cnpj,))
            row = cur.fetchone()
            if not row:
                return None
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))

    def migrar_telefones_invalidos(self) -> int:
        """
        Converte telefones inválidos (' ', 'N/A', 'None', etc.) para NULL.
        Idempotente — seguro rodar múltiplas vezes.
        """
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE empresas SET telefone = NULL "
                "WHERE telefone IS NOT NULL AND ("
                "  TRIM(telefone) = '' "
                "  OR LOWER(telefone) IN ('n/a','none','null','nan','-','0','00000000')"
                ")"
            )
            total = cur.rowcount
            conn.commit()
        return total

    def migrar_categorias_faltantes(self) -> int:
        """
        Reclassifica apenas registros com categorias antigas ou inválidas.
        Também preenche departamento quando ausente.
        Processa em batches de 5.000. Idempotente.
        """
        placeholders = ",".join([PH] * len(_MACRO_SETORES_VALIDOS))
        sql_select = (
            f"SELECT cnpj, cnae FROM empresas "
            f"WHERE categoria_padrao IS NULL OR categoria_padrao = '' "
            f"OR categoria_padrao NOT IN ({placeholders}) "
            f"OR departamento IS NULL OR departamento = '' "
            f"LIMIT 5000"
        )
        sql_update = f"UPDATE empresas SET categoria_padrao = {PH}, departamento = {PH} WHERE cnpj = {PH}"
        total = 0
        while True:
            with _conn() as conn:
                cur = conn.cursor()
                cur.execute(sql_select, tuple(_MACRO_SETORES_VALIDOS))
                rows = cur.fetchall()
                if not rows:
                    break
                updates = []
                for cnpj, cnae in rows:
                    depto, cat = cnae_para_departamento(cnae)
                    updates.append((cat, depto, cnpj))
                cur.executemany(sql_update, updates)
                conn.commit()
                total += len(updates)
        return total

    def remigrar_departamentos(self) -> int:
        """
        Recalcula departamento e categoria_padrao para TODOS os registros com as
        regras atuais de cnae_para_departamento(). Idempotente. Batches de 5.000.
        Chamar apenas quando as regras de classificação mudarem.
        """
        sql_update = f"UPDATE empresas SET departamento = {PH}, categoria_padrao = {PH} WHERE cnpj = {PH}"
        total = 0
        offset = 0
        while True:
            with _conn() as conn:
                cur = conn.cursor()
                cur.execute(f"SELECT cnpj, cnae FROM empresas LIMIT 5000 OFFSET {offset}")
                rows = cur.fetchall()
            if not rows:
                break
            updates = []
            for cnpj, cnae in rows:
                depto, cat = cnae_para_departamento(cnae)
                updates.append((depto, cat, cnpj))
            with _conn() as conn:
                cur = conn.cursor()
                cur.executemany(sql_update, updates)
                conn.commit()
            total += len(rows)
            offset += 5000
        return total

    def listar_departamentos(self) -> list:
        """Retorna hierarquia macro_setor → departamentos com contagem (empresas com telefone)."""
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT categoria_padrao, departamento, COUNT(*) as n
                FROM empresas
                WHERE departamento IS NOT NULL AND departamento != ''
                  AND telefone IS NOT NULL AND telefone != ''
                GROUP BY categoria_padrao, departamento
                ORDER BY categoria_padrao, n DESC
            """)
            resultado = {}
            for setor, depto, n in cur.fetchall():
                resultado.setdefault(setor, []).append({"departamento": depto, "n": n})
            return [{"setor": k, "departamentos": v} for k, v in sorted(resultado.items())]

    def migrar_municipios(self) -> int:
        """
        Converte códigos numéricos de município (IBGE 7 dígitos ou RF CDUM 4 dígitos)
        para nomes de cidades. Processa em batches de 5.000.
        Idempotente — seguro rodar múltiplas vezes.
        """
        from data.ibge_municipios import MUNICIPIOS
        try:
            from data.rf_municipios import RF_MUNICIPIOS
        except ImportError:
            RF_MUNICIPIOS = {}
        # Combina os dois dicts; IBGE tem prioridade em caso de colisão
        combined = {**RF_MUNICIPIOS, **MUNICIPIOS}
        if USE_POSTGRES:
            sql_select = (
                "SELECT cnpj, municipio FROM empresas "
                "WHERE municipio ~ '^[0-9]{1,7}$' LIMIT 5000"
            )
        else:
            sql_select = (
                "SELECT cnpj, municipio FROM empresas "
                "WHERE municipio GLOB '[0-9]*' AND LENGTH(municipio) <= 7 LIMIT 5000"
            )
        sql_update = f"UPDATE empresas SET municipio = {PH} WHERE cnpj = {PH}"
        total = 0
        while True:
            with _conn() as conn:
                cur = conn.cursor()
                cur.execute(sql_select)
                rows = cur.fetchall()
                if not rows:
                    break
                updates = [
                    (combined.get(mun, mun), cnpj)
                    for cnpj, mun in rows
                    if mun and combined.get(mun, mun) != mun
                ]
                if updates:
                    cur.executemany(sql_update, updates)
                    conn.commit()
                    total += len(updates)
                else:
                    break
        return total

    def migrar_cnae(self) -> int:
        """
        Converte códigos CNAE numéricos brutos (ex: "1411801") para descrições
        textuais (ex: "CONFECÇÃO DE ROUPAS ÍNTIMAS"). Processa em batches de 5.000.
        Idempotente — seguro rodar múltiplas vezes.
        """
        try:
            from data.cnae_descricoes import CNAE_DESCRICOES
        except ImportError:
            return 0
        if USE_POSTGRES:
            sql_select = (
                "SELECT cnpj, cnae FROM empresas "
                "WHERE cnae ~ '^[0-9]{4,9}$' LIMIT 5000"
            )
        else:
            sql_select = (
                "SELECT cnpj, cnae FROM empresas "
                "WHERE cnae GLOB '[0-9]*' AND LENGTH(cnae) BETWEEN 4 AND 9 LIMIT 5000"
            )
        sql_update = f"UPDATE empresas SET cnae = {PH} WHERE cnpj = {PH}"
        total = 0
        while True:
            with _conn() as conn:
                cur = conn.cursor()
                cur.execute(sql_select)
                rows = cur.fetchall()
                if not rows:
                    break
                updates = [
                    (CNAE_DESCRICOES[cnae], cnpj)
                    for cnpj, cnae in rows
                    if cnae and cnae in CNAE_DESCRICOES
                ]
                if updates:
                    cur.executemany(sql_update, updates)
                    conn.commit()
                    total += len(updates)
                else:
                    break
        return total

    def listar_categorias(self) -> list:
        """Retorna macro-setores distintos presentes no banco (empresas com telefone)."""
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT categoria_padrao, COUNT(*) as n
                FROM empresas
                WHERE categoria_padrao IS NOT NULL AND categoria_padrao != ''
                  AND telefone IS NOT NULL AND telefone != ''
                GROUP BY categoria_padrao
                ORDER BY n DESC
            """)
            return [{"categoria": r[0], "n": r[1]} for r in cur.fetchall()]

    # Lista unificada de domínios de diretórios / listagens de CNPJ.
    # Usada tanto na limpeza do banco quanto como referência para o agente.
    _DOMINIOS_DIRETORIO = [
        # Diretórios brasileiros de CNPJ
        "%cadastroempresa%", "%cnpj.biz%", "%cnpj.ws%", "%cnpja.com%",
        "%cnpjativos%", "%infocnpj%", "%buscacnpj%", "%consulta-cnpj%",
        "%empresasdobrasil%", "%qsa.me%", "%econodata%", "%minhareceita%",
        "%receitaws%", "%casadosdados%", "%sintegra%",
        "%servicos.receita%", "%portaldatransparencia%",
        # Diretórios internacionais
        "%dnb.com%", "%opencorporates%", "%bloomberg.com/profile%",
        "%crunchbase%", "%zoominfo%", "%manta.com%", "%bizapedia%",
        # Listas e guias
        "%telelistas%", "%guiamais%", "%apontador%", "%yellowpages%",
        "%paginas.amarelas%", "%infobel%", "%hotfrog%",
        "%yelp.com%", "%tripadvisor%", "%foursquare%",
        # Jurídico / institucional
        "%escavador%", "%jusbrasil%",
        # Redes sociais (não são site oficial de empresa)
        "%linkedin.com%",
    ]

    def limpar_sites_diretorio(self) -> int:
        """
        Zera o campo 'site' de registros cujo site é um diretório de empresas
        e não o site real da empresa. Retorna o número de registros atualizados.
        """
        total = 0
        with _conn() as conn:
            cur = conn.cursor()
            for p in self._DOMINIOS_DIRETORIO:
                cur.execute(
                    f"UPDATE empresas SET site='' WHERE site {LIKE} {PH}",
                    (p,)
                )
                total += cur.rowcount
            conn.commit()
        return total

    def corrigir_porte_mei(self, cnpjs: list) -> int:
        """
        Atualiza porte para 'MEI' nos CNPJs informados. Retorna o número de registros atualizados.
        """
        if not cnpjs:
            return 0
        with _conn() as conn:
            cur = conn.cursor()
            placeholders = ",".join([PH] * len(cnpjs))
            cur.execute(
                f"UPDATE empresas SET porte={PH} WHERE cnpj IN ({placeholders})",
                ["MEI"] + list(cnpjs),
            )
            conn.commit()
            return cur.rowcount

    def listar_cnpjs_por_porte(self, porte_val: str, limite: int = 5000) -> list[str]:
        """Returns CNPJs where porte matches the given value (exact match, or IS NULL/empty)."""
        with _conn() as conn:
            cur = conn.cursor()
            if porte_val == "":
                cur.execute(
                    f"SELECT cnpj FROM empresas WHERE (porte IS NULL OR porte = '') LIMIT {int(limite)}"
                )
            else:
                cur.execute(f"SELECT cnpj FROM empresas WHERE porte = {PH} LIMIT {int(limite)}", [porte_val])
            return [r[0] for r in cur.fetchall()]

    def listar_cnaes(self) -> list:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT cnae, COUNT(*) as n
                FROM empresas
                WHERE cnae IS NOT NULL AND cnae != ''
                GROUP BY cnae
                ORDER BY n DESC
                LIMIT 100
            """)
            return [{"cnae": r[0], "n": r[1]} for r in cur.fetchall()]

    def buscar_cnpjs_sem_contato(self, limite: int = 5000, offset: int = 0) -> list:
        """
        Retorna CNPJs de empresas sem email E sem instagram.
        Site não é critério — pode ter site mas scraping não achou contato.
        """
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"""
                SELECT cnpj FROM empresas
                WHERE (email IS NULL OR email = '')
                  AND (instagram IS NULL OR instagram = '')
                ORDER BY atualizado_em ASC
                LIMIT {PH} OFFSET {PH}
            """, (limite, offset))
            return [row[0] for row in cur.fetchall()]

    def cnpjs_baixa_qualidade(self, limite: int = 500) -> list:
        """Retorna CNPJs com qualidade_contato='baixa', priorizando os mais antigos."""
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"SELECT cnpj FROM empresas WHERE qualidade_contato = 'baixa' "
                f"ORDER BY atualizado_em ASC LIMIT {PH}",
                (limite,)
            )
            return [row[0] for row in cur.fetchall()]

    def contar_sem_contato(self) -> int:
        """Conta CNPJs sem email E sem instagram."""
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM empresas
                WHERE (email IS NULL OR email = '')
                  AND (instagram IS NULL OR instagram = '')
            """)
            return cur.fetchone()[0]

    def estatisticas(self) -> dict:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM empresas")
            total = cur.fetchone()[0]   # total real no banco (com e sem telefone)
            cur.execute(
                "SELECT COUNT(*) FROM empresas WHERE telefone IS NOT NULL "
                "AND TRIM(telefone) != '' "
                "AND LOWER(telefone) NOT IN ('n/a','none','null','nan','-')"
            )
            com_tel = cur.fetchone()[0]  # empresas contactáveis (com telefone válido)
            cur.execute("SELECT COUNT(*) FROM empresas WHERE email IS NOT NULL AND email != ''")
            com_email = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE instagram IS NOT NULL AND instagram != ''")
            com_insta = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE site IS NOT NULL AND site != ''")
            com_site = cur.fetchone()[0]
            cur.execute("""
                SELECT uf, COUNT(*) as n FROM empresas
                WHERE telefone IS NOT NULL AND telefone != ''
                  AND uf IS NOT NULL AND uf != ''
                GROUP BY uf ORDER BY n DESC LIMIT 10
            """)
            por_uf = [{"uf": r[0], "n": r[1]} for r in cur.fetchall()]
            cur.execute("""
                SELECT porte, COUNT(*) as n FROM empresas
                WHERE telefone IS NOT NULL AND telefone != ''
                  AND porte IS NOT NULL AND porte != ''
                GROUP BY porte ORDER BY n DESC
            """)
            por_porte = [{"porte": r[0], "n": r[1]} for r in cur.fetchall()]
            progresso = 0
            try:
                cur.execute("SELECT posicao FROM agente_progresso WHERE id = 1")
                row = cur.fetchone()
                if row:
                    progresso = int(row[0])
            except Exception:
                pass
        return {
            "total": total,
            "com_telefone": com_tel,
            "com_email": com_email,
            "com_instagram": com_insta,
            "com_site": com_site,
            "por_uf": por_uf,
            "por_porte": por_porte,
            "progresso_agente": progresso,
        }

    def atividade_diaria(self, dias: int = 30) -> list:
        """Returns [{data, coletadas, enriquecidas}] for the last `dias` days, ascending."""
        from datetime import date, timedelta
        today = date.today()
        dates = [(today - timedelta(days=i)).isoformat() for i in range(dias - 1, -1, -1)]
        cutoff = (today - timedelta(days=dias)).isoformat()
        with _conn() as conn:
            cur = conn.cursor()
            if USE_POSTGRES:
                sql = """
                    SELECT DATE(atualizado_em::timestamp)::text AS d,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE telefone IS NOT NULL AND telefone != '') AS com_tel
                    FROM empresas
                    WHERE atualizado_em >= %s
                    GROUP BY 1 ORDER BY 1
                """
            else:
                sql = """
                    SELECT strftime('%Y-%m-%d', atualizado_em) AS d,
                           COUNT(*) AS total,
                           SUM(CASE WHEN telefone IS NOT NULL AND telefone != '' THEN 1 ELSE 0 END) AS com_tel
                    FROM empresas
                    WHERE atualizado_em >= ?
                    GROUP BY 1 ORDER BY 1
                """
            cur.execute(sql, (cutoff,))
            rows = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        return [{"data": d, "coletadas": rows.get(d, (0, 0))[0],
                 "enriquecidas": rows.get(d, (0, 0))[1]} for d in dates]

    def diagnostico_telefone(self) -> dict:
        """Retorna contagens e últimos registros para diagnóstico de persistência de telefone."""
        _tel_valido = (
            "telefone IS NOT NULL AND TRIM(telefone) != '' "
            "AND LOWER(telefone) NOT IN ('n/a','none','null','nan','-')"
        )
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM empresas")
            total = cur.fetchone()[0]
            cur.execute(f"SELECT COUNT(*) FROM empresas WHERE {_tel_valido}")
            com_telefone = cur.fetchone()[0]
            sem_telefone = total - com_telefone
            cur.execute(
                "SELECT cnpj, razao_social, telefone, atualizado_em "
                "FROM empresas ORDER BY atualizado_em DESC LIMIT 10"
            )
            cols = [d[0] for d in cur.description]
            ultimos_salvos = [dict(zip(cols, r)) for r in cur.fetchall()]
            cur.execute(
                f"SELECT cnpj, razao_social, telefone, atualizado_em "
                f"FROM empresas WHERE {_tel_valido} "
                f"ORDER BY atualizado_em DESC LIMIT 10"
            )
            ultimos_com_tel = [dict(zip(cols, r)) for r in cur.fetchall()]
        return {
            "total_empresas": total,
            "com_telefone": com_telefone,
            "sem_telefone": sem_telefone,
            "ultimos_salvos": ultimos_salvos,
            "ultimos_com_tel": ultimos_com_tel,
        }

    def vacuum(self):
        """
        Executa VACUUM ANALYZE no Postgres para liberar espaço após DELETE em massa.
        No SQLite não é necessário (o arquivo não encolhe automaticamente, mas o overhead é baixo).
        """
        if not USE_POSTGRES:
            return
        conn = psycopg2.connect(DATABASE_URL)
        conn.set_isolation_level(0)  # AUTOCOMMIT obrigatório — VACUUM não pode rodar em transação
        try:
            cur = conn.cursor()
            cur.execute("VACUUM ANALYZE empresas")
            cur.close()
        finally:
            conn.close()

    def limpar_sites_falsos(self) -> int:
        """Alias de limpar_sites_diretorio — mantido para compatibilidade."""
        return self.limpar_sites_diretorio()