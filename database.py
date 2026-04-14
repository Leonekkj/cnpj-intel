"""
Banco de dados — PostgreSQL (produção) ou SQLite (local).
Inclui persistência de progresso do agente.
"""

import os
from datetime import datetime, timedelta, date as date_type

DATABASE_URL = (
    os.environ.get("DATABASE_URL") or
    os.environ.get("DATABASE_PUBLIC_URL") or
    ""
)

print(f"DATABASE_URL detectado: {DATABASE_URL[:40] if DATABASE_URL else 'VAZIO - usando SQLite'}")

USE_POSTGRES = DATABASE_URL.startswith("postgresql") or DATABASE_URL.startswith("postgres")

if USE_POSTGRES:
    import psycopg2
    from psycopg2 import pool as _pg_pool
    from contextlib import contextmanager
    print("Conectando ao PostgreSQL com pool...")
    # Pool de conexões reutilizáveis: elimina overhead de TLS+auth a cada query
    _POOL = _pg_pool.ThreadedConnectionPool(minconn=2, maxconn=20, dsn=DATABASE_URL)

    @contextmanager
    def _conn():
        c = _POOL.getconn()
        try:
            yield c
        finally:
            try:
                _POOL.putconn(c)
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


# ─── Mapeamento CNAE → categoria padronizada ──────────────────────────────────
# Substring (lowercase) encontrado na descrição CNAE → nome da categoria.
# A primeira correspondência vence — ordene do mais específico para o mais geral.
CNAE_CATEGORIAS = {
    "advocat":                  "Advocacia",
    "contábi":                  "Contabilidade",
    "restaurante":              "Restaurantes",
    "padaria":                  "Padaria",
    "confeitaria":              "Padaria",
    "lanchonete":               "Lanchonetes",
    "mercearia":                "Mercearia",
    "minimercado":              "Mercearia",
    "supermercad":              "Supermercado",
    "hipermercad":              "Supermercado",
    "açougue":                  "Açougue",
    "abate":                    "Açougue",
    "odontol":                  "Odontologia",
    "farmáci":                  "Farmácias",
    "farmaci":                  "Farmácias",
    "drogari":                  "Farmácias",
    "veterinári":               "Veterinária",
    "veterinari":               "Veterinária",
    "fisioterapia":             "Fisioterapia",
    "psicolog":                 "Psicologia",
    "cabeleireiro":             "Salão de Beleza",
    "salão de beleza":          "Salão de Beleza",
    "manicure":                 "Salão de Beleza",
    "barbearia":                "Barbearia",
    "estétic":                  "Estética",
    "estetica":                 "Estética",
    "condicionamento físico":   "Academia",
    "academia":                 "Academia",
    "ginástica":                "Academia",
    "software":                 "Software",
    "desenvolvimento de sistem":"Software",
    "informátic":               "Informática",
    "computador":               "Informática",
    "telecomunicaç":            "Telecomunicações",
    "telecom":                  "Telecomunicações",
    "escola":                   "Educação",
    "ensino fundament":         "Educação",
    "ensino médio":             "Educação",
    "ensino superior":          "Educação",
    "curso":                    "Cursos e Treinamentos",
    "treinamento":              "Cursos e Treinamentos",
    "idioma":                   "Idiomas",
    "língua":                   "Idiomas",
    "imobiliár":                "Imobiliária",
    "imobiliari":               "Imobiliária",
    "engenhari":                "Engenharia",
    "consultori":               "Consultoria",
    "construtora":              "Construção",
    "construção de edifíc":     "Construção",
    "obras de albanearia":      "Construção",
    "instalação elétric":       "Elétrica",
    "eletricista":              "Elétrica",
    "instalações hidráulic":    "Hidráulica",
    "encanador":                "Hidráulica",
    "vestuári":                 "Vestuário",
    "confecç":                  "Vestuário",
    "calçado":                  "Calçados",
    "móveis":                   "Móveis",
    "moveleiro":                "Móveis",
    "eletrodoméstic":           "Eletrodomésticos",
    "eletrodomestic":           "Eletrodomésticos",
    "veículos automotores":     "Automóveis",
    "automóvei":                "Automóveis",
    "peças e acessórios":       "Automóveis",
    "combustível":              "Combustível",
    "posto de gasolina":        "Combustível",
    "transporte rodoviário de carga": "Transporte de Carga",
    "transporte de carga":      "Transporte de Carga",
    "transporte rodoviário de passageiro": "Transporte de Passageiros",
    "transporte de passageiro": "Transporte de Passageiros",
    "logístic":                 "Logística",
    "armazenamento":            "Logística",
    "agricultur":               "Agricultura",
    "pecuári":                  "Pecuária",
    "criação de":               "Pecuária",
    "pesca":                    "Pesca",
    "aquicultur":               "Pesca",
    # Saúde — genérico por último (após os específicos)
    "médico":                   "Saúde",
    "clínica":                  "Saúde",
    "hospital":                 "Saúde",
    "laboratori":               "Saúde",
}


def cnae_para_categoria(cnae: str) -> str:
    """Retorna a categoria padronizada a partir da descrição CNAE."""
    cnae_lower = (cnae or "").lower()
    for substr, cat in CNAE_CATEGORIAS.items():
        if substr in cnae_lower:
            return cat
    return ""


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

    def consumir_quota(self, token: str, quantidade: int = 1):
        """Incrementa o contador diário do token."""
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(
                f"UPDATE tokens SET cnpjs_hoje = cnpjs_hoje + {PH} WHERE token = {PH}",
                (quantidade, token)
            )
            conn.commit()

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
                    categoria_padrao TEXT
                )
            """)
            # Adiciona coluna categoria_padrao em bancos já existentes (idempotente)
            try:
                cur.execute("ALTER TABLE empresas ADD COLUMN categoria_padrao TEXT")
                conn.commit()
            except Exception:
                pass  # coluna já existe

            for idx in ["uf", "porte", "email", "cnae", "abertura", "atualizado_em", "categoria_padrao"]:
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

        contatos = ("telefone", "email", "instagram", "site", "rating_google", "avaliacoes")

        def _sets_pg():
            fixos = ["razao_social","nome_fantasia","porte","cnae","situacao",
                     "municipio","uf","socio_principal","atualizado_em","categoria_padrao"]
            partes = [f"{f}=EXCLUDED.{f}" for f in fixos]
            partes += [f"{f}={_coalesce_pg.format(f=f)}" for f in contatos]
            return ", ".join(partes)

        def _sets_sq():
            fixos = ["razao_social","nome_fantasia","porte","cnae","situacao",
                     "municipio","uf","socio_principal","atualizado_em","categoria_padrao"]
            partes = [f"{f}=excluded.{f}" for f in fixos]
            partes += [f"{f}={_coalesce_sq.format(f=f)}" for f in contatos]
            return ", ".join(partes)

        # Deriva categoria_padrao a partir do CNAE (se não vier no perfil)
        cat = perfil.get("categoria_padrao") or cnae_para_categoria(perfil.get("cnae", ""))

        sql_pg = f"""
            INSERT INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em, categoria_padrao)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (cnpj) DO UPDATE SET {_sets_pg()}
        """
        sql_sq = f"""
            INSERT INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em, categoria_padrao)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cnpj) DO UPDATE SET {_sets_sq()}
        """
        valores = (
            perfil.get("cnpj"), perfil.get("razao_social"), perfil.get("nome_fantasia"),
            perfil.get("porte"), perfil.get("cnae"), perfil.get("situacao"),
            perfil.get("abertura"), perfil.get("municipio"), perfil.get("uf"),
            perfil.get("socio_principal"), perfil.get("telefone",""), perfil.get("email",""),
            perfil.get("instagram",""), perfil.get("site",""), perfil.get("rating_google",""),
            perfil.get("avaliacoes",""), perfil.get("atualizado_em"), cat,
        )
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(sql_pg if USE_POSTGRES else sql_sq, valores)
            conn.commit()

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
            has_phone = bool(row[1])
            ttl = dias if has_phone else 3
            limite = (datetime.utcnow() - timedelta(days=ttl)).isoformat()
            return row[0] > limite

    def buscar_empresas(self, q="", uf="", porte="", cnae="", categoria="",
                        abertura_de="", abertura_ate="",
                        com_email=False, com_instagram=False,
                        com_telefone=False, com_site=False,
                        com_contato=True,
                        pagina=1, por_pagina=50) -> dict:
        filtros = ["1=1"]
        params = []

        if q:
            filtros.append(f"(razao_social {LIKE} {PH} OR nome_fantasia {LIKE} {PH} OR cnpj LIKE {PH} OR municipio {LIKE} {PH})")
            like = f"%{q}%"
            params.extend([like, like, like, like])
        if uf:
            filtros.append(f"uf = {PH}")
            params.append(uf.upper())
        if porte:
            filtros.append(f"porte {LIKE} {PH}")
            params.append(f"%{porte}%")
        if categoria:
            # Filtro por categoria padronizada (match exato — campo normalizado)
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
        if com_telefone:
            filtros.append("telefone IS NOT NULL AND telefone != ''")
        if com_site:
            filtros.append("site IS NOT NULL AND site != ''")
        if com_contato:
            # Critério mínimo: telefone obrigatório.
            # E-mail e Instagram serão adicionados futuramente.
            filtros.append("telefone IS NOT NULL AND telefone != ''")

        where = " AND ".join(filtros)
        offset = (pagina - 1) * por_pagina

        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM empresas WHERE {where}", params)
            total = cur.fetchone()[0]
            cur.execute(
                f"SELECT * FROM empresas WHERE {where} ORDER BY atualizado_em DESC LIMIT {PH} OFFSET {PH}",
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
            cur.execute("SELECT COUNT(*) FROM empresas WHERE telefone IS NOT NULL AND telefone != ''")
            com_tel = cur.fetchone()[0]  # empresas contactáveis (com telefone)
            cur.execute("SELECT COUNT(*) FROM empresas WHERE email IS NOT NULL AND email != ''")
            com_email = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE instagram IS NOT NULL AND instagram != ''")
            com_insta = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE site IS NOT NULL AND site != ''")
            com_site = cur.fetchone()[0]
            cur.execute("SELECT uf, COUNT(*) as n FROM empresas GROUP BY uf ORDER BY n DESC LIMIT 10")
            por_uf = [{"uf": r[0], "n": r[1]} for r in cur.fetchall()]
            cur.execute("SELECT porte, COUNT(*) as n FROM empresas GROUP BY porte ORDER BY n DESC")
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

    def limpar_sites_falsos(self) -> int:
        """Alias de limpar_sites_diretorio — mantido para compatibilidade."""
        return self.limpar_sites_diretorio()