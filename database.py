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
    print("Conectando ao PostgreSQL...")
    def _conn():
        return psycopg2.connect(DATABASE_URL)
else:
    import sqlite3
    print("Usando SQLite local")
    def _conn():
        conn = sqlite3.connect("cnpj_intel.db")
        conn.row_factory = sqlite3.Row
        return conn

PH = "%s" if USE_POSTGRES else "?"
LIKE = "ILIKE" if USE_POSTGRES else "LIKE"


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
                    cnpj            TEXT PRIMARY KEY,
                    razao_social    TEXT,
                    nome_fantasia   TEXT,
                    porte           TEXT,
                    cnae            TEXT,
                    situacao        TEXT,
                    abertura        TEXT,
                    municipio       TEXT,
                    uf              TEXT,
                    socio_principal TEXT,
                    telefone        TEXT,
                    email           TEXT,
                    instagram       TEXT,
                    site            TEXT,
                    rating_google   TEXT,
                    avaliacoes      TEXT,
                    atualizado_em   TEXT
                )
            """)
            for idx in ["uf", "porte", "email", "cnae", "abertura"]:
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{idx} ON empresas({idx})")
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
        sql_pg = """
            INSERT INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (cnpj) DO UPDATE SET
                razao_social=EXCLUDED.razao_social,
                nome_fantasia=EXCLUDED.nome_fantasia,
                porte=EXCLUDED.porte, cnae=EXCLUDED.cnae,
                situacao=EXCLUDED.situacao, municipio=EXCLUDED.municipio,
                uf=EXCLUDED.uf, socio_principal=EXCLUDED.socio_principal,
                telefone=EXCLUDED.telefone, email=EXCLUDED.email,
                instagram=EXCLUDED.instagram, site=EXCLUDED.site,
                rating_google=EXCLUDED.rating_google,
                avaliacoes=EXCLUDED.avaliacoes,
                atualizado_em=EXCLUDED.atualizado_em
        """
        sql_sq = """
            INSERT OR REPLACE INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        valores = (
            perfil.get("cnpj"), perfil.get("razao_social"), perfil.get("nome_fantasia"),
            perfil.get("porte"), perfil.get("cnae"), perfil.get("situacao"),
            perfil.get("abertura"), perfil.get("municipio"), perfil.get("uf"),
            perfil.get("socio_principal"), perfil.get("telefone"), perfil.get("email"),
            perfil.get("instagram"), perfil.get("site"), perfil.get("rating_google"),
            perfil.get("avaliacoes"), perfil.get("atualizado_em"),
        )
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(sql_pg if USE_POSTGRES else sql_sq, valores)
            conn.commit()

    def cnpj_existe_recente(self, cnpj: str, dias: int = 30) -> bool:
        limite = (datetime.utcnow() - timedelta(days=dias)).isoformat()
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT atualizado_em FROM empresas WHERE cnpj = {PH}", (cnpj,))
            row = cur.fetchone()
            if row and row[0] and row[0] > limite:
                return True
        return False

    def buscar_empresas(self, q="", uf="", porte="", cnae="",
                        abertura_de="", abertura_ate="",
                        com_email=False, com_instagram=False,
                        com_telefone=False, com_site=False,
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
        if cnae:
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

    def estatisticas(self) -> dict:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM empresas")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE telefone IS NOT NULL AND telefone != ''")
            com_tel = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE email IS NOT NULL AND email != ''")
            com_email = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE instagram IS NOT NULL AND instagram != ''")
            com_insta = cur.fetchone()[0]
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
            "por_uf": por_uf,
            "por_porte": por_porte,
            "progresso_agente": progresso,
        }
