"""
Banco de dados — suporta PostgreSQL (produção) e SQLite (local).
"""

import os
from datetime import datetime, timedelta

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:VgtRplEoWbmyjsUdOguUUTMjSWowdNvg@postgres.railway.internal:5432/railway"
)

# Detecta se é PostgreSQL ou SQLite
USE_POSTGRES = DATABASE_URL.startswith("postgresql") or DATABASE_URL.startswith("postgres")

if USE_POSTGRES:
    import psycopg2
    import psycopg2.extras

    def _conn():
        return psycopg2.connect(DATABASE_URL)
else:
    import sqlite3

    def _conn():
        conn = sqlite3.connect("cnpj_intel.db")
        conn.row_factory = sqlite3.Row
        return conn


class Database:
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_uf    ON empresas(uf)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_porte ON empresas(porte)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_email ON empresas(email)")
            conn.commit()

    def salvar_empresa(self, perfil: dict):
        sql = """
            INSERT INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (cnpj) DO UPDATE SET
                razao_social=EXCLUDED.razao_social,
                nome_fantasia=EXCLUDED.nome_fantasia,
                porte=EXCLUDED.porte,
                cnae=EXCLUDED.cnae,
                situacao=EXCLUDED.situacao,
                municipio=EXCLUDED.municipio,
                uf=EXCLUDED.uf,
                socio_principal=EXCLUDED.socio_principal,
                telefone=EXCLUDED.telefone,
                email=EXCLUDED.email,
                instagram=EXCLUDED.instagram,
                site=EXCLUDED.site,
                rating_google=EXCLUDED.rating_google,
                avaliacoes=EXCLUDED.avaliacoes,
                atualizado_em=EXCLUDED.atualizado_em
        """ if USE_POSTGRES else """
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
            cur.execute(sql, valores)
            conn.commit()

    def cnpj_existe_recente(self, cnpj: str, dias: int = 30) -> bool:
        limite = (datetime.utcnow() - timedelta(days=dias)).isoformat()
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT atualizado_em FROM empresas WHERE cnpj = %s" if USE_POSTGRES else
                        "SELECT atualizado_em FROM empresas WHERE cnpj = ?", (cnpj,))
            row = cur.fetchone()
            if row and row[0] and row[0] > limite:
                return True
        return False

    def buscar_empresas(self, q="", uf="", porte="", com_email=False,
                        com_instagram=False, pagina=1, por_pagina=50) -> dict:
        filtros = ["1=1"]
        params = []
        ph = "%s" if USE_POSTGRES else "?"

        if q:
            filtros.append(f"(razao_social ILIKE {ph} OR nome_fantasia ILIKE {ph} OR cnpj LIKE {ph} OR municipio ILIKE {ph})"
                          if USE_POSTGRES else
                          f"(razao_social LIKE {ph} OR nome_fantasia LIKE {ph} OR cnpj LIKE {ph} OR municipio LIKE {ph})")
            like = f"%{q}%"
            params.extend([like, like, like, like])
        if uf:
            filtros.append(f"uf = {ph}")
            params.append(uf.upper())
        if porte:
            filtros.append(f"porte = {ph}")
            params.append(porte.upper())
        if com_email:
            filtros.append("email != '' AND email IS NOT NULL")
        if com_instagram:
            filtros.append("instagram != '' AND instagram IS NOT NULL")

        where = " AND ".join(filtros)
        offset = (pagina - 1) * por_pagina

        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM empresas WHERE {where}", params)
            total = cur.fetchone()[0]
            cur.execute(
                f"SELECT * FROM empresas WHERE {where} ORDER BY atualizado_em DESC LIMIT {ph} OFFSET {ph}",
                params + [por_pagina, offset]
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]

        return {"total": total, "pagina": pagina, "por_pagina": por_pagina, "dados": rows}

    def estatisticas(self) -> dict:
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM empresas")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE telefone != '' AND telefone IS NOT NULL")
            com_tel = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE email != '' AND email IS NOT NULL")
            com_email = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM empresas WHERE instagram != '' AND instagram IS NOT NULL")
            com_insta = cur.fetchone()[0]
            cur.execute("SELECT uf, COUNT(*) as n FROM empresas GROUP BY uf ORDER BY n DESC LIMIT 10")
            por_uf = [{"uf": r[0], "n": r[1]} for r in cur.fetchall()]
            cur.execute("SELECT porte, COUNT(*) as n FROM empresas GROUP BY porte ORDER BY n DESC")
            por_porte = [{"porte": r[0], "n": r[1]} for r in cur.fetchall()]
        return {
            "total": total, "com_telefone": com_tel,
            "com_email": com_email, "com_instagram": com_insta,
            "por_uf": por_uf, "por_porte": por_porte,
        }
