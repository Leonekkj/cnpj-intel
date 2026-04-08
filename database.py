"""
Banco de dados SQLite para armazenar e consultar empresas enriquecidas.
Em produção, troque SQLite por PostgreSQL (apenas mude a connection string).
"""

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Optional


DB_PATH = "cnpj_intel.db"


class Database:
    def __init__(self, path: str = DB_PATH):
        self.path = path

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def criar_tabelas(self):
        with self._conn() as conn:
            conn.execute("""
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
            # índices para buscas rápidas
            conn.execute("CREATE INDEX IF NOT EXISTS idx_uf       ON empresas(uf)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_porte    ON empresas(porte)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_cnae     ON empresas(cnae)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_has_email ON empresas(email)")
            conn.commit()

    def salvar_empresa(self, perfil: dict):
        with self._conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO empresas
                (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
                 abertura, municipio, uf, socio_principal, telefone, email,
                 instagram, site, rating_google, avaliacoes, atualizado_em)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                perfil.get("cnpj"),
                perfil.get("razao_social"),
                perfil.get("nome_fantasia"),
                perfil.get("porte"),
                perfil.get("cnae"),
                perfil.get("situacao"),
                perfil.get("abertura"),
                perfil.get("municipio"),
                perfil.get("uf"),
                perfil.get("socio_principal"),
                perfil.get("telefone"),
                perfil.get("email"),
                perfil.get("instagram"),
                perfil.get("site"),
                perfil.get("rating_google"),
                perfil.get("avaliacoes"),
                perfil.get("atualizado_em"),
            ))
            conn.commit()

    def cnpj_existe_recente(self, cnpj: str, dias: int = 30) -> bool:
        limite = (datetime.utcnow() - timedelta(days=dias)).isoformat()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT atualizado_em FROM empresas WHERE cnpj = ?", (cnpj,)
            ).fetchone()
            if row and row["atualizado_em"] and row["atualizado_em"] > limite:
                return True
        return False

    def buscar_empresas(
        self,
        q: str = "",
        uf: str = "",
        porte: str = "",
        com_email: bool = False,
        com_instagram: bool = False,
        pagina: int = 1,
        por_pagina: int = 50,
    ) -> dict:
        filtros = ["1=1"]
        params = []

        if q:
            filtros.append("(razao_social LIKE ? OR nome_fantasia LIKE ? OR cnpj LIKE ? OR municipio LIKE ?)")
            like = f"%{q}%"
            params.extend([like, like, like, like])
        if uf:
            filtros.append("uf = ?")
            params.append(uf.upper())
        if porte:
            filtros.append("porte = ?")
            params.append(porte.upper())
        if com_email:
            filtros.append("email != '' AND email IS NOT NULL")
        if com_instagram:
            filtros.append("instagram != '' AND instagram IS NOT NULL")

        where = " AND ".join(filtros)
        offset = (pagina - 1) * por_pagina

        with self._conn() as conn:
            total = conn.execute(f"SELECT COUNT(*) FROM empresas WHERE {where}", params).fetchone()[0]
            rows  = conn.execute(
                f"SELECT * FROM empresas WHERE {where} ORDER BY atualizado_em DESC LIMIT ? OFFSET ?",
                params + [por_pagina, offset]
            ).fetchall()

        return {
            "total":     total,
            "pagina":    pagina,
            "por_pagina": por_pagina,
            "dados":     [dict(r) for r in rows],
        }

    def estatisticas(self) -> dict:
        with self._conn() as conn:
            total    = conn.execute("SELECT COUNT(*) FROM empresas").fetchone()[0]
            com_tel  = conn.execute("SELECT COUNT(*) FROM empresas WHERE telefone != '' AND telefone IS NOT NULL").fetchone()[0]
            com_email = conn.execute("SELECT COUNT(*) FROM empresas WHERE email != '' AND email IS NOT NULL").fetchone()[0]
            com_insta = conn.execute("SELECT COUNT(*) FROM empresas WHERE instagram != '' AND instagram IS NOT NULL").fetchone()[0]
            por_uf   = conn.execute("SELECT uf, COUNT(*) as n FROM empresas GROUP BY uf ORDER BY n DESC LIMIT 10").fetchall()
            por_porte = conn.execute("SELECT porte, COUNT(*) as n FROM empresas GROUP BY porte ORDER BY n DESC").fetchall()
        return {
            "total":      total,
            "com_telefone": com_tel,
            "com_email":  com_email,
            "com_instagram": com_insta,
            "por_uf":     [dict(r) for r in por_uf],
            "por_porte":  [dict(r) for r in por_porte],
        }
