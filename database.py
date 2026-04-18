"""
Banco de dados — PostgreSQL (produção) ou SQLite (local).
Inclui persistência de progresso do agente.
"""

import os
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
                    for attempt in range(5):
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
                            if attempt == 4:
                                raise
                            wait = 2 ** attempt
                            print(f"PostgreSQL indisponível (tentativa {attempt+1}/5), retry em {wait}s: {e}")
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


# Dict com chaves normalizadas (sem acento) para matching robusto
_CNAE_CATS_NORM = {_norm(k): v for k, v in CNAE_CATEGORIAS.items()}


def cnae_para_categoria(cnae: str) -> str:
    """Retorna a categoria padronizada a partir da descrição CNAE."""
    cnae_norm = _norm(cnae or "")
    for substr, cat in _CNAE_CATS_NORM.items():
        if substr in cnae_norm:
            return cat
    return "Outros"


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
            if USE_POSTGRES:
                cur.execute("ALTER TABLE empresas ADD COLUMN IF NOT EXISTS categoria_padrao TEXT")
            else:
                try:
                    cur.execute("ALTER TABLE empresas ADD COLUMN categoria_padrao TEXT")
                except Exception:
                    conn.rollback()  # SQLite não suporta IF NOT EXISTS — reseta transação

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

    def salvar_empresas_batch(self, perfis: list):
        """
        Salva múltiplas empresas em uma única transação.
        Usa executemany para reduzir overhead de conexão/commit.
        """
        if not perfis:
            return

        _coalesce_pg = "CASE WHEN EXCLUDED.{f} != '' THEN EXCLUDED.{f} ELSE empresas.{f} END"
        _coalesce_sq = "CASE WHEN excluded.{f} != '' THEN excluded.{f} ELSE empresas.{f} END"
        contatos = ("telefone", "email", "instagram", "site", "rating_google", "avaliacoes")
        fixos = ["razao_social","nome_fantasia","porte","cnae","situacao",
                 "municipio","uf","socio_principal","atualizado_em","categoria_padrao"]

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
             instagram, site, rating_google, avaliacoes, atualizado_em, categoria_padrao)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (cnpj) DO UPDATE SET {sets_pg}
        """
        sql_sq = f"""
            INSERT INTO empresas
            (cnpj, razao_social, nome_fantasia, porte, cnae, situacao,
             abertura, municipio, uf, socio_principal, telefone, email,
             instagram, site, rating_google, avaliacoes, atualizado_em, categoria_padrao)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(cnpj) DO UPDATE SET {sets_sq}
        """

        rows = []
        for perfil in perfis:
            cat = perfil.get("categoria_padrao") or cnae_para_categoria(perfil.get("cnae", ""))
            rows.append((
                perfil.get("cnpj"), perfil.get("razao_social"), perfil.get("nome_fantasia"),
                perfil.get("porte"), perfil.get("cnae"), perfil.get("situacao"),
                perfil.get("abertura"), perfil.get("municipio"), perfil.get("uf"),
                perfil.get("socio_principal"), perfil.get("telefone", ""), perfil.get("email", ""),
                perfil.get("instagram", ""), perfil.get("site", ""), perfil.get("rating_google", ""),
                perfil.get("avaliacoes", ""), perfil.get("atualizado_em"), cat,
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

    def buscar_empresas(self, q="", uf="", porte="", cnae="", categoria="",
                        abertura_de="", abertura_ate="",
                        com_email=False, com_instagram=False,
                        com_telefone=False, com_site=False,
                        com_contato=False,
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
        Recomputa categoria_padrao para todos os registros sem categoria.
        Usa normalização de acentos — corrige o matching que antes falhava.
        """
        total = 0
        with _conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT cnpj, cnae FROM empresas "
                "WHERE categoria_padrao IS NULL OR categoria_padrao = '' OR categoria_padrao = 'Outros'"
            )
            rows = cur.fetchall()
            updates = []
            for cnpj, cnae in rows:
                cat = cnae_para_categoria(cnae)
                updates.append((cat, cnpj))
                total += 1
            if updates:
                cur.executemany(
                    f"UPDATE empresas SET categoria_padrao = {PH} WHERE cnpj = {PH}",
                    updates,
                )
                conn.commit()
        return total

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
        cur = conn.cursor()
        cur.execute("VACUUM ANALYZE empresas")
        cur.close()
        conn.close()

    def limpar_sites_falsos(self) -> int:
        """Alias de limpar_sites_diretorio — mantido para compatibilidade."""
        return self.limpar_sites_diretorio()