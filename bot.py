import os
import json
import sqlite3
import threading
import time
import calendar
from datetime import datetime, timedelta

import schedule
import telebot
from google import genai
from google.genai import types

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY") or os.environ["GEMINI_API_KEY"]

bot = telebot.TeleBot(TELEGRAM_TOKEN)

client = genai.Client(api_key=GEMINI_API_KEY)
MODEL_NAME = "gemini-2.5-flash-lite"
MODEL_FALLBACK = "gemini-2.5-flash"


# Versão da SYSTEM_INSTRUCTION. Bump esse número sempre que o prompt mudar
# pra invalidar sessões já abertas e o Gemini reler o prompt novo.
SYSTEM_INSTRUCTION_VERSION = "v5"

# user_id -> (versao_prompt, chat_session)
memoria_usuarios = {}

# user_id -> {"acao": str, "dados": dict, "ts": float} — confirmações pendentes
# e perguntas de seleção (ex: "qual cartão?", "tem 2 investimentos, qual?")
pendentes = {}

PENDENTE_TIMEOUT = 120  # segundos


def set_pendente(user_id, acao, dados):
    pendentes[user_id] = {"acao": acao, "dados": dados, "ts": time.time()}


def get_pendente(user_id):
    p = pendentes.get(user_id)
    if not p:
        return None
    if time.time() - p["ts"] > PENDENTE_TIMEOUT:
        pendentes.pop(user_id, None)
        return None
    return p


def limpar_pendente(user_id):
    pendentes.pop(user_id, None)


# ================= MARKDOWN ESCAPE =================

def escape_md(s):
    """Escapa caracteres que quebram Markdown (clássico) do Telegram em
    strings vindas do usuário (nome de cartão/investimento, descrição, etc).
    Telegram Markdown clássico interpreta: * _ ` [ — esses são os perigosos."""
    if s is None:
        return ""
    s = str(s)
    # Caracteres que precisam ser escapados no Markdown clássico
    return (s.replace("\\", "")
             .replace("_", "\\_")
             .replace("*", "\\*")
             .replace("`", "\\`")
             .replace("[", "\\["))


def chamar_ia(user_id, contents, system_instruction):
    """Chama o Gemini mantendo o histórico de conversa com retry.
    Recria a sessão automaticamente se a versão do system_instruction mudou."""
    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        response_mime_type="application/json",
    )

    entry = memoria_usuarios.get(user_id)
    # Cria sessão nova se não existe OU se a versão do prompt mudou
    if not entry or entry[0] != SYSTEM_INSTRUCTION_VERSION:
        chat = client.chats.create(model=MODEL_NAME, config=config)
        memoria_usuarios[user_id] = (SYSTEM_INSTRUCTION_VERSION, chat)
    else:
        chat = entry[1]

    ultima_excecao = None
    for _ in range(3):  # Tenta até 3 vezes se der erro de sobrecarga
        try:
            return chat.send_message(contents)
        except Exception as e:
            ultima_excecao = e
            msg = str(e)
            if "503" in msg or "UNAVAILABLE" in msg or "overloaded" in msg.lower():
                print(f"Gemini sobrecarregado, tentando de novo...")
                time.sleep(2)
                continue
            raise
    raise ultima_excecao

DB_PATH = os.path.join(os.path.dirname(__file__), "financas.db")


# ================= BANCO DE DADOS =================

def db():
    return sqlite3.connect(DB_PATH)


def _column_exists(cursor, table, column):
    cols = [r[1] for r in cursor.execute(f"PRAGMA table_info({table})").fetchall()]
    return column in cols


def _table_exists(cursor, table):
    row = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def criar_banco():
    conn = db()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gastos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL DEFAULT 0,
            data TEXT,
            valor REAL,
            categoria TEXT,
            descricao TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS receitas_fixas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL DEFAULT 0,
            descricao TEXT,
            valor REAL,
            fonte TEXT,
            dia_mes INTEGER,
            ultimo_mes_aplicado TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS receitas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL DEFAULT 0,
            data TEXT,
            valor REAL,
            fonte TEXT,
            descricao TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gastos_fixos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL DEFAULT 0,
            descricao TEXT,
            valor REAL,
            categoria TEXT,
            dia_mes INTEGER,
            ultimo_mes_aplicado TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            chat_id INTEGER PRIMARY KEY,
            primeiro_contato TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS autorizados (
            chat_id INTEGER PRIMARY KEY,
            nome TEXT,
            autorizado_em TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS feedbacks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            nome TEXT,
            mensagem TEXT NOT NULL,
            criado_em TEXT NOT NULL,
            lido INTEGER NOT NULL DEFAULT 0
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cartoes_credito (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            nome TEXT NOT NULL,
            limite REAL NOT NULL,
            dia_fechamento INTEGER NOT NULL,
            dia_vencimento INTEGER NOT NULL,
            criado_em TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gastos_cartao (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            cartao_id INTEGER NOT NULL,
            valor REAL NOT NULL,
            categoria TEXT,
            descricao TEXT,
            data TEXT NOT NULL,
            fatura_mes TEXT NOT NULL,
            pago INTEGER NOT NULL DEFAULT 0,
            pago_em TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS investimentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            tipo TEXT NOT NULL,
            nome TEXT NOT NULL,
            valor REAL NOT NULL,
            data TEXT NOT NULL,
            ativo INTEGER NOT NULL DEFAULT 1
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alertas_enviados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            tipo TEXT NOT NULL,
            referencia TEXT NOT NULL,
            enviado_em TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS parcelamentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            descricao TEXT,
            valor_parcela REAL,
            total_parcelas INTEGER,
            parcelas_pagas INTEGER DEFAULT 0,
            dia_cobranca INTEGER,
            categoria TEXT,
            metodo_pagamento TEXT,
            ultimo_mes_aplicado TEXT,
            criado_em TEXT
        )
    """)

    # Contas bancárias: usuário pode separar dinheiro por banco (Nubank, Itaú, etc.)
    # Lançamentos sem conta_id = "Geral" (pilha sem conta específica).
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS contas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            nome TEXT NOT NULL,
            banco TEXT,
            tipo TEXT,
            criado_em TEXT NOT NULL
        )
    """)

    # Migração: adicionar user_id em tabelas antigas, se faltar
    for tbl in ("gastos", "receitas", "gastos_fixos"):
        if not _column_exists(cursor, tbl, "user_id"):
            cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN user_id INTEGER NOT NULL DEFAULT 0")

    # Migração: adicionar metodo_pagamento em gastos
    if not _column_exists(cursor, "gastos", "metodo_pagamento"):
        cursor.execute("ALTER TABLE gastos ADD COLUMN metodo_pagamento TEXT")

    # Migração: lembrete diário em usuarios
    if not _column_exists(cursor, "usuarios", "lembrete_ativo"):
        cursor.execute("ALTER TABLE usuarios ADD COLUMN lembrete_ativo INTEGER DEFAULT 1")

    # Migração: vincular parcelamento a um cartão de crédito (opcional)
    if _table_exists(cursor, "parcelamentos") and not _column_exists(cursor, "parcelamentos", "cartao_id"):
        cursor.execute("ALTER TABLE parcelamentos ADD COLUMN cartao_id INTEGER")

    # Migração MULTI-CONTA: adiciona conta_id (NULL = sem conta específica = Geral)
    # em todas as tabelas de movimentação. Nada se quebra: dados antigos ficam
    # com conta_id NULL e seguem aparecendo no relatório geral.
    for tbl in ("gastos", "receitas", "gastos_fixos", "receitas_fixas",
                "parcelamentos", "investimentos", "gastos_cartao"):
        if _table_exists(cursor, tbl) and not _column_exists(cursor, tbl, "conta_id"):
            cursor.execute(f"ALTER TABLE {tbl} ADD COLUMN conta_id INTEGER")
    # Cartão de crédito tem uma conta de PAGAMENTO (de onde sai o dinheiro
    # quando paga a fatura). Pode ser NULL (paga genericamente).
    if not _column_exists(cursor, "cartoes_credito", "conta_id_pagamento"):
        cursor.execute("ALTER TABLE cartoes_credito ADD COLUMN conta_id_pagamento INTEGER")

    # orcamentos e metas precisam de chave composta (user_id, mes)
    # Recria se não tiver user_id
    if _table_exists(cursor, "orcamentos") and not _column_exists(cursor, "orcamentos", "user_id"):
        cursor.execute("DROP TABLE orcamentos")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orcamentos (
            user_id INTEGER NOT NULL,
            mes TEXT NOT NULL,
            valor REAL,
            PRIMARY KEY (user_id, mes)
        )
    """)

    if _table_exists(cursor, "metas") and not _column_exists(cursor, "metas", "user_id"):
        cursor.execute("DROP TABLE metas")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS metas (
            user_id INTEGER NOT NULL,
            mes TEXT NOT NULL,
            valor REAL,
            PRIMARY KEY (user_id, mes)
        )
    """)

    # Migração: saldo base persistente por conta (persiste entre meses)
    if not _column_exists(cursor, "contas", "saldo_base"):
        cursor.execute("ALTER TABLE contas ADD COLUMN saldo_base REAL DEFAULT 0")

    conn.commit()
    conn.close()


# ================= WHITELIST DE USUÁRIOS =================

def usuario_autorizado(chat_id):
    """Admin sempre tem acesso. Outros precisam estar na whitelist."""
    if chat_id == ADMIN_CHAT_ID:
        return True
    conn = db()
    row = conn.execute(
        "SELECT 1 FROM autorizados WHERE chat_id = ?", (chat_id,)
    ).fetchone()
    conn.close()
    return row is not None


def autorizar_usuario(chat_id, nome=None):
    conn = db()
    conn.execute(
        "INSERT OR REPLACE INTO autorizados (chat_id, nome, autorizado_em) VALUES (?, ?, ?)",
        (chat_id, nome or "", datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


def desautorizar_usuario(chat_id):
    conn = db()
    n = conn.execute("DELETE FROM autorizados WHERE chat_id = ?", (chat_id,)).rowcount
    conn.commit()
    conn.close()
    return n


def listar_autorizados():
    conn = db()
    rows = conn.execute(
        "SELECT chat_id, nome, autorizado_em FROM autorizados ORDER BY autorizado_em DESC"
    ).fetchall()
    conn.close()
    return rows


# ================= FEEDBACK =================

def salvar_feedback(user_id, nome, mensagem):
    conn = db()
    cur = conn.execute(
        "INSERT INTO feedbacks (user_id, nome, mensagem, criado_em) VALUES (?, ?, ?, ?)",
        (user_id, nome or "", mensagem, datetime.now().isoformat()),
    )
    fb_id = cur.lastrowid
    conn.commit()
    conn.close()
    return fb_id


def listar_feedbacks(apenas_nao_lidos=True, limite=20):
    conn = db()
    if apenas_nao_lidos:
        rows = conn.execute(
            "SELECT id, user_id, nome, mensagem, criado_em, lido FROM feedbacks "
            "WHERE lido = 0 ORDER BY criado_em DESC LIMIT ?",
            (limite,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, user_id, nome, mensagem, criado_em, lido FROM feedbacks "
            "ORDER BY criado_em DESC LIMIT ?",
            (limite,),
        ).fetchall()
    conn.close()
    return rows


def buscar_feedback(fb_id):
    conn = db()
    row = conn.execute(
        "SELECT id, user_id, nome, mensagem, criado_em, lido FROM feedbacks WHERE id = ?",
        (fb_id,),
    ).fetchone()
    conn.close()
    return row


def marcar_feedback_lido(fb_id):
    conn = db()
    n = conn.execute(
        "UPDATE feedbacks SET lido = 1 WHERE id = ?", (fb_id,)
    ).rowcount
    conn.commit()
    conn.close()
    return n


def contar_feedbacks_nao_lidos():
    conn = db()
    n = conn.execute("SELECT COUNT(*) FROM feedbacks WHERE lido = 0").fetchone()[0]
    conn.close()
    return n


# ================= CARTÕES DE CRÉDITO =================

def criar_cartao(user_id, nome, limite, dia_fechamento, dia_vencimento, conta_id_pagamento=None):
    conn = db()
    cur = conn.execute(
        "INSERT INTO cartoes_credito (user_id, nome, limite, dia_fechamento, dia_vencimento, criado_em, conta_id_pagamento) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (user_id, nome, limite, dia_fechamento, dia_vencimento, datetime.now().isoformat(), conta_id_pagamento),
    )
    cid = cur.lastrowid
    conn.commit()
    conn.close()
    return cid


def vincular_cartao_a_conta(user_id, cartao_id, conta_id):
    """Define qual conta paga a fatura desse cartão. Pode passar None pra desvincular."""
    conn = db()
    n = conn.execute(
        "UPDATE cartoes_credito SET conta_id_pagamento = ? WHERE user_id = ? AND id = ?",
        (conta_id, user_id, cartao_id),
    ).rowcount
    conn.commit()
    conn.close()
    return n


def listar_cartoes(user_id):
    conn = db()
    rows = conn.execute(
        "SELECT id, nome, limite, dia_fechamento, dia_vencimento FROM cartoes_credito "
        "WHERE user_id = ? ORDER BY nome",
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def buscar_cartao(user_id, nome=None, cartao_id=None):
    conn = db()
    if cartao_id:
        row = conn.execute(
            "SELECT id, nome, limite, dia_fechamento, dia_vencimento FROM cartoes_credito "
            "WHERE user_id = ? AND id = ?",
            (user_id, cartao_id),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT id, nome, limite, dia_fechamento, dia_vencimento FROM cartoes_credito "
            "WHERE user_id = ? AND LOWER(nome) = LOWER(?)",
            (user_id, nome),
        ).fetchone()
    conn.close()
    return row


def remover_cartao(user_id, cartao_id):
    conn = db()
    n = conn.execute(
        "DELETE FROM cartoes_credito WHERE user_id = ? AND id = ?",
        (user_id, cartao_id),
    ).rowcount
    if n:
        conn.execute("DELETE FROM gastos_cartao WHERE user_id = ? AND cartao_id = ?", (user_id, cartao_id))
    conn.commit()
    conn.close()
    return n


def calcular_fatura_mes(data_compra, dia_fechamento):
    """Retorna 'YYYY-MM' do mês de vencimento da fatura à qual essa compra pertence."""
    if isinstance(data_compra, str):
        data_compra = datetime.strptime(data_compra[:10], "%Y-%m-%d")
    # Se a compra foi ATÉ o dia de fechamento, vai pra fatura que vence no PRÓXIMO mês
    # Se foi DEPOIS, vai pra fatura do mês seguinte ao próximo
    ano, mes, dia = data_compra.year, data_compra.month, data_compra.day
    if dia <= dia_fechamento:
        venc_mes = mes + 1
        venc_ano = ano
    else:
        venc_mes = mes + 2
        venc_ano = ano
    if venc_mes > 12:
        venc_mes -= 12
        venc_ano += 1
    return f"{venc_ano:04d}-{venc_mes:02d}"


def registrar_gasto_cartao(user_id, cartao_id, valor, categoria, descricao, data=None):
    """Registra compra no cartão. A conta_id é HERDADA do cartão (campo
    conta_id_pagamento) — então a fatura sai sempre da mesma conta."""
    conn = db()
    data = data or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cartao = conn.execute(
        "SELECT dia_fechamento, conta_id_pagamento FROM cartoes_credito WHERE id = ?", (cartao_id,)
    ).fetchone()
    if not cartao:
        conn.close()
        return None
    fatura_mes = calcular_fatura_mes(data[:10], cartao[0])
    conta_id = cartao[1]
    cur = conn.execute(
        "INSERT INTO gastos_cartao (user_id, cartao_id, valor, categoria, descricao, data, fatura_mes, conta_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, cartao_id, valor, categoria, descricao, data, fatura_mes, conta_id),
    )
    gid = cur.lastrowid
    conn.commit()
    conn.close()
    return gid, fatura_mes


def fatura_aberta(user_id, cartao_id, fatura_mes=None):
    """Retorna lista de gastos da fatura aberta (não pagos) e o total."""
    conn = db()
    if fatura_mes:
        rows = conn.execute(
            "SELECT id, valor, categoria, descricao, data FROM gastos_cartao "
            "WHERE user_id = ? AND cartao_id = ? AND fatura_mes = ? AND pago = 0 ORDER BY data",
            (user_id, cartao_id, fatura_mes),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, valor, categoria, descricao, data FROM gastos_cartao "
            "WHERE user_id = ? AND cartao_id = ? AND pago = 0 ORDER BY fatura_mes, data",
            (user_id, cartao_id),
        ).fetchall()
    conn.close()
    total = sum(r[1] for r in rows)
    return rows, total


def total_fatura_aberta_todos_cartoes(user_id):
    """Soma o valor de todas as faturas em aberto de todos os cartões do usuário."""
    conn = db()
    total = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM gastos_cartao WHERE user_id = ? AND pago = 0",
        (user_id,),
    ).fetchone()[0]
    conn.close()
    return total or 0


def pagar_fatura(user_id, cartao_id, fatura_mes=None, conta_id_pagamento=None):
    """Marca todos os gastos da fatura como pagos e cria 1 gasto consolidado.
    O gasto consolidado sai da conta_id_pagamento (parâmetro) ou, se não
    informado, da conta_id_pagamento padrão do cartão."""
    rows, total = fatura_aberta(user_id, cartao_id, fatura_mes)
    if not rows:
        return 0, 0
    cartao_info = db().execute(
        "SELECT nome, conta_id_pagamento FROM cartoes_credito WHERE user_id = ? AND id = ?",
        (user_id, cartao_id),
    ).fetchone()
    nome_cartao = cartao_info[0] if cartao_info else "Cartão"
    if conta_id_pagamento is None and cartao_info:
        conta_id_pagamento = cartao_info[1]
    conn = db()
    agora = datetime.now().isoformat()
    if fatura_mes:
        conn.execute(
            "UPDATE gastos_cartao SET pago = 1, pago_em = ? "
            "WHERE user_id = ? AND cartao_id = ? AND fatura_mes = ? AND pago = 0",
            (agora, user_id, cartao_id, fatura_mes),
        )
    else:
        conn.execute(
            "UPDATE gastos_cartao SET pago = 1, pago_em = ? "
            "WHERE user_id = ? AND cartao_id = ? AND pago = 0",
            (agora, user_id, cartao_id),
        )
    desc = f"Fatura {fatura_mes or 'aberta'} - {nome_cartao}"
    conn.execute(
        "INSERT INTO gastos (user_id, valor, categoria, descricao, data, metodo_pagamento, conta_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (user_id, total, "Cartão de Crédito", desc,
         datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Crédito", conta_id_pagamento),
    )
    conn.commit()
    conn.close()
    return len(rows), total


def percentual_limite_usado(user_id, cartao_id):
    cartao = buscar_cartao(user_id, cartao_id=cartao_id)
    if not cartao:
        return 0, 0, 0
    limite = cartao[2]
    _, usado = fatura_aberta(user_id, cartao_id)
    pct = (usado / limite * 100) if limite > 0 else 0
    return usado, limite, pct


def proxima_data_vencimento(dia_vencimento, hoje=None):
    hoje = hoje or datetime.now().date()
    try:
        venc = hoje.replace(day=dia_vencimento)
    except ValueError:
        # dia inválido pro mês (ex: dia 31 em fevereiro) → último dia do mês
        import calendar
        ult = calendar.monthrange(hoje.year, hoje.month)[1]
        venc = hoje.replace(day=min(dia_vencimento, ult))
    if venc < hoje:
        # já passou — próximo mês
        mes = hoje.month + 1
        ano = hoje.year
        if mes > 12:
            mes = 1
            ano += 1
        import calendar
        ult = calendar.monthrange(ano, mes)[1]
        venc = datetime(ano, mes, min(dia_vencimento, ult)).date()
    return venc


# ================= CONTAS BANCÁRIAS =================
#
# Cada usuário pode cadastrar contas (Nubank, Itaú, etc.) e tagear gastos/
# receitas/fixos/parcelamentos/investimentos com a conta de origem. Lançamentos
# sem conta_id (NULL) são "Geral" — entram no relatório consolidado mas não
# aparecem em relatórios filtrados por conta. Isso preserva 100% dos dados
# antigos (todos eles têm conta_id NULL).

TIPOS_CONTA = ["Corrente", "Poupança", "Digital", "Salário", "Dinheiro", "Outro"]


def normalizar_tipo_conta(t):
    if not t:
        return "Corrente"
    t = _s(t).strip().lower()
    mapa = {
        "corrente": "Corrente", "cc": "Corrente", "conta corrente": "Corrente",
        "poupanca": "Poupança", "poupança": "Poupança", "savings": "Poupança",
        "digital": "Digital", "fintech": "Digital", "neobank": "Digital",
        "salario": "Salário", "salário": "Salário",
        "dinheiro": "Dinheiro", "cash": "Dinheiro", "espécie": "Dinheiro", "especie": "Dinheiro",
        "outro": "Outro", "outros": "Outro",
    }
    return mapa.get(t, t.title())


def criar_conta(user_id, nome, banco=None, tipo=None):
    nome = _s(nome).strip()
    banco = _s(banco).strip() or None
    tipo = normalizar_tipo_conta(tipo)
    conn = db()
    cur = conn.execute(
        "INSERT INTO contas (user_id, nome, banco, tipo, criado_em) VALUES (?, ?, ?, ?, ?)",
        (user_id, nome, banco, tipo, datetime.now().isoformat()),
    )
    cid = cur.lastrowid
    conn.commit()
    conn.close()
    return cid


def listar_contas(user_id):
    """Retorna lista de tuplas (id, nome, banco, tipo, criado_em)."""
    conn = db()
    rows = conn.execute(
        "SELECT id, nome, banco, tipo, criado_em FROM contas WHERE user_id = ? ORDER BY nome",
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def buscar_conta(user_id, nome=None, conta_id=None):
    """Busca conta por id (preferencial) ou por nome (case-insensitive, exato).
    Se nome não bater exato, tenta sub-string única (ex: 'nu' acha 'Nubank')."""
    conn = db()
    if conta_id:
        row = conn.execute(
            "SELECT id, nome, banco, tipo, criado_em FROM contas WHERE user_id = ? AND id = ?",
            (user_id, conta_id),
        ).fetchone()
        conn.close()
        return row
    nome = _s(nome).strip()
    if not nome:
        conn.close()
        return None
    # 1) match exato (case-insensitive)
    row = conn.execute(
        "SELECT id, nome, banco, tipo, criado_em FROM contas "
        "WHERE user_id = ? AND LOWER(nome) = LOWER(?)",
        (user_id, nome),
    ).fetchone()
    if row:
        conn.close()
        return row
    # 2) match por banco exato
    row = conn.execute(
        "SELECT id, nome, banco, tipo, criado_em FROM contas "
        "WHERE user_id = ? AND LOWER(COALESCE(banco,'')) = LOWER(?)",
        (user_id, nome),
    ).fetchone()
    if row:
        conn.close()
        return row
    # 3) sub-string única (ex: 'nu' acha só 'Nubank' se for o único que contém 'nu')
    rows = conn.execute(
        "SELECT id, nome, banco, tipo, criado_em FROM contas "
        "WHERE user_id = ? AND (LOWER(nome) LIKE LOWER(?) OR LOWER(COALESCE(banco,'')) LIKE LOWER(?))",
        (user_id, f"%{nome}%", f"%{nome}%"),
    ).fetchall()
    conn.close()
    if len(rows) == 1:
        return rows[0]
    return None  # zero matches OU ambíguo (>1)


def renomear_conta(user_id, conta_id, novo_nome=None, novo_banco=None, novo_tipo=None):
    sets = []
    vals = []
    if novo_nome:
        sets.append("nome = ?")
        vals.append(_s(novo_nome).strip())
    if novo_banco is not None:  # permite "" pra limpar
        sets.append("banco = ?")
        vals.append(_s(novo_banco).strip() or None)
    if novo_tipo:
        sets.append("tipo = ?")
        vals.append(normalizar_tipo_conta(novo_tipo))
    if not sets:
        return 0
    vals.extend([user_id, conta_id])
    conn = db()
    n = conn.execute(
        f"UPDATE contas SET {', '.join(sets)} WHERE user_id = ? AND id = ?", vals
    ).rowcount
    conn.commit()
    conn.close()
    return n


def remover_conta(user_id, conta_id, transferir_para=None):
    """Remove conta. Por padrão, lançamentos vinculados ficam órfãos (conta_id=NULL,
    voltam para a pilha 'Geral'). Se transferir_para for outro conta_id, remarca
    todos os lançamentos pra essa conta antes de deletar."""
    conn = db()
    if transferir_para:
        for tbl in ("gastos", "receitas", "gastos_fixos", "receitas_fixas",
                    "parcelamentos", "investimentos", "gastos_cartao"):
            conn.execute(
                f"UPDATE {tbl} SET conta_id = ? WHERE user_id = ? AND conta_id = ?",
                (transferir_para, user_id, conta_id),
            )
        conn.execute(
            "UPDATE cartoes_credito SET conta_id_pagamento = ? "
            "WHERE user_id = ? AND conta_id_pagamento = ?",
            (transferir_para, user_id, conta_id),
        )
    else:
        for tbl in ("gastos", "receitas", "gastos_fixos", "receitas_fixas",
                    "parcelamentos", "investimentos", "gastos_cartao"):
            conn.execute(
                f"UPDATE {tbl} SET conta_id = NULL WHERE user_id = ? AND conta_id = ?",
                (user_id, conta_id),
            )
        conn.execute(
            "UPDATE cartoes_credito SET conta_id_pagamento = NULL "
            "WHERE user_id = ? AND conta_id_pagamento = ?",
            (user_id, conta_id),
        )
    n = conn.execute(
        "DELETE FROM contas WHERE user_id = ? AND id = ?", (user_id, conta_id)
    ).rowcount
    conn.commit()
    conn.close()
    return n


def contar_lancamentos_conta(user_id, conta_id):
    """Conta quantos lançamentos (de qualquer tipo) estão na conta — pra avisar
    o usuário antes de remover."""
    conn = db()
    total = 0
    for tbl in ("gastos", "receitas", "gastos_fixos", "receitas_fixas",
                "parcelamentos", "investimentos", "gastos_cartao"):
        total += conn.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE user_id = ? AND conta_id = ?",
            (user_id, conta_id),
        ).fetchone()[0]
    conn.close()
    return total


def total_gasto_mes_conta(user_id, conta_id, mes=None):
    """Total de gastos do mês para uma conta específica (ou todas se conta_id=None)."""
    return total_gasto_mes(user_id, mes=mes, conta_id=conta_id)


def total_receita_mes_conta(user_id, conta_id, mes=None):
    return total_receita_mes(user_id, mes=mes, conta_id=conta_id)


def saldo_conta(user_id, conta_id):
    """Saldo ACUMULADO de uma conta (all-time: saldo_base + todas receitas - todos gastos).
    Persiste entre meses. conta_id=None → lançamentos sem conta específica (conta_id IS NULL)."""
    conn = db()
    if conta_id is None:
        total_r = conn.execute(
            "SELECT COALESCE(SUM(valor), 0) FROM receitas WHERE user_id = ? AND conta_id IS NULL",
            (user_id,),
        ).fetchone()[0] or 0.0
        total_g = conn.execute(
            "SELECT COALESCE(SUM(valor), 0) FROM gastos WHERE user_id = ? AND conta_id IS NULL",
            (user_id,),
        ).fetchone()[0] or 0.0
        conn.close()
        return total_r - total_g
    row = conn.execute(
        "SELECT COALESCE(saldo_base, 0) FROM contas WHERE id = ? AND user_id = ?",
        (conta_id, user_id),
    ).fetchone()
    saldo_base = row[0] if row else 0.0
    total_r = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM receitas WHERE user_id = ? AND conta_id = ?",
        (user_id, conta_id),
    ).fetchone()[0] or 0.0
    total_g = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM gastos WHERE user_id = ? AND conta_id = ?",
        (user_id, conta_id),
    ).fetchone()[0] or 0.0
    conn.close()
    return saldo_base + total_r - total_g


def definir_saldo_base_conta(user_id, conta_id, novo_saldo_desejado):
    """Ajusta saldo_base para que saldo_conta() bata com novo_saldo_desejado.
    Isso garante que o saldo 'real' da conta reflita o valor informado pelo usuário,
    independente do mês — o saldo persiste e não zera na virada do mês."""
    conn = db()
    total_r = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM receitas WHERE user_id = ? AND conta_id = ?",
        (user_id, conta_id),
    ).fetchone()[0] or 0.0
    total_g = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM gastos WHERE user_id = ? AND conta_id = ?",
        (user_id, conta_id),
    ).fetchone()[0] or 0.0
    novo_base = novo_saldo_desejado - total_r + total_g
    conn.execute(
        "UPDATE contas SET saldo_base = ? WHERE id = ? AND user_id = ?",
        (novo_base, conta_id, user_id),
    )
    conn.commit()
    conn.close()


def resolver_conta(user_id, conta_nome):
    """Resolve um nome de conta vindo do usuário/IA pra um conta_id.

    Retorna (status, conta_id, conta_obj) onde status é:
      - 'ok'         : achou a conta certinha (use conta_id)
      - 'nenhuma'    : usuário não tem nenhuma conta cadastrada (use NULL = Geral)
      - 'sem_filtro' : usuário não especificou conta E tem múltiplas (use NULL)
      - 'unica'      : usuário não especificou mas tem só 1 conta — use ela
      - 'nao_encontrada' : nome dado mas não bate com nenhuma conta
      - 'ambigua'    : nome dado bate em várias contas
    """
    contas = listar_contas(user_id)
    nome = _s(conta_nome).strip()
    if not nome:
        if not contas:
            return ("nenhuma", None, None)
        if len(contas) == 1:
            c = contas[0]
            return ("unica", c[0], c)
        return ("sem_filtro", None, None)
    if not contas:
        return ("nenhuma", None, None)
    achou = buscar_conta(user_id, nome=nome)
    if achou:
        return ("ok", achou[0], achou)
    # Verifica se foi ambíguo (sub-string com múltiplos matches)
    conn = db()
    rows = conn.execute(
        "SELECT id, nome FROM contas "
        "WHERE user_id = ? AND (LOWER(nome) LIKE LOWER(?) OR LOWER(COALESCE(banco,'')) LIKE LOWER(?))",
        (user_id, f"%{nome}%", f"%{nome}%"),
    ).fetchall()
    conn.close()
    if len(rows) > 1:
        return ("ambigua", None, rows)
    return ("nao_encontrada", None, None)


def transferir_entre_contas(user_id, conta_origem_id, conta_destino_id, valor):
    """Cria 1 gasto na conta origem ('Transferência saída') e 1 receita na destino
    ('Transferência entrada'). Retorna (gasto_id, receita_id)."""
    conta_origem = buscar_conta(user_id, conta_id=conta_origem_id)
    conta_destino = buscar_conta(user_id, conta_id=conta_destino_id)
    nome_origem = conta_origem[1] if conta_origem else "?"
    nome_destino = conta_destino[1] if conta_destino else "?"
    salvar_gasto(
        user_id, valor, "Transferência",
        f"Transferência para {nome_destino}",
        metodo_pagamento="Transferência",
        conta_id=conta_origem_id,
    )
    salvar_receita(
        user_id, valor, "Transferência",
        f"Transferência de {nome_origem}",
        conta_id=conta_destino_id,
    )


# ================= INVESTIMENTOS =================

TIPOS_INVESTIMENTO = ["Reserva", "Renda Fixa", "Ações", "FIIs", "Cripto", "Outros"]


def normalizar_tipo_investimento(t):
    if not t:
        return "Outros"
    t = t.strip().lower()
    mapa = {
        "reserva": "Reserva", "reserva de emergência": "Reserva", "emergencia": "Reserva",
        "emergência": "Reserva", "poupança": "Reserva", "poupanca": "Reserva",
        "renda fixa": "Renda Fixa", "tesouro": "Renda Fixa", "cdb": "Renda Fixa",
        "lci": "Renda Fixa", "lca": "Renda Fixa", "rf": "Renda Fixa",
        "ações": "Ações", "acoes": "Ações", "ação": "Ações", "acao": "Ações",
        "bolsa": "Ações", "fii": "FIIs", "fiis": "FIIs", "fundo imobiliário": "FIIs",
        "fundo imobiliario": "FIIs", "imóveis": "FIIs",
        "cripto": "Cripto", "criptomoeda": "Cripto", "bitcoin": "Cripto", "btc": "Cripto",
    }
    return mapa.get(t, "Outros")


def registrar_investimento(user_id, tipo, nome, valor, conta_id=None):
    conn = db()
    cur = conn.execute(
        "INSERT INTO investimentos (user_id, tipo, nome, valor, data, conta_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, normalizar_tipo_investimento(tipo), nome, valor, datetime.now().isoformat(), conta_id),
    )
    iid = cur.lastrowid
    conn.commit()
    conn.close()
    return iid


def listar_investimentos(user_id):
    conn = db()
    rows = conn.execute(
        "SELECT id, tipo, nome, valor, data FROM investimentos "
        "WHERE user_id = ? AND ativo = 1 ORDER BY tipo, nome",
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def total_investimentos(user_id):
    conn = db()
    total = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM investimentos WHERE user_id = ? AND ativo = 1",
        (user_id,),
    ).fetchone()[0]
    conn.close()
    return total or 0


def resgatar_investimento(user_id, nome, valor, tipo_hint=None):
    """Reduz o valor do investimento e adiciona como receita. Se zerar, marca inativo.
    Aceita identificadores variados: nome exato, parcial, id (3 ou #3), ou tipo (Renda Fixa).
    Retorna (valor_resgatado, nome_real) ou None."""
    # 1) Tenta achar usando o buscador inteligente (id, #id, nome parcial)
    inv = None
    if nome:
        inv = buscar_investimento(user_id, nome)
    # 2) Se não achou e veio um tipo (ou o "nome" parece um tipo), busca por tipo
    if not inv:
        candidato_tipo = tipo_hint or nome
        if candidato_tipo:
            tipo_norm = normalizar_tipo_investimento(candidato_tipo)
            conn_t = db()
            rows_tipo = conn_t.execute(
                "SELECT id, nome, valor FROM investimentos "
                "WHERE user_id = ? AND ativo = 1 AND tipo = ? ORDER BY data DESC",
                (user_id, tipo_norm),
            ).fetchall()
            conn_t.close()
            if len(rows_tipo) == 1:
                inv = (rows_tipo[0][0], tipo_norm, rows_tipo[0][1], rows_tipo[0][2])
            elif len(rows_tipo) > 1:
                return ("varios_no_tipo", tipo_norm, [(r[0], r[1], r[2]) for r in rows_tipo])
    if not inv:
        return None
    inv_id, _tipo, nome_real, atual = inv
    conn = db()
    row = conn.execute(
        "SELECT valor FROM investimentos WHERE id = ? AND ativo = 1", (inv_id,)
    ).fetchone()
    if not row:
        conn.close()
        return None
    atual = row[0]
    nome = nome_real
    if valor >= atual:
        valor = atual
        conn.execute("UPDATE investimentos SET valor = 0, ativo = 0 WHERE id = ?", (inv_id,))
    else:
        conn.execute("UPDATE investimentos SET valor = valor - ? WHERE id = ?", (valor, inv_id))
    # Volta o dinheiro como receita
    conn.execute(
        "INSERT INTO receitas (user_id, valor, fonte, descricao, data) VALUES (?, ?, ?, ?, ?)",
        (user_id, valor, "Resgate", f"Resgate de {nome}", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    )
    conn.commit()
    conn.close()
    return (valor, nome)


def buscar_investimento(user_id, identificador):
    """Encontra investimento ativo por ID (int) ou nome (busca parcial, case-insensitive).
    Aceita identificadores como '3', '#3', 'investimento 3', 'caixinha'."""
    if isinstance(identificador, str):
        identificador = identificador.strip().lstrip("#").strip()
        # Se vier algo tipo "investimento 3" ou "aporte 5", pega só o número
        import re as _re
        m = _re.fullmatch(r"(?:investimento|aporte|caixinha|n[uú]mero|n[oº°.]*)?\s*#?\s*(\d+)", identificador, _re.IGNORECASE)
        if m:
            identificador = m.group(1)
    conn = db()
    row = None
    if isinstance(identificador, int) or (isinstance(identificador, str) and identificador.isdigit()):
        row = conn.execute(
            "SELECT id, tipo, nome, valor FROM investimentos "
            "WHERE user_id = ? AND id = ? AND ativo = 1",
            (user_id, int(identificador)),
        ).fetchone()
    if not row and isinstance(identificador, str):
        # Tenta nome exato
        row = conn.execute(
            "SELECT id, tipo, nome, valor FROM investimentos "
            "WHERE user_id = ? AND ativo = 1 AND LOWER(nome) = LOWER(?) "
            "ORDER BY data DESC LIMIT 1",
            (user_id, identificador.strip()),
        ).fetchone()
        # Senão, busca parcial
        if not row:
            row = conn.execute(
                "SELECT id, tipo, nome, valor FROM investimentos "
                "WHERE user_id = ? AND ativo = 1 AND LOWER(nome) LIKE LOWER(?) "
                "ORDER BY data DESC LIMIT 1",
                (user_id, f"%{identificador.strip()}%"),
            ).fetchone()
    conn.close()
    return row


def aportar_em_investimento(user_id, identificador, valor):
    """SOMA `valor` ao valor de um investimento existente (sem mexer no saldo).
    Diferente de editar_investimento(novo_valor=...) que SUBSTITUI o valor.
    Retorna ('ok', atualizado, antigo), ('valor_invalido', inv) ou None se não achou."""
    inv = buscar_investimento(user_id, identificador)
    if not inv:
        return None
    inv_id, tipo_atual, nome_atual, valor_atual = inv
    if valor is None or valor <= 0:
        return ("valor_invalido", inv)
    novo_total = float(valor_atual) + float(valor)
    conn = db()
    conn.execute(
        "UPDATE investimentos SET valor = ?, ativo = 1 WHERE id = ?",
        (novo_total, inv_id),
    )
    conn.commit()
    atualizado = conn.execute(
        "SELECT id, tipo, nome, valor FROM investimentos WHERE id = ?", (inv_id,)
    ).fetchone()
    conn.close()
    return ("ok", atualizado, inv)


def editar_investimento(user_id, identificador, novo_nome=None, novo_tipo=None, novo_valor=None):
    """Renomeia/recategoriza/SUBSTITUI valor de um investimento existente. NÃO mexe no saldo.
    ATENÇÃO: novo_valor SUBSTITUI o valor atual (não soma). Pra somar use aportar_em_investimento."""
    inv = buscar_investimento(user_id, identificador)
    if not inv:
        return None
    inv_id, tipo_atual, nome_atual, valor_atual = inv
    novos = {}
    if novo_nome and novo_nome.strip() and novo_nome.strip().lower() != nome_atual.lower():
        novos["nome"] = novo_nome.strip()
    if novo_tipo:
        tipo_norm = normalizar_tipo_investimento(novo_tipo)
        if tipo_norm != tipo_atual:
            novos["tipo"] = tipo_norm
    # Aceita zerar (>= 0). NÃO aceita negativo nem None.
    if novo_valor is not None and novo_valor >= 0 and abs(novo_valor - valor_atual) > 0.01:
        novos["valor"] = float(novo_valor)
    if not novos:
        return ("nada", inv)
    sets = ", ".join(f"{k} = ?" for k in novos)
    args = list(novos.values()) + [inv_id]
    conn = db()
    conn.execute(f"UPDATE investimentos SET {sets} WHERE id = ?", args)
    conn.commit()
    atualizado = conn.execute(
        "SELECT id, tipo, nome, valor FROM investimentos WHERE id = ?", (inv_id,)
    ).fetchone()
    conn.close()
    return ("ok", atualizado, inv)


def transferir_investimento(user_id, nome_origem, nome_destino, valor, tipo_destino=None):
    """Move R$valor de uma aplicação para outra (cria nova ou soma à existente).
    NÃO afeta saldo (dinheiro nunca saiu da carteira de investimentos)."""
    origem = buscar_investimento(user_id, nome_origem)
    if not origem:
        return ("origem_nao_encontrada", None, None)
    inv_id_orig, tipo_orig, nome_orig, valor_orig = origem
    if valor <= 0:
        return ("valor_invalido", None, None)
    if valor > valor_orig + 0.01:
        return ("saldo_insuficiente", origem, None)

    conn = db()
    # Reduz origem (zera e inativa se esvaziar)
    if abs(valor - valor_orig) < 0.01:
        conn.execute("UPDATE investimentos SET valor = 0, ativo = 0 WHERE id = ?", (inv_id_orig,))
    else:
        conn.execute("UPDATE investimentos SET valor = valor - ? WHERE id = ?", (valor, inv_id_orig))

    # Cria/soma destino
    destino = conn.execute(
        "SELECT id, tipo, nome, valor FROM investimentos "
        "WHERE user_id = ? AND ativo = 1 AND LOWER(nome) = LOWER(?) ORDER BY data DESC LIMIT 1",
        (user_id, nome_destino.strip()),
    ).fetchone()
    tipo_dest = normalizar_tipo_investimento(tipo_destino) if tipo_destino else (destino[1] if destino else "Outros")
    if destino:
        conn.execute("UPDATE investimentos SET valor = valor + ?, tipo = ? WHERE id = ?",
                     (valor, tipo_dest, destino[0]))
        novo_id = destino[0]
    else:
        cur = conn.execute(
            "INSERT INTO investimentos (user_id, tipo, nome, valor, data, ativo) "
            "VALUES (?, ?, ?, ?, ?, 1)",
            (user_id, tipo_dest, nome_destino.strip(), valor,
             datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        novo_id = cur.lastrowid
    conn.commit()
    info_destino = conn.execute(
        "SELECT id, tipo, nome, valor FROM investimentos WHERE id = ?", (novo_id,)
    ).fetchone()
    conn.close()
    return ("ok", origem, info_destino)


def remover_investimento(user_id, identificador):
    """Apaga DEFINITIVAMENTE um investimento (ativo ou não). NÃO mexe no saldo."""
    # Normaliza o identificador (remove #, "investimento", etc) — mesma lógica do buscar
    if isinstance(identificador, str):
        ident_norm = identificador.strip().lstrip("#").strip()
        import re as _re
        m = _re.fullmatch(r"(?:investimento|aporte|caixinha|n[uú]mero|n[oº°.]*)?\s*#?\s*(\d+)", ident_norm, _re.IGNORECASE)
        if m:
            ident_norm = m.group(1)
        identificador = ident_norm
    inv = buscar_investimento(user_id, identificador)
    if not inv:
        # Tenta achar mesmo se já estiver inativo
        conn = db()
        if isinstance(identificador, str) and identificador.isdigit():
            row = conn.execute(
                "SELECT id, tipo, nome, valor FROM investimentos WHERE user_id = ? AND id = ?",
                (user_id, int(identificador)),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT id, tipo, nome, valor FROM investimentos "
                "WHERE user_id = ? AND LOWER(nome) LIKE LOWER(?) ORDER BY data DESC LIMIT 1",
                (user_id, f"%{str(identificador).strip()}%"),
            ).fetchone()
        conn.close()
        if not row:
            return None
        inv = row
    inv_id, tipo, nome, valor = inv
    conn = db()
    conn.execute("DELETE FROM investimentos WHERE user_id = ? AND id = ?", (user_id, inv_id))
    conn.commit()
    conn.close()
    return (inv_id, tipo, nome, valor)


def patrimonio_texto(user_id):
    """Saldo + investimentos - faturas abertas = patrimônio líquido."""
    conn = db()
    total_receitas = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM receitas WHERE user_id = ?", (user_id,)
    ).fetchone()[0] or 0
    total_gastos = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM gastos WHERE user_id = ?", (user_id,)
    ).fetchone()[0] or 0
    conn.close()
    saldo = total_receitas - total_gastos
    invest = total_investimentos(user_id)
    fatura = total_fatura_aberta_todos_cartoes(user_id)
    patrimonio = saldo + invest - fatura

    invs = listar_investimentos(user_id)
    por_tipo = {}
    for _, tipo, _, valor, _ in invs:
        por_tipo[tipo] = por_tipo.get(tipo, 0) + valor

    texto = "💎 *Seu Patrimônio*\n\n"
    texto += f"💰 Saldo em conta: R$ {saldo:.2f}\n"
    texto += f"📈 Investimentos: R$ {invest:.2f}\n"
    if fatura > 0:
        texto += f"💳 Fatura aberta (cartão): -R$ {fatura:.2f}\n"
    texto += f"\n🏆 *Patrimônio líquido: R$ {patrimonio:.2f}*\n"

    if invs and invest > 0:
        texto += "\n*Distribuição dos investimentos:*\n"
        for tipo, val in sorted(por_tipo.items(), key=lambda x: -x[1]):
            pct = val / invest * 100
            texto += f"• {tipo}: R$ {val:.2f} ({pct:.0f}%)\n"
    return texto


def dica_investimento_texto(user_id):
    """Gera dica educativa personalizada usando dados reais do usuário (sem recomendar ativos)."""
    conn = db()
    total_receitas = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM receitas WHERE user_id = ?", (user_id,)
    ).fetchone()[0] or 0
    total_gastos = conn.execute(
        "SELECT COALESCE(SUM(valor), 0) FROM gastos WHERE user_id = ?", (user_id,)
    ).fetchone()[0] or 0
    conn.close()
    saldo = total_receitas - total_gastos
    invest = total_investimentos(user_id)

    # estimativa de gasto médio mensal
    gasto_mensal = total_gasto_mes(user_id)
    if gasto_mensal == 0:
        gasto_mensal = total_gastos / 6 if total_gastos > 0 else 0

    invs = listar_investimentos(user_id)
    por_tipo = {}
    for _, tipo, _, valor, _ in invs:
        por_tipo[tipo] = por_tipo.get(tipo, 0) + valor
    reserva = por_tipo.get("Reserva", 0)
    reserva_ideal = gasto_mensal * 6

    contexto = (
        f"Dados financeiros do usuário (apenas para análise):\n"
        f"- Saldo em conta: R$ {saldo:.2f}\n"
        f"- Total investido: R$ {invest:.2f}\n"
        f"- Reserva de emergência atual: R$ {reserva:.2f}\n"
        f"- Reserva ideal (6 meses de gastos): R$ {reserva_ideal:.2f}\n"
        f"- Gasto médio mensal: R$ {gasto_mensal:.2f}\n"
        f"- Distribuição dos investimentos: {por_tipo if por_tipo else 'nenhum'}\n"
    )
    instr = (
        "Você é um educador financeiro amigável. Com base nos dados acima, "
        "dê 1 dica EDUCATIVA personalizada (máx 4 frases curtas) sobre investimentos. "
        "REGRAS OBRIGATÓRIAS:\n"
        "- NUNCA recomende ativos específicos (não diga 'compre PETR4', 'invista em Tesouro Selic', etc).\n"
        "- Foque em conceitos: reserva de emergência, diversificação, perfil, prazo.\n"
        "- Use português brasileiro coloquial, com 1-2 emojis.\n"
        "- Se faltar reserva, priorize falar dela.\n"
        "- Termine com uma pergunta motivadora ou próximo passo educativo."
    )
    # IMPORTANTE: chamada DIRETA ao Gemini (sem JSON forçado, sem poluir histórico do chat).
    # Isso evita que a dica venha embrulhada em JSON e que polua a memória do chat natural.
    try:
        config = types.GenerateContentConfig(system_instruction=instr)
        resposta = client.models.generate_content(
            model=MODEL_NAME,
            contents=contexto,
            config=config,
        )
        texto = (resposta.text or "").strip()
        if not texto:
            raise ValueError("resposta vazia do Gemini")
        return f"💡 *Dica de investimento*\n\n{texto}"
    except Exception as e:
        print(f"Erro dica investimento: {e}")
        return (
            "💡 *Dica de investimento*\n\n"
            f"Sua reserva ideal seria R$ {reserva_ideal:.2f} (6 meses de gastos). "
            f"Hoje você tem R$ {reserva:.2f} guardado. "
            "Antes de pensar em ações ou cripto, garante essa reserva — ela é o que te protege em emergências. 💪"
        )


# ================= ALERTAS (registro pra não duplicar) =================

def alerta_ja_enviado(user_id, tipo, referencia):
    conn = db()
    row = conn.execute(
        "SELECT 1 FROM alertas_enviados WHERE user_id = ? AND tipo = ? AND referencia = ?",
        (user_id, tipo, referencia),
    ).fetchone()
    conn.close()
    return row is not None


def marcar_alerta_enviado(user_id, tipo, referencia):
    conn = db()
    conn.execute(
        "INSERT INTO alertas_enviados (user_id, tipo, referencia, enviado_em) VALUES (?, ?, ?, ?)",
        (user_id, tipo, referencia, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


def verificar_alertas_cartoes():
    """Roda diariamente: alerta de fatura próxima do vencimento (3 dias antes)."""
    conn = db()
    cartoes = conn.execute(
        "SELECT user_id, id, nome, dia_vencimento FROM cartoes_credito"
    ).fetchall()
    conn.close()
    hoje = datetime.now().date()
    for user_id, cid, nome, dia_venc in cartoes:
        try:
            venc = proxima_data_vencimento(dia_venc, hoje)
            dias = (venc - hoje).days
            if dias == 3:
                _, total = fatura_aberta(user_id, cid)
                if total <= 0:
                    continue
                ref = f"venc-{cid}-{venc.isoformat()}"
                if alerta_ja_enviado(user_id, "vencimento_fatura", ref):
                    continue
                bot.send_message(
                    user_id,
                    f"⏰ *Lembrete de fatura*\n\n"
                    f"💳 Cartão *{nome}* vence em 3 dias ({venc.strftime('%d/%m')})\n"
                    f"💸 Valor: R$ {total:.2f}\n\n"
                    f"_Quando pagar, manda:_ `/pagar_fatura {nome}`",
                    parse_mode="Markdown",
                )
                marcar_alerta_enviado(user_id, "vencimento_fatura", ref)
        except Exception as e:
            print(f"Erro alerta vencimento {cid}: {e}")


def verificar_alerta_limite(user_id, cartao_id):
    """Chamado após cada gasto no cartão: alerta se passou de 70% do limite."""
    try:
        usado, limite, pct = percentual_limite_usado(user_id, cartao_id)
        if pct < 70 or limite <= 0:
            return
        cartao = buscar_cartao(user_id, cartao_id=cartao_id)
        if not cartao:
            return
        nome = cartao[1]
        # Usa mês atual como referência pra não spammar (1 alerta por mês por cartão)
        ref = f"limite70-{cartao_id}-{mes_atual()}"
        if alerta_ja_enviado(user_id, "limite_cartao", ref):
            return
        bot.send_message(
            user_id,
            f"⚠️ *Atenção com o limite!*\n\n"
            f"💳 Cartão *{nome}* já usou *{pct:.0f}%* do limite.\n"
            f"💸 Usado: R$ {usado:.2f} de R$ {limite:.2f}",
            parse_mode="Markdown",
        )
        marcar_alerta_enviado(user_id, "limite_cartao", ref)
    except Exception as e:
        print(f"Erro alerta limite: {e}")


def registrar_usuario(chat_id):
    conn = db()
    conn.execute(
        "INSERT OR IGNORE INTO usuarios (chat_id, primeiro_contato) VALUES (?, ?)",
        (chat_id, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


# ================= HELPERS =================

def parse_valor(v):
    """Converte qualquer coisa pra float seguro. Nunca lança exceção:
    None, lista, dict, string maluca → 0.0. Aceita formato BR ('1.234,56')."""
    if v is None:
        return 0.0
    if isinstance(v, bool):  # bool é subclasse de int, mas não é valor monetário
        return 0.0
    if isinstance(v, (int, float)):
        try:
            f = float(v)
            # NaN/Inf não fazem sentido aqui
            if f != f or f in (float("inf"), float("-inf")):
                return 0.0
            return f
        except (ValueError, TypeError, OverflowError):
            return 0.0
    if isinstance(v, str):
        s = v.strip().replace("R$", "").replace("r$", "").replace(" ", "")
        if not s:
            return 0.0
        # Formato BR: "1.234,56" → "1234.56". Se tem vírgula, trata ponto como milhar.
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        try:
            f = float(s)
            if f != f or f in (float("inf"), float("-inf")):
                return 0.0
            return f
        except (ValueError, TypeError):
            return 0.0
    # listas, dicts, outros — fallback seguro
    return 0.0


def _s(v, default=""):
    """Lê campo de texto da IA de forma segura. Aceita None, lista, dict, número.
    Retorna sempre uma string já stripada (ou default se vazio)."""
    if v is None:
        return default
    if isinstance(v, str):
        s = v.strip()
        return s if s else default
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, list):
        # IA às vezes devolve ["a","b"] — junta com vírgula
        partes = [str(x).strip() for x in v if x is not None and str(x).strip()]
        return ", ".join(partes) if partes else default
    if isinstance(v, dict):
        # último recurso — tenta extrair "nome"/"valor"/"texto"
        for k in ("nome", "valor", "texto", "descricao"):
            if k in v and v[k] is not None:
                return _s(v[k], default)
        return default
    return default


def _i(v, default=None, minimo=None, maximo=None):
    """Lê campo inteiro da IA de forma segura. Retorna default se inválido,
    ou None se default=None. Opcionalmente clampa em [minimo, maximo]."""
    if v is None or isinstance(v, bool):
        return default
    try:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return default
        n = int(float(v))
        if minimo is not None and n < minimo:
            return default
        if maximo is not None and n > maximo:
            return default
        return n
    except (ValueError, TypeError, OverflowError):
        return default


def _dia_clamp_no_mes(dia, ano=None, mes=None):
    """Clampa dia pro último dia do mês corrente (ou do ano/mes informado).
    Ex: dia=31 em fevereiro → 28 (ou 29 em ano bissexto)."""
    hoje = datetime.now()
    a = ano or hoje.year
    m = mes or hoje.month
    ult = calendar.monthrange(a, m)[1]
    return max(1, min(int(dia), ult))


def _ultimo_dia_do_mes(ano=None, mes=None):
    hoje = datetime.now()
    a = ano or hoje.year
    m = mes or hoje.month
    return calendar.monthrange(a, m)[1]


def mes_atual():
    return datetime.now().strftime("%Y-%m")


def mes_anterior():
    hoje = datetime.now()
    primeiro = hoje.replace(day=1)
    ultimo_mes_anterior = primeiro - timedelta(days=1)
    return ultimo_mes_anterior.strftime("%Y-%m")


def fmt_mes(mes):
    nomes = ["", "janeiro", "fevereiro", "março", "abril", "maio", "junho",
             "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
    try:
        a, m = mes.split("-")
        return f"{nomes[int(m)]}/{a}"
    except Exception:
        return mes


# ================= GASTOS =================

METODOS_VALIDOS = {"crédito", "credito", "débito", "debito", "pix", "dinheiro", "boleto"}


def normalizar_metodo(m):
    if not m:
        return None
    m = m.strip().lower()
    mapa = {
        "credito": "Crédito", "crédito": "Crédito", "cartão de crédito": "Crédito",
        "cartao de credito": "Crédito", "cc": "Crédito",
        "debito": "Débito", "débito": "Débito", "cartão de débito": "Débito",
        "cartao de debito": "Débito",
        "pix": "Pix",
        "dinheiro": "Dinheiro", "espécie": "Dinheiro", "especie": "Dinheiro",
        "boleto": "Boleto",
        "transferência": "Transferência", "transferencia": "Transferência",
        "ted": "Transferência", "doc": "Transferência",
    }
    # Se não bater com nenhum método conhecido, retorna None pra evitar
    # zoológico de variações ("Subscrição", "Aplicativo", etc)
    return mapa.get(m)


def salvar_gasto(user_id, valor, categoria, descricao, data=None, metodo_pagamento=None, conta_id=None):
    conn = db()
    data = data or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO gastos (user_id, data, valor, categoria, descricao, metodo_pagamento, conta_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (user_id, data, valor, categoria, descricao, metodo_pagamento, conta_id),
    )
    conn.commit()
    conn.close()


def total_gasto_mes(user_id, mes=None, conta_id=None):
    """Total de gastos do mês. conta_id=None → todas as contas (incluindo NULL/Geral)."""
    mes = mes or mes_atual()
    conn = db()
    if conta_id is None:
        total = conn.execute(
            "SELECT SUM(valor) FROM gastos WHERE user_id = ? AND strftime('%Y-%m', data) = ?",
            (user_id, mes),
        ).fetchone()[0]
    else:
        total = conn.execute(
            "SELECT SUM(valor) FROM gastos WHERE user_id = ? AND strftime('%Y-%m', data) = ? AND conta_id = ?",
            (user_id, mes, conta_id),
        ).fetchone()[0]
    conn.close()
    return total or 0.0


def apagar_ultimo_gasto(user_id):
    """DEPRECATED — use apagar_ultimo_lancamento. Mantido por compatibilidade."""
    conn = db()
    row = conn.execute(
        "SELECT id, valor, categoria, descricao FROM gastos WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    if row:
        conn.execute("DELETE FROM gastos WHERE id = ?", (row[0],))
        conn.commit()
    conn.close()
    return row


def apagar_ultimo_lancamento(user_id):
    """Apaga o último lançamento do usuário, considerando GASTOS comuns
    E gastos no CARTÃO de crédito (não pagos). Pega o mais recente entre os dois.
    Retorna (origem, id, valor, categoria, descricao) onde origem é
    'gasto' ou 'cartao', ou None se nada pra apagar."""
    conn = db()
    g = conn.execute(
        "SELECT id, valor, categoria, descricao, data FROM gastos "
        "WHERE user_id = ? ORDER BY id DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    c = conn.execute(
        "SELECT id, valor, categoria, descricao, data FROM gastos_cartao "
        "WHERE user_id = ? AND pago = 0 ORDER BY id DESC LIMIT 1",
        (user_id,),
    ).fetchone()
    escolhido = None
    origem = None
    if g and c:
        # Compara por data de lançamento
        if (c[4] or "") >= (g[4] or ""):
            escolhido, origem = c, "cartao"
        else:
            escolhido, origem = g, "gasto"
    elif g:
        escolhido, origem = g, "gasto"
    elif c:
        escolhido, origem = c, "cartao"

    if not escolhido:
        conn.close()
        return None

    if origem == "gasto":
        conn.execute("DELETE FROM gastos WHERE id = ?", (escolhido[0],))
    else:
        conn.execute("DELETE FROM gastos_cartao WHERE id = ?", (escolhido[0],))
    conn.commit()
    conn.close()
    return (origem, escolhido[0], escolhido[1], escolhido[2], escolhido[3])


def editar_gasto(user_id, gasto_id, novo_valor=None, nova_categoria=None, nova_descricao=None):
    """Edita um gasto específico (tabela gastos). Retorna (antigo, novo) ou None."""
    conn = db()
    antigo = conn.execute(
        "SELECT id, valor, categoria, descricao FROM gastos WHERE id = ? AND user_id = ?",
        (gasto_id, user_id),
    ).fetchone()
    if not antigo:
        conn.close()
        return None
    sets = []
    args = []
    if novo_valor is not None and novo_valor > 0:
        sets.append("valor = ?")
        args.append(float(novo_valor))
    if nova_categoria:
        sets.append("categoria = ?")
        args.append(nova_categoria.strip())
    if nova_descricao is not None:
        sets.append("descricao = ?")
        args.append(nova_descricao.strip())
    if not sets:
        conn.close()
        return ("nada", antigo)
    args.extend([gasto_id, user_id])
    conn.execute(f"UPDATE gastos SET {', '.join(sets)} WHERE id = ? AND user_id = ?", args)
    conn.commit()
    novo = conn.execute(
        "SELECT id, valor, categoria, descricao FROM gastos WHERE id = ?", (gasto_id,),
    ).fetchone()
    conn.close()
    return ("ok", antigo, novo)


def apagar_gasto_por_id(user_id, gasto_id):
    """Apaga um gasto específico por ID. Retorna (id, valor, categoria, descricao) ou None."""
    conn = db()
    row = conn.execute(
        "SELECT id, valor, categoria, descricao FROM gastos WHERE id = ? AND user_id = ?",
        (gasto_id, user_id),
    ).fetchone()
    if not row:
        conn.close()
        return None
    conn.execute("DELETE FROM gastos WHERE id = ? AND user_id = ?", (gasto_id, user_id))
    conn.commit()
    conn.close()
    return row


def editar_receita(user_id, receita_id, novo_valor=None, nova_fonte=None, nova_descricao=None):
    """Edita uma receita específica. Retorna ('ok', antigo, novo) ou ('nada', antigo) ou None."""
    conn = db()
    antigo = conn.execute(
        "SELECT id, valor, fonte, descricao FROM receitas WHERE id = ? AND user_id = ?",
        (receita_id, user_id),
    ).fetchone()
    if not antigo:
        conn.close()
        return None
    sets = []
    args = []
    if novo_valor is not None and novo_valor > 0:
        sets.append("valor = ?")
        args.append(float(novo_valor))
    if nova_fonte:
        sets.append("fonte = ?")
        args.append(nova_fonte.strip())
    if nova_descricao is not None:
        sets.append("descricao = ?")
        args.append(nova_descricao.strip())
    if not sets:
        conn.close()
        return ("nada", antigo)
    args.extend([receita_id, user_id])
    conn.execute(f"UPDATE receitas SET {', '.join(sets)} WHERE id = ? AND user_id = ?", args)
    conn.commit()
    novo = conn.execute(
        "SELECT id, valor, fonte, descricao FROM receitas WHERE id = ?", (receita_id,),
    ).fetchone()
    conn.close()
    return ("ok", antigo, novo)


def apagar_receita_por_id(user_id, receita_id):
    """Apaga uma receita específica. Retorna (id, valor, fonte, descricao) ou None."""
    conn = db()
    row = conn.execute(
        "SELECT id, valor, fonte, descricao FROM receitas WHERE id = ? AND user_id = ?",
        (receita_id, user_id),
    ).fetchone()
    if not row:
        conn.close()
        return None
    conn.execute("DELETE FROM receitas WHERE id = ? AND user_id = ?", (receita_id, user_id))
    conn.commit()
    conn.close()
    return row


def listar_ultimos_gastos(user_id, n=10):
    conn = db()
    rows = conn.execute(
        "SELECT id, data, valor, categoria, descricao FROM gastos "
        "WHERE user_id = ? ORDER BY id DESC LIMIT ?",
        (user_id, n),
    ).fetchall()
    conn.close()
    return rows


def listar_ultimas_receitas(user_id, n=10):
    conn = db()
    rows = conn.execute(
        "SELECT id, data, valor, fonte, descricao FROM receitas "
        "WHERE user_id = ? ORDER BY id DESC LIMIT ?",
        (user_id, n),
    ).fetchall()
    conn.close()
    return rows


def editar_cartao(user_id, cartao_id, novo_nome=None, novo_limite=None,
                  novo_dia_fec=None, novo_dia_venc=None):
    """Edita atributos do cartão (não mexe nos lançamentos). Retorna ('ok', antigo, novo) ou ('nada', antigo) ou None."""
    antigo = buscar_cartao(user_id, cartao_id=cartao_id)
    if not antigo:
        return None
    sets = []
    args = []
    if novo_nome and novo_nome.strip().lower() != antigo[1].lower():
        sets.append("nome = ?")
        args.append(novo_nome.strip())
    if novo_limite is not None and novo_limite > 0 and abs(novo_limite - antigo[2]) > 0.01:
        sets.append("limite = ?")
        args.append(float(novo_limite))
    if novo_dia_fec and 1 <= novo_dia_fec <= 31 and novo_dia_fec != antigo[3]:
        sets.append("dia_fechamento = ?")
        args.append(int(novo_dia_fec))
    if novo_dia_venc and 1 <= novo_dia_venc <= 31 and novo_dia_venc != antigo[4]:
        sets.append("dia_vencimento = ?")
        args.append(int(novo_dia_venc))
    if not sets:
        return ("nada", antigo)
    args.extend([cartao_id, user_id])
    conn = db()
    conn.execute(
        f"UPDATE cartoes_credito SET {', '.join(sets)} WHERE id = ? AND user_id = ?",
        args,
    )
    conn.commit()
    conn.close()
    novo = buscar_cartao(user_id, cartao_id=cartao_id)
    return ("ok", antigo, novo)


# ================= RECEITAS =================

def adicionar_receita_fixa(user_id, descricao, valor, fonte, dia_mes, conta_id=None):
    conn = db()
    conn.execute(
        """INSERT INTO receitas_fixas (user_id, descricao, valor, fonte, dia_mes, ultimo_mes_aplicado, conta_id)
           VALUES (?, ?, ?, ?, ?, NULL, ?)""",
        (user_id, descricao, valor, fonte, dia_mes, conta_id),
    )
    conn.commit()
    conn.close()

def listar_receitas_fixas(user_id):
    """Retorna (id, descricao, valor, fonte, dia_mes, conta_id)."""
    conn = db()
    rows = conn.execute(
        "SELECT id, descricao, valor, fonte, dia_mes, conta_id FROM receitas_fixas "
        "WHERE user_id = ? ORDER BY dia_mes",
        (user_id,),
    ).fetchall()
    conn.close()
    return rows

def remover_receita_fixa(user_id, fixo_id):
    conn = db()
    n = conn.execute(
        "DELETE FROM receitas_fixas WHERE id = ? AND user_id = ?", (fixo_id, user_id)
    ).rowcount
    conn.commit()
    conn.close()
    return n

def aplicar_receitas_fixas_do_dia():
    hoje = datetime.now()
    dia = hoje.day
    mes = hoje.strftime("%Y-%m")
    ultimo = _ultimo_dia_do_mes(hoje.year, hoje.month)
    conn = db()
    # Pega receitas fixas com dia_mes = hoje. NO ÚLTIMO DIA DO MÊS, também
    # pega as com dia_mes > último (ex: dia 31 em fev) — pra não pular o mês.
    if dia == ultimo:
        rows = conn.execute(
            """SELECT id, user_id, descricao, valor, fonte, conta_id FROM receitas_fixas
               WHERE dia_mes >= ? AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
            (dia, mes),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT id, user_id, descricao, valor, fonte, conta_id FROM receitas_fixas
               WHERE dia_mes = ? AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
            (dia, mes),
        ).fetchall()
    aplicados = 0
    for fid, user_id, desc, valor, fonte, conta_id in rows:
        conn.execute(
            "INSERT INTO receitas (user_id, data, valor, fonte, descricao, conta_id) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, hoje.strftime("%Y-%m-%d %H:%M:%S"), valor, fonte, desc, conta_id),
        )
        conn.execute(
            "UPDATE receitas_fixas SET ultimo_mes_aplicado = ? WHERE id = ?", (mes, fid)
        )
        aplicados += 1
    conn.commit()
    conn.close()
    if aplicados:
        print(f"Aplicadas {aplicados} receitas fixas hoje ({hoje.date()})")
def salvar_receita(user_id, valor, fonte, descricao, conta_id=None):
    conn = db()
    conn.execute(
        "INSERT INTO receitas (user_id, data, valor, fonte, descricao, conta_id) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), valor, fonte, descricao, conta_id),
    )
    conn.commit()
    conn.close()


def total_receita_mes(user_id, mes=None, conta_id=None):
    mes = mes or mes_atual()
    conn = db()
    if conta_id is None:
        total = conn.execute(
            "SELECT SUM(valor) FROM receitas WHERE user_id = ? AND strftime('%Y-%m', data) = ?",
            (user_id, mes),
        ).fetchone()[0]
    else:
        total = conn.execute(
            "SELECT SUM(valor) FROM receitas WHERE user_id = ? AND strftime('%Y-%m', data) = ? AND conta_id = ?",
            (user_id, mes, conta_id),
        ).fetchone()[0]
    conn.close()
    return total or 0.0


# ================= ORÇAMENTO =================

def obter_orcamento(user_id, mes=None):
    mes = mes or mes_atual()
    conn = db()
    row = conn.execute(
        "SELECT valor FROM orcamentos WHERE user_id = ? AND mes = ?", (user_id, mes)
    ).fetchone()
    conn.close()
    return row[0] if row else None


def definir_orcamento(user_id, valor, mes=None):
    mes = mes or mes_atual()
    conn = db()
    conn.execute(
        """INSERT INTO orcamentos (user_id, mes, valor) VALUES (?, ?, ?)
           ON CONFLICT(user_id, mes) DO UPDATE SET valor = excluded.valor""",
        (user_id, mes, valor),
    )
    conn.commit()
    conn.close()


def status_orcamento_texto(user_id):
    orc = obter_orcamento(user_id)
    if orc is None:
        return None
    gasto = total_gasto_mes(user_id)
    restante = orc - gasto
    pct = (gasto / orc * 100) if orc > 0 else 0
    if restante < 0:
        alerta = "🚨 Você ultrapassou o orçamento!"
    elif pct >= 90:
        alerta = "⚠️ Atenção: já usou 90% ou mais do orçamento."
    elif pct >= 75:
        alerta = "⚠️ Já usou mais de 75% do orçamento."
    else:
        alerta = ""
    linha = (
        f"📅 Orçamento de {fmt_mes(mes_atual())}: R$ {orc:.2f}\n"
        f"💸 Gasto: R$ {gasto:.2f} ({pct:.0f}%)\n"
        f"💰 Restante: R$ {restante:.2f}"
    )
    if alerta:
        linha += f"\n{alerta}"
    return linha


# ================= METAS =================

def definir_meta(user_id, valor, mes=None):
    mes = mes or mes_atual()
    conn = db()
    conn.execute(
        """INSERT INTO metas (user_id, mes, valor) VALUES (?, ?, ?)
           ON CONFLICT(user_id, mes) DO UPDATE SET valor = excluded.valor""",
        (user_id, mes, valor),
    )
    conn.commit()
    conn.close()


def obter_meta(user_id, mes=None):
    mes = mes or mes_atual()
    conn = db()
    row = conn.execute(
        "SELECT valor FROM metas WHERE user_id = ? AND mes = ?", (user_id, mes)
    ).fetchone()
    conn.close()
    return row[0] if row else None


def status_meta_texto(user_id):
    meta = obter_meta(user_id)
    if meta is None:
        return None
    receitas = total_receita_mes(user_id)
    gastos = total_gasto_mes(user_id)
    economia = receitas - gastos
    pct = (economia / meta * 100) if meta > 0 else 0
    pct = max(0, pct)
    if economia >= meta:
        emoji, msg = "🎉", "Meta batida! Parabéns!"
    elif pct >= 70:
        emoji, msg = "💪", "Você está perto da meta, continua firme!"
    elif economia < 0:
        emoji, msg = "😬", "Você está gastando mais do que ganha esse mês."
    else:
        emoji, msg = "📈", "Continue acompanhando seus gastos."
    return (
        f"🎯 *Meta de economia ({fmt_mes(mes_atual())}):* R$ {meta:.2f}\n"
        f"💵 Receitas: R$ {receitas:.2f}\n"
        f"💸 Gastos: R$ {gastos:.2f}\n"
        f"💰 Economia atual: R$ {economia:.2f} ({pct:.0f}%)\n"
        f"{emoji} {msg}"
    )


# ================= GASTOS FIXOS =================

def adicionar_gasto_fixo(user_id, descricao, valor, categoria, dia_mes, conta_id=None):
    conn = db()
    conn.execute(
        """INSERT INTO gastos_fixos (user_id, descricao, valor, categoria, dia_mes, ultimo_mes_aplicado, conta_id)
           VALUES (?, ?, ?, ?, ?, NULL, ?)""",
        (user_id, descricao, valor, categoria, dia_mes, conta_id),
    )
    conn.commit()
    conn.close()


def listar_gastos_fixos(user_id):
    """Retorna (id, descricao, valor, categoria, dia_mes, conta_id)."""
    conn = db()
    rows = conn.execute(
        "SELECT id, descricao, valor, categoria, dia_mes, conta_id FROM gastos_fixos "
        "WHERE user_id = ? ORDER BY dia_mes",
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def remover_gasto_fixo(user_id, fixo_id):
    conn = db()
    cur = conn.execute(
        "DELETE FROM gastos_fixos WHERE id = ? AND user_id = ?", (fixo_id, user_id)
    )
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def aplicar_gastos_fixos_do_dia():
    """Aplica gastos fixos do dia para todos os usuários.
    No último dia do mês, também aplica fixos cujo dia_mes excede o último
    dia (ex: dia 31 em fevereiro), pra não pular o mês."""
    hoje = datetime.now()
    dia = hoje.day
    mes = hoje.strftime("%Y-%m")
    ultimo = _ultimo_dia_do_mes(hoje.year, hoje.month)
    conn = db()
    if dia == ultimo:
        rows = conn.execute(
            """SELECT id, user_id, descricao, valor, categoria, conta_id FROM gastos_fixos
               WHERE dia_mes >= ? AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
            (dia, mes),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT id, user_id, descricao, valor, categoria, conta_id FROM gastos_fixos
               WHERE dia_mes = ? AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
            (dia, mes),
        ).fetchall()
    aplicados = 0
    for fid, user_id, desc, valor, categoria, conta_id in rows:
        conn.execute(
            "INSERT INTO gastos (user_id, data, valor, categoria, descricao, metodo_pagamento, conta_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, hoje.strftime("%Y-%m-%d %H:%M:%S"), valor, categoria, desc, None, conta_id),
        )
        conn.execute(
            "UPDATE gastos_fixos SET ultimo_mes_aplicado = ? WHERE id = ?", (mes, fid)
        )
        aplicados += 1
    conn.commit()
    conn.close()
    if aplicados:
        print(f"Aplicados {aplicados} gastos fixos hoje ({hoje.date()})")


# ================= PARCELAMENTOS =================

def adicionar_parcelamento(user_id, descricao, valor_total, total_parcelas,
                           dia_cobranca, categoria, metodo_pagamento, cartao_id=None,
                           conta_id=None):
    """Cria parcelamento. Se cartao_id for informado, as parcelas são lançadas
    como compras no cartão (gastos_cartao) e batem na fatura/limite. Se não,
    são lançadas como gasto comum (gastos). Se cartao_id estiver presente, a
    conta_id do parcelamento é ignorada — usa a conta_id_pagamento do cartão."""
    valor_parcela = round(valor_total / total_parcelas, 2)
    conn = db()
    cur = conn.execute(
        """INSERT INTO parcelamentos
           (user_id, descricao, valor_parcela, total_parcelas, parcelas_pagas,
            dia_cobranca, categoria, metodo_pagamento, criado_em, cartao_id, conta_id)
           VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?)""",
        (user_id, descricao, valor_parcela, total_parcelas, dia_cobranca,
         categoria, metodo_pagamento, datetime.now().isoformat(), cartao_id, conta_id),
    )
    pid = cur.lastrowid
    conn.commit()
    conn.close()
    # Aplica a primeira parcela imediatamente se hoje >= dia_cobranca
    aplicar_parcelamentos_do_dia(forcar_id=pid)
    return valor_parcela


def listar_parcelamentos(user_id):
    conn = db()
    rows = conn.execute(
        """SELECT id, descricao, valor_parcela, total_parcelas, parcelas_pagas,
                  dia_cobranca, categoria, metodo_pagamento
           FROM parcelamentos WHERE user_id = ? AND parcelas_pagas < total_parcelas
           ORDER BY id""",
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def remover_parcelamento(user_id, parc_id):
    conn = db()
    n = conn.execute(
        "DELETE FROM parcelamentos WHERE id = ? AND user_id = ?", (parc_id, user_id)
    ).rowcount
    conn.commit()
    conn.close()
    return n


def aplicar_parcelamentos_do_dia(forcar_id=None):
    """Cobra parcela do mês quando o dia chega; aplica primeira parcela imediatamente
    se forcar_id for passado (mesmo que o dia já tenha passado neste mês)."""
    hoje = datetime.now()
    dia = hoje.day
    mes = hoje.strftime("%Y-%m")
    conn = db()
    if forcar_id:
        rows = conn.execute(
            """SELECT id, user_id, descricao, valor_parcela, total_parcelas,
                      parcelas_pagas, categoria, metodo_pagamento, cartao_id, conta_id
               FROM parcelamentos WHERE id = ?
                 AND parcelas_pagas < total_parcelas
                 AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
            (forcar_id, mes),
        ).fetchall()
    else:
        # No último dia do mês, também pega parcelas com dia_cobranca > último
        # (ex: parcela todo dia 31 em fevereiro) — pra não pular a cobrança.
        ultimo = _ultimo_dia_do_mes(hoje.year, hoje.month)
        if dia == ultimo:
            rows = conn.execute(
                """SELECT id, user_id, descricao, valor_parcela, total_parcelas,
                          parcelas_pagas, categoria, metodo_pagamento, cartao_id, conta_id
                   FROM parcelamentos
                   WHERE dia_cobranca >= ? AND parcelas_pagas < total_parcelas
                     AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
                (dia, mes),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT id, user_id, descricao, valor_parcela, total_parcelas,
                          parcelas_pagas, categoria, metodo_pagamento, cartao_id, conta_id
                   FROM parcelamentos
                   WHERE dia_cobranca = ? AND parcelas_pagas < total_parcelas
                     AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
                (dia, mes),
            ).fetchall()
    aplicados = 0
    for pid, user_id, desc, vp, total, pagas, cat, metodo, cartao_id, conta_id in rows:
        nova = pagas + 1
        descricao_completa = f"{desc} ({nova}/{total})"
        if cartao_id:
            # Lança no cartão (afeta fatura e limite). conta_id da parcela é
            # ignorada — herda da conta_id_pagamento do cartão.
            cartao_row = conn.execute(
                "SELECT dia_fechamento, conta_id_pagamento FROM cartoes_credito WHERE id = ? AND user_id = ?",
                (cartao_id, user_id),
            ).fetchone()
            if cartao_row:
                data_lanc = hoje.strftime("%Y-%m-%d %H:%M:%S")
                fatura_mes = calcular_fatura_mes(data_lanc[:10], cartao_row[0])
                cartao_conta_id = cartao_row[1]
                conn.execute(
                    "INSERT INTO gastos_cartao (user_id, cartao_id, valor, categoria, descricao, data, fatura_mes, conta_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (user_id, cartao_id, vp, cat, descricao_completa, data_lanc, fatura_mes, cartao_conta_id),
                )
            else:
                # Cartão sumiu — fallback pra gasto comum
                conn.execute(
                    "INSERT INTO gastos (user_id, data, valor, categoria, descricao, metodo_pagamento, conta_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (user_id, hoje.strftime("%Y-%m-%d %H:%M:%S"), vp, cat, descricao_completa, metodo, conta_id),
                )
        else:
            conn.execute(
                "INSERT INTO gastos (user_id, data, valor, categoria, descricao, metodo_pagamento, conta_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (user_id, hoje.strftime("%Y-%m-%d %H:%M:%S"), vp, cat, descricao_completa, metodo, conta_id),
            )
        conn.execute(
            "UPDATE parcelamentos SET parcelas_pagas = ?, ultimo_mes_aplicado = ? WHERE id = ?",
            (nova, mes, pid),
        )
        aplicados += 1
    conn.commit()
    conn.close()
    if aplicados:
        print(f"Aplicadas {aplicados} parcelas hoje ({hoje.date()})")


# ================= LEMBRETE DIÁRIO =================

def definir_lembrete(user_id, ativo):
    conn = db()
    conn.execute(
        "UPDATE usuarios SET lembrete_ativo = ? WHERE chat_id = ?",
        (1 if ativo else 0, user_id),
    )
    conn.commit()
    conn.close()


def lembrete_ativo(user_id):
    conn = db()
    row = conn.execute(
        "SELECT lembrete_ativo FROM usuarios WHERE chat_id = ?", (user_id,)
    ).fetchone()
    conn.close()
    return bool(row[0]) if row else True


def enviar_lembretes_diarios():
    print("Enviando lembretes diários...")
    conn = db()
    chats = [r[0] for r in conn.execute(
        "SELECT chat_id FROM usuarios WHERE lembrete_ativo = 1"
    ).fetchall()]
    conn.close()
    hoje = datetime.now().strftime("%Y-%m-%d")
    for chat_id in chats:
        c = db()
        n = c.execute(
            "SELECT COUNT(*) FROM gastos WHERE user_id = ? AND data >= ?",
            (chat_id, hoje + " 00:00:00"),
        ).fetchone()[0]
        c.close()
        if n == 0:
            try:
                bot.send_message(
                    chat_id,
                    "👋 Oi! Você não registrou nenhum gasto hoje. "
                    "Quer me contar como foi o dia financeiro? 💰\n\n"
                    "_Pra desativar esses lembretes, é só me dizer 'desativa lembrete'._",
                    parse_mode="Markdown",
                )
            except Exception as e:
                print(f"Erro lembrete pra {chat_id}: {e}")


# ================= BUSCA =================

def buscar_gastos(user_id, texto=None, categoria=None, periodo="esse_mes"):
    """Busca gastos com filtros opcionais — incluindo compras no CARTÃO de crédito
    (faturas em aberto OU já pagas) pra que 'quanto gastei com X' não esconda
    nada do usuário."""
    where = ["user_id = ?"]
    params = [user_id]
    if periodo == "esse_mes":
        where.append("strftime('%Y-%m', data) = ?")
        params.append(mes_atual())
    elif periodo == "mes_passado":
        where.append("strftime('%Y-%m', data) = ?")
        params.append(mes_anterior())
    elif periodo == "semana":
        inicio = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        where.append("data >= ?")
        params.append(inicio)
    # periodo "tudo" não filtra data
    if categoria:
        where.append("LOWER(categoria) LIKE ?")
        params.append(f"%{categoria.lower()}%")
    if texto:
        where.append("LOWER(descricao) LIKE ?")
        params.append(f"%{texto.lower()}%")

    where_sql = " AND ".join(where)

    # UNION dos dois bancos de gastos. metodo_pagamento sintético "Crédito (Cartão)"
    # pra deixar claro de onde veio.
    sql = f"""
        SELECT data, valor, categoria, descricao, COALESCE(metodo_pagamento, '') AS metodo
        FROM gastos WHERE {where_sql}
        UNION ALL
        SELECT data, valor, categoria, descricao, 'Crédito (Cartão)' AS metodo
        FROM gastos_cartao WHERE {where_sql}
        ORDER BY data DESC LIMIT 30
    """
    # params aparece duas vezes (uma pra cada SELECT)
    conn = db()
    rows = conn.execute(sql, params + params).fetchall()
    conn.close()
    return rows


# ================= CONSELHEIRO IA =================

def conselho_financeiro(user_id):
    """Gera conselho personalizado com base nos dados reais do usuário."""
    mes = mes_atual()
    receitas = total_receita_mes(user_id, mes)
    gastos = total_gasto_mes(user_id, mes)
    orc = obter_orcamento(user_id, mes)
    meta = obter_meta(user_id, mes)
    fatura_aberta_total = total_fatura_aberta_todos_cartoes(user_id)

    conn = db()
    por_cat = conn.execute(
        """SELECT categoria, SUM(valor) FROM gastos
           WHERE user_id = ? AND strftime('%Y-%m', data) = ?
           GROUP BY categoria ORDER BY SUM(valor) DESC LIMIT 10""",
        (user_id, mes),
    ).fetchall()
    por_metodo = conn.execute(
        """SELECT COALESCE(metodo_pagamento, 'Não informado'), SUM(valor)
           FROM gastos WHERE user_id = ? AND strftime('%Y-%m', data) = ?
           GROUP BY metodo_pagamento""",
        (user_id, mes),
    ).fetchall()
    # Compras no cartão de crédito do mês corrente
    cartao_mes = conn.execute(
        """SELECT SUM(valor) FROM gastos_cartao
           WHERE user_id = ? AND strftime('%Y-%m', data) = ?""",
        (user_id, mes),
    ).fetchone()[0] or 0.0
    g_ant = total_gasto_mes(user_id, mes_anterior())
    conn.close()

    if gastos == 0 and receitas == 0 and cartao_mes == 0:
        return ("Você ainda não registrou gastos nem receitas esse mês. "
                "Comece anotando seus gastos do dia pra eu poder te ajudar com dicas! 😉")

    contexto = f"""Dados financeiros do usuário em {fmt_mes(mes)}:
- Receitas: R$ {receitas:.2f}
- Gastos pagos (débito/pix/dinheiro): R$ {gastos:.2f}
- Compras no cartão de crédito (mês atual): R$ {cartao_mes:.2f}
- Fatura(s) em aberto (dívida no cartão): R$ {fatura_aberta_total:.2f}
- Saldo do mês (receitas - gastos pagos): R$ {receitas - gastos:.2f}
- Saldo realista (descontando dívida do cartão): R$ {receitas - gastos - fatura_aberta_total:.2f}
- Orçamento: {f'R$ {orc:.2f}' if orc else 'não definido'}
- Meta de economia: {f'R$ {meta:.2f}' if meta else 'não definida'}
- Gastos no mês anterior: R$ {g_ant:.2f}

Top categorias de gasto:
""" + "\n".join(f"- {c}: R$ {v:.2f}" for c, v in por_cat)

    if por_metodo:
        contexto += "\n\nPor método de pagamento:\n" + "\n".join(
            f"- {m}: R$ {v:.2f}" for m, v in por_metodo
        )

    instrucao = """Você é um consultor financeiro pessoal brasileiro, amigável e direto.
Com base nos dados reais do usuário, dê 3 a 5 conselhos PRÁTICOS, ESPECÍFICOS e PERSONALIZADOS
em português brasileiro. Use linguagem próxima e tom positivo. Pode usar emojis com moderação.
Aponte o que está bom, o que merece atenção, e sugira ações concretas (não genéricas).
NÃO retorne JSON. Responda em texto direto, organizado em tópicos curtos com bullets.
Máximo 250 palavras."""

    config = types.GenerateContentConfig(system_instruction=instrucao)
    for modelo in (MODEL_NAME, MODEL_FALLBACK):
        try:
            resp = client.models.generate_content(
                model=modelo, contents=contexto, config=config
            )
            return resp.text
        except Exception as e:
            print(f"Erro conselho ({modelo}): {e}")
            time.sleep(1)
    return "Não consegui gerar o conselho agora. Tenta de novo daqui a pouco?"


# ================= LEITURA DE COMPROVANTE (FOTO) =================

def extrair_dados_comprovante(image_bytes):
    """Usa Gemini Vision pra extrair valor, descrição, categoria e método de pagamento de um comprovante."""
    instrucao = """Analise a imagem de um comprovante, recibo ou nota fiscal brasileiro
e extraia os dados do gasto. Retorne APENAS JSON:
{
  "valor": <float — valor TOTAL pago, com ponto decimal>,
  "descricao": "<onde foi feita a compra ou o que foi comprado, curto>",
  "categoria": "<Alimentação, Transporte, Mercado, Saúde, Lazer, Moradia, etc>",
  "metodo_pagamento": "<Crédito, Débito, Pix, Dinheiro, Boleto ou vazio se não der pra identificar>",
  "data": "<YYYY-MM-DD se aparecer na imagem, vazio se não>"
}
Se não conseguir identificar um valor claro, retorne valor: 0."""
    config = types.GenerateContentConfig(
        system_instruction=instrucao, response_mime_type="application/json"
    )
    image_part = types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg")
    for modelo in (MODEL_NAME, MODEL_FALLBACK):
        try:
            resp = client.models.generate_content(
                model=modelo,
                contents=[image_part, "Extraia os dados desse comprovante."],
                config=config,
            )
            return json.loads(resp.text)
        except Exception as e:
            print(f"Erro vision ({modelo}): {e}")
            time.sleep(1)
    return None


# ================= COMPARAÇÃO =================

def comparar_meses_texto(user_id):
    atual = mes_atual()
    anterior = mes_anterior()
    g_atual = total_gasto_mes(user_id, atual)
    g_ant = total_gasto_mes(user_id, anterior)
    r_atual = total_receita_mes(user_id, atual)
    r_ant = total_receita_mes(user_id, anterior)

    def diff_pct(novo, antigo):
        if antigo == 0:
            return None
        return (novo - antigo) / antigo * 100

    def setinha(p):
        if p is None:
            return ""
        if p > 0:
            return f" 📈 (+{p:.0f}%)"
        if p < 0:
            return f" 📉 ({p:.0f}%)"
        return " (igual)"

    return (
        f"📊 *Comparação {fmt_mes(anterior)} → {fmt_mes(atual)}*\n\n"
        f"💸 Gastos:\n"
        f"  • {fmt_mes(anterior)}: R$ {g_ant:.2f}\n"
        f"  • {fmt_mes(atual)}: R$ {g_atual:.2f}{setinha(diff_pct(g_atual, g_ant))}\n\n"
        f"💵 Receitas:\n"
        f"  • {fmt_mes(anterior)}: R$ {r_ant:.2f}\n"
        f"  • {fmt_mes(atual)}: R$ {r_atual:.2f}{setinha(diff_pct(r_atual, r_ant))}"
    )


# ================= RESUMO DIÁRIO =================

def resumo_diario_texto(user_id, dia=None, tipo="ambos"):
    """
    Retorna entradas e/ou saídas de um dia específico.
    dia: 'YYYY-MM-DD' ou None (hoje)
    tipo: 'gastos', 'receitas' ou 'ambos'
    """
    if dia is None:
        dia = datetime.now().strftime("%Y-%m-%d")
    inicio = f"{dia} 00:00:00"
    fim = f"{dia} 23:59:59"

    try:
        data_pt = datetime.strptime(dia, "%Y-%m-%d").strftime("%d/%m/%Y")
    except ValueError:
        return "Não entendi a data. Tenta 'gastos de hoje', 'extrato de ontem' ou 'movimentações do dia 15/04'."

    hoje_str = datetime.now().strftime("%Y-%m-%d")
    ontem_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if dia == hoje_str:
        rotulo = "hoje"
    elif dia == ontem_str:
        rotulo = "ontem"
    else:
        rotulo = data_pt

    conn = db()
    gastos = []
    receitas = []
    if tipo in ("gastos", "ambos"):
        gastos = conn.execute(
            """SELECT data, valor, categoria, descricao, metodo_pagamento
               FROM gastos WHERE user_id = ? AND data >= ? AND data <= ?
               ORDER BY data ASC""",
            (user_id, inicio, fim),
        ).fetchall()
    if tipo in ("receitas", "ambos"):
        receitas = conn.execute(
            """SELECT data, valor, fonte, descricao
               FROM receitas WHERE user_id = ? AND data >= ? AND data <= ?
               ORDER BY data ASC""",
            (user_id, inicio, fim),
        ).fetchall()
    conn.close()

    total_g = sum(r[1] for r in gastos)
    total_r = sum(r[1] for r in receitas)

    if not gastos and not receitas:
        if tipo == "gastos":
            return f"Você não registrou nenhum gasto {rotulo}. 🎉"
        if tipo == "receitas":
            return f"Você não registrou nenhuma receita {rotulo}."
        return f"Você não tem movimentação registrada {rotulo}."

    titulo_tipo = {
        "gastos": "Saídas",
        "receitas": "Entradas",
        "ambos": "Movimentações",
    }[tipo]
    texto = f"📅 *{titulo_tipo} de {rotulo}* ({data_pt})\n"

    if receitas:
        texto += f"\n💵 *Entradas — R$ {total_r:.2f}*"
        for data, valor, fonte, desc in receitas:
            try:
                hora = datetime.strptime(data, "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
            except Exception:
                hora = ""
            linha = f"\n  • {hora} R$ {valor:.2f} — {fonte}"
            if desc:
                linha += f" ({desc})"
            texto += linha

    if gastos:
        texto += f"\n\n💸 *Saídas — R$ {total_g:.2f}* ({len(gastos)} lançamentos)"
        for data, valor, cat, desc, metodo in gastos:
            try:
                hora = datetime.strptime(data, "%Y-%m-%d %H:%M:%S").strftime("%H:%M")
            except Exception:
                hora = ""
            linha = f"\n  • {hora} R$ {valor:.2f} — {cat}"
            if desc:
                linha += f" ({desc})"
            if metodo:
                linha += f" • {metodo}"
            texto += linha

    if tipo == "ambos" and (gastos or receitas):
        saldo = total_r - total_g
        emoji = "📈" if saldo >= 0 else "📉"
        texto += f"\n\n{emoji} *Saldo do dia: R$ {saldo:.2f}*"

    return texto


# ================= RESUMO SEMANAL =================

def resumo_semanal_texto(user_id):
    hoje = datetime.now()
    inicio = (hoje - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    conn = db()
    total = conn.execute(
        "SELECT SUM(valor) FROM gastos WHERE user_id = ? AND data >= ?", (user_id, inicio)
    ).fetchone()[0] or 0.0
    receitas = conn.execute(
        "SELECT SUM(valor) FROM receitas WHERE user_id = ? AND data >= ?", (user_id, inicio)
    ).fetchone()[0] or 0.0
    cats = conn.execute(
        """SELECT categoria, SUM(valor) FROM gastos
           WHERE user_id = ? AND data >= ?
           GROUP BY categoria ORDER BY SUM(valor) DESC LIMIT 5""",
        (user_id, inicio),
    ).fetchall()
    conn.close()

    texto = "📅 *Resumo dos últimos 7 dias*\n\n"
    texto += f"💸 Total gasto: R$ {total:.2f}\n"
    texto += f"💵 Receitas: R$ {receitas:.2f}\n"
    texto += f"💰 Saldo: R$ {receitas - total:.2f}\n"
    if cats:
        texto += "\n*Top categorias:*"
        for c, v in cats:
            texto += f"\n• {c}: R$ {v:.2f}"
    return texto


def enviar_resumos_semanais():
    print("Enviando resumos semanais...")
    conn = db()
    chats = [r[0] for r in conn.execute("SELECT chat_id FROM usuarios").fetchall()]
    conn.close()
    for chat_id in chats:
        try:
            bot.send_message(chat_id, resumo_semanal_texto(chat_id), parse_mode="Markdown")
        except Exception as e:
            print(f"Erro enviando resumo pra {chat_id}: {e}")


# ================= COMANDOS =================

ADMIN_CHAT_ID = 6828246680  # Ryan Lucas

# Usuários que confirmaram /resetar (chat_id -> timestamp)
_pending_reset = {}


def apagar_dados_usuario(user_id):
    conn = db()
    for tabela in ("gastos", "receitas", "orcamentos", "metas", "gastos_fixos", "parcelamentos"):
        conn.execute(f"DELETE FROM {tabela} WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    
    # Adicione esta linha para zerar a memória da IA também:
    memoria_usuarios.pop(user_id, None)


@bot.message_handler(commands=["resetar", "reiniciar"])
def cmd_resetar(message):
    user_id = message.chat.id
    registrar_usuario(user_id)
    _pending_reset[user_id] = time.time()
    bot.reply_to(
        message,
        "⚠️ *Atenção!* Isso vai apagar TODOS os seus dados:\n"
        "• Gastos, receitas\n"
        "• Orçamento e metas\n"
        "• Gastos fixos e parcelamentos\n\n"
        "Essa ação *não pode ser desfeita*. Pra confirmar, mande:\n"
        "`/confirmar_reset`\n\n"
        "Você tem 60 segundos.",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["confirmar_reset"])
def cmd_confirmar_reset(message):
    user_id = message.chat.id
    ts = _pending_reset.get(user_id)
    if not ts or time.time() - ts > 60:
        bot.reply_to(message, "Nada pra confirmar. Mande /resetar primeiro se quiser apagar seus dados.")
        return
    apagar_dados_usuario(user_id)
    _pending_reset.pop(user_id, None)
    bot.reply_to(message, "🧹 Pronto! Seus dados foram apagados. Tá com a casa limpa pra recomeçar. 💪")



@bot.message_handler(commands=["autorizar"])
def cmd_autorizar(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    partes = message.text.split(maxsplit=2)
    if len(partes) < 2:
        bot.reply_to(message, "Uso: `/autorizar <chat_id> [nome opcional]`", parse_mode="Markdown")
        return
    try:
        chat_id = int(partes[1])
    except ValueError:
        bot.reply_to(message, "chat_id inválido. Precisa ser um número.")
        return
    nome = partes[2] if len(partes) > 2 else None
    autorizar_usuario(chat_id, nome)
    extra = f" ({nome})" if nome else ""
    bot.reply_to(message, f"✅ Usuário `{chat_id}`{extra} autorizado a usar o bot.", parse_mode="Markdown")


@bot.message_handler(commands=["desautorizar"])
def cmd_desautorizar(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    partes = message.text.split(maxsplit=1)
    if len(partes) < 2:
        bot.reply_to(message, "Uso: `/desautorizar <chat_id>`", parse_mode="Markdown")
        return
    try:
        chat_id = int(partes[1])
    except ValueError:
        bot.reply_to(message, "chat_id inválido. Precisa ser um número.")
        return
    n = desautorizar_usuario(chat_id)
    if n:
        bot.reply_to(message, f"🚫 Usuário `{chat_id}` removido da whitelist.", parse_mode="Markdown")
    else:
        bot.reply_to(message, f"Usuário `{chat_id}` não estava autorizado.", parse_mode="Markdown")


@bot.message_handler(commands=["listar_autorizados"])
def cmd_listar_autorizados(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    rows = listar_autorizados()
    if not rows:
        bot.reply_to(message, "Ninguém na whitelist (só você tem acesso).")
        return
    texto = f"✅ *Usuários autorizados ({len(rows)}):*\n"
    for chat_id, nome, quando in rows:
        try:
            data_fmt = datetime.fromisoformat(quando).strftime("%d/%m/%Y %H:%M")
        except Exception:
            data_fmt = quando or "?"
        linha = f"\n• `{chat_id}` — {data_fmt}"
        if nome:
            linha += f" ({nome})"
        texto += linha
    bot.reply_to(message, texto, parse_mode="Markdown")


@bot.message_handler(commands=["feedback"])
def cmd_feedback(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        bot.reply_to(
            message,
            f"🔒 Esse bot é privado. Pra pedir acesso, mande seu ID (`{user_id}`) pro dono.",
            parse_mode="Markdown",
        )
        return

    partes = message.text.split(maxsplit=1)
    if len(partes) < 2 or not partes[1].strip():
        bot.reply_to(
            message,
            "💬 *Mande sua sugestão, crítica ou relato de bug assim:*\n"
            "`/feedback aqui vai a sua mensagem`\n\n"
            "Vou ler tudo e responder se fizer sentido. 🙏",
            parse_mode="Markdown",
        )
        return

    mensagem = partes[1].strip()
    if len(mensagem) > 2000:
        bot.reply_to(message, "Sua mensagem ficou muito longa (máx. 2000 caracteres). Resume um pouco?")
        return

    nome = (message.from_user.first_name or "").strip() if message.from_user else ""
    if message.from_user and message.from_user.username:
        nome = f"{nome} (@{message.from_user.username})".strip()

    fb_id = salvar_feedback(user_id, nome, mensagem)
    bot.reply_to(message, f"✅ Recebi seu feedback (#{fb_id}). Obrigado! 🙌")

    # Notifica o admin no privado
    try:
        bot.send_message(
            ADMIN_CHAT_ID,
            f"📥 *Novo feedback #{fb_id}*\n"
            f"👤 De: {nome or 'sem nome'} (`{user_id}`)\n"
            f"🕒 {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
            f"{mensagem}\n\n"
            f"_Responder:_ `/responder {fb_id} sua resposta aqui`",
            parse_mode="Markdown",
        )
    except Exception as e:
        print(f"Erro ao notificar admin sobre feedback: {e}")


@bot.message_handler(commands=["feedbacks"])
def cmd_feedbacks(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    rows = listar_feedbacks(apenas_nao_lidos=True, limite=20)
    if not rows:
        bot.reply_to(message, "📭 Nenhum feedback novo.")
        return
    texto = f"📥 *Feedbacks não lidos ({len(rows)}):*\n"
    for fb_id, uid, nome, msg, quando, _lido in rows:
        try:
            data_fmt = datetime.fromisoformat(quando).strftime("%d/%m %H:%M")
        except Exception:
            data_fmt = quando or "?"
        preview = msg if len(msg) <= 200 else msg[:200] + "..."
        texto += (
            f"\n━━━━━━━━━━━━━━━━━━━\n"
            f"*#{fb_id}* — {data_fmt}\n"
            f"👤 {nome or 'sem nome'} (`{uid}`)\n"
            f"{preview}\n"
            f"_Marcar lido:_ `/lido {fb_id}` — _Responder:_ `/responder {fb_id} ...`"
        )
    # quebra em pedaços se passar de 4000 chars
    while texto:
        bot.send_message(ADMIN_CHAT_ID, texto[:4000], parse_mode="Markdown")
        texto = texto[4000:]


@bot.message_handler(commands=["feedbacks_todos"])
def cmd_feedbacks_todos(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    rows = listar_feedbacks(apenas_nao_lidos=False, limite=20)
    if not rows:
        bot.reply_to(message, "📭 Nenhum feedback ainda.")
        return
    texto = f"📋 *Últimos {len(rows)} feedbacks (todos):*\n"
    for fb_id, uid, nome, msg, quando, lido in rows:
        try:
            data_fmt = datetime.fromisoformat(quando).strftime("%d/%m %H:%M")
        except Exception:
            data_fmt = quando or "?"
        marcador = "✅" if lido else "🆕"
        preview = msg if len(msg) <= 150 else msg[:150] + "..."
        texto += (
            f"\n━━━━━━━━━━━━━━━━━━━\n"
            f"{marcador} *#{fb_id}* — {data_fmt}\n"
            f"👤 {nome or 'sem nome'} (`{uid}`)\n"
            f"{preview}"
        )
    while texto:
        bot.send_message(ADMIN_CHAT_ID, texto[:4000], parse_mode="Markdown")
        texto = texto[4000:]


@bot.message_handler(commands=["lido"])
def cmd_lido(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    partes = message.text.split(maxsplit=1)
    if len(partes) < 2:
        bot.reply_to(message, "Uso: `/lido <id>`", parse_mode="Markdown")
        return
    try:
        fb_id = int(partes[1])
    except ValueError:
        bot.reply_to(message, "ID inválido. Precisa ser um número.")
        return
    n = marcar_feedback_lido(fb_id)
    if n:
        bot.reply_to(message, f"✅ Feedback #{fb_id} marcado como lido.")
    else:
        bot.reply_to(message, f"Feedback #{fb_id} não encontrado.")


@bot.message_handler(commands=["responder"])
def cmd_responder(message):
    if message.chat.id != ADMIN_CHAT_ID:
        return
    partes = message.text.split(maxsplit=2)
    if len(partes) < 3:
        bot.reply_to(message, "Uso: `/responder <id> <mensagem>`", parse_mode="Markdown")
        return
    try:
        fb_id = int(partes[1])
    except ValueError:
        bot.reply_to(message, "ID inválido. Precisa ser um número.")
        return

    fb = buscar_feedback(fb_id)
    if not fb:
        bot.reply_to(message, f"Feedback #{fb_id} não encontrado.")
        return

    _, dest_user_id, _, _, _, _ = fb
    resposta = partes[2]
    try:
        bot.send_message(
            dest_user_id,
            f"💬 *Resposta do dono do bot ao seu feedback #{fb_id}:*\n\n{resposta}",
            parse_mode="Markdown",
        )
        marcar_feedback_lido(fb_id)
        bot.reply_to(message, f"✅ Resposta enviada pro usuário `{dest_user_id}` e feedback marcado como lido.", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Não consegui enviar a resposta: {e}")


@bot.message_handler(commands=["cartao_novo"])
def cmd_cartao_novo(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split()
    if len(partes) < 5:
        bot.reply_to(
            message,
            "Uso: `/cartao_novo <nome> <limite> <dia_fechamento> <dia_vencimento>`\n"
            "Exemplo: `/cartao_novo Nubank 5000 28 5`",
            parse_mode="Markdown",
        )
        return
    try:
        nome = partes[1]
        limite = float(partes[2].replace(",", "."))
        dia_fec = int(partes[3])
        dia_venc = int(partes[4])
        if not (1 <= dia_fec <= 31 and 1 <= dia_venc <= 31):
            raise ValueError("dia inválido")
    except ValueError:
        bot.reply_to(message, "Valores inválidos. Limite tem que ser número e os dias entre 1 e 31.")
        return
    if buscar_cartao(user_id, nome=nome):
        bot.reply_to(message, f"Você já tem um cartão chamado *{nome}*. Escolhe outro nome.", parse_mode="Markdown")
        return
    cid = criar_cartao(user_id, nome, limite, dia_fec, dia_venc)
    bot.reply_to(
        message,
        f"✅ Cartão *{nome}* cadastrado!\n"
        f"💳 Limite: R$ {limite:.2f}\n"
        f"📅 Fecha dia {dia_fec}, vence dia {dia_venc}",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["cartoes"])
def cmd_cartoes(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    cartoes = listar_cartoes(user_id)
    if not cartoes:
        bot.reply_to(
            message,
            "Você ainda não tem cartões cadastrados.\n"
            "Cadastre com: `/cartao_novo Nubank 5000 28 5`",
            parse_mode="Markdown",
        )
        return
    texto = "💳 *Seus cartões:*\n"
    for cid, nome, limite, dia_fec, dia_venc in cartoes:
        usado, _, pct = percentual_limite_usado(user_id, cid)
        disponivel = limite - usado
        texto += (
            f"\n━━━━━━━━━━━━━━━━━━━\n"
            f"🏷️ *{nome}* (id {cid})\n"
            f"💰 Limite: R$ {limite:.2f} | Usado: R$ {usado:.2f} ({pct:.0f}%)\n"
            f"✅ Disponível: R$ {disponivel:.2f}\n"
            f"📅 Fecha dia {dia_fec} | Vence dia {dia_venc}"
        )
    bot.reply_to(message, texto, parse_mode="Markdown")


@bot.message_handler(commands=["fatura"])
def cmd_fatura(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=1)
    cartoes = listar_cartoes(user_id)
    if not cartoes:
        bot.reply_to(message, "Você não tem cartões cadastrados ainda.")
        return
    if len(partes) < 2:
        if len(cartoes) == 1:
            cartao = cartoes[0]
        else:
            nomes = ", ".join(c[1] for c in cartoes)
            bot.reply_to(message, f"Você tem mais de um cartão. Use: `/fatura <nome>`\nCartões: {nomes}", parse_mode="Markdown")
            return
    else:
        nome = partes[1].strip()
        cartao = buscar_cartao(user_id, nome=nome)
        if not cartao:
            bot.reply_to(message, f"Não achei cartão com nome *{nome}*.", parse_mode="Markdown")
            return
    cid, nome, limite, dia_fec, dia_venc = cartao
    rows, total = fatura_aberta(user_id, cid)
    if not rows:
        bot.reply_to(message, f"💳 *{nome}*: nenhuma fatura aberta. ✨", parse_mode="Markdown")
        return
    venc = proxima_data_vencimento(dia_venc)
    texto = (
        f"💳 *Fatura {nome}*\n"
        f"📅 Próximo vencimento: {venc.strftime('%d/%m/%Y')}\n"
        f"💸 Total: *R$ {total:.2f}*\n\n"
        f"*Lançamentos:*"
    )
    for gid, valor, cat, desc, data in rows[:25]:
        try:
            d = datetime.strptime(data[:10], "%Y-%m-%d").strftime("%d/%m")
        except Exception:
            d = data[:10]
        cat = cat or "—"
        desc = desc or ""
        texto += f"\n• {d} | R$ {valor:.2f} | {cat}{(' — ' + desc) if desc else ''}"
    if len(rows) > 25:
        texto += f"\n_(+{len(rows) - 25} lançamentos)_"
    texto += f"\n\n_Quando pagar:_ `/pagar_fatura {nome}`"
    bot.reply_to(message, texto, parse_mode="Markdown")


@bot.message_handler(commands=["pagar_fatura"])
def cmd_pagar_fatura(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=1)
    cartoes = listar_cartoes(user_id)
    if not cartoes:
        bot.reply_to(message, "Você não tem cartões cadastrados.")
        return
    if len(partes) < 2:
        if len(cartoes) == 1:
            cartao = cartoes[0]
        else:
            bot.reply_to(message, "Use: `/pagar_fatura <nome do cartão>`", parse_mode="Markdown")
            return
    else:
        cartao = buscar_cartao(user_id, nome=partes[1].strip())
        if not cartao:
            bot.reply_to(message, f"Cartão *{partes[1]}* não encontrado.", parse_mode="Markdown")
            return
    cid, nome, *_ = cartao
    n, total = pagar_fatura(user_id, cid)
    if n == 0:
        bot.reply_to(message, f"Não tinha fatura aberta no *{nome}*.", parse_mode="Markdown")
        return
    bot.reply_to(
        message,
        f"✅ Fatura do *{nome}* paga!\n"
        f"💸 Total: R$ {total:.2f} ({n} lançamento(s))\n"
        f"📊 Já lancei como gasto no seu saldo.",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["cartao_remover"])
def cmd_cartao_remover(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=1)
    if len(partes) < 2:
        bot.reply_to(message, "Uso: `/cartao_remover <nome>`", parse_mode="Markdown")
        return
    cartao = buscar_cartao(user_id, nome=partes[1].strip())
    if not cartao:
        bot.reply_to(message, f"Cartão *{escape_md(partes[1])}* não encontrado.", parse_mode="Markdown")
        return
    # Conta quantos lançamentos serão apagados pra dar contexto
    conn = db()
    n_lanc = conn.execute(
        "SELECT COUNT(*) FROM gastos_cartao WHERE user_id = ? AND cartao_id = ?",
        (user_id, cartao[0]),
    ).fetchone()[0]
    conn.close()
    set_pendente(user_id, "remover_cartao", {"cartao_id": cartao[0], "nome": cartao[1]})
    bot.reply_to(
        message,
        f"⚠️ Confirmar remoção do cartão *{escape_md(cartao[1])}*?\n"
        f"Vai apagar também *{n_lanc}* lançamento(s) registrado(s) nele.\n\n"
        f"Responde *sim* pra confirmar ou *não* pra cancelar.\n"
        f"_(Esta confirmação expira em 2 minutos.)_",
        parse_mode="Markdown",
    )


# ================= CONTAS BANCÁRIAS (slash commands) =================

@bot.message_handler(commands=["contas"])
def cmd_contas(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    contas = listar_contas(user_id)
    if not contas:
        bot.reply_to(
            message,
            "Você ainda não cadastrou nenhuma conta. Use `/conta_nova <nome> [tipo] [banco]` "
            "ou diga 'cria uma conta Nubank'.\n\n"
            "Lançamentos sem conta vão pra pilha *Geral*.",
            parse_mode="Markdown",
        )
        return
    texto = "🏦 *Suas contas:*\n"
    saldo_total = 0.0
    for cid, nome, banco, tipo, _criado in contas:
        saldo = saldo_conta(user_id, cid)
        saldo_total += saldo
        detalhes = tipo or "Conta"
        if banco and banco.lower() != nome.lower():
            detalhes += f" · {banco}"
        texto += f"\n• #{cid} *{escape_md(nome)}* ({detalhes})\n  Saldo atual: R$ {saldo:.2f}"
    texto += f"\n\n💰 *Total: R$ {saldo_total:.2f}*"
    bot.reply_to(message, texto, parse_mode="Markdown")


@bot.message_handler(commands=["conta_nova"])
def cmd_conta_nova(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=3)
    if len(partes) < 2:
        bot.reply_to(
            message,
            "Uso: `/conta_nova <nome> [tipo] [banco]`\n"
            "Ex: `/conta_nova Nubank corrente Nubank`\n"
            "Tipos válidos: corrente, poupança, digital, conjunta, salário",
            parse_mode="Markdown",
        )
        return
    nome = partes[1].strip()
    tipo = partes[2].strip() if len(partes) >= 3 else ""
    banco = partes[3].strip() if len(partes) >= 4 else nome
    if buscar_conta(user_id, nome=nome):
        bot.reply_to(message, f"Você já tem uma conta chamada *{escape_md(nome)}*.", parse_mode="Markdown")
        return
    cid = criar_conta(user_id, nome, banco=banco, tipo=tipo)
    bot.reply_to(
        message,
        f"🏦 Conta *{escape_md(nome)}* cadastrada (#{cid}, {normalizar_tipo_conta(tipo)})!",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["conta_remover"])
def cmd_conta_remover(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=1)
    if len(partes) < 2:
        bot.reply_to(message, "Uso: `/conta_remover <nome>`", parse_mode="Markdown")
        return
    nome = partes[1].strip()
    status, cid, obj = resolver_conta(user_id, nome)
    if status == "ambigua":
        nomes = ", ".join(c[1] for c in obj)
        bot.reply_to(message, f"Ambíguo. Tem mais de uma conta com esse nome ({nomes}).")
        return
    if status != "ok":
        bot.reply_to(message, f"Conta *{escape_md(nome)}* não encontrada.", parse_mode="Markdown")
        return
    n_lanc = contar_lancamentos_conta(user_id, cid)
    set_pendente(user_id, "remover_conta", {"conta_id": cid, "nome": obj[1]})
    aviso = f"\nEla tem *{n_lanc}* lançamento(s) — eles viram 'Geral' (não são apagados)." if n_lanc else ""
    bot.reply_to(
        message,
        f"⚠️ Confirmar remoção da conta *{escape_md(obj[1])}*?{aviso}\n\n"
        f"Responde *sim* pra confirmar ou *não* pra cancelar.",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["saldo"])
def cmd_saldo(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=1)
    nome_filtro = partes[1].strip() if len(partes) >= 2 else ""
    if nome_filtro:
        status, cid, obj = resolver_conta(user_id, nome_filtro)
        if status == "ambigua":
            nomes = ", ".join(c[1] for c in obj)
            bot.reply_to(message, f"Ambíguo: {nomes}.")
            return
        if status != "ok":
            bot.reply_to(message, f"Conta *{escape_md(nome_filtro)}* não encontrada.", parse_mode="Markdown")
            return
        saldo_atual = saldo_conta(user_id, cid)
        rec = total_receita_mes(user_id, conta_id=cid)
        gas = total_gasto_mes(user_id, conta_id=cid)
        bot.reply_to(
            message,
            f"🏦 *{escape_md(obj[1])}*\n"
            f"💰 Saldo atual: *R$ {saldo_atual:.2f}*\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"📅 Este mês ({fmt_mes(mes_atual())}):\n"
            f"  ➕ Entradas: R$ {rec:.2f}\n  ➖ Saídas: R$ {gas:.2f}",
            parse_mode="Markdown",
        )
        return
    contas = listar_contas(user_id)
    rec_geral = total_receita_mes(user_id)
    gas_geral = total_gasto_mes(user_id)
    saldo_total = sum(saldo_conta(user_id, c[0]) for c in contas)
    texto = (
        f"💰 *Saldo total: R$ {saldo_total:.2f}*\n"
        f"━━━━━━━━━━━━━━━━━\n"
        f"📅 Esse mês ({fmt_mes(mes_atual())}):\n"
        f"  ➕ Entradas: R$ {rec_geral:.2f}\n  ➖ Saídas: R$ {gas_geral:.2f}"
    )
    if contas:
        texto += "\n\n*Por conta:*"
        for cid, nome_c, _b, _t, _c in contas:
            s = saldo_conta(user_id, cid)
            texto += f"\n• {escape_md(nome_c)}: R$ {s:.2f}"
    bot.reply_to(message, texto, parse_mode="Markdown")


@bot.message_handler(commands=["investir"])
def cmd_investir(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=3)
    if len(partes) < 4:
        bot.reply_to(
            message,
            "Uso: `/investir <valor> <tipo> <nome>`\n"
            "Tipos: Reserva, Renda Fixa, Ações, FIIs, Cripto, Outros\n"
            "Exemplo: `/investir 1000 Reserva Tesouro Selic`",
            parse_mode="Markdown",
        )
        return
    try:
        valor = float(partes[1].replace(",", "."))
    except ValueError:
        bot.reply_to(message, "Valor inválido.")
        return
    tipo = partes[2]
    nome = partes[3]
    iid = registrar_investimento(user_id, tipo, nome, valor)
    bot.reply_to(
        message,
        f"📈 Investimento registrado (#{iid})\n"
        f"💰 R$ {valor:.2f} em *{nome}* ({normalizar_tipo_investimento(tipo)})",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["investimentos"])
def cmd_investimentos(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    invs = listar_investimentos(user_id)
    if not invs:
        bot.reply_to(
            message,
            "Você ainda não registrou investimentos.\n"
            "Comece com: `/investir 1000 Reserva Tesouro Selic`",
            parse_mode="Markdown",
        )
        return
    total = total_investimentos(user_id)
    por_tipo = {}
    for _, tipo, _, valor, _ in invs:
        por_tipo.setdefault(tipo, []).append(valor)
    texto = f"📈 *Sua carteira:* R$ {total:.2f}\n"
    for tipo, vals in sorted(por_tipo.items()):
        soma = sum(vals)
        pct = soma / total * 100 if total > 0 else 0
        texto += f"\n*{tipo}* — R$ {soma:.2f} ({pct:.0f}%)"
    texto += "\n\n*Aportes registrados:*"
    for iid, tipo, nome, valor, data in invs[:20]:
        try:
            d = datetime.fromisoformat(data).strftime("%d/%m/%y")
        except Exception:
            d = data[:10]
        texto += f"\n• #{iid} {d} | R$ {valor:.2f} | {tipo} — {nome}"
    if len(invs) > 20:
        texto += f"\n_(+{len(invs) - 20} aportes)_"
    bot.reply_to(message, texto, parse_mode="Markdown")


@bot.message_handler(commands=["resgatar"])
def cmd_resgatar(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=2)
    if len(partes) < 3:
        bot.reply_to(message, "Uso: `/resgatar <valor> <nome>`\nEx: `/resgatar 500 Tesouro Selic`", parse_mode="Markdown")
        return
    try:
        valor = float(partes[1].replace(",", "."))
    except ValueError:
        bot.reply_to(message, "Valor inválido.")
        return
    nome = partes[2]
    resgatado = resgatar_investimento(user_id, nome, valor)
    if resgatado is None:
        bot.reply_to(message, f"Não achei investimento ativo chamado *{nome}*.", parse_mode="Markdown")
        return
    bot.reply_to(
        message,
        f"💸 Resgatado: R$ {resgatado:.2f} de *{nome}*\n"
        f"📥 Lancei como receita no seu saldo.",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["editar_investir"])
def cmd_editar_investir(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=1)
    if len(partes) < 2:
        bot.reply_to(
            message,
            "Uso: `/editar_investir <id_ou_nome> [tipo:NovoTipo] [nome:Novo Nome] [valor:1234]`\n\n"
            "Exemplos:\n"
            "• `/editar_investir caixinha tipo:Reserva nome:Reserva de Emergência`\n"
            "• `/editar_investir 3 valor:2500`\n"
            "• `/editar_investir Tesouro nome:Tesouro Selic 2030`",
            parse_mode="Markdown",
        )
        return
    args = partes[1].strip()
    novo_tipo = None
    novo_nome = None
    novo_valor = None
    import re as _re
    matches = list(_re.finditer(r"(tipo|nome|valor)\s*:\s*", args, _re.IGNORECASE))
    if not matches:
        bot.reply_to(message, "Não achei nenhum campo (tipo:, nome: ou valor:) pra editar.\nUse `/editar_investir` sem nada pra ver exemplos.", parse_mode="Markdown")
        return
    identificador = args[:matches[0].start()].strip()
    for i, m in enumerate(matches):
        chave = m.group(1).lower()
        ini = m.end()
        fim = matches[i + 1].start() if i + 1 < len(matches) else len(args)
        valor_raw = args[ini:fim].strip()
        if chave == "tipo":
            novo_tipo = valor_raw
        elif chave == "nome":
            novo_nome = valor_raw
        elif chave == "valor":
            try:
                novo_valor = float(valor_raw.replace("R$", "").replace(",", ".").strip())
            except ValueError:
                bot.reply_to(message, f"Valor inválido: `{valor_raw}`", parse_mode="Markdown")
                return
    if not identificador:
        bot.reply_to(message, "Você precisa dizer qual investimento editar (id ou nome).", parse_mode="Markdown")
        return
    res = editar_investimento(user_id, identificador, novo_nome, novo_tipo, novo_valor)
    if res is None:
        bot.reply_to(message, f"Não achei investimento ativo com *{identificador}*.", parse_mode="Markdown")
        return
    if res[0] == "nada":
        bot.reply_to(message, "Nada pra mudar — os valores informados são iguais aos atuais.")
        return
    _, atualizado, antigo = res
    bot.reply_to(
        message,
        f"✏️ *Investimento atualizado!*\n"
        f"De: {antigo[2]} ({antigo[1]}) — R$ {antigo[3]:.2f}\n"
        f"Para: *{atualizado[2]}* ({atualizado[1]}) — R$ {atualizado[3]:.2f}",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["transferir_investir"])
def cmd_transferir_investir(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    txt = message.text.split(maxsplit=1)
    if len(txt) < 2:
        bot.reply_to(
            message,
            "Uso: `/transferir_investir <valor> <origem> para <destino> [tipo:X]`\n"
            "Ex: `/transferir_investir 500 caixinha para Reserva de Emergência tipo:Reserva`",
            parse_mode="Markdown",
        )
        return
    args = txt[1]
    import re as _re
    tipo_destino = None
    m_tipo = _re.search(r"tipo\s*:\s*([\w\sçãéí]+)$", args, _re.IGNORECASE)
    if m_tipo:
        tipo_destino = m_tipo.group(1).strip()
        args = args[:m_tipo.start()].strip()
    m = _re.match(r"^([\d\.,]+)\s+(.+?)\s+(?:para|pra|->|→)\s+(.+)$", args, _re.IGNORECASE)
    if not m:
        bot.reply_to(message, "Formato inválido. Use: `<valor> <origem> para <destino>`", parse_mode="Markdown")
        return
    try:
        valor = float(m.group(1).replace(",", "."))
    except ValueError:
        bot.reply_to(message, "Valor inválido.")
        return
    origem_nome = m.group(2).strip()
    destino_nome = m.group(3).strip()
    status, origem, destino = transferir_investimento(user_id, origem_nome, destino_nome, valor, tipo_destino)
    if status == "origem_nao_encontrada":
        bot.reply_to(message, f"Não achei investimento ativo chamado *{origem_nome}*.", parse_mode="Markdown")
        return
    if status == "valor_invalido":
        bot.reply_to(message, "O valor da transferência tem que ser maior que zero.")
        return
    if status == "saldo_insuficiente":
        bot.reply_to(
            message,
            f"💸 Saldo insuficiente em *{origem[2]}* (você tem R$ {origem[3]:.2f}).",
            parse_mode="Markdown",
        )
        return
    bot.reply_to(
        message,
        f"🔄 *Transferência feita!*\n"
        f"R$ {valor:.2f} saiu de *{origem[2]}* ({origem[1]})\n"
        f"➡️ Foi pra *{destino[2]}* ({destino[1]}) — agora total R$ {destino[3]:.2f}\n"
        f"_O dinheiro continua na sua carteira de investimentos, só mudou de lugar._",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["remover_investir"])
def cmd_remover_investir(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    partes = message.text.split(maxsplit=1)
    if len(partes) < 2:
        bot.reply_to(
            message,
            "Uso: `/remover_investir <id_ou_nome>`\n"
            "Ex: `/remover_investir caixinha` ou `/remover_investir 3`",
            parse_mode="Markdown",
        )
        return
    identificador = partes[1].strip()
    res = remover_investimento(user_id, identificador)
    if res is None:
        bot.reply_to(message, f"Não achei nenhum investimento com *{identificador}*.", parse_mode="Markdown")
        return
    _, tipo, nome, valor = res
    bot.reply_to(
        message,
        f"🗑️ *Investimento removido!*\n"
        f"{nome} ({tipo}) — R$ {valor:.2f}\n"
        f"_Não mexi no seu saldo._",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["patrimonio"])
def cmd_patrimonio(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    bot.reply_to(message, patrimonio_texto(user_id), parse_mode="Markdown")


@bot.message_handler(commands=["dica_investir"])
def cmd_dica_investir(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        return
    bot.send_chat_action(message.chat.id, "typing")
    bot.reply_to(message, dica_investimento_texto(user_id), parse_mode="Markdown")


@bot.message_handler(commands=["meu_id"])
def cmd_meu_id(message):
    """Comando público pra quem quer pedir acesso ao admin."""
    bot.reply_to(
        message,
        f"Seu ID é: `{message.chat.id}`\n"
        f"Mande esse número pro dono do bot pra pedir acesso.",
        parse_mode="Markdown",
    )


@bot.message_handler(commands=["stats"])
def cmd_stats(message):
    if message.chat.id != ADMIN_CHAT_ID:
        bot.reply_to(message, "Esse comando é só pro dono do bot 🤐")
        return
    conn = db()
    total_usuarios = conn.execute("SELECT COUNT(*) FROM usuarios").fetchone()[0]
    ativos_30d = conn.execute(
        "SELECT COUNT(DISTINCT user_id) FROM gastos WHERE data >= datetime('now', '-30 days')"
    ).fetchone()[0]
    ativos_7d = conn.execute(
        "SELECT COUNT(DISTINCT user_id) FROM gastos WHERE data >= datetime('now', '-7 days')"
    ).fetchone()[0]
    total_gastos = conn.execute("SELECT COUNT(*) FROM gastos").fetchone()[0]
    total_receitas = conn.execute("SELECT COUNT(*) FROM receitas").fetchone()[0]
    soma_gastos = conn.execute("SELECT COALESCE(SUM(valor), 0) FROM gastos").fetchone()[0]
    soma_receitas = conn.execute("SELECT COALESCE(SUM(valor), 0) FROM receitas").fetchone()[0]
    novos_7d = conn.execute(
        "SELECT COUNT(*) FROM usuarios WHERE primeiro_contato >= datetime('now', '-7 days')"
    ).fetchone()[0]
    top = conn.execute(
        "SELECT user_id, COUNT(*) c FROM gastos GROUP BY user_id ORDER BY c DESC LIMIT 5"
    ).fetchall()
    conn.close()

    texto = (
        "📊 *Estatísticas do bot*\n\n"
        f"👥 Usuários totais: *{total_usuarios}*\n"
        f"🆕 Novos (últimos 7 dias): *{novos_7d}*\n"
        f"🟢 Ativos (últimos 7 dias): *{ativos_7d}*\n"
        f"🔵 Ativos (últimos 30 dias): *{ativos_30d}*\n\n"
        f"💸 Gastos registrados: *{total_gastos}* (R$ {soma_gastos:.2f})\n"
        f"💰 Receitas registradas: *{total_receitas}* (R$ {soma_receitas:.2f})\n"
    )
    if top:
        texto += "\n*Top 5 usuários por nº de gastos:*\n"
        for i, (uid, c) in enumerate(top, 1):
            texto += f"{i}. `{uid}` — {c} gastos\n"
    bot.reply_to(message, texto, parse_mode="Markdown")


@bot.message_handler(commands=["start", "help"])
def send_welcome(message):
    registrar_usuario(message.chat.id)
    texto = (
        "Olá! Eu sou o *Gerenciador Financeiro* 💰\n"
        "_Criado por Ryan Lucas._\n\n"
        "Seus dados são privados — só você vê seus próprios gastos, orçamento e metas.\n\n"
        "💬 *Você pode falar comigo naturalmente:*\n"
        "• 'gastei 50 no mercado no débito'\n"
        "• 'gastei 80 no ifood no crédito Nubank' (vai pra fatura!)\n"
        "• 'gastei 30 no mercado pelo Itaú' (debita de uma conta específica)\n"
        "• 'recebi 3000 de salário no Nubank'\n"
        "• 'transferi 500 do Nubank pro Itaú'\n"
        "• 'qual o saldo do Itaú?'\n"
        "• 'gasto de 200 vence dia 15/05/2026' (agendado)\n"
        "• 'comprei celular 1200 em 12x' (parcelamento)\n"
        "• 'recebi 3000 de salário'\n"
        "• 'meu orçamento é 2000'\n"
        "• 'quero economizar 500 esse mês' (meta)\n"
        "• 'aluguel 1200 todo dia 5' (gasto fixo)\n"
        "• 'recebo 828 todo dia 15' (receita fixa)\n"
        "• 'investi 1000 na reserva de emergência'\n"
        "• 'soma 100 na reserva' (aporta em existente — ou cria se não existir)\n"
        "• 'qual meu patrimônio?'\n"
        "• 'me dá uma dica de investimento'\n"
        "• 'minha fatura do Nubank' / 'paguei a fatura'\n"
        "• 'quanto gastei com uber?' (busca)\n"
        "• 'mostra meus últimos gastos' / 'últimas receitas'\n"
        "• 'edita gasto #15 valor 80' / 'apaga gasto #15'\n"
        "• 'edita receita #4 fonte: bônus'\n"
        "• 'muda o limite do Nubank pra 8000'\n"
        "• 'me dá um conselho' (análise IA)\n"
        "• 'compara com mês passado' / 'resumo da semana'\n"
        "• 'meus gastos de hoje' / 'extrato de ontem' / 'gastos do dia 15'\n"
        "• 'contas a pagar' (gastos futuros do mês)\n"
        "• 'apaga o último'\n\n"
        "🏦 *Contas bancárias (comandos):*\n"
        "• /contas — lista suas contas e saldos\n"
        "• /conta\\_nova `<nome> [tipo] [banco]` — cadastra conta\n"
        "• /conta\\_remover `<nome>` — remove conta (lançamentos viram 'Geral')\n"
        "• /saldo `[nome]` — saldo geral ou de uma conta específica\n\n"
        "💳 *Cartão de crédito (comandos):*\n"
        "• /cartao\\_novo `<nome> <limite> <fec> <venc>` — cadastra cartão\n"
        "• /cartoes — lista cartões e limites\n"
        "• /fatura `<nome>` — vê fatura aberta\n"
        "• /pagar\\_fatura `<nome>` — quita fatura\n"
        "• /cartao\\_remover `<nome>` — remove cartão\n\n"
        "📈 *Investimentos (comandos):*\n"
        "• /investir `<valor> <tipo> <nome>` — registra aporte\n"
        "• /investimentos — vê sua carteira\n"
        "• /resgatar `<valor> <nome>` — resgata aplicação\n"
        "• /patrimonio — saldo + investimentos - faturas\n"
        "• /dica\\_investir — dica personalizada com IA\n\n"
        "🛠️ *Outros comandos úteis:*\n"
        "• /relatorio — extrato do mês\n"
        "• /fixos — gastos e receitas fixas\n"
        "• /feedback `<msg>` — manda sugestão pro Ryan\n"
        "• /resetar — apaga TODOS os seus dados\n\n"
        "📸 *Manda uma foto de comprovante ou nota fiscal* que eu leio e registro o gasto.\n\n"
        "🔔 *Alertas automáticos:*\n"
        "• Resumo da semana todo domingo às 18h\n"
        "• Lembrete diário às 20h se você esquecer de registrar\n"
        "• Aviso 3 dias antes do vencimento da fatura\n"
        "• Aviso quando passar de 70% do limite do cartão"
    )
    bot.reply_to(message, texto, parse_mode="Markdown")


@bot.message_handler(commands=["relatorio"])
def gerar_relatorio(message, completo=None):
    """Relatório do mês. Por padrão simples; completo=True mostra detalhamento.
    Se chamado via /relatorio completo (ou variações), força modo detalhado."""
    registrar_usuario(message.chat.id)
    user_id = message.chat.id

    # Detecta se o usuário pediu modo completo via comando slash
    if completo is None:
        args = message.text.split(maxsplit=1)
        arg = args[1].strip().lower() if len(args) > 1 else ""
        completo = any(p in arg for p in ("completo", "detalhado", "extrato", "tudo", "full", "detalhe"))

    agora = datetime.now()
    hoje_str = agora.strftime("%Y-%m-%d %H:%M:%S")
    mes = mes_atual()

    conn = db()

    gastos_reais = conn.execute(
        "SELECT SUM(valor) FROM gastos WHERE user_id = ? AND strftime('%Y-%m', data) = ? AND data <= ?",
        (user_id, mes, hoje_str),
    ).fetchone()[0] or 0.0

    gastos_futuros_pontuais = conn.execute(
        "SELECT SUM(valor) FROM gastos WHERE user_id = ? AND strftime('%Y-%m', data) = ? AND data > ?",
        (user_id, mes, hoje_str),
    ).fetchone()[0] or 0.0

    gastos_fixos_futuros = conn.execute(
        """SELECT SUM(valor) FROM gastos_fixos
           WHERE user_id = ? AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
        (user_id, mes),
    ).fetchone()[0] or 0.0

    parcelas_futuras = conn.execute(
        """SELECT SUM(valor_parcela) FROM parcelamentos
           WHERE user_id = ? AND parcelas_pagas < total_parcelas
           AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
        (user_id, mes),
    ).fetchone()[0] or 0.0

    gastos_futuros = gastos_futuros_pontuais + gastos_fixos_futuros + parcelas_futuras
    receitas_mes = total_receita_mes(user_id, mes)
    fatura_total = total_fatura_aberta_todos_cartoes(user_id)
    saldo_mes = receitas_mes - gastos_reais
    saldo_previsto = receitas_mes - (gastos_reais + gastos_futuros)

    # ── VERSÃO SIMPLES (padrão) ────────────────────────────────────────────
    if not completo:
        contas = listar_contas(user_id)

        # Saldo total = soma apenas das contas cadastradas
        saldo_total = sum(saldo_conta(user_id, c[0]) for c in contas)

        texto = f"📊 *Resumo de {fmt_mes(mes)}*\n\n"
        texto += f"📅 *Esse mês:*\n"
        texto += f"💵 Receitas: R$ {receitas_mes:.2f}\n"
        texto += f"💸 Gastos: R$ {gastos_reais:.2f}\n"
        if gastos_futuros > 0:
            texto += f"⏳ Gastos futuros: R$ {gastos_futuros:.2f}\n"
        if fatura_total > 0:
            texto += f"💳 Fatura aberta: R$ {fatura_total:.2f}\n"
        texto += f"\n💰 *Saldo total: R$ {saldo_total:.2f}*\n"
        if gastos_futuros > 0:
            texto += f"🔮 Previsto fim do mês: R$ {saldo_previsto:.2f}\n"

        if contas:
            texto += "\n🏦 *Saldo das contas:*\n"
            for cid, nome_c, _b, _t, _c in contas:
                s = saldo_conta(user_id, cid)
                texto += f"• {escape_md(nome_c)}: R$ {s:.2f}\n"

        texto += "\n_Quer o extrato completo? Diga 'relatório completo'._"
        conn.close()
        bot.reply_to(message, texto, parse_mode="Markdown")
        return

    # ── VERSÃO COMPLETA (detalhada) ────────────────────────────────────────
    por_categoria = conn.execute(
        """SELECT categoria, SUM(valor) FROM gastos
           WHERE user_id = ? AND strftime('%Y-%m', data) = ? AND data <= ?
           GROUP BY categoria ORDER BY SUM(valor) DESC""",
        (user_id, mes, hoje_str),
    ).fetchall()
    ultimos_gastos = conn.execute(
        """SELECT data, valor, categoria, descricao FROM gastos
           WHERE user_id = ? AND strftime('%Y-%m', data) = ? AND data <= ?
           ORDER BY id DESC LIMIT 10""",
        (user_id, mes, hoje_str),
    ).fetchall()
    por_fonte = conn.execute(
        """SELECT fonte, SUM(valor) FROM receitas
           WHERE user_id = ? AND strftime('%Y-%m', data) = ?
           GROUP BY fonte ORDER BY SUM(valor) DESC""",
        (user_id, mes),
    ).fetchall()
    ultimas_receitas = conn.execute(
        """SELECT data, valor, fonte, descricao FROM receitas
           WHERE user_id = ? AND strftime('%Y-%m', data) = ?
           ORDER BY id DESC LIMIT 10""",
        (user_id, mes),
    ).fetchall()
    conn.close()

    cartao_mes_total = 0.0
    cartoes_user = listar_cartoes(user_id)
    detalhe_cartoes = []
    if cartoes_user:
        conn2 = db()
        for cid, nome_c, _lim, _df, _dv in cartoes_user:
            comp_mes = conn2.execute(
                """SELECT COALESCE(SUM(valor),0) FROM gastos_cartao
                   WHERE user_id = ? AND cartao_id = ? AND strftime('%Y-%m', data) = ?""",
                (user_id, cid, mes),
            ).fetchone()[0] or 0.0
            _, total_aberta = fatura_aberta(user_id, cid)
            cartao_mes_total += comp_mes
            if comp_mes > 0 or total_aberta > 0:
                detalhe_cartoes.append((nome_c, comp_mes, total_aberta))
        conn2.close()

    contas = listar_contas(user_id)
    saldo_total = sum(saldo_conta(user_id, c[0]) for c in contas)
    saldo_previsto_fim = saldo_total - gastos_futuros
    saldo_realista = saldo_total - fatura_total

    texto = f"📊 *Extrato Completo — {fmt_mes(mes)}*\n"
    texto += f"\n📅 *Esse mês:*\n"
    texto += f"💵 Receitas: R$ {receitas_mes:.2f}\n"
    texto += f"💸 Gastos realizados: R$ {gastos_reais:.2f}\n"
    if gastos_futuros > 0:
        texto += f"⏳ Gastos futuros: R$ {gastos_futuros:.2f}\n"
    if cartao_mes_total > 0 or fatura_total > 0:
        texto += f"💳 Compras no cartão (mês): R$ {cartao_mes_total:.2f}\n"
        texto += f"💳 Fatura(s) em aberto: R$ {fatura_total:.2f}\n"
    texto += f"\n💰 *Saldo total: R$ {saldo_total:.2f}*\n"
    if gastos_futuros > 0:
        texto += f"🔮 Previsto fim do mês: R$ {saldo_previsto_fim:.2f}\n"
    if fatura_total > 0:
        texto += f"💎 Realista (sem dívida cartão): R$ {saldo_realista:.2f}\n"

    if contas:
        texto += "\n🏦 *Saldo atual das contas:*\n"
        for cid, nome_c, _b, _t, _c in contas:
            s = saldo_conta(user_id, cid)
            texto += f"• {escape_md(nome_c)}: R$ {s:.2f}\n"

    if detalhe_cartoes:
        texto += "\n💳 *DETALHAMENTO DE CARTÕES*\n"
        for nome_c, comp, aberta in detalhe_cartoes:
            linha = f"• {escape_md(nome_c)}: gastos do mês R$ {comp:.2f}"
            if aberta > 0:
                linha += f" • fatura aberta R$ {aberta:.2f}"
            texto += linha + "\n"

    if por_fonte or ultimas_receitas:
        texto += "\n🟢 *DETALHAMENTO DE ENTRADAS*\n"
        if por_fonte:
            for fonte, tot in por_fonte:
                pct = (tot / receitas_mes * 100) if receitas_mes > 0 else 0
                texto += f"• {fonte}: R$ {tot:.2f} ({pct:.0f}%)\n"
        if ultimas_receitas:
            texto += "\n_Últimas entradas:_\n"
            for data, valor, fonte, desc in ultimas_receitas:
                try:
                    dia = datetime.strptime(data, "%Y-%m-%d %H:%M:%S").strftime("%d/%m")
                except Exception:
                    dia = data
                d = f" — {desc}" if desc else ""
                texto += f"• {dia} | R$ {valor:.2f} | {fonte}{d}\n"

    if por_categoria or ultimos_gastos:
        texto += "\n🔴 *DETALHAMENTO DE SAÍDAS*\n"
        if por_categoria:
            for cat, tot in por_categoria:
                pct = (tot / gastos_reais * 100) if gastos_reais > 0 else 0
                texto += f"• {cat}: R$ {tot:.2f} ({pct:.0f}%)\n"
        if ultimos_gastos:
            texto += "\n_Últimas saídas:_\n"
            for data, valor, cat, desc in ultimos_gastos:
                try:
                    dia = datetime.strptime(data, "%Y-%m-%d %H:%M:%S").strftime("%d/%m")
                except Exception:
                    dia = data
                d = f" — {desc}" if desc else ""
                texto += f"• {dia} | R$ {valor:.2f} | {cat}{d}\n"

    bot.reply_to(message, texto, parse_mode="Markdown")

@bot.message_handler(commands=["fixos"])
def comando_fixos(message):
    """Lista gastos fixos E receitas fixas em uma única tela."""
    user_id = message.chat.id
    registrar_usuario(user_id)
    fixos = listar_gastos_fixos(user_id)
    receitas = listar_receitas_fixas(user_id)
    if not fixos and not receitas:
        bot.reply_to(
            message,
            "Você ainda não tem nada fixo cadastrado.\n"
            "• Gasto fixo: 'aluguel 1200 todo dia 5'\n"
            "• Receita fixa: 'recebo 828 todo dia 15'",
        )
        return
    texto = ""
    if fixos:
        texto += "📋 *Seus gastos fixos:*\n"
        for fid, desc, valor, cat, dia in fixos:
            texto += f"\n• #{fid} | dia {dia:02d} | R$ {valor:.2f} | {escape_md(cat)} — {escape_md(desc)}"
        texto += "\n_Pra remover: 'remove gasto fixo #ID'_\n"
    if receitas:
        if texto:
            texto += "\n"
        texto += "💵 *Suas receitas fixas:*\n"
        for fid, desc, valor, fonte, dia in receitas:
            texto += f"\n• #{fid} | dia {dia:02d} | R$ {valor:.2f} | {escape_md(fonte)} — {escape_md(desc)}"
        texto += "\n_Pra remover: 'remove receita fixa #ID'_"
    bot.reply_to(message, texto, parse_mode="Markdown")


# ================= IA / INTENÇÕES =================

SYSTEM_INSTRUCTION = """Você é o "Gerenciador Financeiro", um bot do Telegram CRIADO POR RYAN LUCAS,
que ajuda o usuário a controlar suas finanças de forma amigável e em português brasileiro.
Tom: brincalhão, próximo, direto, com emojis com moderação.
REGRA DE SEGURANÇA MÁXIMA: Você NÃO PODE inventar que fez ações que não estão na lista abaixo. Se o usuário pedir algo fora da lista, classifique como "conversa" e diga a verdade: "Ainda não fui programado para fazer isso". NUNCA diga que apagou ou ajustou algo se não usou a intenção correta.

IMPORTANTE: Sempre que o usuário perguntar quem te criou, quem te fez, quem é seu autor,
quem desenvolveu, ou pedir pra você se apresentar/explicar o que faz,
mencione "Ryan Lucas" como o criador (na "resposta" da intenção "conversa").

Classifique a mensagem em UMA das intenções:

- "registrar_gasto":despesa real PAGA NO DÉBITO/PIX/DINHEIRO/BOLETO. Se o usuário mencionar uma data futura ou disser que algo 'vai vencer' ou é 'para o dia X', extraia a data no formato YYYY-MM-DD no campo "data_futura".(ex: "gastei 50 no mercado", "uber 20", "almoço 35 no débito", "paguei 100 no pix").
  Se o usuário mencionar forma de pagamento (débito, pix, dinheiro, boleto), extraia em "metodo_pagamento".
  REGRA CRÍTICA: se o usuário disser "crédito" ou "no crédito" ou "cartão de crédito", NÃO use esta intenção — use "registrar_gasto_credito".
  Se o usuário disser apenas "cartão" sem especificar, assuma DÉBITO (o crédito precisa ser explícito).
- "registrar_gasto_credito": despesa paga no CARTÃO DE CRÉDITO. Use SEMPRE que o usuário mencionar "crédito", "no crédito", "cartão de crédito", "credit", "cc" (ex: "gastei 80 no ifood no crédito", "comprei tênis 200 no crédito Nubank").
  Em "cartao_nome" coloque o nome do cartão se mencionado (ex: "Nubank", "Inter"), senão deixe vazio.
  Categoria, valor, descrição funcionam igual ao registrar_gasto.
- "registrar_receita": entrada de dinheiro (ex: "recebi 3000 de salário", "freela 500").
- "apagar_receita": apagar/remover todas as receitas ou entradas de dinheiro do mês atual.
- "definir_orcamento": definir/alterar orçamento do mês (ex: "orçamento de 2000").
- "consultar_orcamento": ver orçamento ou quanto sobra.
- "apagar_orcamento": remover o orçamento do mês.
- "definir_meta": meta de economia mensal (ex: "quero economizar 500", "meta de 1000").
- "consultar_meta": ver progresso da meta de economia.
- "apagar_meta": remover a meta do mês.
- "adicionar_gasto_fixo": cadastrar gasto que se repete todo mês
  (ex: "aluguel 1200 todo dia 5", "Netflix 40 dia 10").
- "listar_gastos_fixos": ver gastos fixos cadastrados.
- "remover_gasto_fixo": remover um gasto fixo (extraia o ID em "fixo_id" se mencionado).
- "adicionar_parcelamento": registrar uma compra parcelada
  (ex: "comprei celular 1200 em 12x no crédito dia 10", "parcelei a TV em 6x de 300").
  Em "valor" coloque o VALOR TOTAL da compra (ou se o usuário só falou o valor da parcela,
  multiplique pelo número de parcelas pra ter o total). Em "total_parcelas" o número de parcelas.
  Em "dia_mes" o dia de cobrança da fatura (assuma dia 10 se não mencionado).
- "listar_parcelamentos": ver compras parceladas em andamento.
- "remover_parcelamento": cancelar parcelamento (extraia "parc_id").
- "consultar_relatorio": ver resumo/relatório/extrato do mês. Por padrão mostra versão simples (entradas, saídas, saldo). Se o usuário pedir explicitamente "completo", "detalhado", "extrato completo", "tudo", "categorias", "histórico" ou "últimos lançamentos", coloque "relatorio_completo": true no JSON.
- "comparar_meses": comparar mês atual com o anterior.
- "resumo_semanal": ver resumo da semana.
- "apagar_ultimo": apagar/desfazer o último gasto registrado.
- "buscar_gastos": pesquisar gastos por palavra ou categoria
  (ex: "quanto gastei com uber?", "mostra gastos em alimentação", "gastos com mercado mês passado").
  Em "texto" coloque a palavra-chave; em "categoria" se for nome de categoria; em "periodo" use
  "esse_mes", "mes_passado", "semana" ou "tudo".
- "conselho": usuário pede dicas, conselhos, análise ou opinião sobre as finanças
  (ex: "como posso economizar?", "me dá uma dica", "tá bom como tô gastando?").
- "ativar_lembrete": usuário quer ativar/ligar lembretes diários.
- "desativar_lembrete": usuário quer desativar/desligar/parar lembretes diários.
- "consultar_fatura": ver fatura aberta de um cartão de crédito (ex: "minha fatura do Nubank", "quanto tem na fatura?", "fatura do crédito").
  Em "cartao_nome" coloque o nome do cartão se mencionado.
- "pagar_fatura": usuário quer marcar fatura como paga (ex: "paguei a fatura do Nubank", "quitei o cartão").
  Em "cartao_nome" coloque o nome do cartão.
- "listar_cartoes": ver cartões de crédito cadastrados, limites e saldos disponíveis.
- "registrar_investimento": usuário aplicou dinheiro em investimento (ex: "investi 1000 no tesouro", "apliquei 500 em CDB", "guardei 300 na reserva").
  Em "tipo_inv" coloque um de: Reserva, Renda Fixa, Ações, FIIs, Cripto, Outros.
  Em "nome_inv" coloque o nome do investimento (ex: "Tesouro Selic", "CDB Inter", "Bitcoin").
- "listar_investimentos": ver carteira de investimentos, quanto tem aplicado.
- "resgatar_investimento": usuário tirou dinheiro de um investimento PARA O SALDO/CONTA (ex: "resgatei 500 do tesouro", "quero resgatar 28 da renda fixa", "tirei 1000 da reserva pra conta", "saquei 200 da poupança", "resgatei 28 do #2", "resgata 100 da caixinha do nubank").
  Em "valor" o valor resgatado.
  Em "nome_inv" coloque o que identificar o investimento — pode ser o nome ("Tesouro Selic", "caixinha do nubank"), o id ("2" ou "#2"), OU o tipo ("renda fixa", "reserva", "cripto") se for o único jeito que o usuário se referiu.
  Em "tipo_inv" coloque o tipo se mencionado explicitamente (Reserva, Renda Fixa, Ações, FIIs, Cripto, Outros).
  ATENÇÃO: NÃO use esta intenção se o usuário quiser mover entre investimentos — use "transferir_investimento". Nem se ele quiser apagar — use "remover_investimento".
- "aportar_investimento": usuário quer SOMAR/ADICIONAR/ACRESCENTAR um valor a um investimento que JÁ EXISTE,
  identificado por id (ex: "#3") ou pelo nome (ex: "soma 50 na reserva de emergência", "adicione 28 a #3",
  "acrescenta 100 ao tesouro selic", "põe mais 200 na caixinha", "joga 50 no investimento 2",
  "aporta 300 no #1", "incrementa 30 na reserva", "+50 no #3", "coloca mais 100 na caixinha").
  Em "inv_origem" coloque o id (apenas o número, sem #) OU o nome do investimento alvo.
  Em "valor" o quanto SOMAR ao valor atual.
  REGRA CRÍTICA: esta intenção SOMA ao valor atual. NÃO confunda com "editar_investimento" (que SUBSTITUI o valor)
  nem com "registrar_investimento" (que cria um aporte NOVO sem id alvo).
  Os verbos-chave são: "adicionar", "adicione", "soma", "somar", "acrescentar", "acrescenta", "põe mais",
  "coloca mais", "incrementa", "joga", "aporta em" (com alvo identificado), "+X em/no/na".
- "editar_investimento": usuário quer RENOMEAR, RECATEGORIZAR ou DEFINIR/SUBSTITUIR EXATAMENTE o valor de um investimento sem mover dinheiro
  (ex: "muda a caixinha pra reserva de emergência", "renomeia o tesouro pra Tesouro Selic 2030",
  "muda o tipo do CDB pra Renda Fixa", "ajusta o valor do bitcoin pra 5000", "define o #3 como 1000",
  "zera o valor do investimento #3", "corrige o valor do tesouro pra 800").
  Em "inv_origem" coloque o nome OU o número/id do investimento atual (se vier "#3", coloque "3").
  Em "nome_inv" coloque o NOVO nome (se mudar nome). Em "tipo_inv" o NOVO tipo (se mudar tipo).
  Em "valor" o NOVO valor (só se for SUBSTITUIR, ex: "ajusta cripto pra 5000", "zera o valor" → valor 0,
  "define o #3 como 1000" → valor 1000). NUNCA use esta intenção para SOMAR ao valor — pra somar use "aportar_investimento".
  REGRA: só use esta intenção quando o usuário quiser ALTERAR/SUBSTITUIR algo (nome, tipo, valor) —
  se ele quiser SOMAR ao valor, use "aportar_investimento"; se quiser APAGAR o registro, use "remover_investimento".
- "transferir_investimento": usuário quer MOVER dinheiro de um investimento pra outro (sem cair no saldo)
  (ex: "passa 500 da caixinha pra reserva", "transfere 1000 do tesouro pro CDB", "move 200 da poupança pra cripto").
  Em "valor" o valor a transferir, em "inv_origem" o nome do investimento de origem,
  em "nome_inv" o nome do investimento de destino, em "tipo_inv" o tipo do destino se mencionado.
- "remover_investimento": usuário quer APAGAR/EXCLUIR/DELETAR/REMOVER/SUMIR/TIRAR DA LISTA um investimento que cadastrou (ex: "apaga a caixinha", "apague o investimento #1", "remove o investimento da poupança", "deleta o tesouro", "exclui o aporte do bitcoin", "quero excluir o investimento #2", "tira esse investimento da lista").
  Em "inv_origem" coloque o nome OU o número/id do investimento a remover (se vier "#3", coloque "3"; se vier "investimento 5", coloque "5").
  REGRA ABSOLUTA: se o usuário usar QUALQUER um dos verbos "apagar", "apague", "excluir", "exclui", "deletar", "deleta", "remover", "remove", "tirar da lista", "sumir com" referindo-se a um investimento, a intenção é SEMPRE "remover_investimento" — NUNCA "editar_investimento" nem "resgatar_investimento".
  ATENÇÃO: não confundir com "resgatar" (que tira dinheiro pro saldo). "Remover" simplesmente apaga o registro do banco.
- "consultar_patrimonio": ver patrimônio total (saldo + investimentos - faturas) (ex: "qual meu patrimônio?", "quanto tenho no total?", "minha situação geral").
- "dica_investimento": usuário pede dica, conselho ou orientação sobre investimentos (ex: "dica de investimento", "onde investir?", "como começar a investir?").
- "conversa": qualquer outra coisa (saudação, dúvida, agradecimento).
- "adicionar_receita_fixa": cadastrar entrada de dinheiro automática (ex: "recebo 828 de salário todo dia 15", "adiantamento 500 dia 20"). Extraia o "dia_mes".
- "listar_receitas_fixas": ver receitas fixas cadastradas.
- "remover_receita_fixa": remover uma receita fixa (extraia o ID em "fixo_id" se mencionado).
- "apagar_receita": remover TODAS as receitas do mês atual (ex: "zere minhas receitas", "apague as receitas de abril"). Ação destrutiva — o sistema vai pedir confirmação.
- "ajustar_saldo": ajustar o saldo do mês (não confundir com gasto/receita normal). Tem 3 MODOS — você DEVE escolher o certo no campo "modo_ajuste":
    * "definir": forçar o saldo a bater EXATAMENTE com um valor (ex: "ajuste meu saldo para 100", "meu saldo é 50", "saldo deveria ser 200", "corrige o saldo pra 1000"). Em "valor" o saldo desejado.
    * "subtrair": tirar/remover/abater um valor do saldo atual SEM ser um gasto categorizado (ex: "tira 0,23 do saldo", "abate 5 do meu saldo", "diminui 10 do saldo", "remove 2 reais do saldo atual"). Em "valor" o quanto SAIR.
    * "somar": somar/adicionar/colocar um valor extra no saldo SEM ser uma receita categorizada (ex: "soma 5 no saldo", "adiciona 10 ao saldo", "joga 20 no saldo"). Em "valor" o quanto ENTRAR.
  REGRA CRÍTICA: se o usuário disser "tire X do saldo", "abate X", "diminui X do saldo" — modo SEMPRE "subtrair", NUNCA "definir". O valor é o quanto sai, NÃO o saldo desejado.
  Default = "definir" só se o usuário não usou verbo de movimento (tirar/somar/abater).
  Se o usuário disser uma conta específica ("ajusta o saldo do nubank pra 50"), preencha "conta" com o nome.
- "listar_gastos_futuros": listar ou mostrar contas a pagar, gastos futuros, agendados, ou o que falta pagar neste mês.
- "resumo_diario": usuário pede para ver entradas, saídas, gastos ou receitas de um dia específico
  (ex: "meus gastos de hoje", "o que entrou hoje", "extrato de ontem", "gastos do dia 15", "movimentações de hoje", "quanto gastei hoje?", "meu relatório de hoje").
  Em "tipo_dia" coloque "gastos" se pedir só saídas/gastos, "receitas" se pedir só entradas/receitas,
  ou "ambos" se pedir extrato/relatório/movimentação/resumo do dia inteiro.
  Em "dia_relativo" coloque "hoje", "ontem" ou "outro" — NUNCA invente datas absolutas.
  Em "data_futura" SÓ preencha (formato YYYY-MM-DD) se o usuário mencionar uma data ESPECÍFICA E COMPLETA
  (ex: "15/04/2026"). Se ele só falar "dia 15" sem ano, deixe "data_futura" como null e coloque
  o número do dia em "dia_mes". Para "hoje" ou "ontem", deixe "data_futura" como null — o sistema
  resolve a data sozinho.
- "listar_gastos_recentes": usuário quer ver os últimos lançamentos de gasto pra escolher um e editar/apagar (ex: "mostra meus últimos gastos", "lista os 10 últimos", "quais foram meus gastos recentes").
- "listar_receitas_recentes": idem pra receitas (ex: "mostra minhas últimas receitas", "lista entradas recentes").
- "editar_gasto": usuário quer editar um GASTO específico já registrado, identificado pelo ID
  (ex: "muda o valor do gasto #15 pra 80", "edita o gasto 23: descrição mercado quinzenal", "altera categoria do gasto #7 pra Lazer").
  Em "alvo_id" coloque o ID do gasto. Em "valor" o novo valor (se trocar valor). Em "categoria" a nova categoria. Em "descricao" a nova descrição.
- "apagar_gasto": usuário quer apagar UM gasto específico pelo ID (ex: "apaga o gasto #15", "remove o gasto 23"). Em "alvo_id" o ID. NÃO confundir com "apagar_ultimo" (que apaga o mais recente).
- "editar_receita": editar uma receita específica pelo ID (ex: "edita receita #4 fonte: bônus", "muda valor da receita #2 pra 1500"). Em "alvo_id" o ID. Use "fonte", "valor", "descricao" pros novos valores.
- "apagar_receita_id": apagar UMA receita específica pelo ID (ex: "apaga a receita #4", "remove receita 7"). Em "alvo_id" o ID. NÃO confundir com "apagar_receita" (que apaga TODAS do mês).
- "editar_cartao": editar dados de um cartão de crédito (ex: "muda o limite do Nubank pra 8000", "renomeia Inter pra Inter Black", "muda dia de fechamento do Nubank pra 25"). Em "cartao_nome" o nome atual. Em "novo_cartao_nome" o novo nome (se renomear). Em "valor" o novo limite (se mudar limite). Em "dia_mes" o novo dia de fechamento. Em "dia_venc" o novo dia de vencimento.
- "confirmar": usuário responde afirmativamente a uma pergunta de confirmação anterior do bot ("sim", "confirma", "pode apagar", "isso aí", "manda ver", "tá certo", "ok"). Use SOMENTE quando a mensagem é APENAS uma confirmação curta e o usuário acabou de receber uma pergunta de confirmação (apagar, criar, escolher). NÃO use "confirmar" como intenção pra outras confirmações.
- "cancelar": usuário cancela a ação pendente ("não", "cancela", "deixa quieto", "esquece").

# CONTAS BANCÁRIAS (multi-conta) =========================================
# O usuário pode ter VÁRIAS contas (Nubank, Itaú, Flash, etc.) e querer separar
# o dinheiro por conta. Em QUALQUER intenção de movimentação (registrar_gasto,
# registrar_receita, registrar_gasto_credito, registrar_investimento,
# adicionar_gasto_fixo, adicionar_receita_fixa, adicionar_parcelamento, ajustar_saldo)
# se o usuário mencionar de qual conta saiu/entrou o dinheiro, preencha "conta"
# com o nome do banco/conta. Se ele NÃO disser, deixe "conta" vazio.
#
# PADRÕES COMUNS para identificar a conta:
#   "paguei 50 no mercado pelo nubank"       → conta: "nubank"
#   "gastei 30 no ifood no flash"            → conta: "flash"
#   "gastei 5 com chiclete no flash"         → conta: "flash"  ← "no [banco]" no FIM da frase = conta
#   "salário 3000 caiu na conta itau"        → conta: "itau"
#   "tirei 100 do santander"                 → conta: "santander"
#   "uber 20 no débito nubank"               → conta: "nubank"
# ATENÇÃO: "no [nome_banco]" ao FINAL de uma frase de gasto quase sempre indica
# a conta usada, NÃO o estabelecimento. Priorize a conta cadastrada.

- "criar_conta": usuário quer cadastrar uma nova conta bancária
  (ex: "cria uma conta nubank", "adiciona o banco itau", "nova conta santander corrente",
  "registra minha conta caixa poupança", "cadastra a conta inter").
  Em "conta" coloque o nome da conta (geralmente o banco — ex: "Nubank", "Itau", "Inter").
  Em "tipo_conta" coloque um de: Corrente, Poupança, Digital, Salário, Dinheiro, Outro
  (default Corrente se não mencionado).
- "listar_contas": ver todas as contas cadastradas (ex: "minhas contas", "lista as contas", "quais bancos eu uso", "mostra meus bancos").
- "remover_conta": apagar uma conta (ex: "remove a conta nubank", "apaga o banco itau", "exclui minha conta caixa", "deleta o santander").
  Em "conta" o nome da conta a remover. Ação destrutiva — o sistema vai pedir confirmação.
- "renomear_conta": renomear ou alterar dados de uma conta (ex: "renomeia o nubank pra Nu Pessoa Física", "muda o tipo da caixa pra poupança", "altera o banco do itau pra Itaú Personnalité").
  Em "conta" o nome ATUAL. Em "novo_conta_nome" o novo nome (se renomear).
  Em "tipo_conta" o novo tipo (se mudar tipo). Em "banco_nome" o novo banco (se mudar banco).
- "transferir_conta": transferir dinheiro entre 2 contas (ex: "transferi 500 do nubank pro itau", "passa 100 da caixa pro inter", "move 1000 do santander pra reserva").
  Em "valor" o valor. Em "conta" a conta de ORIGEM. Em "conta_destino" a conta de DESTINO.
  Não confundir com "transferir_investimento" (que move entre investimentos).
- "consultar_saldo": usuário quer ver o saldo de uma conta específica OU de todas
  (ex: "saldo do nubank", "quanto tem no itau", "qual meu saldo", "saldo das contas", "saldo geral").
  Em "conta" coloque o nome da conta se mencionada. Vazio = saldo de todas.

Retorne SEMPRE este JSON:
{
  "intencao": "<uma das opções>",
  "valor": <float ou null>,
  "categoria": "<string ou vazio>",
  "descricao": "<string ou vazio>",
  "fonte": "<string ou vazio — pra receita>",
  "metodo_pagamento": "<Débito|Pix|Dinheiro|Boleto|Transferência ou vazio — NÃO use Crédito aqui, use registrar_gasto_credito>",
  "cartao_nome": "<nome do cartão de crédito ou vazio>",
  "novo_cartao_nome": "<novo nome do cartão, só pra editar_cartao, ou vazio>",
  "tipo_inv": "<Reserva|Renda Fixa|Ações|FIIs|Cripto|Outros ou vazio>",
  "nome_inv": "<nome do investimento (ou nome de DESTINO em transferências/edições) ou vazio>",
  "inv_origem": "<nome ou id do investimento de ORIGEM em editar/transferir/remover, ou vazio>",
  "dia_mes": <int 1-31 ou null>,
  "dia_venc": <int 1-31 ou null — só pra editar_cartao>,
  "total_parcelas": <int ou null — pra parcelamento>,
  "fixo_id": <int ou null>,
  "parc_id": <int ou null>,
  "alvo_id": <int ou null — id do gasto/receita pra editar/apagar>,
  "texto": "<palavra-chave pra busca, vazio se não aplicável>",
  "periodo": "<esse_mes|mes_passado|semana|tudo — pra busca>",
  "tipo_dia": "<gastos|receitas|ambos ou vazio — pra resumo_diario>",
  "dia_relativo": "<hoje|ontem|outro ou vazio — pra resumo_diario>",
  "data_futura": "<YYYY-MM-DD ou null>",
  "modo_ajuste": "<definir|somar|subtrair ou vazio — só pra ajustar_saldo>",
  "conta": "<nome da conta bancária se mencionada (origem em transferir_conta), ou vazio>",
  "conta_destino": "<conta de destino, só pra transferir_conta, ou vazio>",
  "novo_conta_nome": "<novo nome da conta, só pra renomear_conta, ou vazio>",
  "tipo_conta": "<Corrente|Poupança|Digital|Salário|Dinheiro|Outro ou vazio>",
  "banco_nome": "<nome do banco da conta (pra criar_conta/renomear_conta), ou vazio>",
  "relatorio_completo": <true|false — true só se pediu explicitamente relatório detalhado/completo/extrato; false pra resumo simples>,
  "resposta": "<texto curto e amigável em PT-BR — preencha em 'conversa' ou pra pedir esclarecimento>"
}

CATEGORIAS CANÔNICAS (use SEMPRE uma destas pra "categoria" de gastos, NUNCA invente nem traduza):
Alimentação, Mercado, Transporte, Moradia, Saúde, Educação, Lazer, Vestuário,
Assinaturas, Pets, Beleza, Presentes, Viagem, Impostos, Tarifas Bancárias,
Cartão de Crédito, Investimentos, Outros.
Pra receitas use: Salário, Freelance, Bônus, Aluguel Recebido, Resgate, Reembolso,
Vendas, Presente, Outros.
NUNCA use "Orçamento", "Meta" ou "Ajuste de Saldo" como categoria escolhida pelo usuário.

Se a mensagem for ambígua (ex: só "20"), use intencao "conversa" e peça detalhes na "resposta".
Se a mensagem for um número isolado depois que o bot pediu confirmação ou um ID pra escolher,
use a intenção que faça sentido (ex: confirmar, ou a ação completa com o ID preenchido)."""


def executar_pendente(user_id, message):
    """Executa a ação que estava pendente de confirmação do usuário."""
    pend = get_pendente(user_id)
    if not pend:
        return
    acao = pend.get("acao")
    dados = pend.get("dados", {}) or {}
    limpar_pendente(user_id)

    try:
        if acao == "remover_cartao":
            cartao_id = dados.get("cartao_id")
            nome = dados.get("nome", "?")
            remover_cartao(user_id, cartao_id)
            bot.reply_to(
                message,
                f"🗑️ Cartão *{escape_md(nome)}* removido (junto com os lançamentos dele).",
                parse_mode="Markdown",
            )

        elif acao == "remover_conta":
            conta_id = dados.get("conta_id")
            nome = dados.get("nome", "?")
            n = remover_conta(user_id, conta_id)
            if n:
                bot.reply_to(
                    message,
                    f"🗑️ Conta *{escape_md(nome)}* removida. Lançamentos antigos viraram 'Geral'.",
                    parse_mode="Markdown",
                )
            else:
                bot.reply_to(message, "Conta não encontrada (talvez já tenha sido removida).")

        elif acao == "apagar_receitas_mes":
            mes = dados.get("mes") or mes_atual()
            conn = db()
            n = conn.execute(
                "DELETE FROM receitas WHERE user_id = ? AND strftime('%Y-%m', data) = ?",
                (user_id, mes),
            ).rowcount
            conn.commit()
            conn.close()
            bot.reply_to(message, f"🗑️ {n} receita(s) de {fmt_mes(mes)} apagada(s).")

        elif acao == "apagar_orcamento_mes":
            mes = dados.get("mes") or mes_atual()
            conn = db()
            n = conn.execute(
                "DELETE FROM orcamentos WHERE user_id = ? AND mes = ?", (user_id, mes),
            ).rowcount
            conn.commit()
            conn.close()
            bot.reply_to(message, f"🗑️ Orçamento de {fmt_mes(mes)} removido." if n else "Não havia orçamento.")

        elif acao == "apagar_meta_mes":
            mes = dados.get("mes") or mes_atual()
            conn = db()
            n = conn.execute(
                "DELETE FROM metas WHERE user_id = ? AND mes = ?", (user_id, mes),
            ).rowcount
            conn.commit()
            conn.close()
            bot.reply_to(message, "🗑️ Meta removida." if n else "Não havia meta.")

        elif acao == "criar_e_aportar":
            nome = (dados.get("nome") or "").strip() or "Investimento"
            valor = float(dados.get("valor") or 0)
            tipo = (dados.get("tipo") or "Outros").strip() or "Outros"
            iid = registrar_investimento(user_id, tipo, nome, valor)
            bot.reply_to(
                message,
                f"📈 Investimento criado (#{iid})\n"
                f"💰 R$ {valor:.2f} em *{escape_md(nome)}* ({normalizar_tipo_investimento(tipo)})",
                parse_mode="Markdown",
            )

        elif acao == "registrar_gasto_credito_escolha_cartao":
            # O usuário tinha vários cartões; agora respondeu o nome do cartão
            # Mas como só passa por aqui se respondeu "sim", desambiguação
            # de cartão é feita por nome — só lança se tiver cartao_id no dados.
            cid = dados.get("cartao_id")
            if not cid:
                bot.reply_to(message, "Não consegui identificar o cartão. Tenta de novo dizendo o nome.")
                return
            valor = float(dados.get("valor") or 0)
            categoria = dados.get("categoria") or "Outros"
            descricao = dados.get("descricao") or ""
            data_compra = dados.get("data_compra")
            _, fmes = registrar_gasto_cartao(user_id, cid, valor, categoria, descricao, data=data_compra)
            try:
                ano, m = fmes.split("-")
                venc_label = f"{m}/{ano}"
            except Exception:
                venc_label = fmes
            cartao_obj = buscar_cartao(user_id, cartao_id=cid)
            nome_c = cartao_obj[1] if cartao_obj else "cartão"
            bot.reply_to(
                message,
                f"💳 *Lançado no cartão {escape_md(nome_c)}*\n"
                f"💰 R$ {valor:.2f} — {escape_md(categoria)}\n"
                f"📅 Fatura {venc_label}",
                parse_mode="Markdown",
            )

        else:
            bot.reply_to(message, "Confirmação recebida, mas não sei mais o que fazer com ela. Pode repetir o pedido?")
    except Exception as e:
        print(f"[{user_id}] Erro ao executar pendente {acao}: {e}")
        bot.reply_to(message, "Tive um problema pra confirmar essa ação. Pode tentar de novo?")


@bot.message_handler(content_types=["photo"])
def processar_foto(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        bot.reply_to(
            message,
            f"🔒 Esse bot é privado. Pra pedir acesso, mande seu ID (`{user_id}`) pro dono.",
            parse_mode="Markdown",
        )
        return
    registrar_usuario(user_id)
    print(f"[{user_id}] 📸 Foto recebida")
    try:
        bot.send_chat_action(message.chat.id, "typing")
        # Pega a versão de maior resolução da foto
        file_id = message.photo[-1].file_id
        file_info = bot.get_file(file_id)
        image_bytes = bot.download_file(file_info.file_path)
        print(f"[{user_id}] Foto baixada: {len(image_bytes)} bytes")

        dados = extrair_dados_comprovante(image_bytes)
        if not dados:
            bot.reply_to(message, "Não consegui ler esse comprovante 😕 Tenta uma foto mais nítida ou me diga o gasto por texto.")
            return

        valor = parse_valor(dados.get("valor"))
        if valor <= 0:
            bot.reply_to(message, "Não consegui identificar o valor no comprovante. Pode me dizer quanto foi por texto?")
            return

        descricao = (dados.get("descricao") or "").strip()
        categoria = (dados.get("categoria") or "Outros").strip() or "Outros"
        metodo = normalizar_metodo(dados.get("metodo_pagamento"))

        data_compra = (dados.get("data") or "").strip()
        if data_compra:
            try:
                datetime.strptime(data_compra, "%Y-%m-%d")
                data_compra = data_compra + " 12:00:00"
            except ValueError:
                data_compra = None
        else:
            data_compra = None

        # Se o comprovante diz Crédito, tenta rotear pra cartão (1 cartão = automático,
        # vários = pede confirmação)
        if metodo == "Crédito":
            cartoes = listar_cartoes(user_id)
            if len(cartoes) == 1:
                cid, nome_c = cartoes[0][0], cartoes[0][1]
                _, fmes = registrar_gasto_cartao(user_id, cid, valor, categoria, descricao, data=data_compra)
                try:
                    ano, m = fmes.split("-")
                    venc_label = f"{m}/{ano}"
                except Exception:
                    venc_label = fmes
                resp = (
                    f"📸 Comprovante registrado no cartão *{escape_md(nome_c)}*!\n"
                    f"💰 R$ {valor:.2f} — {escape_md(categoria)}"
                )
                if descricao:
                    resp += f"\n📝 {escape_md(descricao)}"
                if data_compra:
                    resp += f"\n📅 {datetime.strptime(data_compra, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')}"
                resp += f"\n📅 Fatura {venc_label}"
                bot.reply_to(message, resp, parse_mode="Markdown")
                verificar_alerta_limite(user_id, cid)
                return
            elif len(cartoes) > 1:
                # Não dá pra escolher sozinho — registra como gasto comum e avisa
                salvar_gasto(user_id, valor, categoria, descricao, data=data_compra, metodo_pagamento="Crédito")
                nomes = ", ".join(c[1] for c in cartoes)
                bot.reply_to(
                    message,
                    f"📸 Comprovante registrado como gasto (R$ {valor:.2f} — {escape_md(categoria)}).\n\n"
                    f"⚠️ O comprovante diz *Crédito* mas você tem vários cartões ({escape_md(nomes)}). "
                    f"Pra ir pra fatura certa, me diga: 'esse foi no Nubank' (ou outro cartão).",
                    parse_mode="Markdown",
                )
                return
            # 0 cartões → cai pro fluxo padrão abaixo

        salvar_gasto(user_id, valor, categoria, descricao, data=data_compra, metodo_pagamento=metodo)
        resp = f"📸 Comprovante registrado!\n💰 R$ {valor:.2f} — {escape_md(categoria)}"
        if descricao:
            resp += f"\n📝 {escape_md(descricao)}"
        if metodo:
            resp += f"\n💳 {metodo}"
        if data_compra:
            resp += f"\n📅 {datetime.strptime(data_compra, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')}"
        s = status_orcamento_texto(user_id)
        if s:
            resp += f"\n\n{s}"
        bot.reply_to(message, resp, parse_mode="Markdown")
    except Exception as e:
        print(f"Erro foto: {e}")
        bot.reply_to(message, "Tive um problema pra ler a foto. Pode tentar de novo?")


def _resolver_conta_intent(user_id, conta_nome, message):
    """Helper de dispatcher: resolve nome de conta vindo da IA pra conta_id (int) ou
    None (sem filtro / Geral). Se a conta não bater, JÁ RESPONDE no chat e retorna False
    pra o handler interromper. Retornos:
        int   → use esse conta_id no insert
        None  → sem conta específica (Geral)
        False → erro já reportado, dê 'return' no handler
    """
    nome = _s(conta_nome).strip() if conta_nome else ""
    if not nome:
        return None
    status, cid, obj = resolver_conta(user_id, nome)
    if status == "ok":
        return cid
    if status == "ambigua":
        nomes = ", ".join(c[1] for c in obj)
        bot.reply_to(message, f"Tem mais de uma conta com esse nome ({nomes}). Seja mais específico.")
        return False
    if status == "nao_encontrada":
        bot.reply_to(message, f"Não achei conta chamada *{escape_md(nome)}*. Use /contas pra listar.", parse_mode="Markdown")
        return False
    if status == "nenhuma":
        # usuário citou conta mas não tem nenhuma cadastrada — só ignora o filtro
        bot.reply_to(message, "Você ainda não cadastrou nenhuma conta. Use /conta_nova ou diga 'cria uma conta nubank'.")
        return False
    return None


@bot.message_handler(func=lambda message: True)
def processar_mensagem(message):
    user_id = message.chat.id
    if not usuario_autorizado(user_id):
        bot.reply_to(
            message,
            f"🔒 Esse bot é privado. Pra pedir acesso, mande seu ID (`{user_id}`) pro dono.",
            parse_mode="Markdown",
        )
        return
    registrar_usuario(user_id)
    texto_usuario = message.text

    try:
        bot.send_chat_action(message.chat.id, "typing")

        # ===== Atalho: confirmações pendentes =====
        # Se o usuário tem uma confirmação pendente e respondeu sim/não,
        # processa direto sem chamar a IA (mais rápido e mais previsível).
        pend = get_pendente(user_id)
        if pend:
            txt_lower = (texto_usuario or "").strip().lower()
            sim_words = {"sim", "s", "confirma", "confirmar", "ok", "okay",
                         "isso", "manda", "manda ver", "pode", "pode ser",
                         "yes", "y", "tá", "ta", "ta certo", "tá certo",
                         "claro", "vai", "blz", "beleza", "fechou", "👍", "✅"}
            nao_words = {"não", "nao", "n", "cancela", "cancelar", "deixa",
                         "deixa quieto", "esquece", "no", "negativo", "❌"}
            if txt_lower in sim_words:
                executar_pendente(user_id, message)
                return
            if txt_lower in nao_words:
                limpar_pendente(user_id)
                bot.reply_to(message, "Beleza, cancelado. 👍")
                return
            # Se não foi sim nem não, segue pro fluxo normal (mas o pendente
            # só expira no timeout natural)

        resposta_ia = chamar_ia(user_id, texto_usuario, SYSTEM_INSTRUCTION)
        print(f"[{user_id}] IA: {resposta_ia.text}")
        try:
            dados = json.loads(resposta_ia.text)
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            print(f"[{user_id}] JSON inválido da IA: {e}")
            bot.reply_to(message, "Não entendi sua resposta. Pode reformular?")
            return
        # Defensiva: se a IA retornar lista/string em vez de dict, recupera
        if not isinstance(dados, dict):
            print(f"[{user_id}] IA retornou tipo inesperado: {type(dados).__name__}")
            bot.reply_to(message, "Não entendi sua resposta. Pode reformular?")
            return
        intencao = _s(dados.get("intencao"), default="conversa") or "conversa"

        # ===== Confirmar/Cancelar via intenção da IA =====
        if intencao == "confirmar":
            if get_pendente(user_id):
                executar_pendente(user_id, message)
            else:
                bot.reply_to(message, "Não tenho nada pendente pra confirmar. 🙂")
            return
        if intencao == "cancelar":
            if get_pendente(user_id):
                limpar_pendente(user_id)
                bot.reply_to(message, "Beleza, cancelado. 👍")
            else:
                bot.reply_to(message, "Não tinha nada pra cancelar.")
            return

        if intencao == "registrar_gasto":
            valor = parse_valor(dados.get("valor"))
            if valor <= 0:
                bot.reply_to(message, "Não consegui identificar o valor 🤔 Pode me dizer quanto foi?")
                return
            categoria = (dados.get("categoria") or "Outros").strip() or "Outros"
            descricao = (dados.get("descricao") or "").strip()
            metodo = normalizar_metodo(dados.get("metodo_pagamento"))

            # Pega a data futura se a IA identificou, senão usa None (data atual).
            # IMPORTANTE: data_futura vem como "YYYY-MM-DD" (sem hora). Sem hora
            # as comparações lexicográficas com "YYYY-MM-DD HH:MM:SS" do banco
            # quebram (data agendada pra hoje sumiria do "futuro"). Adiciona meio-dia.
            data_lancamento = dados.get("data_futura")
            if data_lancamento and len(data_lancamento) == 10:
                try:
                    datetime.strptime(data_lancamento, "%Y-%m-%d")
                    data_lancamento = data_lancamento + " 12:00:00"
                except ValueError:
                    data_lancamento = None  # IA inventou data inválida — ignora

            conta_id_g = _resolver_conta_intent(user_id, dados.get("conta"), message)
            if conta_id_g is False:
                return

            salvar_gasto(user_id, valor, categoria, descricao, data=data_lancamento,
                         metodo_pagamento=metodo, conta_id=conta_id_g)

            # Mensagem de confirmação inteligente
            prefixo = "⏳ Agendado!" if data_lancamento else "✅ Anotado!"
            resp = f"{prefixo}\n💰 R$ {valor:.2f} — {escape_md(categoria)}"
            if descricao:
                resp += f"\n📝 {escape_md(descricao)}"
            if metodo:
                resp += f"\n💳 {metodo}"
            if conta_id_g:
                conta_obj_g = buscar_conta(user_id, conta_id=conta_id_g)
                if conta_obj_g:
                    resp += f"\n🏦 {escape_md(conta_obj_g[1])}"
            if data_lancamento:
                data_pt = datetime.strptime(data_lancamento, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y")
                resp += f"\n📅 Vencimento: {data_pt}"

            bot.reply_to(message, resp, parse_mode="Markdown")

        elif intencao == "registrar_gasto_credito":
            valor = parse_valor(dados.get("valor"))
            if valor <= 0:
                bot.reply_to(message, "Não consegui identificar o valor 🤔 Pode me dizer quanto foi?")
                return
            categoria = (dados.get("categoria") or "Outros").strip() or "Outros"
            descricao = (dados.get("descricao") or "").strip()
            cartao_nome = (dados.get("cartao_nome") or "").strip()

            cartoes = listar_cartoes(user_id)
            if not cartoes:
                bot.reply_to(
                    message,
                    "💳 Você ainda não cadastrou nenhum cartão de crédito.\n"
                    "Cadastre primeiro: `/cartao_novo Nubank 5000 28 5`",
                    parse_mode="Markdown",
                )
                return

            cartao = None
            if cartao_nome:
                cartao = buscar_cartao(user_id, nome=cartao_nome)
                if not cartao:
                    nomes = ", ".join(escape_md(c[1]) for c in cartoes)
                    bot.reply_to(
                        message,
                        f"Não achei o cartão *{escape_md(cartao_nome)}*. Cartões cadastrados: {nomes}",
                        parse_mode="Markdown",
                    )
                    return
            elif len(cartoes) == 1:
                cartao = cartoes[0]
            else:
                nomes = ", ".join(escape_md(c[1]) for c in cartoes)
                bot.reply_to(
                    message,
                    f"Você tem mais de um cartão. Em qual lançar? ({nomes})\n"
                    f"Tenta: 'gastei {valor:.0f} no crédito {escape_md(cartoes[0][1])}'",
                    parse_mode="Markdown",
                )
                return

            cid = cartao[0]
            nome_cartao = cartao[1]
            # Honra data_futura também pro cartão (compra antiga ou agendada)
            data_compra = dados.get("data_futura")
            if data_compra and len(data_compra) == 10:
                try:
                    datetime.strptime(data_compra, "%Y-%m-%d")
                    data_compra = data_compra + " 12:00:00"
                except ValueError:
                    data_compra = None
            _, fmes = registrar_gasto_cartao(user_id, cid, valor, categoria, descricao, data=data_compra)
            try:
                ano, m = fmes.split("-")
                venc_label = f"{m}/{ano}"
            except Exception:
                venc_label = fmes
            resp = (
                f"💳 *Lançado no cartão {escape_md(nome_cartao)}*\n"
                f"💰 R$ {valor:.2f} — {escape_md(categoria)}"
            )
            if descricao:
                resp += f"\n📝 {escape_md(descricao)}"
            if data_compra:
                resp += f"\n📅 Compra: {datetime.strptime(data_compra, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')}"
            resp += f"\n📅 Fatura {venc_label}"
            usado, lim, pct = percentual_limite_usado(user_id, cid)
            resp += f"\n📊 Limite: {pct:.0f}% usado (R$ {usado:.2f} / R$ {lim:.2f})"
            bot.reply_to(message, resp, parse_mode="Markdown")
            verificar_alerta_limite(user_id, cid)

        elif intencao == "consultar_fatura":
            cartao_nome = _s(dados.get("cartao_nome"))
            cartoes = listar_cartoes(user_id)
            if not cartoes:
                bot.reply_to(message, "Você não tem cartões cadastrados.")
                return
            cartao = None
            if cartao_nome:
                cartao = buscar_cartao(user_id, nome=cartao_nome)
            elif len(cartoes) == 1:
                cartao = cartoes[0]
            else:
                nomes = ", ".join(escape_md(c[1]) for c in cartoes)
                bot.reply_to(message, f"De qual cartão? ({nomes})\nUse: `/fatura <nome>`", parse_mode="Markdown")
                return
            if not cartao:
                bot.reply_to(message, f"Cartão *{escape_md(cartao_nome)}* não encontrado.", parse_mode="Markdown")
                return
            cid, nome, _, _, dia_venc = cartao
            rows, total = fatura_aberta(user_id, cid)
            if not rows:
                bot.reply_to(message, f"💳 *{escape_md(nome)}*: nenhuma fatura aberta. ✨", parse_mode="Markdown")
                return
            venc = proxima_data_vencimento(dia_venc)
            texto = (
                f"💳 *Fatura {escape_md(nome)}*\n"
                f"📅 Vence em: {venc.strftime('%d/%m/%Y')}\n"
                f"💸 Total: *R$ {total:.2f}*\n\n*Lançamentos:*"
            )
            for _, valor, cat, desc, data in rows[:25]:
                try:
                    d = datetime.strptime(data[:10], "%Y-%m-%d").strftime("%d/%m")
                except Exception:
                    d = (data or "")[:10]
                texto += f"\n• {d} | R$ {valor:.2f} | {escape_md(cat or '—')}{(' — ' + escape_md(desc)) if desc else ''}"
            if len(rows) > 25:
                texto += f"\n_(+{len(rows) - 25} lançamentos)_"
            bot.reply_to(message, texto, parse_mode="Markdown")

        elif intencao == "pagar_fatura":
            cartao_nome = _s(dados.get("cartao_nome"))
            cartoes = listar_cartoes(user_id)
            if not cartoes:
                bot.reply_to(message, "Você não tem cartões cadastrados.")
                return
            cartao = None
            if cartao_nome:
                cartao = buscar_cartao(user_id, nome=cartao_nome)
            elif len(cartoes) == 1:
                cartao = cartoes[0]
            else:
                nomes = ", ".join(escape_md(c[1]) for c in cartoes)
                bot.reply_to(message, f"Qual cartão? ({nomes})\nUse: `/pagar_fatura <nome>`", parse_mode="Markdown")
                return
            if not cartao:
                bot.reply_to(message, f"Cartão *{escape_md(cartao_nome)}* não encontrado.", parse_mode="Markdown")
                return
            n, total = pagar_fatura(user_id, cartao[0])
            if n == 0:
                bot.reply_to(message, f"Não tinha fatura aberta no *{escape_md(cartao[1])}*.", parse_mode="Markdown")
            else:
                bot.reply_to(
                    message,
                    f"✅ Fatura do *{escape_md(cartao[1])}* paga!\n💸 R$ {total:.2f} ({n} lançamentos)",
                    parse_mode="Markdown",
                )

        elif intencao == "listar_cartoes":
            cmd_cartoes(message)

        elif intencao == "registrar_investimento":
            valor = parse_valor(dados.get("valor"))
            if valor <= 0:
                bot.reply_to(message, "Não consegui identificar o valor do aporte 🤔")
                return
            tipo = (dados.get("tipo_inv") or "Outros").strip()
            nome = (dados.get("nome_inv") or dados.get("descricao") or "Investimento").strip()
            conta_id_inv = _resolver_conta_intent(user_id, dados.get("conta"), message)
            if conta_id_inv is False:
                return
            iid = registrar_investimento(user_id, tipo, nome, valor, conta_id=conta_id_inv)
            resp_inv = (
                f"📈 Investimento registrado (#{iid})\n"
                f"💰 R$ {valor:.2f} em *{nome}* ({normalizar_tipo_investimento(tipo)})"
            )
            if conta_id_inv:
                conta_obj_inv = buscar_conta(user_id, conta_id=conta_id_inv)
                if conta_obj_inv:
                    resp_inv += f"\n🏦 Saiu de: {escape_md(conta_obj_inv[1])}"
            bot.reply_to(message, resp_inv, parse_mode="Markdown")

        elif intencao == "listar_investimentos":
            cmd_investimentos(message)

        elif intencao == "resgatar_investimento":
            valor = parse_valor(dados.get("valor"))
            # Tenta nome_inv, depois inv_origem (caso a IA tenha confundido), depois tipo_inv
            nome = (dados.get("nome_inv") or dados.get("inv_origem") or "").strip()
            tipo_hint = (dados.get("tipo_inv") or "").strip() or None
            if valor <= 0 or (not nome and not tipo_hint):
                bot.reply_to(message, "Pra resgatar preciso do valor e do nome (ou tipo) do investimento.\nEx: 'resgatei 500 do Tesouro Selic' ou 'resgatei 100 da renda fixa'")
                return
            resgatado = resgatar_investimento(user_id, nome, valor, tipo_hint)
            if resgatado is None:
                ref = nome or tipo_hint or ""
                bot.reply_to(message, f"Não achei investimento ativo com *{escape_md(ref)}*.", parse_mode="Markdown")
            elif isinstance(resgatado, tuple) and resgatado[0] == "varios_no_tipo":
                _, tipo_norm, lista = resgatado
                opcoes = "\n".join(f"• #{i} — {escape_md(n)} (R$ {v:.2f})" for i, n, v in lista)
                bot.reply_to(
                    message,
                    f"🤔 Você tem mais de um investimento em *{escape_md(tipo_norm)}*. Qual?\n{opcoes}\n\nResponde com o nome ou o id (ex: 'resgatei {valor:.0f} do #{lista[0][0]}').",
                    parse_mode="Markdown",
                )
            else:
                valor_resg, nome_real = resgatado
                bot.reply_to(
                    message,
                    f"💸 Resgatado: R$ {valor_resg:.2f} de *{escape_md(nome_real)}*\n📥 Lancei como receita.",
                    parse_mode="Markdown",
                )

        elif intencao == "aportar_investimento":
            origem = _s(dados.get("inv_origem")) or _s(dados.get("nome_inv"))
            valor = parse_valor(dados.get("valor"))
            tipo = _s(dados.get("tipo_inv"))
            if not origem or valor <= 0:
                bot.reply_to(
                    message,
                    "Pra somar a um investimento preciso do valor e de qual investimento.\n"
                    "Ex: 'adicione 28 ao #3' ou 'soma 100 na reserva de emergência'.",
                )
                return
            res = aportar_em_investimento(user_id, origem, valor)
            if res is None:
                # Não achou — em vez de só erro, oferece criar e aportar
                set_pendente(user_id, "criar_e_aportar", {
                    "nome": origem, "valor": valor, "tipo": tipo or "Outros",
                })
                tipo_msg = f" ({tipo})" if tipo else ""
                bot.reply_to(
                    message,
                    f"Não achei nenhum investimento com *{escape_md(origem)}*.\n\n"
                    f"Quer que eu crie um novo agora com R$ {valor:.2f} em *{escape_md(origem)}*{tipo_msg}?\n"
                    f"Responde *sim* pra criar ou *não* pra cancelar.",
                    parse_mode="Markdown",
                )
            elif res[0] == "valor_invalido":
                bot.reply_to(message, "Valor inválido pra aporte (precisa ser maior que zero).")
            else:
                _, atu, ant = res
                bot.reply_to(
                    message,
                    f"➕ *Aporte somado!*\n"
                    f"{escape_md(atu[2])} ({atu[1]})\n"
                    f"De: R$ {ant[3]:.2f}\n"
                    f"Para: *R$ {atu[3]:.2f}* (+R$ {valor:.2f})",
                    parse_mode="Markdown",
                )

        elif intencao == "editar_investimento":
            origem = _s(dados.get("inv_origem"))
            if not origem:
                bot.reply_to(message, "Pra editar preciso saber qual investimento. Ex: 'muda a caixinha pra reserva de emergência'")
                return
            novo_nome = _s(dados.get("nome_inv")) or None
            novo_tipo = _s(dados.get("tipo_inv")) or None
            # CRÍTICO: precisa diferenciar None ("não falou de valor") de 0 ("zere")
            valor_raw = dados.get("valor")
            if valor_raw is None:
                novo_valor = None
            else:
                novo_valor = parse_valor(valor_raw)
                if novo_valor < 0:
                    novo_valor = None
            res = editar_investimento(user_id, origem, novo_nome, novo_tipo, novo_valor)
            if res is None:
                bot.reply_to(message, f"Não achei investimento ativo com *{escape_md(origem)}*.", parse_mode="Markdown")
            elif res[0] == "nada":
                bot.reply_to(message, "Nada pra mudar — os valores são iguais aos atuais.")
            else:
                _, atu, ant = res
                bot.reply_to(
                    message,
                    f"✏️ *Investimento atualizado!*\n"
                    f"De: {escape_md(ant[2])} ({escape_md(ant[1])}) — R$ {ant[3]:.2f}\n"
                    f"Para: *{escape_md(atu[2])}* ({escape_md(atu[1])}) — R$ {atu[3]:.2f}",
                    parse_mode="Markdown",
                )

        elif intencao == "transferir_investimento":
            valor = parse_valor(dados.get("valor"))
            origem = _s(dados.get("inv_origem"))
            destino = _s(dados.get("nome_inv"))
            tipo_destino = _s(dados.get("tipo_inv")) or None
            if valor <= 0 or not origem or not destino:
                bot.reply_to(message, "Pra transferir preciso do valor, da origem e do destino.\nEx: 'passa 500 da caixinha pra reserva de emergência'")
                return
            status, info_orig, info_dest = transferir_investimento(user_id, origem, destino, valor, tipo_destino)
            if status == "origem_nao_encontrada":
                bot.reply_to(message, f"Não achei investimento ativo chamado *{escape_md(origem)}*.", parse_mode="Markdown")
            elif status == "saldo_insuficiente":
                bot.reply_to(
                    message,
                    f"💸 Saldo insuficiente em *{escape_md(info_orig[2])}* (você tem R$ {info_orig[3]:.2f}).",
                    parse_mode="Markdown",
                )
            elif status == "valor_invalido":
                bot.reply_to(message, "Valor inválido pra transferência.")
            else:
                bot.reply_to(
                    message,
                    f"🔄 *Transferência feita!*\n"
                    f"R$ {valor:.2f} saiu de *{escape_md(info_orig[2])}* ({escape_md(info_orig[1])})\n"
                    f"➡️ Foi pra *{escape_md(info_dest[2])}* ({escape_md(info_dest[1])}) — agora total R$ {info_dest[3]:.2f}\n"
                    f"_Continua tudo na sua carteira de investimentos._",
                    parse_mode="Markdown",
                )

        elif intencao == "remover_investimento":
            origem = _s(dados.get("inv_origem")) or _s(dados.get("nome_inv"))
            if not origem:
                bot.reply_to(message, "Pra apagar preciso saber qual investimento. Ex: 'apaga a caixinha'")
                return
            res = remover_investimento(user_id, origem)
            if res is None:
                bot.reply_to(message, f"Não achei investimento com *{escape_md(origem)}*.", parse_mode="Markdown")
            else:
                _, tipo, nome, valor = res
                bot.reply_to(
                    message,
                    f"🗑️ *Investimento removido!*\n"
                    f"{escape_md(nome)} ({escape_md(tipo)}) — R$ {valor:.2f}\n"
                    f"_Não mexi no seu saldo._",
                    parse_mode="Markdown",
                )

        elif intencao == "consultar_patrimonio":
            bot.reply_to(message, patrimonio_texto(user_id), parse_mode="Markdown")

        elif intencao == "dica_investimento":
            bot.reply_to(message, dica_investimento_texto(user_id), parse_mode="Markdown")

        elif intencao == "adicionar_receita_fixa":
            valor = parse_valor(dados.get("valor"))
            dia = _i(dados.get("dia_mes"), default=None, minimo=1, maximo=31)
            descricao = _s(dados.get("descricao"), default="Receita Fixa")
            fonte = _s(dados.get("fonte")) or _s(dados.get("categoria")) or "Salário"
            if valor <= 0:
                bot.reply_to(message, "Qual o valor da receita fixa? Ex: 'recebo 828 todo dia 15'.")
                return
            if dia is None:
                bot.reply_to(message, "Em qual dia do mês você recebe? (1-31). Ex: 'recebo 828 todo dia 15'.")
                return
            conta_id_rf = _resolver_conta_intent(user_id, dados.get("conta"), message)
            if conta_id_rf is False:
                return
            adicionar_receita_fixa(user_id, descricao, valor, fonte, dia, conta_id=conta_id_rf)
            extra_rf = ""
            if conta_id_rf:
                conta_obj_rf = buscar_conta(user_id, conta_id=conta_id_rf)
                if conta_obj_rf:
                    extra_rf = f"\n• Cai na conta *{escape_md(conta_obj_rf[1])}*."
            bot.reply_to(
                message,
                f"💸 Receita automática programada!\n• {descricao} — R$ {valor:.2f} ({fonte})\n"
                f"• Vai cair na conta todo dia {dia:02d}.{extra_rf}",
                parse_mode="Markdown",
            )

        elif intencao == "listar_receitas_fixas":
            fixas = listar_receitas_fixas(user_id)
            if not fixas:
                bot.reply_to(message, "Você não tem receitas fixas programadas.")
            else:
                texto = "💵 *Suas receitas automáticas:*\n"
                for fid, desc, valor, fonte, dia in fixas:
                    texto += f"\n• #{fid} | dia {dia:02d} | R$ {valor:.2f} | {fonte} — {desc}"
                texto += "\n\nPra remover: 'remove receita fixa #ID'"
                bot.reply_to(message, texto, parse_mode="Markdown")

        elif intencao == "remover_receita_fixa":
            fid = _i(dados.get("fixo_id"), default=None, minimo=1)
            if fid is None:
                bot.reply_to(message, "Qual receita fixa? Diga 'listar receitas fixas' pra ver os IDs.")
                return
            n = remover_receita_fixa(user_id, fid)
            bot.reply_to(message, f"🗑️ Receita automática #{fid} removida." if n else f"Não encontrei a receita #{fid}.")
        elif intencao == "registrar_receita":
            valor = parse_valor(dados.get("valor"))
            if valor <= 0:
                bot.reply_to(message, "Qual o valor da receita?")
                return
            fonte = (dados.get("fonte") or dados.get("categoria") or "Outros").strip() or "Outros"
            descricao = (dados.get("descricao") or "").strip()
            conta_id_r = _resolver_conta_intent(user_id, dados.get("conta"), message)
            if conta_id_r is False:
                return
            salvar_receita(user_id, valor, fonte, descricao, conta_id=conta_id_r)
            resp = f"💵 Receita anotada!\n+R$ {valor:.2f} — {fonte}"
            if descricao:
                resp += f"\n📝 {descricao}"
            if conta_id_r:
                conta_obj_r = buscar_conta(user_id, conta_id=conta_id_r)
                if conta_obj_r:
                    resp += f"\n🏦 {conta_obj_r[1]}"
            saldo = total_receita_mes(user_id) - total_gasto_mes(user_id)
            resp += f"\n\n💰 Saldo do mês: R$ {saldo:.2f}"
            m = status_meta_texto(user_id)
            if m:
                resp += f"\n\n{m}"
            bot.reply_to(message, resp, parse_mode="Markdown" if m else None)
       
        elif intencao == "apagar_receita":
            # Ação destrutiva — pede confirmação ANTES de apagar
            conn = db()
            n = conn.execute(
                "SELECT COUNT(*) FROM receitas WHERE user_id = ? AND strftime('%Y-%m', data) = ?",
                (user_id, mes_atual()),
            ).fetchone()[0]
            conn.close()
            if n == 0:
                bot.reply_to(message, "Você não tinha nenhuma receita registrada nesse mês para apagar.")
            else:
                set_pendente(user_id, "apagar_receitas_mes", {"mes": mes_atual()})
                bot.reply_to(
                    message,
                    f"⚠️ Confirmar: apagar *todas* as {n} receita(s) de {fmt_mes(mes_atual())}?\n"
                    f"Responde *sim* pra confirmar ou *não* pra cancelar.\n"
                    f"_(Esta ação não pode ser desfeita.)_",
                    parse_mode="Markdown",
                )

        elif intencao == "ajustar_saldo":
            # 3 modos: 'definir' (set saldo PARA valor), 'somar' (entrada extra),
            # 'subtrair' (saída extra). Default = 'definir'. Resolve conta primeiro.
            modo = (dados.get("modo_ajuste") or dados.get("modo") or "definir").strip().lower()
            valor = parse_valor(dados.get("valor"))
            if valor < 0:
                bot.reply_to(message, "Valor inválido pra ajuste de saldo.")
                return
            # Conta opcional — se passar, ajuste só afeta saldo daquela conta
            conta_id_ajuste = None
            conta_nome_ajuste = (dados.get("conta") or "").strip()
            if conta_nome_ajuste:
                status_c, conta_id_ajuste, conta_obj = resolver_conta(user_id, conta_nome_ajuste)
                if status_c == "ambigua":
                    nomes = ", ".join(c[1] for c in conta_obj)
                    bot.reply_to(message, f"Você tem várias contas com esse nome ({nomes}). Seja mais específico.")
                    return
                if status_c == "nao_encontrada":
                    bot.reply_to(message, f"Não achei conta chamada *{conta_nome_ajuste}*. Use /contas pra ver.", parse_mode="Markdown")
                    return
            sufixo_conta = ""
            if conta_id_ajuste:
                conta_obj = buscar_conta(user_id, conta_id=conta_id_ajuste)
                sufixo_conta = f" na conta {conta_obj[1]}" if conta_obj else ""

            if modo in ("somar", "soma", "adicionar", "adiciona", "entrada", "credito", "crédito", "+"):
                if valor == 0:
                    bot.reply_to(message, "Valor zero não muda nada no saldo.")
                    return
                salvar_receita(user_id, valor, "Ajuste de Saldo", "Ajuste manual do sistema (entrada)", conta_id=conta_id_ajuste)
                novo = total_receita_mes(user_id, conta_id=conta_id_ajuste) - total_gasto_mes(user_id, conta_id=conta_id_ajuste) if conta_id_ajuste else total_receita_mes(user_id) - total_gasto_mes(user_id)
                bot.reply_to(message, f"⚖️ Entrada extra de R$ {valor:.2f} lançada{sufixo_conta}.\n💰 Saldo do mês agora: R$ {novo:.2f}")
            elif modo in ("subtrair", "subtrai", "tirar", "tira", "remover", "remove", "saída", "saida", "debito", "débito", "-"):
                if valor == 0:
                    bot.reply_to(message, "Valor zero não muda nada no saldo.")
                    return
                salvar_gasto(user_id, valor, "Ajuste de Saldo", "Ajuste manual do sistema (saída)",
                             metodo_pagamento="Outros", conta_id=conta_id_ajuste)
                novo = (total_receita_mes(user_id, conta_id=conta_id_ajuste) - total_gasto_mes(user_id, conta_id=conta_id_ajuste)) if conta_id_ajuste else (total_receita_mes(user_id) - total_gasto_mes(user_id))
                bot.reply_to(message, f"⚖️ Saída extra de R$ {valor:.2f} lançada{sufixo_conta}.\n💰 Saldo do mês agora: R$ {novo:.2f}")
            else:
                # modo == "definir" → forçar saldo a valor exato
                novo_saldo_desejado = valor
                if conta_id_ajuste:
                    # Com conta específica: ajusta saldo_base para persistir entre meses
                    saldo_atual = saldo_conta(user_id, conta_id_ajuste)
                    if abs(saldo_atual - novo_saldo_desejado) < 0.01:
                        bot.reply_to(message, f"O saldo{sufixo_conta} já está em R$ {novo_saldo_desejado:.2f}, nada pra ajustar.")
                    else:
                        definir_saldo_base_conta(user_id, conta_id_ajuste, novo_saldo_desejado)
                        bot.reply_to(
                            message,
                            f"✅ Saldo{sufixo_conta} definido para *R$ {novo_saldo_desejado:.2f}*.\n"
                            f"_Esse valor vai persistir entre os meses — é o saldo real da conta._",
                            parse_mode="Markdown",
                        )
                else:
                    # Sem conta: ajuste no pool geral do mês (comportamento original)
                    saldo_atual = total_receita_mes(user_id) - total_gasto_mes(user_id)
                    diferenca = novo_saldo_desejado - saldo_atual
                    if diferenca > 0:
                        salvar_receita(user_id, diferenca, "Ajuste de Saldo", "Ajuste manual do sistema")
                        bot.reply_to(message, f"⚖️ Lancei uma entrada de R$ {diferenca:.2f} pra fechar o saldo em R$ {novo_saldo_desejado:.2f}.")
                    elif diferenca < 0:
                        salvar_gasto(user_id, abs(diferenca), "Ajuste de Saldo", "Ajuste manual do sistema", metodo_pagamento="Outros")
                        bot.reply_to(message, f"⚖️ Lancei uma saída de R$ {abs(diferenca):.2f} pra fechar o saldo em R$ {novo_saldo_desejado:.2f}.")
                    else:
                        bot.reply_to(message, f"O seu saldo já está exatamente em R$ {novo_saldo_desejado:.2f}, não precisa ajustar.")

        elif intencao == "definir_orcamento":
            valor = parse_valor(dados.get("valor"))
            if valor <= 0:
                bot.reply_to(message, "Qual valor você quer pro orçamento? Ex: 'orçamento de 2000'.")
                return
            definir_orcamento(user_id, valor)
            bot.reply_to(message, f"✅ Orçamento atualizado!\n\n{status_orcamento_texto(user_id)}")

        elif intencao == "consultar_orcamento":
            s = status_orcamento_texto(user_id)
            bot.reply_to(message, s or "Você ainda não definiu um orçamento esse mês. Diga 'orçamento de 2000' pra configurar.")

        elif intencao == "apagar_orcamento":
            orc = obter_orcamento(user_id)
            if orc is None:
                bot.reply_to(message, "Você não tinha orçamento esse mês.")
            else:
                set_pendente(user_id, "apagar_orcamento_mes", {"mes": mes_atual()})
                bot.reply_to(
                    message,
                    f"⚠️ Apagar o orçamento de {fmt_mes(mes_atual())} (R$ {orc:.2f})?\n"
                    f"Responde *sim* pra confirmar ou *não* pra cancelar.",
                    parse_mode="Markdown",
                )

        elif intencao == "definir_meta":
            valor = parse_valor(dados.get("valor"))
            if valor <= 0:
                bot.reply_to(message, "Qual o valor da meta? Ex: 'quero economizar 500'.")
                return
            definir_meta(user_id, valor)
            bot.reply_to(message, f"🎯 Meta de economia definida!\n\n{status_meta_texto(user_id)}", parse_mode="Markdown")

        elif intencao == "consultar_meta":
            m = status_meta_texto(user_id)
            bot.reply_to(message, m or "Você ainda não tem meta esse mês. Diga 'quero economizar 500' pra definir.", parse_mode="Markdown" if m else None)

        elif intencao == "apagar_meta":
            meta = obter_meta(user_id)
            if meta is None:
                bot.reply_to(message, "Você não tinha meta esse mês.")
            else:
                set_pendente(user_id, "apagar_meta_mes", {"mes": mes_atual()})
                bot.reply_to(
                    message,
                    f"⚠️ Apagar a meta de {fmt_mes(mes_atual())} (R$ {meta:.2f})?\n"
                    f"Responde *sim* pra confirmar ou *não* pra cancelar.",
                    parse_mode="Markdown",
                )

        elif intencao == "adicionar_gasto_fixo":
            valor = parse_valor(dados.get("valor"))
            dia = _i(dados.get("dia_mes"), default=None, minimo=1, maximo=31)
            descricao = _s(dados.get("descricao"))
            categoria = _s(dados.get("categoria"), default="Fixo") or "Fixo"
            if valor <= 0 or not descricao:
                bot.reply_to(message, "Pra cadastrar um gasto fixo preciso de: descrição, valor e dia do mês. Ex: 'aluguel 1200 todo dia 5'.")
                return
            if dia is None:
                bot.reply_to(message, "Em qual dia do mês esse gasto cai? (1-31).")
                return
            conta_id_gf = _resolver_conta_intent(user_id, dados.get("conta"), message)
            if conta_id_gf is False:
                return
            adicionar_gasto_fixo(user_id, descricao, valor, categoria, dia, conta_id=conta_id_gf)
            extra_gf = ""
            if conta_id_gf:
                conta_obj_gf = buscar_conta(user_id, conta_id=conta_id_gf)
                if conta_obj_gf:
                    extra_gf = f"\n• Sai da conta *{escape_md(conta_obj_gf[1])}*."
            bot.reply_to(
                message,
                f"📌 Gasto fixo cadastrado!\n• {descricao} — R$ {valor:.2f} ({categoria})\n"
                f"• Lança automaticamente todo dia {dia:02d}.{extra_gf}",
                parse_mode="Markdown",
            )

        elif intencao == "listar_gastos_fixos":
            comando_fixos(message)

        elif intencao == "remover_gasto_fixo":
            fid = _i(dados.get("fixo_id"), default=None, minimo=1)
            if fid is None:
                bot.reply_to(message, "Qual gasto fixo? Use /fixos pra ver os IDs e diga 'remove gasto fixo #2'.")
                return
            n = remover_gasto_fixo(user_id, fid)
            bot.reply_to(message, f"🗑️ Gasto fixo #{fid} removido." if n else f"Não encontrei o gasto fixo #{fid}.")

        elif intencao == "consultar_relatorio":
            gerar_relatorio(message, completo=bool(dados.get("relatorio_completo", False)))

        elif intencao == "comparar_meses":
            bot.reply_to(message, comparar_meses_texto(user_id), parse_mode="Markdown")

        elif intencao == "resumo_semanal":
            bot.reply_to(message, resumo_semanal_texto(user_id), parse_mode="Markdown")

        elif intencao == "apagar_ultimo":
            res = apagar_ultimo_lancamento(user_id)
            if res:
                origem, _id, valor, categoria, desc = res
                rotulo = "gasto no cartão" if origem == "cartao" else "gasto"
                d = f" — {escape_md(desc)}" if desc else ""
                bot.reply_to(
                    message,
                    f"🗑️ Último {rotulo} removido: R$ {valor:.2f} ({escape_md(categoria)}{d})",
                    parse_mode="Markdown",
                )
            else:
                bot.reply_to(message, "Não há lançamentos pra apagar.")

        elif intencao == "adicionar_parcelamento":
            valor_total = parse_valor(dados.get("valor"))
            total_parcelas = _i(dados.get("total_parcelas"), default=None, minimo=1, maximo=120)
            descricao = _s(dados.get("descricao"))
            categoria = _s(dados.get("categoria"), default="Outros") or "Outros"
            metodo = normalizar_metodo(dados.get("metodo_pagamento")) or "Crédito"
            dia = _i(dados.get("dia_mes"), default=10, minimo=1, maximo=31)
            cartao_nome = _s(dados.get("cartao_nome"))
            if valor_total <= 0 or total_parcelas is None or not descricao:
                bot.reply_to(message, "Pra cadastrar um parcelamento preciso de: descrição, valor total e número de parcelas. Ex: 'comprei celular 1200 em 12x'.")
                return

            # Se é parcelamento no crédito, tenta vincular ao cartão pra contar fatura/limite
            cartao_id = None
            cartao_obj = None
            if metodo == "Crédito":
                cartoes = listar_cartoes(user_id)
                if cartao_nome:
                    cartao_obj = buscar_cartao(user_id, nome=cartao_nome)
                elif len(cartoes) == 1:
                    cartao_obj = cartoes[0]
                if cartao_obj:
                    cartao_id = cartao_obj[0]

            conta_id_pc = _resolver_conta_intent(user_id, dados.get("conta"), message)
            if conta_id_pc is False:
                return

            vp = adicionar_parcelamento(user_id, descricao, valor_total, total_parcelas, dia,
                                        categoria, metodo, cartao_id=cartao_id, conta_id=conta_id_pc)
            resp = (
                f"💳 Parcelamento cadastrado!\n"
                f"• {escape_md(descricao)} — R$ {valor_total:.2f} em {total_parcelas}x de R$ {vp:.2f}\n"
                f"• {metodo}, cobrança todo dia {dia:02d}\n"
            )
            if cartao_obj:
                resp += f"• Vinculado ao cartão *{escape_md(cartao_obj[1])}* (vai bater na fatura/limite)\n"
            elif metodo == "Crédito":
                resp += (f"• ⚠️ Não vinculei a nenhum cartão (não cadastrou ou são vários — "
                         f"as parcelas vão como gasto comum).\n")
            if conta_id_pc:
                conta_obj_pc = buscar_conta(user_id, conta_id=conta_id_pc)
                if conta_obj_pc:
                    resp += f"• Conta: *{escape_md(conta_obj_pc[1])}*\n"
            resp += "• 1ª parcela já lançada."
            bot.reply_to(message, resp, parse_mode="Markdown")

        elif intencao == "listar_parcelamentos":
            parcs = listar_parcelamentos(user_id)
            if not parcs:
                bot.reply_to(message, "Você não tem parcelamentos em andamento.")
            else:
                texto = "💳 *Parcelamentos em andamento:*\n"
                for pid, desc, vp, total, pagas, dia, cat, metodo in parcs:
                    restantes = total - pagas
                    texto += (
                        f"\n• #{pid} {escape_md(desc)}\n"
                        f"   {pagas}/{total} pagas — restam {restantes}x de R$ {vp:.2f}\n"
                        f"   {escape_md(cat)} • {metodo or 'Crédito'} • dia {dia:02d}"
                    )
                texto += "\n\n_Pra cancelar: 'remove parcelamento #ID'_"
                bot.reply_to(message, texto, parse_mode="Markdown")

        elif intencao == "remover_parcelamento":
            pid = _i(dados.get("parc_id"), default=None, minimo=1)
            if pid is None:
                bot.reply_to(message, "Qual parcelamento? Diga 'meus parcelamentos' pra ver os IDs.")
                return
            n = remover_parcelamento(user_id, pid)
            bot.reply_to(message, f"🗑️ Parcelamento #{pid} cancelado." if n else f"Não encontrei o parcelamento #{pid}.")

        elif intencao == "buscar_gastos":
            texto_busca = (dados.get("texto") or "").strip() or None
            cat_busca = (dados.get("categoria") or "").strip() or None
            periodo = (dados.get("periodo") or "esse_mes").strip() or "esse_mes"
            rows = buscar_gastos(user_id, texto=texto_busca, categoria=cat_busca, periodo=periodo)
            if not rows:
                bot.reply_to(message, "Não encontrei nenhum gasto com esses critérios.")
            else:
                titulo_periodo = {
                    "esse_mes": fmt_mes(mes_atual()),
                    "mes_passado": fmt_mes(mes_anterior()),
                    "semana": "últimos 7 dias",
                    "tudo": "histórico completo",
                }.get(periodo, "")
                filtros = []
                if texto_busca:
                    filtros.append(f"'{texto_busca}'")
                if cat_busca:
                    filtros.append(f"categoria '{cat_busca}'")
                titulo = f"🔎 *Busca em {titulo_periodo}*"
                if filtros:
                    titulo += f" — {', '.join(filtros)}"
                total_busca = sum(r[1] for r in rows)
                texto = f"{titulo}\n\n💰 *Total encontrado: R$ {total_busca:.2f}* ({len(rows)} lançamentos)\n"
                for data, valor, cat, desc, metodo in rows[:20]:
                    try:
                        dia = datetime.strptime(data, "%Y-%m-%d %H:%M:%S").strftime("%d/%m")
                    except Exception:
                        dia = (data or "")[:10]
                    d = f" — {escape_md(desc)}" if desc else ""
                    m = f" [{escape_md(metodo)}]" if metodo else ""
                    texto += f"\n• {dia} | R$ {valor:.2f} | {escape_md(cat or '—')}{d}{m}"
                if len(rows) > 20:
                    texto += f"\n\n_(mostrando 20 de {len(rows)})_"
                bot.reply_to(message, texto, parse_mode="Markdown")

        elif intencao == "conselho":
            bot.send_chat_action(message.chat.id, "typing")
            texto = conselho_financeiro(user_id)
            bot.reply_to(message, f"💡 *Sua análise financeira:*\n\n{texto}", parse_mode="Markdown")

        elif intencao == "listar_gastos_recentes":
            n = dados.get("total_parcelas") or 10  # reaproveita campo, default 10
            try:
                n = max(1, min(int(n), 30))
            except (ValueError, TypeError):
                n = 10
            rows = listar_ultimos_gastos(user_id, n=n)
            if not rows:
                bot.reply_to(message, "Você ainda não tem gastos registrados.")
            else:
                texto = f"📋 *Últimos {len(rows)} gastos:*\n"
                for gid, data, valor, cat, desc in rows:
                    try:
                        d = datetime.strptime(data, "%Y-%m-%d %H:%M:%S").strftime("%d/%m")
                    except Exception:
                        d = (data or "")[:10]
                    extra = f" — {escape_md(desc)}" if desc else ""
                    texto += f"\n• #{gid} | {d} | R$ {valor:.2f} | {escape_md(cat)}{extra}"
                texto += "\n\n_Pra editar: 'edita gasto #ID valor 80'. Pra apagar: 'apaga gasto #ID'._"
                bot.reply_to(message, texto, parse_mode="Markdown")

        elif intencao == "listar_receitas_recentes":
            n = dados.get("total_parcelas") or 10
            try:
                n = max(1, min(int(n), 30))
            except (ValueError, TypeError):
                n = 10
            rows = listar_ultimas_receitas(user_id, n=n)
            if not rows:
                bot.reply_to(message, "Você ainda não tem receitas registradas.")
            else:
                texto = f"💵 *Últimas {len(rows)} receitas:*\n"
                for rid, data, valor, fonte, desc in rows:
                    try:
                        d = datetime.strptime(data, "%Y-%m-%d %H:%M:%S").strftime("%d/%m")
                    except Exception:
                        d = (data or "")[:10]
                    extra = f" — {escape_md(desc)}" if desc else ""
                    texto += f"\n• #{rid} | {d} | R$ {valor:.2f} | {escape_md(fonte)}{extra}"
                texto += "\n\n_Pra editar: 'edita receita #ID valor 1500'. Pra apagar: 'apaga receita #ID'._"
                bot.reply_to(message, texto, parse_mode="Markdown")

        elif intencao == "editar_gasto":
            alvo = dados.get("alvo_id")
            if not alvo:
                bot.reply_to(message, "Pra editar preciso do ID do gasto. Diga 'mostra meus últimos gastos' pra ver.")
                return
            try:
                alvo = int(alvo)
            except (ValueError, TypeError):
                bot.reply_to(message, "ID inválido.")
                return
            valor_raw = dados.get("valor")
            novo_valor = parse_valor(valor_raw) if valor_raw is not None else None
            if novo_valor is not None and novo_valor <= 0:
                novo_valor = None
            nova_cat = (dados.get("categoria") or "").strip() or None
            nova_desc_raw = dados.get("descricao")
            nova_desc = nova_desc_raw.strip() if isinstance(nova_desc_raw, str) and nova_desc_raw.strip() else None
            res = editar_gasto(user_id, alvo, novo_valor=novo_valor, nova_categoria=nova_cat, nova_descricao=nova_desc)
            if res is None:
                bot.reply_to(message, f"Não achei gasto com ID #{alvo}.")
            elif res[0] == "nada":
                bot.reply_to(message, "Nada pra mudar — você não me passou nenhum campo novo.")
            else:
                # res = ("ok", antigo, novo); cada um = (id, valor, categoria, descricao)
                _, ant, atu = res
                bot.reply_to(
                    message,
                    f"✏️ *Gasto #{alvo} atualizado!*\n"
                    f"De: R$ {ant[1]:.2f} | {escape_md(ant[2])}{(' — ' + escape_md(ant[3])) if ant[3] else ''}\n"
                    f"Para: *R$ {atu[1]:.2f}* | *{escape_md(atu[2])}*{(' — ' + escape_md(atu[3])) if atu[3] else ''}",
                    parse_mode="Markdown",
                )

        elif intencao == "apagar_gasto":
            alvo = dados.get("alvo_id")
            if not alvo:
                bot.reply_to(message, "Pra apagar preciso do ID. Diga 'mostra meus últimos gastos' pra ver.")
                return
            try:
                alvo = int(alvo)
            except (ValueError, TypeError):
                bot.reply_to(message, "ID inválido.")
                return
            res = apagar_gasto_por_id(user_id, alvo)
            if res is None:
                bot.reply_to(message, f"Não achei gasto com ID #{alvo}.")
            else:
                # row = (id, valor, categoria, descricao)
                _id, v, c, d = res
                extra = f" — {escape_md(d)}" if d else ""
                bot.reply_to(message, f"🗑️ Gasto #{alvo} apagado: R$ {v:.2f} | {escape_md(c)}{extra}", parse_mode="Markdown")

        elif intencao == "editar_receita":
            alvo = dados.get("alvo_id")
            if not alvo:
                bot.reply_to(message, "Pra editar preciso do ID da receita. Diga 'mostra minhas últimas receitas' pra ver.")
                return
            try:
                alvo = int(alvo)
            except (ValueError, TypeError):
                bot.reply_to(message, "ID inválido.")
                return
            valor_raw = dados.get("valor")
            novo_valor = parse_valor(valor_raw) if valor_raw is not None else None
            if novo_valor is not None and novo_valor <= 0:
                novo_valor = None
            nova_fonte = (dados.get("fonte") or dados.get("categoria") or "").strip() or None
            nova_desc_raw = dados.get("descricao")
            nova_desc = nova_desc_raw.strip() if isinstance(nova_desc_raw, str) and nova_desc_raw.strip() else None
            res = editar_receita(user_id, alvo, novo_valor=novo_valor, nova_fonte=nova_fonte, nova_descricao=nova_desc)
            if res is None:
                bot.reply_to(message, f"Não achei receita com ID #{alvo}.")
            elif res[0] == "nada":
                bot.reply_to(message, "Nada pra mudar — você não me passou nenhum campo novo.")
            else:
                # res = ("ok", antigo, novo); cada um = (id, valor, fonte, descricao)
                _, ant, atu = res
                bot.reply_to(
                    message,
                    f"✏️ *Receita #{alvo} atualizada!*\n"
                    f"De: R$ {ant[1]:.2f} | {escape_md(ant[2])}{(' — ' + escape_md(ant[3])) if ant[3] else ''}\n"
                    f"Para: *R$ {atu[1]:.2f}* | *{escape_md(atu[2])}*{(' — ' + escape_md(atu[3])) if atu[3] else ''}",
                    parse_mode="Markdown",
                )

        elif intencao == "apagar_receita_id":
            alvo = dados.get("alvo_id")
            if not alvo:
                bot.reply_to(message, "Pra apagar preciso do ID. Diga 'mostra minhas últimas receitas'.")
                return
            try:
                alvo = int(alvo)
            except (ValueError, TypeError):
                bot.reply_to(message, "ID inválido.")
                return
            res = apagar_receita_por_id(user_id, alvo)
            if res is None:
                bot.reply_to(message, f"Não achei receita com ID #{alvo}.")
            else:
                # row = (id, valor, fonte, descricao)
                _id, v, f, d = res
                extra = f" — {escape_md(d)}" if d else ""
                bot.reply_to(message, f"🗑️ Receita #{alvo} apagada: R$ {v:.2f} | {escape_md(f)}{extra}", parse_mode="Markdown")

        elif intencao == "editar_cartao":
            cartao_nome = (dados.get("cartao_nome") or "").strip()
            if not cartao_nome:
                bot.reply_to(message, "Qual cartão você quer editar? Diga 'meus cartões' pra ver os nomes.")
                return
            cartao = buscar_cartao(user_id, nome=cartao_nome)
            if not cartao:
                bot.reply_to(message, f"Não achei cartão com *{escape_md(cartao_nome)}*.", parse_mode="Markdown")
                return
            cid = cartao[0]
            novo_nome = (dados.get("novo_cartao_nome") or "").strip() or None
            valor_raw = dados.get("valor")
            novo_limite = parse_valor(valor_raw) if valor_raw is not None else None
            if novo_limite is not None and novo_limite <= 0:
                novo_limite = None
            novo_dia_fech = dados.get("dia_mes")
            novo_dia_venc = dados.get("dia_venc")
            try:
                novo_dia_fech = int(novo_dia_fech) if novo_dia_fech else None
                novo_dia_venc = int(novo_dia_venc) if novo_dia_venc else None
                if novo_dia_fech is not None and not (1 <= novo_dia_fech <= 31):
                    raise ValueError
                if novo_dia_venc is not None and not (1 <= novo_dia_venc <= 31):
                    raise ValueError
            except (ValueError, TypeError):
                bot.reply_to(message, "Dia inválido (precisa ser entre 1 e 31).")
                return
            res = editar_cartao(user_id, cid, novo_nome=novo_nome, novo_limite=novo_limite,
                                novo_dia_fec=novo_dia_fech, novo_dia_venc=novo_dia_venc)
            if res is None:
                bot.reply_to(message, "Não consegui atualizar — algo errado com o cartão.")
            elif res[0] == "nada":
                bot.reply_to(message, "Nada pra mudar — você não me passou nenhum campo novo.")
            else:
                # res = ("ok", antigo, novo); buscar_cartao retorna (id, nome, limite, dia_fech, dia_venc)
                _, ant, atu = res
                bot.reply_to(
                    message,
                    f"✏️ *Cartão atualizado!*\n"
                    f"De: {escape_md(ant[1])} | R$ {ant[2]:.2f} | fecha dia {ant[3]:02d} | vence dia {ant[4]:02d}\n"
                    f"Para: *{escape_md(atu[1])}* | *R$ {atu[2]:.2f}* | fecha dia {atu[3]:02d} | vence dia {atu[4]:02d}",
                    parse_mode="Markdown",
                )

        elif intencao == "ativar_lembrete":
            definir_lembrete(user_id, True)
            bot.reply_to(message, "🔔 Lembretes diários ativados! Vou te lembrar todo dia às 20h se você esquecer de registrar gastos.")

        elif intencao == "listar_gastos_futuros":
            agora = datetime.now()
            hoje_str = agora.strftime("%Y-%m-%d %H:%M:%S")
            mes = mes_atual()
            dia_atual = agora.day

            conn = db()
            # 1. Busca gastos pontuais futuros
            pontuais = conn.execute(
                "SELECT data, valor, descricao FROM gastos WHERE user_id = ? AND strftime('%Y-%m', data) = ? AND data > ?",
                (user_id, mes, hoje_str)
            ).fetchall()

            # 2. Busca gastos fixos pendentes no mês
            fixos = conn.execute(
                """SELECT dia_mes, valor, descricao FROM gastos_fixos
                   WHERE user_id = ? 
                   AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)
                   ORDER BY dia_mes""",
                (user_id, mes)
            ).fetchall()

            # 3. Busca parcelamentos pendentes no mês
            parcelas = conn.execute(
                """SELECT dia_cobranca, valor_parcela, descricao, parcelas_pagas, total_parcelas
                   FROM parcelamentos
                   WHERE user_id = ? AND parcelas_pagas < total_parcelas
                   AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)
                   ORDER BY dia_cobranca""",
                (user_id, mes)
            ).fetchall()
            conn.close()

            if not pontuais and not fixos and not parcelas:
                bot.reply_to(message, "Você não tem nenhum gasto futuro, fixo ou parcela pendente para o resto deste mês! 🎉")
            else:
                texto = "⏳ *Suas Contas a Pagar (Restante do mês)*\n"
                if pontuais:
                    texto += "\n*Lançamentos Agendados:*\n"
                    for d, v, desc in pontuais:
                        try:
                            dia_fmt = datetime.strptime(d, "%Y-%m-%d %H:%M:%S").strftime("%d/%m")
                        except Exception:
                            dia_fmt = (d or "")[:10]
                        texto += f"• {dia_fmt} | R$ {v:.2f} — {escape_md(desc)}\n"
                if fixos:
                    texto += "\n*Gastos Fixos:*\n"
                    for d, v, desc in fixos:
                        marca = "⚠️ atrasado " if d < dia_atual else ""
                        texto += f"• {marca}Dia {d:02d} | R$ {v:.2f} — {escape_md(desc)}\n"
                if parcelas:
                    texto += "\n*Parcelamentos:*\n"
                    for d, v, desc, pagas, total in parcelas:
                        marca = "⚠️ atrasado " if d < dia_atual else ""
                        texto += f"• {marca}Dia {d:02d} | R$ {v:.2f} — {escape_md(desc)} ({pagas+1}/{total})\n"
                if any(d < dia_atual for d, _, _ in fixos) or any(d < dia_atual for d, _, _, _, _ in parcelas):
                    texto += "\n_⚠️ = dia já passou neste mês mas ainda não foi lançado (vai cair na próxima virada de dia)._"

                bot.reply_to(message, texto, parse_mode="Markdown")
                
        elif intencao == "resumo_diario":
            tipo_dia = (dados.get("tipo_dia") or "ambos").strip().lower()
            if tipo_dia not in ("gastos", "receitas", "ambos"):
                tipo_dia = "ambos"

            dia_relativo = (dados.get("dia_relativo") or "").strip().lower()
            data_futura = (dados.get("data_futura") or "").strip() or None
            dia_mes_num = dados.get("dia_mes")

            # Resolve a data localmente — não confia em datas absolutas inventadas pela IA
            if dia_relativo == "hoje" or (not dia_relativo and not data_futura and not dia_mes_num):
                dia = datetime.now().strftime("%Y-%m-%d")
            elif dia_relativo == "ontem":
                dia = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            elif dia_relativo in ("anteontem", "ante-ontem"):
                dia = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            elif dia_relativo in ("amanhã", "amanha"):
                dia = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            elif dia_mes_num:
                # Usuário disse "dia 15" — assume mês/ano atual.
                # CUIDADO: dia 31 em fev quebra. Clamp pro último dia do mês.
                try:
                    d = int(dia_mes_num)
                    hoje = datetime.now()
                    import calendar as _cal
                    ult = _cal.monthrange(hoje.year, hoje.month)[1]
                    d_clamp = max(1, min(d, ult))
                    dia = hoje.replace(day=d_clamp).strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    dia = datetime.now().strftime("%Y-%m-%d")
            elif data_futura:
                # Só aceita data absoluta se for do ano corrente ou anterior (sanity check)
                try:
                    dt = datetime.strptime(data_futura, "%Y-%m-%d")
                    ano_atual = datetime.now().year
                    if dt.year < ano_atual - 1 or dt.year > ano_atual + 1:
                        dia = datetime.now().strftime("%Y-%m-%d")
                    else:
                        dia = data_futura
                except ValueError:
                    dia = datetime.now().strftime("%Y-%m-%d")
            else:
                dia = datetime.now().strftime("%Y-%m-%d")

            bot.reply_to(
                message,
                resumo_diario_texto(user_id, dia=dia, tipo=tipo_dia),
                parse_mode="Markdown",
            )

        elif intencao == "desativar_lembrete":
            definir_lembrete(user_id, False)
            bot.reply_to(message, "🔕 Lembretes diários desativados. Pode reativar a qualquer momento dizendo 'ativa lembrete'.")

        # ========== CONTAS BANCÁRIAS (multi-conta) ==========
        elif intencao == "criar_conta":
            nome = _s(dados.get("conta") or dados.get("banco_nome") or "").strip()
            if not nome:
                bot.reply_to(message, "Qual o nome da conta? Ex: 'cria uma conta Nubank'.")
                return
            if buscar_conta(user_id, nome=nome):
                bot.reply_to(message, f"Você já tem uma conta chamada *{escape_md(nome)}*.", parse_mode="Markdown")
                return
            banco = _s(dados.get("banco_nome") or "").strip() or nome
            tipo = _s(dados.get("tipo_conta") or "").strip()
            cid = criar_conta(user_id, nome, banco=banco, tipo=tipo)
            bot.reply_to(
                message,
                f"🏦 Conta *{escape_md(nome)}* cadastrada (#{cid}, {normalizar_tipo_conta(tipo)})!\n"
                f"Agora pode dizer coisas como: 'gastei 50 no mercado pelo {escape_md(nome)}'.",
                parse_mode="Markdown",
            )

        elif intencao == "listar_contas":
            contas = listar_contas(user_id)
            if not contas:
                bot.reply_to(
                    message,
                    "Você ainda não cadastrou nenhuma conta. Cadastre dizendo 'cria uma conta Nubank' "
                    "ou via /conta_nova.\n\n"
                    "Lançamentos sem conta vão pra pilha *Geral* — continuam funcionando normalmente.",
                    parse_mode="Markdown",
                )
                return
            texto = "🏦 *Suas contas:*\n"
            saldo_total = 0.0
            for cid, nome, banco, tipo, _criado in contas:
                saldo = saldo_conta(user_id, cid)
                saldo_total += saldo
                detalhes = tipo or "Conta"
                if banco and banco.lower() != nome.lower():
                    detalhes += f" · {banco}"
                texto += f"\n• #{cid} *{escape_md(nome)}* ({detalhes})\n  Saldo atual: R$ {saldo:.2f}"
            texto += f"\n\n💰 *Total: R$ {saldo_total:.2f}*"
            bot.reply_to(message, texto, parse_mode="Markdown")

        elif intencao == "remover_conta":
            nome = _s(dados.get("conta") or "").strip()
            if not nome:
                bot.reply_to(message, "Qual conta você quer remover? Ex: 'remove a conta Nubank'.")
                return
            cid = _resolver_conta_intent(user_id, nome, message)
            if cid is False:
                return
            if cid is None:
                bot.reply_to(message, "Você não tem essa conta cadastrada.")
                return
            n_lanc = contar_lancamentos_conta(user_id, cid)
            obj = buscar_conta(user_id, conta_id=cid)
            set_pendente(user_id, "remover_conta", {"conta_id": cid, "nome": obj[1] if obj else nome})
            aviso = ""
            if n_lanc:
                aviso = f"\n\n⚠️ Essa conta tem {n_lanc} lançamento(s). Eles vão virar 'Geral' (sem conta) — *não* serão apagados."
            bot.reply_to(
                message,
                f"⚠️ Confirmar: remover a conta *{escape_md(obj[1] if obj else nome)}*?{aviso}\n\n"
                f"Responde *sim* pra confirmar ou *não* pra cancelar.",
                parse_mode="Markdown",
            )

        elif intencao == "renomear_conta":
            nome = _s(dados.get("conta") or "").strip()
            if not nome:
                bot.reply_to(message, "Qual conta você quer alterar? Ex: 'renomeia o nubank pra Nu'.")
                return
            cid = _resolver_conta_intent(user_id, nome, message)
            if cid is False:
                return
            if cid is None:
                bot.reply_to(message, "Você não tem essa conta cadastrada.")
                return
            novo_nome = _s(dados.get("novo_conta_nome") or "").strip() or None
            novo_banco = dados.get("banco_nome")
            novo_banco = _s(novo_banco).strip() if novo_banco is not None else None
            novo_tipo = _s(dados.get("tipo_conta") or "").strip() or None
            if not (novo_nome or novo_banco or novo_tipo):
                bot.reply_to(message, "O que você quer mudar? Diz: 'renomeia X pra Y' ou 'muda o tipo de X pra poupança'.")
                return
            if novo_nome and buscar_conta(user_id, nome=novo_nome):
                bot.reply_to(message, f"Já existe uma conta chamada *{escape_md(novo_nome)}*. Escolhe outro nome.", parse_mode="Markdown")
                return
            renomear_conta(user_id, cid, novo_nome=novo_nome, novo_banco=novo_banco, novo_tipo=novo_tipo)
            obj = buscar_conta(user_id, conta_id=cid)
            bot.reply_to(
                message,
                f"✅ Conta atualizada!\n• Nome: *{escape_md(obj[1])}*\n"
                f"• Tipo: {obj[3] or '—'}\n• Banco: {obj[2] or '—'}",
                parse_mode="Markdown",
            )

        elif intencao == "transferir_conta":
            valor = parse_valor(dados.get("valor"))
            if valor <= 0:
                bot.reply_to(message, "Qual valor da transferência?")
                return
            origem = _s(dados.get("conta") or "").strip()
            destino = _s(dados.get("conta_destino") or "").strip()
            if not origem or not destino:
                bot.reply_to(message, "Diga origem e destino. Ex: 'transferi 500 do nubank pro itau'.")
                return
            cid_origem = _resolver_conta_intent(user_id, origem, message)
            if cid_origem is False:
                return
            cid_destino = _resolver_conta_intent(user_id, destino, message)
            if cid_destino is False:
                return
            if cid_origem is None or cid_destino is None:
                bot.reply_to(message, "Não achei uma das contas. Use /contas pra ver.")
                return
            if cid_origem == cid_destino:
                bot.reply_to(message, "Origem e destino são a mesma conta. 🤔")
                return
            transferir_entre_contas(user_id, cid_origem, cid_destino, valor)
            obj_o = buscar_conta(user_id, conta_id=cid_origem)
            obj_d = buscar_conta(user_id, conta_id=cid_destino)
            saldo_o = saldo_conta(user_id, cid_origem)
            saldo_d = saldo_conta(user_id, cid_destino)
            bot.reply_to(
                message,
                f"🔁 Transferência de R$ {valor:.2f}:\n"
                f"➖ {escape_md(obj_o[1])}: R$ {saldo_o:.2f}\n"
                f"➕ {escape_md(obj_d[1])}: R$ {saldo_d:.2f}",
                parse_mode="Markdown",
            )

        elif intencao == "consultar_saldo":
            nome = _s(dados.get("conta") or "").strip()
            if nome:
                cid = _resolver_conta_intent(user_id, nome, message)
                if cid is False:
                    return
                if cid is None:
                    rec = total_receita_mes(user_id)
                    gas = total_gasto_mes(user_id)
                    bot.reply_to(
                        message,
                        f"💰 *Saldo geral — {fmt_mes(mes_atual())}*\n"
                        f"➕ Entradas: R$ {rec:.2f}\n"
                        f"➖ Saídas: R$ {gas:.2f}\n"
                        f"💵 Saldo do mês: R$ {rec - gas:.2f}",
                        parse_mode="Markdown",
                    )
                    return
                obj = buscar_conta(user_id, conta_id=cid)
                saldo_atual = saldo_conta(user_id, cid)
                rec_mes = total_receita_mes(user_id, conta_id=cid)
                gas_mes = total_gasto_mes(user_id, conta_id=cid)
                bot.reply_to(
                    message,
                    f"🏦 *{escape_md(obj[1])}*\n"
                    f"💰 Saldo atual: *R$ {saldo_atual:.2f}*\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"📅 Este mês ({fmt_mes(mes_atual())}):\n"
                    f"  ➕ Entradas: R$ {rec_mes:.2f}\n"
                    f"  ➖ Saídas: R$ {gas_mes:.2f}",
                    parse_mode="Markdown",
                )
            else:
                contas = listar_contas(user_id)
                rec_geral = total_receita_mes(user_id)
                gas_geral = total_gasto_mes(user_id)
                saldo_total = sum(saldo_conta(user_id, c[0]) for c in contas)
                texto = (
                    f"💰 *Saldo total: R$ {saldo_total:.2f}*\n"
                    f"━━━━━━━━━━━━━━━━━\n"
                    f"📅 Esse mês ({fmt_mes(mes_atual())}):\n"
                    f"  ➕ Entradas: R$ {rec_geral:.2f}\n"
                    f"  ➖ Saídas: R$ {gas_geral:.2f}"
                )
                if contas:
                    texto += "\n\n*Por conta:*"
                    for cid, nome_c, _b, _t, _c in contas:
                        s = saldo_conta(user_id, cid)
                        texto += f"\n• {escape_md(nome_c)}: R$ {s:.2f}"
                bot.reply_to(message, texto, parse_mode="Markdown")

        else:
            resposta = dados.get("resposta") or "Pode me contar mais? Posso anotar gastos e receitas, definir orçamento, metas ou mostrar relatórios."
            bot.reply_to(message, resposta)

    except Exception as e:
        print(f"Erro: {e}")
        bot.reply_to(message, "Ops, tive um problema pra processar isso. Pode tentar de novo?")


# ================= AGENDADOR =================

def scheduler_loop():
    schedule.every().sunday.at("18:00").do(enviar_resumos_semanais)
    schedule.every().day.at("00:05").do(aplicar_gastos_fixos_do_dia)
    schedule.every().day.at("00:05").do(aplicar_receitas_fixas_do_dia)
    schedule.every().day.at("00:10").do(aplicar_parcelamentos_do_dia)
    schedule.every().day.at("20:00").do(enviar_lembretes_diarios)
    schedule.every().day.at("09:00").do(verificar_alertas_cartoes)
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            print(f"Erro no scheduler: {e}")
        time.sleep(30)


# ================= INICIALIZAÇÃO =================

if __name__ == "__main__":
    criar_banco()
    aplicar_gastos_fixos_do_dia()
    aplicar_parcelamentos_do_dia()
    aplicar_receitas_fixas_do_dia()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    print("🤖 Bot rodando (multi-usuário)...")
    bot.infinity_polling()