import os
import json
import sqlite3
import threading
import time
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


memoria_usuarios = {}

def chamar_ia(user_id, contents, system_instruction):
    """Chama o Gemini mantendo o histórico de conversa com retry."""
    config = types.GenerateContentConfig(
        system_instruction=system_instruction,
        response_mime_type="application/json",
    )
    
    # Se for a primeira vez do usuário, cria uma sessão com memória
    if user_id not in memoria_usuarios:
        memoria_usuarios[user_id] = client.chats.create(
            model=MODEL_NAME, 
            config=config
        )
        
    chat = memoria_usuarios[user_id]
    ultima_excecao = None
    
    for _ in range(3): # Tenta até 3 vezes se der erro de sobrecarga
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

    conn.commit()
    conn.close()


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
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        v = v.replace("R$", "").replace(" ", "").replace(",", ".")
        return float(v) if v else 0.0
    return 0.0


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
    }
    return mapa.get(m, m.capitalize())


def salvar_gasto(user_id, valor, categoria, descricao, data=None, metodo_pagamento=None):
    conn = db()
    data = data or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.execute(
        "INSERT INTO gastos (user_id, data, valor, categoria, descricao, metodo_pagamento) VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, data, valor, categoria, descricao, metodo_pagamento),
    )
    conn.commit()
    conn.close()


def total_gasto_mes(user_id, mes=None):
    mes = mes or mes_atual()
    conn = db()
    total = conn.execute(
        "SELECT SUM(valor) FROM gastos WHERE user_id = ? AND strftime('%Y-%m', data) = ?",
        (user_id, mes),
    ).fetchone()[0]
    conn.close()
    return total or 0.0


def apagar_ultimo_gasto(user_id):
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


# ================= RECEITAS =================

def adicionar_receita_fixa(user_id, descricao, valor, fonte, dia_mes):
    conn = db()
    conn.execute(
        """INSERT INTO receitas_fixas (user_id, descricao, valor, fonte, dia_mes, ultimo_mes_aplicado)
           VALUES (?, ?, ?, ?, ?, NULL)""",
        (user_id, descricao, valor, fonte, dia_mes),
    )
    conn.commit()
    conn.close()

def listar_receitas_fixas(user_id):
    conn = db()
    rows = conn.execute(
        "SELECT id, descricao, valor, fonte, dia_mes FROM receitas_fixas WHERE user_id = ? ORDER BY dia_mes",
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
    conn = db()
    rows = conn.execute(
        """SELECT id, user_id, descricao, valor, fonte FROM receitas_fixas
           WHERE dia_mes = ? AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
        (dia, mes),
    ).fetchall()
    aplicados = 0
    for fid, user_id, desc, valor, fonte in rows:
        conn.execute(
            "INSERT INTO receitas (user_id, data, valor, fonte, descricao) VALUES (?, ?, ?, ?, ?)",
            (user_id, hoje.strftime("%Y-%m-%d %H:%M:%S"), valor, fonte, desc),
        )
        conn.execute(
            "UPDATE receitas_fixas SET ultimo_mes_aplicado = ? WHERE id = ?", (mes, fid)
        )
        aplicados += 1
    conn.commit()
    conn.close()
    if aplicados:
        print(f"Aplicadas {aplicados} receitas fixas hoje ({hoje.date()})")
def salvar_receita(user_id, valor, fonte, descricao):
    conn = db()
    conn.execute(
        "INSERT INTO receitas (user_id, data, valor, fonte, descricao) VALUES (?, ?, ?, ?, ?)",
        (user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), valor, fonte, descricao),
    )
    conn.commit()
    conn.close()


def total_receita_mes(user_id, mes=None):
    mes = mes or mes_atual()
    conn = db()
    total = conn.execute(
        "SELECT SUM(valor) FROM receitas WHERE user_id = ? AND strftime('%Y-%m', data) = ?",
        (user_id, mes),
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

def adicionar_gasto_fixo(user_id, descricao, valor, categoria, dia_mes):
    conn = db()
    conn.execute(
        """INSERT INTO gastos_fixos (user_id, descricao, valor, categoria, dia_mes, ultimo_mes_aplicado)
           VALUES (?, ?, ?, ?, ?, NULL)""",
        (user_id, descricao, valor, categoria, dia_mes),
    )
    conn.commit()
    conn.close()


def listar_gastos_fixos(user_id):
    conn = db()
    rows = conn.execute(
        "SELECT id, descricao, valor, categoria, dia_mes FROM gastos_fixos WHERE user_id = ? ORDER BY dia_mes",
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
    """Aplica gastos fixos do dia para todos os usuários."""
    hoje = datetime.now()
    dia = hoje.day
    mes = hoje.strftime("%Y-%m")
    conn = db()
    rows = conn.execute(
        """SELECT id, user_id, descricao, valor, categoria FROM gastos_fixos
           WHERE dia_mes = ? AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
        (dia, mes),
    ).fetchall()
    aplicados = 0
    for fid, user_id, desc, valor, categoria in rows:
        conn.execute(
            "INSERT INTO gastos (user_id, data, valor, categoria, descricao, metodo_pagamento) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, hoje.strftime("%Y-%m-%d %H:%M:%S"), valor, categoria, desc, None),
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
                           dia_cobranca, categoria, metodo_pagamento):
    valor_parcela = round(valor_total / total_parcelas, 2)
    conn = db()
    cur = conn.execute(
        """INSERT INTO parcelamentos
           (user_id, descricao, valor_parcela, total_parcelas, parcelas_pagas,
            dia_cobranca, categoria, metodo_pagamento, criado_em)
           VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)""",
        (user_id, descricao, valor_parcela, total_parcelas, dia_cobranca,
         categoria, metodo_pagamento, datetime.now().isoformat()),
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
                      parcelas_pagas, categoria, metodo_pagamento
               FROM parcelamentos WHERE id = ?
                 AND parcelas_pagas < total_parcelas
                 AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
            (forcar_id, mes),
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT id, user_id, descricao, valor_parcela, total_parcelas,
                      parcelas_pagas, categoria, metodo_pagamento
               FROM parcelamentos
               WHERE dia_cobranca = ? AND parcelas_pagas < total_parcelas
                 AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
            (dia, mes),
        ).fetchall()
    aplicados = 0
    for pid, user_id, desc, vp, total, pagas, cat, metodo in rows:
        nova = pagas + 1
        descricao_completa = f"{desc} ({nova}/{total})"
        conn.execute(
            "INSERT INTO gastos (user_id, data, valor, categoria, descricao, metodo_pagamento) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, hoje.strftime("%Y-%m-%d %H:%M:%S"), vp, cat, descricao_completa, metodo),
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
    """Busca gastos com filtros opcionais."""
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

    sql = f"""SELECT data, valor, categoria, descricao, metodo_pagamento
              FROM gastos WHERE {' AND '.join(where)}
              ORDER BY id DESC LIMIT 30"""
    conn = db()
    rows = conn.execute(sql, params).fetchall()
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
    g_ant = total_gasto_mes(user_id, mes_anterior())
    conn.close()

    if gastos == 0 and receitas == 0:
        return ("Você ainda não registrou gastos nem receitas esse mês. "
                "Comece anotando seus gastos do dia pra eu poder te ajudar com dicas! 😉")

    contexto = f"""Dados financeiros do usuário em {fmt_mes(mes)}:
- Receitas: R$ {receitas:.2f}
- Gastos totais: R$ {gastos:.2f}
- Saldo: R$ {receitas - gastos:.2f}
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
        "Você pode falar comigo naturalmente:\n"
        "• 'gastei 50 no mercado no crédito' (gasto + forma de pagamento)\n"
        "• 'comprei celular 1200 em 12x' (parcelamento)\n"
        "• 'recebi 3000 de salário'\n"
        "• 'meu orçamento é 2000'\n"
        "• 'quero economizar 500 esse mês' (meta)\n"
        "• 'aluguel 1200 todo dia 5' (gasto fixo)\n"
        "• 'quanto gastei com uber?' (busca)\n"
        "• 'me dá um conselho' (análise IA)\n"
        "• 'compara com mês passado'\n"
        "• 'resumo da semana'\n"
        "• 'apaga o último'\n"
        "• 'desativa lembrete' (controla lembretes diários)\n\n"
        "📸 *Manda uma foto de comprovante ou nota fiscal* que eu leio e registro o gasto pra você.\n\n"
        "📅 Todo domingo às 18h te mando um resumo da semana automaticamente.\n"
        "🔔 Todo dia às 20h te lembro de registrar gastos (se esqueceu)."
    )
    bot.reply_to(message, texto, parse_mode="Markdown")


@bot.message_handler(commands=["relatorio"])
def gerar_relatorio(message):
    registrar_usuario(message.chat.id)
    user_id = message.chat.id
    agora = datetime.now()
    hoje_str = agora.strftime("%Y-%m-%d %H:%M:%S")
    mes = mes_atual()
    dia_atual = agora.day

    conn = db()
    
    # 1. Gastos que já aconteceram (até agora)
    gastos_reais = conn.execute(
        "SELECT SUM(valor) FROM gastos WHERE user_id = ? AND strftime('%Y-%m', data) = ? AND data <= ?",
        (user_id, mes, hoje_str)
    ).fetchone()[0] or 0.0

    # 2. Gastos futuros (lançados com data futura no banco principal)
    gastos_futuros_pontuais = conn.execute(
        "SELECT SUM(valor) FROM gastos WHERE user_id = ? AND strftime('%Y-%m', data) = ? AND data > ?",
        (user_id, mes, hoje_str)
    ).fetchone()[0] or 0.0

    # 3. Gastos Fixos que ainda não caíram este mês (Pendentes)
    gastos_fixos_futuros = conn.execute(
        """SELECT SUM(valor) FROM gastos_fixos 
           WHERE user_id = ? 
           AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
        (user_id, mes)
    ).fetchone()[0] or 0.0

    # 4. Parcelamentos que ainda não caíram este mês (Pendentes)
    parcelas_futuras = conn.execute(
        """SELECT SUM(valor_parcela) FROM parcelamentos 
           WHERE user_id = ? AND parcelas_pagas < total_parcelas 
           AND (ultimo_mes_aplicado IS NULL OR ultimo_mes_aplicado != ?)""",
        (user_id, mes)
    ).fetchone()[0] or 0.0

    gastos_futuros = gastos_futuros_pontuais + gastos_fixos_futuros + parcelas_futuras
    
    receitas_mes = total_receita_mes(user_id, mes)
    
    # --- BUSCAS PARA DETALHAMENTO ---
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

    saldo_atual = receitas_mes - gastos_reais
    saldo_previsto = receitas_mes - (gastos_reais + gastos_futuros)

    # Monta o texto do Extrato
    texto = f"📊 *Relatório e Extrato de {fmt_mes(mes)}*\n"
    texto += f"💵 *Receitas:* R$ {receitas_mes:.2f}\n"
    texto += f"💸 *Gastos realizados:* R$ {gastos_reais:.2f}\n"
    texto += f"⏳ *Gastos futuros:* R$ {gastos_futuros:.2f}\n"
    texto += f"\n💰 *Saldo Atual:* R$ {saldo_atual:.2f}\n"
    texto += f"🔮 *Saldo Previsto (Final do mês):* R$ {saldo_previsto:.2f}\n"

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
    registrar_usuario(message.chat.id)
    fixos = listar_gastos_fixos(message.chat.id)
    if not fixos:
        bot.reply_to(message, "Você ainda não tem gastos fixos cadastrados.")
        return
    texto = "📋 *Seus gastos fixos:*\n"
    for fid, desc, valor, cat, dia in fixos:
        texto += f"\n• #{fid} | dia {dia:02d} | R$ {valor:.2f} | {cat} — {desc}"
    texto += "\n\nPra remover: 'remove gasto fixo #ID'"
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

- "registrar_gasto":despesa real. Se o usuário mencionar uma data futura ou disser que algo 'vai vencer' ou é 'para o dia X', extraia a data no formato YYYY-MM-DD no campo "data_futura".(ex: "gastei 50 no mercado", "uber 20", "almoço 35 no crédito").
  Se o usuário mencionar forma de pagamento (crédito, débito, pix, dinheiro, boleto), extraia em "metodo_pagamento".
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
- "consultar_relatorio": ver extrato, entradas e saídas, detalhamento, saldo, categorias, histórico e relatório.
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
- "conversa": qualquer outra coisa (saudação, dúvida, agradecimento).
- "adicionar_receita_fixa": cadastrar entrada de dinheiro automática (ex: "recebo 828 de salário todo dia 15", "adiantamento 500 dia 20"). Extraia o "dia_mes".
- "listar_receitas_fixas": ver receitas fixas cadastradas.
- "remover_receita_fixa": remover uma receita fixa (extraia o ID em "fixo_id" se mencionado).
- "apagar_receita": remover todas as receitas do mês atual (ex: "zere minhas receitas", "apague as receitas de abril").
- "ajustar_saldo": forçar o saldo a bater com um valor exato (ex: "ajuste meu saldo para 100", "meu saldo é 50").
- "adicionar_receita_fixa": entrada de dinheiro automática mensal (ex: "recebo 828 todo dia 15"). Extraia o "dia_mes".
- "listar_receitas_fixas": ver receitas fixas cadastradas.
- "remover_receita_fixa": remover receita fixa (extraia "fixo_id" se mencionado).
- "listar_gastos_futuros": listar ou mostrar contas a pagar, gastos futuros, agendados, ou o que falta pagar neste mês.

Retorne SEMPRE este JSON:
{
  "intencao": "<uma das opções>",
  "valor": <float ou null>,
  "categoria": "<string ou vazio>",
  "descricao": "<string ou vazio>",
  "fonte": "<string ou vazio — pra receita>",
  "metodo_pagamento": "<Crédito|Débito|Pix|Dinheiro|Boleto ou vazio>",
  "dia_mes": <int 1-31 ou null>,
  "total_parcelas": <int ou null — pra parcelamento>,
  "fixo_id": <int ou null>,
  "parc_id": <int ou null>,
  "texto": "<palavra-chave pra busca, vazio se não aplicável>",
  "periodo": "<esse_mes|mes_passado|semana|tudo — pra busca>",
  "data_futura": "<YYYY-MM-DD ou null>",
  "resposta": "<texto curto e amigável em PT-BR — preencha em 'conversa' ou pra pedir esclarecimento>"
}

Categorias devem ser curtas e naturais (Alimentação, Transporte, Lazer, Saúde,
Mercado, Moradia, Educação, etc). NUNCA use "Orçamento" ou "Meta" como categoria.

Se a mensagem for ambígua (ex: só "20"), use intencao "conversa" e peça detalhes na "resposta"."""


@bot.message_handler(content_types=["photo"])
def processar_foto(message):
    registrar_usuario(message.chat.id)
    user_id = message.chat.id
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

        salvar_gasto(user_id, valor, categoria, descricao, data=data_compra, metodo_pagamento=metodo)
        resp = f"📸 Comprovante registrado!\n💰 R$ {valor:.2f} — {categoria}"
        if descricao:
            resp += f"\n📝 {descricao}"
        if metodo:
            resp += f"\n💳 {metodo}"
        if data_compra:
            resp += f"\n📅 {datetime.strptime(data_compra, '%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y')}"
        s = status_orcamento_texto(user_id)
        if s:
            resp += f"\n\n{s}"
        bot.reply_to(message, resp)
    except Exception as e:
        print(f"Erro foto: {e}")
        bot.reply_to(message, "Tive um problema pra ler a foto. Pode tentar de novo?")


@bot.message_handler(func=lambda message: True)
def processar_mensagem(message):
    registrar_usuario(message.chat.id)
    user_id = message.chat.id
    texto_usuario = message.text

    try:
        bot.send_chat_action(message.chat.id, "typing")

        resposta_ia = chamar_ia(user_id, texto_usuario, SYSTEM_INSTRUCTION)
        print(f"[{user_id}] IA: {resposta_ia.text}")
        dados = json.loads(resposta_ia.text)
        intencao = dados.get("intencao", "conversa")

        if intencao == "registrar_gasto":
            valor = parse_valor(dados.get("valor"))
            if valor <= 0:
                bot.reply_to(message, "Não consegui identificar o valor 🤔 Pode me dizer quanto foi?")
                return
            categoria = (dados.get("categoria") or "Outros").strip() or "Outros"
            descricao = (dados.get("descricao") or "").strip()
            metodo = normalizar_metodo(dados.get("metodo_pagamento"))
            
            # Pega a data futura se a IA identificou, senão usa None (data atual)
            data_lancamento = dados.get("data_futura")
            
            salvar_gasto(user_id, valor, categoria, descricao, data=data_lancamento, metodo_pagamento=metodo)
            
            # Mensagem de confirmação inteligente
            prefixo = "⏳ Agendado!" if data_lancamento else "✅ Anotado!"
            resp = f"{prefixo}\n💰 R$ {valor:.2f} — {categoria}"
            if data_lancamento:
                data_pt = datetime.strptime(data_lancamento, "%Y-%m-%d").strftime("%d/%m/%Y")
                resp += f"\n📅 Vencimento: {data_pt}"
            
            bot.reply_to(message, resp)

        elif intencao == "adicionar_receita_fixa":
            valor = parse_valor(dados.get("valor"))
            dia = dados.get("dia_mes")
            descricao = (dados.get("descricao") or "Receita Fixa").strip()
            fonte = (dados.get("fonte") or dados.get("categoria") or "Salário").strip()
            if valor <= 0 or not dia:
                bot.reply_to(message, "Para cadastrar uma receita fixa preciso do valor e do dia do mês. Ex: 'recebo 828 todo dia 15'.")
                return
            try:
                dia = int(dia)
                if dia < 1 or dia > 31:
                    raise ValueError
            except (ValueError, TypeError):
                bot.reply_to(message, "Dia do mês inválido (precisa ser entre 1 e 31).")
                return
            adicionar_receita_fixa(user_id, descricao, valor, fonte, dia)
            bot.reply_to(
                message,
                f"💸 Receita automática programada!\n• {descricao} — R$ {valor:.2f} ({fonte})\n• Vai cair na conta todo dia {dia:02d}.",
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
            fid = dados.get("fixo_id")
            if not fid:
                bot.reply_to(message, "Qual receita fixa? Diga 'listar receitas fixas' pra ver os IDs.")
                return
            n = remover_receita_fixa(user_id, int(fid))
            bot.reply_to(message, f"🗑️ Receita automática #{fid} removida." if n else f"Não encontrei a receita #{fid}.")
        elif intencao == "registrar_receita":
            valor = parse_valor(dados.get("valor"))
            if valor <= 0:
                bot.reply_to(message, "Qual o valor da receita?")
                return
            fonte = (dados.get("fonte") or dados.get("categoria") or "Outros").strip() or "Outros"
            descricao = (dados.get("descricao") or "").strip()
            salvar_receita(user_id, valor, fonte, descricao)
            resp = f"💵 Receita anotada!\n+R$ {valor:.2f} — {fonte}"
            if descricao:
                resp += f"\n📝 {descricao}"
            saldo = total_receita_mes(user_id) - total_gasto_mes(user_id)
            resp += f"\n\n💰 Saldo do mês: R$ {saldo:.2f}"
            m = status_meta_texto(user_id)
            if m:
                resp += f"\n\n{m}"
            bot.reply_to(message, resp, parse_mode="Markdown" if m else None)
       
        elif intencao == "apagar_receita":
            conn = db()
            n = conn.execute(
                "DELETE FROM receitas WHERE user_id = ? AND strftime('%Y-%m', data) = ?", 
                (user_id, mes_atual())
            ).rowcount
            conn.commit()
            conn.close()
            if n > 0:
                bot.reply_to(message, f"🗑️ {n} receita(s) de {fmt_mes(mes_atual())} apagada(s) com sucesso!")
            else:
                bot.reply_to(message, "Você não tinha nenhuma receita registrada nesse mês para apagar.")

        elif intencao == "ajustar_saldo":
            novo_saldo_desejado = parse_valor(dados.get("valor"))
            saldo_atual = total_receita_mes(user_id) - total_gasto_mes(user_id)
            diferenca = novo_saldo_desejado - saldo_atual

            if diferenca > 0:
                # O saldo atual é menor que o desejado, então adicionamos uma receita compensatória
                salvar_receita(user_id, diferenca, "Ajuste de Saldo", "Ajuste manual do sistema")
                bot.reply_to(message, f"⚖️ Entendido! Lancei uma entrada de R$ {diferenca:.2f} para o seu saldo bater exatamente os R$ {novo_saldo_desejado:.2f} que você pediu.")
            elif diferenca < 0:
                # O saldo atual é maior, então adicionamos um gasto compensatório
                salvar_gasto(user_id, abs(diferenca), "Ajuste de Saldo", "Ajuste manual do sistema", metodo_pagamento="Outros")
                bot.reply_to(message, f"⚖️ Entendido! Lancei uma saída de R$ {abs(diferenca):.2f} para o seu saldo bater exatamente os R$ {novo_saldo_desejado:.2f}.")
            else:
                bot.reply_to(message, f"O seu saldo já está exatamente em R$ {novo_saldo_desejado:.2f}! Nenhuma alteração foi necessária. 😉")
        elif intencao == "apagar_receita":
            conn = db()
            # Deleta as receitas do usuário atual no mês atual
            n = conn.execute(
                "DELETE FROM receitas WHERE user_id = ? AND strftime('%Y-%m', data) = ?", 
                (user_id, mes_atual())
            ).rowcount
            conn.commit()
            conn.close()
            
            if n > 0:
                bot.reply_to(message, f"🗑️ {n} receita(s) de {fmt_mes(mes_atual())} apagada(s) com sucesso! A casa tá limpa.")
            else:
                bot.reply_to(message, "Você não tinha nenhuma receita registrada nesse mês para apagar.")    

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
            conn = db()
            n = conn.execute(
                "DELETE FROM orcamentos WHERE user_id = ? AND mes = ?", (user_id, mes_atual())
            ).rowcount
            conn.commit()
            conn.close()
            bot.reply_to(message, f"🗑️ Orçamento de {fmt_mes(mes_atual())} removido." if n else "Você não tinha orçamento esse mês.")

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
            conn = db()
            n = conn.execute(
                "DELETE FROM metas WHERE user_id = ? AND mes = ?", (user_id, mes_atual())
            ).rowcount
            conn.commit()
            conn.close()
            bot.reply_to(message, "🗑️ Meta removida." if n else "Você não tinha meta esse mês.")

        elif intencao == "adicionar_gasto_fixo":
            valor = parse_valor(dados.get("valor"))
            dia = dados.get("dia_mes")
            descricao = (dados.get("descricao") or "").strip()
            categoria = (dados.get("categoria") or "Fixo").strip() or "Fixo"
            if valor <= 0 or not dia or not descricao:
                bot.reply_to(message, "Pra cadastrar um gasto fixo preciso de: descrição, valor e dia do mês. Ex: 'aluguel 1200 todo dia 5'.")
                return
            try:
                dia = int(dia)
                if dia < 1 or dia > 31:
                    raise ValueError
            except (ValueError, TypeError):
                bot.reply_to(message, "Dia do mês inválido (precisa ser entre 1 e 31).")
                return
            adicionar_gasto_fixo(user_id, descricao, valor, categoria, dia)
            bot.reply_to(
                message,
                f"📌 Gasto fixo cadastrado!\n• {descricao} — R$ {valor:.2f} ({categoria})\n• Lança automaticamente todo dia {dia:02d}.",
            )

        elif intencao == "listar_gastos_fixos":
            comando_fixos(message)

        elif intencao == "remover_gasto_fixo":
            fid = dados.get("fixo_id")
            if not fid:
                bot.reply_to(message, "Qual gasto fixo? Use /fixos pra ver os IDs e diga 'remove gasto fixo #2'.")
                return
            n = remover_gasto_fixo(user_id, int(fid))
            bot.reply_to(message, f"🗑️ Gasto fixo #{fid} removido." if n else f"Não encontrei o gasto fixo #{fid}.")

        elif intencao == "consultar_relatorio":
            gerar_relatorio(message)

        elif intencao == "comparar_meses":
            bot.reply_to(message, comparar_meses_texto(user_id), parse_mode="Markdown")

        elif intencao == "resumo_semanal":
            bot.reply_to(message, resumo_semanal_texto(user_id), parse_mode="Markdown")

        elif intencao == "apagar_ultimo":
            row = apagar_ultimo_gasto(user_id)
            if row:
                _, valor, categoria, desc = row
                d = f" — {desc}" if desc else ""
                bot.reply_to(message, f"🗑️ Último gasto removido: R$ {valor:.2f} ({categoria}{d})")
            else:
                bot.reply_to(message, "Não há gastos pra apagar.")

        elif intencao == "adicionar_parcelamento":
            valor_total = parse_valor(dados.get("valor"))
            total_parcelas = dados.get("total_parcelas")
            descricao = (dados.get("descricao") or "").strip()
            categoria = (dados.get("categoria") or "Outros").strip() or "Outros"
            metodo = normalizar_metodo(dados.get("metodo_pagamento")) or "Crédito"
            dia = dados.get("dia_mes") or 10
            if valor_total <= 0 or not total_parcelas or not descricao:
                bot.reply_to(message, "Pra cadastrar um parcelamento preciso de: descrição, valor total e número de parcelas. Ex: 'comprei celular 1200 em 12x'.")
                return
            try:
                total_parcelas = int(total_parcelas)
                dia = int(dia)
                if total_parcelas < 1 or dia < 1 or dia > 31:
                    raise ValueError
            except (ValueError, TypeError):
                bot.reply_to(message, "Número de parcelas ou dia inválido.")
                return
            vp = adicionar_parcelamento(user_id, descricao, valor_total, total_parcelas, dia, categoria, metodo)
            bot.reply_to(
                message,
                f"💳 Parcelamento cadastrado!\n"
                f"• {descricao} — R$ {valor_total:.2f} em {total_parcelas}x de R$ {vp:.2f}\n"
                f"• {metodo}, cobrança todo dia {dia:02d}\n"
                f"• 1ª parcela já lançada como gasto.",
            )

        elif intencao == "listar_parcelamentos":
            parcs = listar_parcelamentos(user_id)
            if not parcs:
                bot.reply_to(message, "Você não tem parcelamentos em andamento.")
            else:
                texto = "💳 *Parcelamentos em andamento:*\n"
                for pid, desc, vp, total, pagas, dia, cat, metodo in parcs:
                    restantes = total - pagas
                    texto += (
                        f"\n• #{pid} {desc}\n"
                        f"   {pagas}/{total} pagas — restam {restantes}x de R$ {vp:.2f}\n"
                        f"   {cat} • {metodo or 'Crédito'} • dia {dia:02d}"
                    )
                texto += "\n\nPra cancelar: 'remove parcelamento #ID'"
                bot.reply_to(message, texto, parse_mode="Markdown")

        elif intencao == "remover_parcelamento":
            pid = dados.get("parc_id")
            if not pid:
                bot.reply_to(message, "Qual parcelamento? Diga 'meus parcelamentos' pra ver os IDs.")
                return
            n = remover_parcelamento(user_id, int(pid))
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
                        dia = data
                    d = f" — {desc}" if desc else ""
                    m = f" [{metodo}]" if metodo else ""
                    texto += f"\n• {dia} | R$ {valor:.2f} | {cat}{d}{m}"
                if len(rows) > 20:
                    texto += f"\n\n_(mostrando 20 de {len(rows)})_"
                bot.reply_to(message, texto, parse_mode="Markdown")

        elif intencao == "conselho":
            bot.send_chat_action(message.chat.id, "typing")
            texto = conselho_financeiro(user_id)
            bot.reply_to(message, f"💡 *Sua análise financeira:*\n\n{texto}", parse_mode="Markdown")

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

            if not pontuais and not fixos and not parcelas:
                bot.reply_to(message, "Você não tem nenhum gasto futuro, fixo ou parcela pendente para o resto deste mês! 🎉")
            else:
                texto = "⏳ *Suas Contas a Pagar (Restante do mês)*\n"
                if pontuais:
                    texto += "\n*Lançamentos Agendados:*\n"
                    for d, v, desc in pontuais:
                        try:
                            dia_fmt = datetime.strptime(d, "%Y-%m-%d %H:%M:%S").strftime("%d/%m")
                        except:
                            dia_fmt = d
                        texto += f"• {dia_fmt} | R$ {v:.2f} — {desc}\n"
                if fixos:
                    texto += "\n*Gastos Fixos:*\n"
                    for d, v, desc in fixos:
                        texto += f"• Dia {d:02d} | R$ {v:.2f} — {desc}\n"
                if parcelas:
                    texto += "\n*Parcelamentos:*\n"
                    for d, v, desc, pagas, total in parcelas:
                        texto += f"• Dia {d:02d} | R$ {v:.2f} — {desc} ({pagas+1}/{total})\n"

                bot.reply_to(message, texto, parse_mode="Markdown")
                
        elif intencao == "desativar_lembrete":
            definir_lembrete(user_id, False)
            bot.reply_to(message, "🔕 Lembretes diários desativados. Pode reativar a qualquer momento dizendo 'ativa lembrete'.")

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
