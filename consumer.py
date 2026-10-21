"""
CONSUMER - lê transações do Kafka, salva TODAS no SQLite e aplica as regras de fraude.

Regras (todas baseadas no histórico do MESMO cliente):
    1. ALTO_VALOR : valor >= 10.000
    2. TEMPO_60s  : 4 (ou mais) transações em menos de 60 segundos
    3. GEO_10m    : transações em cidades diferentes dentro de 10 minutos

Uso:
    python consumer.py
"""
import json
import sqlite3
from datetime import datetime, timedelta

from colorama import Fore, Style, init as colorama_init
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from config import (
    ARQUIVO_BANCO, GEO_JANELA_SEG, KAFKA_SERVIDOR, LIMITE_ALTO_VALOR,
    TEMPO_JANELA_SEG, TEMPO_QTD_TRANSACOES, TOPICO,
)

colorama_init()  # habilita cores ANSI também no Windows

# Cor de cada tipo de alerta no console
CORES = {
    "ALTO_VALOR": Fore.RED,
    "TEMPO_60s": Fore.YELLOW,
    "GEO_10m": Fore.MAGENTA,
}


# ----------------------------------------------------------------------
# BANCO DE DADOS
# ----------------------------------------------------------------------
def conectar_banco() -> sqlite3.Connection:
    """Abre o SQLite e cria as tabelas caso ainda não existam."""
    conn = sqlite3.connect(ARQUIVO_BANCO)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS transacoes (
            id_transacao TEXT PRIMARY KEY,
            id_cliente   TEXT NOT NULL,
            valor        REAL NOT NULL,
            timestamp    TEXT NOT NULL,   -- ISO 8601 em UTC
            cidade       TEXT NOT NULL
        )
    """)
    # Índice deixa as consultas de histórico por cliente rápidas
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cliente_ts ON transacoes (id_cliente, timestamp)")
    # Tabela extra (bônus): guarda os alertas gerados, útil para mostrar no trabalho
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alertas (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            id_transacao TEXT NOT NULL,
            id_cliente   TEXT NOT NULL,
            tipo         TEXT NOT NULL,
            detalhe      TEXT NOT NULL,
            criado_em    TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def salvar_transacao(conn: sqlite3.Connection, t: dict) -> None:
    """Salva a transação. INSERT OR IGNORE evita duplicar se a mensagem chegar 2x."""
    conn.execute(
        "INSERT OR IGNORE INTO transacoes VALUES (?, ?, ?, ?, ?)",
        (t["id_transacao"], t["id_cliente"], t["valor"], t["timestamp"], t["cidade"]),
    )
    conn.commit()


def salvar_alerta(conn: sqlite3.Connection, t: dict, tipo: str, detalhe: str) -> None:
    conn.execute(
        "INSERT INTO alertas (id_transacao, id_cliente, tipo, detalhe, criado_em) VALUES (?, ?, ?, ?, ?)",
        (t["id_transacao"], t["id_cliente"], tipo, detalhe, datetime.now().isoformat(timespec="seconds")),
    )
    conn.commit()


# ----------------------------------------------------------------------
# REGRAS DE FRAUDE
# Cada regra devolve um texto (detalhe do alerta) ou None se não houve fraude.
# IMPORTANTE: as regras são chamadas DEPOIS de salvar a transação atual,
# então a consulta de histórico já inclui a transação que acabou de chegar.
# ----------------------------------------------------------------------
def regra_alto_valor(t: dict) -> str | None:
    if t["valor"] >= LIMITE_ALTO_VALOR:
        return f"Valor R$ {t['valor']:,.2f} >= R$ {LIMITE_ALTO_VALOR:,.2f}"
    return None


def regra_tempo(conn: sqlite3.Connection, t: dict) -> str | None:
    """4 ou mais transações do mesmo cliente nos últimos 60 segundos."""
    agora = datetime.fromisoformat(t["timestamp"])
    inicio = (agora - timedelta(seconds=TEMPO_JANELA_SEG)).isoformat(timespec="seconds")

    (qtd,) = conn.execute(
        "SELECT COUNT(*) FROM transacoes WHERE id_cliente = ? AND timestamp > ? AND timestamp <= ?",
        (t["id_cliente"], inicio, t["timestamp"]),
    ).fetchone()

    if qtd >= TEMPO_QTD_TRANSACOES:
        return f"{qtd} transações em menos de {TEMPO_JANELA_SEG}s"
    return None


def regra_geo(conn: sqlite3.Connection, t: dict) -> str | None:
    """Cliente com transação em OUTRA cidade nos últimos 10 minutos."""
    agora = datetime.fromisoformat(t["timestamp"])
    inicio = (agora - timedelta(seconds=GEO_JANELA_SEG)).isoformat(timespec="seconds")

    linha = conn.execute(
        """SELECT cidade, timestamp FROM transacoes
           WHERE id_cliente = ? AND timestamp >= ? AND timestamp <= ? AND cidade <> ?
           ORDER BY timestamp DESC LIMIT 1""",
        (t["id_cliente"], inicio, t["timestamp"], t["cidade"]),
    ).fetchone()

    if linha:
        cidade_anterior, ts_anterior = linha
        return f"{cidade_anterior} ({ts_anterior}) -> {t['cidade']} em menos de {GEO_JANELA_SEG // 60} min"
    return None


def analisar(conn: sqlite3.Connection, t: dict) -> list[tuple[str, str]]:
    """Roda as 3 regras e devolve a lista de alertas [(tipo, detalhe), ...]."""
    alertas = []
    for tipo, resultado in (
        ("ALTO_VALOR", regra_alto_valor(t)),
        ("TEMPO_60s", regra_tempo(conn, t)),
        ("GEO_10m", regra_geo(conn, t)),
    ):
        if resultado:
            alertas.append((tipo, resultado))
    return alertas


# ----------------------------------------------------------------------
# PROGRAMA PRINCIPAL
# ----------------------------------------------------------------------
def criar_consumer() -> KafkaConsumer:
    try:
        return KafkaConsumer(
            TOPICO,
            bootstrap_servers=KAFKA_SERVIDOR,
            # group_id: o Kafka lembra até onde esse "grupo" já leu (offset).
            # Se você parar e voltar, ele continua de onde parou.
            group_id="detector-fraude",
            # Primeira vez no grupo: começa do início do tópico
            auto_offset_reset="earliest",
            # bytes -> JSON -> dicionário Python
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
    except NoBrokersAvailable:
        print("❌ Não consegui conectar no Kafka em", KAFKA_SERVIDOR)
        print("   O Docker está rodando? Execute: docker compose up -d")
        raise SystemExit(1)


def main():
    conn = conectar_banco()
    consumer = criar_consumer()
    print(f"✅ Consumer conectado. Aguardando mensagens do tópico '{TOPICO}'... (Ctrl+C para parar)\n")

    try:
        # Este for fica "travado" esperando mensagens novas chegarem
        for mensagem in consumer:
            t = mensagem.value

            # 1) Salva SEMPRE (fraude ou não)
            salvar_transacao(conn, t)

            # 2) Aplica as regras de fraude
            alertas = analisar(conn, t)

            # 3) Mostra no console
            resumo = (f"cliente={t['id_cliente']} valor=R$ {t['valor']:>9,.2f} "
                      f"cidade={t['cidade']:<15} {t['timestamp']}")
            if not alertas:
                print(f"{Fore.GREEN}✔ OK      | {resumo}{Style.RESET_ALL}")
            else:
                print(f"{Style.BRIGHT}🚨 SUSPEITA | {resumo}{Style.RESET_ALL}")
                for tipo, detalhe in alertas:
                    print(f"{CORES[tipo]}{Style.BRIGHT}   ⚠ ALERTA {tipo:<10} {detalhe}{Style.RESET_ALL}")
                    salvar_alerta(conn, t, tipo, detalhe)
    except KeyboardInterrupt:
        print("\n🛑 Consumer encerrado.")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
