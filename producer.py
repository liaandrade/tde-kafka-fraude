"""
PRODUCER - gera transações aleatórias e envia para o Kafka.

Uso:
    python producer.py                 # 1 transação a cada 3 segundos (padrão do trabalho)
    python producer.py --intervalo 1   # mais rápido (bom para testar as regras)
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from config import KAFKA_SERVIDOR, TOPICO

# Poucos clientes de propósito: assim o MESMO cliente aparece várias vezes
# e as regras de histórico (TEMPO_60s e GEO_10m) têm chance de disparar.
# Cada cliente tem uma "cidade de origem" onde ele compra na maior parte das vezes.
CLIENTES = {
    "C001": "São Paulo",
    "C002": "Rio de Janeiro",
    "C003": "Fortaleza",
    "C004": "Belo Horizonte",
    "C005": "Curitiba",
}
CIDADES = sorted(set(CLIENTES.values()) | {"Salvador", "Porto Alegre", "Recife"})


def gerar_transacao() -> dict:
    """Cria uma transação aleatória no formato JSON padrão do trabalho."""
    id_cliente = random.choice(list(CLIENTES))
    cidade_origem = CLIENTES[id_cliente]

    # 85% das vezes o cliente compra na cidade de origem; 15% em outra cidade (gera GEO_10m)
    if random.random() < 0.85:
        cidade = cidade_origem
    else:
        cidade = random.choice([c for c in CIDADES if c != cidade_origem])

    # ~8% das transações são de valor alto (>= 10.000); o resto é valor comum
    if random.random() < 0.08:
        valor = random.uniform(10_000, 25_000)
    else:
        valor = random.uniform(10, 2_500)

    return {
        "id_transacao": str(uuid.uuid4()),
        "id_cliente": id_cliente,
        "valor": round(valor, 2),
        # Horário em UTC, formato ISO 8601 (ex.: 2026-10-21T14:30:05+00:00)
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "cidade": cidade,
    }


def criar_producer() -> KafkaProducer:
    """Conecta no Kafka. Converte dicionário Python -> JSON -> bytes automaticamente."""
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_SERVIDOR,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            # A chave define a partição: mesmo cliente => mesma partição => ordem preservada
            key_serializer=lambda k: k.encode("utf-8"),
        )
    except NoBrokersAvailable:
        print("❌ Não consegui conectar no Kafka em", KAFKA_SERVIDOR)
        print("   O Docker está rodando? Execute: docker compose up -d")
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(description="Producer de transações")
    parser.add_argument("--intervalo", type=float, default=3.0,
                        help="segundos entre transações (padrão: 3)")
    args = parser.parse_args()

    producer = criar_producer()
    print(f"✅ Producer conectado. Enviando 1 transação a cada {args.intervalo}s "
          f"para o tópico '{TOPICO}'. (Ctrl+C para parar)\n")

    try:
        while True:
            transacao = gerar_transacao()

            # send() é assíncrono; .get() espera a confirmação do Kafka
            metadados = producer.send(
                TOPICO, key=transacao["id_cliente"], value=transacao
            ).get(timeout=10)

            print(f"📤 Enviada | cliente={transacao['id_cliente']} "
                  f"valor=R$ {transacao['valor']:>9,.2f} cidade={transacao['cidade']:<15} "
                  f"(partição {metadados.partition}, offset {metadados.offset})")

            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\n🛑 Producer encerrado.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
