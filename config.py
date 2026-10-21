"""
Configurações compartilhadas entre Producer e Consumer.
Ficam num arquivo único para você alterar em um só lugar.
"""

# Endereço do Kafka (o mesmo anunciado no docker-compose.yml)
KAFKA_SERVIDOR = "localhost:9092"

# Nome do tópico: pense nele como um "canal" / "fila" onde as transações trafegam
TOPICO = "transacoes"

# Arquivo do banco SQLite (é criado automaticamente pelo consumer)
ARQUIVO_BANCO = "transacoes.db"

# ---- Parâmetros das regras de fraude ----
LIMITE_ALTO_VALOR = 10_000      # valor >= 10.000  -> ALTO_VALOR
TEMPO_QTD_TRANSACOES = 4        # 4 transações...
TEMPO_JANELA_SEG = 60           # ...em menos de 60 segundos -> TEMPO_60s
GEO_JANELA_SEG = 10 * 60        # cidades diferentes em 10 minutos -> GEO_10m
