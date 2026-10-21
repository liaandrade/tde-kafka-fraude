"""
Utilitário opcional: mostra o que foi salvo no SQLite (útil para provar no trabalho
que TODAS as transações foram gravadas).

Uso:  python consultar_banco.py
"""
import sqlite3

from config import ARQUIVO_BANCO

conn = sqlite3.connect(ARQUIVO_BANCO)

total = conn.execute("SELECT COUNT(*) FROM transacoes").fetchone()[0]
print(f"Total de transações salvas: {total}\n")

print("Alertas por tipo:")
for tipo, qtd in conn.execute("SELECT tipo, COUNT(*) FROM alertas GROUP BY tipo"):
    print(f"  {tipo:<12} {qtd}")

print("\nÚltimas 10 transações:")
for linha in conn.execute(
    "SELECT id_cliente, valor, cidade, timestamp FROM transacoes ORDER BY timestamp DESC LIMIT 10"
):
    print(f"  {linha[0]}  R$ {linha[1]:>9,.2f}  {linha[2]:<15} {linha[3]}")

print("\nÚltimos 10 alertas:")
for linha in conn.execute(
    "SELECT tipo, id_cliente, detalhe FROM alertas ORDER BY id DESC LIMIT 10"
):
    print(f"  [{linha[0]}] {linha[1]}: {linha[2]}")

conn.close()
