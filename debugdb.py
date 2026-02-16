"""
Script para diagnosticar problemas no banco de dados
"""
import sqlite3
import pandas as pd
from pathlib import Path

db_path = Path("data/database/covid19.db")

if not db_path.exists():
    print("❌ Banco de dados não encontrado!")
    exit(1)

conn = sqlite3.connect(db_path)

print("="*60)
print("DIAGNÓSTICO DO BANCO DE DADOS")
print("="*60)

# Verificar tabelas
print("\n📊 TABELAS:")
tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
print(tables)

# Contar registros
print("\n📈 CONTAGEM DE REGISTROS:")
for table in tables['name']:
    count = pd.read_sql(f"SELECT COUNT(*) as count FROM {table}", conn)
    print(f"  {table}: {count['count'].iloc[0]:,} registros")

# Verificar global_daily_stats
print("\n🌍 GLOBAL_DAILY_STATS (primeiros 5):")
try:
    global_stats = pd.read_sql("""
        SELECT * FROM global_daily_stats 
        ORDER BY date DESC 
        LIMIT 5
    """, conn)
    print(global_stats)
    
    # Verificar NaN
    print("\n❓ Valores NaN:")
    print(global_stats.isna().sum())
    
except Exception as e:
    print(f"❌ Erro: {e}")

# Verificar aggregated_stats
print("\n📊 AGGREGATED_STATS (primeiros 5):")
try:
    agg_stats = pd.read_sql("""
        SELECT * FROM aggregated_stats 
        ORDER BY total_cases DESC 
        LIMIT 5
    """, conn)
    print(agg_stats)
except Exception as e:
    print(f"❌ Erro: {e}")

conn.close()
print("\n" + "="*60)