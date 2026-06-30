# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Monitoramento (P3)
# MAGIC Falhas de ingestão, latência, volume processado, alertas.

# COMMAND ----------
CATALOG = "workspace"

# TODO (P3): coletar métricas do streaming (StreamingQuery.lastProgress)
# Exemplo:
# query = spark.streams.active[0]
# progress = query.lastProgress
# print("inputRowsPerSecond:", progress["inputRowsPerSecond"])
# print("processedRowsPerSecond:", progress["processedRowsPerSecond"])
# print("latência (ms):", progress["durationMs"])

# COMMAND ----------
# Monitoramento das tabelas Delta
tabelas = [
    f"{CATALOG}.bronze.avaliacao_alfabetizacao",
    f"{CATALOG}.bronze.eventos_streaming",
    f"{CATALOG}.silver.alfabetizacao",
    f"{CATALOG}.gold.indicador_municipio",
]
for t in tabelas:
    try:
        n = spark.table(t).count()
        print(f"✓ {t.split('.')[-1]}: {n:,} linhas")
    except Exception as e:
        print(f"✗ {t.split('.')[-1]}: não disponível ({e})")
