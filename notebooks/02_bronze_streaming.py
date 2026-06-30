# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Streaming (P3) — SIMULAÇÃO
# MAGIC Producer grava eventos na landing zone (Unity Catalog Volume).
# MAGIC Structured Streaming lê e faz append no Bronze Delta.
# MAGIC (Mesma API leria de Kafka em produção: `.format("kafka")`.)

# COMMAND ----------
CATALOG = "workspace"
LANDING  = f"/Volumes/{CATALOG}/bronze/streaming_landing"
CHK_PATH = f"/Volumes/{CATALOG}/bronze/streaming_landing/_checkpoints"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Producer — simula novas medições

# COMMAND ----------
import json, random
from datetime import datetime
from pyspark.sql import functions as F

# TODO (P3): gerar N eventos e salvar como JSON na LANDING
# Exemplo de evento:
evento_exemplo = {
    "ano": 2024,
    "sigla_uf": "SP",
    "id_municipio": "3550308",
    "taxa_alfabetizacao": round(random.uniform(0.70, 0.99), 4),
    "ts": datetime.utcnow().isoformat()
}
print("Exemplo de evento:", json.dumps(evento_exemplo, indent=2))
print(f"Landing zone: {LANDING}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Consumer — Structured Streaming → Bronze Delta

# COMMAND ----------
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

schema = (StructType()
    .add("ano", IntegerType())
    .add("sigla_uf", StringType())
    .add("id_municipio", StringType())
    .add("taxa_alfabetizacao", DoubleType())
    .add("ts", StringType()))

stream = (spark.readStream
    .schema(schema)
    .json(LANDING))

# TODO (P3): adicionar métricas (foreachBatch) antes de escrever
(stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHK_PATH)
    .toTable(f"{CATALOG}.bronze.eventos_streaming"))
