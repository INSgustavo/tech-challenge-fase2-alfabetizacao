# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Streaming
# MAGIC Simula um producer em JSON e processa os eventos com Structured Streaming.

# COMMAND ----------
CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/bronze/streaming_landing/"
CHECKPOINT = f"/Volumes/{CATALOG}/observability/checkpoints/bronze_streaming/"
TARGET = f"{CATALOG}.bronze.eventos_streaming"

# COMMAND ----------
import json
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)

SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("schema_version", StringType(), False),
    StructField("ano", IntegerType(), False),
    StructField("sigla_uf", StringType(), False),
    StructField("id_municipio", StringType(), False),
    StructField("rede", IntegerType(), False),
    StructField("taxa_alfabetizacao", DoubleType(), False),
    StructField("source", StringType(), False),
])

# COMMAND ----------
# Producer de demonstração. Em produção, substituir por Kafka/Event Hubs.
evento = {
    "event_id": str(uuid.uuid4()),
    "event_time": datetime.now(timezone.utc).isoformat(),
    "schema_version": "1.0",
    "ano": 2025,
    "sigla_uf": "SP",
    "id_municipio": "3550308",
    "rede": 3,
    "taxa_alfabetizacao": 0.8125,
    "source": "simulador_medicoes",
}

dbutils.fs.put(
    f"{LANDING}event-{evento['event_id']}.json",
    json.dumps(evento),
    overwrite=False,
)
print(json.dumps(evento, indent=2, ensure_ascii=False))

# COMMAND ----------
stream = (
    spark.readStream
    .schema(SCHEMA)
    .json(LANDING)
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_payload_hash", F.sha2(F.to_json(F.struct("*")), 256))
)

query = (
    stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TARGET)
)

query.awaitTermination()
print(f"Eventos processados em {TARGET}")
