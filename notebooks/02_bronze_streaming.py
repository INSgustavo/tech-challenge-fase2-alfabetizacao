# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Streaming (P3)
# MAGIC Producer simulado em JSON + consumer Structured Streaming (`AvailableNow`).
# MAGIC O consumer **valida o contrato**, envia payload inválido para a quarentena
# MAGIC e **deduplica por `event_id`** (idempotência garantida com MERGE).

# COMMAND ----------
import json
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
)

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/bronze/streaming_landing/"
CHECKPOINT = f"/Volumes/{CATALOG}/observability/checkpoints/bronze_streaming/"
TARGET = f"{CATALOG}.bronze.eventos_streaming"
QUARANTINE = f"{CATALOG}.observability.quarantine_records"
RUN_ID = str(uuid.uuid4())

UFS = ["AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
       "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"]

SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("schema_version", StringType(), True),
    StructField("ano", IntegerType(), True),
    StructField("sigla_uf", StringType(), True),
    StructField("id_municipio", StringType(), True),
    StructField("rede", IntegerType(), True),
    StructField("taxa_alfabetizacao", DoubleType(), True),
    StructField("source", StringType(), True),
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Producer de demonstração
# MAGIC Publica 3 eventos que exercitam o pipeline: **válido**, **duplicado** (mesmo
# MAGIC `event_id` → deve ser ignorado) e **inválido** (taxa > 1 → deve cair na quarentena).
# MAGIC Em produção, substituir por Kafka / Event Hubs.

# COMMAND ----------
def novo_evento(**overrides):
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
    evento.update(overrides)
    return evento

ev_valido = novo_evento()
ev_duplicado = dict(ev_valido)  # mesmo event_id: dedup deve descartar
ev_invalido = novo_evento(taxa_alfabetizacao=1.25, sigla_uf="XX")  # quarentena

for sufixo, ev in [("a", ev_valido), ("b", ev_duplicado), ("c", ev_invalido)]:
    dbutils.fs.put(f"{LANDING}event-{ev['event_id']}-{sufixo}.json", json.dumps(ev), overwrite=True)
    print(f"→ publicado ({sufixo}):", ev["event_id"], "taxa:", ev["taxa_alfabetizacao"])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Consumer com validação de contrato + quarentena + dedup

# COMMAND ----------
regras_validade = (
    F.col("event_id").isNotNull()
    & F.col("event_time").isNotNull()
    & F.col("ano").between(2000, 2100)
    & F.col("sigla_uf").isin(UFS)
    & F.col("id_municipio").rlike("^[0-9]{7}$")
    & F.col("rede").isin(0, 2, 3, 5)
    & F.col("taxa_alfabetizacao").between(0.0, 1.0)
)

def process_batch(df, batch_id):
    df = df.withColumn("_ingestion_timestamp", F.current_timestamp()) \
           .withColumn("_source_file", F.col("_metadata.file_path")) \
           .withColumn("_pipeline_run_id", F.lit(RUN_ID)) \
           .dropDuplicates(["event_id"])

    invalidos = df.filter(~regras_validade)
    validos = df.filter(regras_validade)

    if invalidos.limit(1).count() > 0:
        (invalidos
         .select(
             F.lit(RUN_ID).alias("run_id"),
             F.lit("02_bronze_streaming").alias("task_name"),
             F.lit("contrato_violado").alias("rejection_reason"),
             F.to_json(F.struct(*SCHEMA.fieldNames())).alias("payload"),
             F.col("_ingestion_timestamp").alias("ingestion_timestamp"),
         )
         .write.mode("append").saveAsTable(QUARANTINE))

    if not spark.catalog.tableExists(TARGET):
        validos.write.format("delta").saveAsTable(TARGET)
    else:
        from delta.tables import DeltaTable
        (DeltaTable.forName(spark, TARGET).alias("t")
         .merge(validos.alias("s"), "t.event_id = s.event_id")
         .whenNotMatchedInsertAll()
         .execute())

query = (
    spark.readStream
    .schema(SCHEMA)
    .json(LANDING)
    .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .foreachBatch(process_batch)
    .start()
)
query.awaitTermination()

# COMMAND ----------
total = spark.table(TARGET).count() if spark.catalog.tableExists(TARGET) else 0
quar = spark.table(QUARANTINE).filter(F.col("run_id") == RUN_ID).count()
print(f"✓ eventos válidos acumulados em {TARGET}: {total}")
print(f"✓ eventos deste run enviados à quarentena: {quar}")
