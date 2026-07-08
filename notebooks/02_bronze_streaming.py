# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Streaming (P3)
# MAGIC Producer simulado em JSON + consumer Structured Streaming (`AvailableNow`).
# MAGIC O consumer **valida o contrato**, envia payload inválido para a quarentena
# MAGIC e **deduplica por `event_id`** via MERGE (idempotência real).

# COMMAND ----------
import json
import random
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
REDES = {0: "total", 2: "estadual", 3: "municipal", 5: "privada"}

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
# MAGIC ## Producer — gerador de eventos sintéticos
# MAGIC Municípios reais por UF (código IBGE); taxa varia por rede. Em produção,
# MAGIC substituir por Kafka / Event Hubs.

# COMMAND ----------
MUNICIPIOS_EXEMPLO = {
    "SP": ["3550308", "3509502", "3543402", "3518800"],
    "RJ": ["3304557", "3303500", "3301009", "3302270"],
    "MG": ["3106200", "3143302", "3118601", "3170206"],
    "BA": ["2927408", "2910800", "2919207", "2930709"],
    "PR": ["4106902", "4125506", "4115200", "4119905"],
}

# Faixas de taxa por rede (privada tende a taxas maiores)
FAIXA_TAXA = {0: (0.45, 0.85), 2: (0.40, 0.80), 3: (0.35, 0.78), 5: (0.60, 0.95)}


def gerar_evento(ano=None, uf=None, rede=None, **overrides):
    """Gera um evento sintético de medição de alfabetização."""
    ano = ano or random.choice([2024, 2025])
    uf = uf or random.choice(list(MUNICIPIOS_EXEMPLO.keys()))
    rede = rede if rede is not None else random.choice([2, 3, 5])
    faixa = FAIXA_TAXA[rede]
    evento = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
        "ano": ano,
        "sigla_uf": uf,
        "id_municipio": random.choice(MUNICIPIOS_EXEMPLO[uf]),
        "rede": rede,
        "taxa_alfabetizacao": round(random.uniform(*faixa), 4),
        "source": "simulador_medicoes",
    }
    evento.update(overrides)
    return evento


def publicar(evento, sufixo=""):
    dbutils.fs.put(f"{LANDING}event-{evento['event_id']}{sufixo}.json",
                   json.dumps(evento, ensure_ascii=False), overwrite=True)

# COMMAND ----------
# Cenário de demonstração: 8 eventos válidos + 1 duplicado + 1 inválido.
eventos = [gerar_evento() for _ in range(8)]
for ev in eventos:
    publicar(ev)

publicar(dict(eventos[0]), sufixo="-dup")                       # mesmo event_id → dedup descarta
publicar(gerar_evento(taxa_alfabetizacao=1.25, sigla_uf="XX"))  # inválido → quarentena

print(f"→ 10 eventos publicados em {LANDING} (1 duplicado + 1 inválido)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Consumer — validação de contrato + quarentena + dedup

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
    # Em serverless o foreachBatch roda em processo isolado: usar a sessão do
    # micro-batch, nunca o `spark` global do notebook.
    session = df.sparkSession
    df = (df
          .withColumn("_ingestion_timestamp", F.current_timestamp())
          .withColumn("_pipeline_run_id", F.lit(RUN_ID))
          .dropDuplicates(["event_id"]))

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

    if not session.catalog.tableExists(TARGET):
        validos.write.format("delta").saveAsTable(TARGET)
    else:
        from delta.tables import DeltaTable
        (DeltaTable.forName(session, TARGET).alias("t")
         .merge(validos.alias("s"), "t.event_id = s.event_id")
         .whenNotMatchedInsertAll()
         .execute())


query = (
    spark.readStream
    .schema(SCHEMA)
    .json(LANDING)
    # _metadata materializada no plano do stream (não dentro do foreachBatch)
    .select("*", F.col("_metadata.file_path").alias("_source_file"))
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
print(f"✓ eventos deste run na quarentena: {quar}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## (Opcional) Demo interativa — stream contínuo por 60s
# MAGIC Para a gravação do vídeo: descomente, rode esta célula e publique eventos
# MAGIC pelo producer em outra aba. Micro-batches a cada 10s; para sozinho.

# COMMAND ----------
# import time
# query = (
#     spark.readStream.schema(SCHEMA).json(LANDING)
#     .select("*", F.col("_metadata.file_path").alias("_source_file"))
#     .writeStream
#     .option("checkpointLocation", CHECKPOINT)
#     .trigger(processingTime="10 seconds")
#     .foreachBatch(process_batch)
#     .start()
# )
# for _ in range(6):
#     time.sleep(10)
#     print(f"[{datetime.now():%H:%M:%S}] eventos: {spark.table(TARGET).count():,}")
# query.stop()
