# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver canônica (P4)
# MAGIC Normaliza chaves, escalas e integra medições batch e streaming.
# MAGIC
# MAGIC **Decisões desta camada (ver CONTRACT.md):**
# MAGIC - A fonte batch (avaliação SAEB) tem **grão UF** — `id_municipio` é nulo para batch
# MAGIC   e a coluna `grao` identifica o nível territorial de cada registro;
# MAGIC - `taxa_alfabetizacao` chega em **percentual (0–100)** no batch e em **fração (0–1)**
# MAGIC   no streaming → aqui tudo é normalizado para **0–1**;
# MAGIC - `taxa_alfabetizacao` **já é** o Indicador Criança Alfabetizada (% de alunos ≥ 743).
# MAGIC   O corte 743 é por aluno; aplicado à média agregada vira apenas um sinal auxiliar,
# MAGIC   por isso a coluna se chama `media_atinge_corte` (e não "alfabetizado").

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"
RULE_VERSION = "1.1"

REDE_MAP = {0: "total", 2: "estadual", 3: "municipal", 5: "privada"}
rede_mapping = F.create_map([F.lit(x) for pair in REDE_MAP.items() for x in pair])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Batch (grão UF)

# COMMAND ----------
batch = spark.table(f"{CATALOG}.bronze.avaliacao_alfabetizacao")

batch_canonical = (
    batch
    .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    .withColumn("id_municipio", F.lit(None).cast("string"))  # fonte tem grão UF
    .withColumn("grao", F.lit("uf"))
    .withColumn("serie", F.col("serie").cast("int"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("rede_label", rede_mapping[F.col("rede")])
    # normalização de escala: percentual (0–100) → fração (0–1)
    .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double") / 100.0)
    .withColumn("media_portugues", F.col("media_portugues").cast("double"))
    .withColumn("media_atinge_corte",
                F.when(F.col("media_portugues").isNotNull(), F.col("media_portugues") >= 743))
    .withColumn("event_id", F.lit(None).cast("string"))
    .withColumn("event_time", F.lit(None).cast("timestamp"))
    .withColumn("source", F.lit("batch_inep"))
    .withColumn("schema_version", F.lit("1.0"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming (grão município)

# COMMAND ----------
columns = [
    "ano", "sigla_uf", "id_municipio", "grao", "serie", "rede", "rede_label",
    "media_portugues", "taxa_alfabetizacao", "media_atinge_corte",
    "event_id", "event_time", "source", "schema_version",
]

if spark.catalog.tableExists(f"{CATALOG}.bronze.eventos_streaming"):
    events = spark.table(f"{CATALOG}.bronze.eventos_streaming")
    stream_canonical = (
        events
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .withColumn("grao", F.lit("municipio"))
        .withColumn("serie", F.lit(None).cast("int"))
        .withColumn("rede", F.col("rede").cast("int"))
        .withColumn("rede_label", rede_mapping[F.col("rede")])
        .withColumn("media_portugues", F.lit(None).cast("double"))
        .withColumn("media_atinge_corte", F.lit(None).cast("boolean"))
        # streaming já chega em 0–1 (contrato do evento)
        .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double"))
        .select(*columns)
    )
else:
    stream_canonical = spark.createDataFrame([], batch_canonical.select(*columns).schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## União + deduplicação por chave de negócio
# MAGIC `record_id` é o hash da **chave de negócio** (sem `event_id`): se a mesma medição
# MAGIC chegar duas vezes por eventos diferentes, vence a de `event_time` mais recente.

# COMMAND ----------
business_key = ["ano", "sigla_uf", "id_municipio", "serie", "rede", "source"]
w = Window.partitionBy(*business_key).orderBy(F.col("event_time").desc_nulls_last())

silver = (
    batch_canonical.select(*columns)
    .unionByName(stream_canonical, allowMissingColumns=True)
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn(
        "record_id",
        F.sha2(F.concat_ws("|",
            F.col("ano"), F.col("sigla_uf"),
            F.coalesce(F.col("id_municipio"), F.lit("uf")),
            F.coalesce(F.col("serie").cast("string"), F.lit("na")),
            F.col("rede"), F.col("source")), 256),
    )
    .withColumn("alfabetizacao_rule_version", F.lit(RULE_VERSION))
    .withColumn("processed_at", F.current_timestamp())
)

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.medicoes_alfabetizacao")
)

print(f"Silver gravada com {silver.count():,} registros")
