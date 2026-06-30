# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver canônica
# MAGIC Normaliza chaves e integra medições batch e streaming.

# COMMAND ----------
CATALOG = "workspace"
from pyspark.sql import functions as F

REDE_MAP = {0: "total", 2: "estadual", 3: "municipal", 5: "privada"}
rede_mapping = F.create_map([F.lit(x) for pair in REDE_MAP.items() for x in pair])

# COMMAND ----------
batch = spark.table(f"{CATALOG}.bronze.avaliacao_alfabetizacao")
batch_cols = set(batch.columns)

def _col(name, cast_to):
    """Usa a coluna se ela existir na fonte; caso contrario devolve null tipado.
    O arquivo batch (_uf) nao tem id_municipio e pode nao ter taxa_alfabetizacao."""
    return F.col(name).cast(cast_to) if name in batch_cols else F.lit(None).cast(cast_to)

batch_canonical = (
    batch
    .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    .withColumn("id_municipio", F.lpad(_col("id_municipio", "string"), 7, "0"))
    .withColumn("serie", _col("serie", "string"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("rede_label", rede_mapping[F.col("rede")])
    .withColumn("taxa_alfabetizacao", _col("taxa_alfabetizacao", "double"))
    .withColumn("media_portugues", _col("media_portugues", "double"))
    .withColumn("alfabetizado", F.when(F.col("media_portugues").isNotNull(), F.col("media_portugues") >= 743))
    .withColumn("event_id", F.lit(None).cast("string"))
    .withColumn("event_time", F.lit(None).cast("timestamp"))
    .withColumn("source", F.lit("batch_inep"))
    .withColumn("schema_version", F.lit("1.0"))
)

# COMMAND ----------
if spark.catalog.tableExists(f"{CATALOG}.bronze.eventos_streaming"):
    events = spark.table(f"{CATALOG}.bronze.eventos_streaming")
    stream_canonical = (
        events
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .withColumn("rede", F.col("rede").cast("int"))
        .withColumn("rede_label", rede_mapping[F.col("rede")])
        .withColumn("media_portugues", F.lit(None).cast("double"))
        .withColumn("alfabetizado", F.lit(None).cast("boolean"))
        .withColumn("serie", F.lit(None).cast("string"))
    )
else:
    stream_canonical = spark.createDataFrame([], batch_canonical.schema)

# COMMAND ----------
columns = [
    "ano", "sigla_uf", "id_municipio", "serie", "rede", "rede_label",
    "media_portugues", "taxa_alfabetizacao", "alfabetizado",
    "event_id", "event_time", "source", "schema_version",
]

silver = (
    batch_canonical.select(*columns)
    .unionByName(stream_canonical.select(*columns), allowMissingColumns=True)
    .withColumn(
        "record_id",
        F.sha2(F.concat_ws("|", "ano", "sigla_uf", "id_municipio", "serie", "rede", "source", F.coalesce("event_id", F.lit("batch"))), 256),
    )
    .withColumn("processed_at", F.current_timestamp())
    .dropDuplicates(["record_id"])
)

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.silver.medicoes_alfabetizacao")
)

print(f"Silver gravada com {silver.count():,} registros")
