# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Batch (P2)
# MAGIC Lê as fontes e grava em Delta. Bronze = dado bruto, **sem regra de negócio**,
# MAGIC apenas metadados técnicos de auditoria (`_ingestion_timestamp`, `_source_file`, ...).
# MAGIC
# MAGIC ## Como subir os dados (P2)
# MAGIC 1. Menu lateral: **Catalog → workspace → bronze → raw_files**
# MAGIC 2. Clique em **Upload to this volume**
# MAGIC 3. Suba cada CSV e use o caminho `/Volumes/workspace/bronze/raw_files/nome_arquivo.csv`

# COMMAND ----------
import uuid
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType
)

CATALOG = "workspace"
VOLUME_RAW = f"/Volumes/{CATALOG}/bronze/raw_files"
RUN_ID = str(uuid.uuid4())
SCHEMA_VERSION = "1.1"


def com_metadados(df, source_system: str):
    """Acrescenta somente metadados técnicos exigidos pelo CONTRACT.md."""
    return (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_pipeline_run_id", F.lit(RUN_ID))
        .withColumn("_schema_version", F.lit(SCHEMA_VERSION))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Avaliação Alfabetização (SAEB / Base dos Dados — grão UF)
# MAGIC Schema **explícito** (sem `inferSchema`): tipagem é contrato, não adivinhação.
# MAGIC A fonte tem grão `ano × sigla_uf × serie × rede` — **não há `id_municipio`**.
# MAGIC `taxa_alfabetizacao` chega em percentual (0–100); a normalização para 0–1 acontece na Silver.

# COMMAND ----------
SCHEMA_AVALIACAO = StructType([
    StructField("ano", IntegerType(), True),
    StructField("sigla_uf", StringType(), True),
    StructField("serie", IntegerType(), True),
    StructField("rede", IntegerType(), True),
    StructField("taxa_alfabetizacao", DoubleType(), True),
    StructField("media_portugues", DoubleType(), True),
    *[StructField(f"proporcao_aluno_nivel_{i}", DoubleType(), True) for i in range(9)],
])

df_avaliacao = com_metadados(
    spark.read
    .option("header", True)
    .schema(SCHEMA_AVALIACAO)
    .csv(f"{VOLUME_RAW}/br_inep_avaliacao_alfabetizacao_uf.csv.gz"),
    source_system="basedosdados_inep_avaliacao",
)

# FinOps: tabela minúscula (~150 linhas) — particionar aqui só cria overhead de
# arquivos pequenos. Decisão consciente: SEM partitionBy (ver seção FinOps do README).
(df_avaliacao.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.avaliacao_alfabetizacao"))

print("✓ avaliacao_alfabetizacao gravada:", df_avaliacao.count(), "linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Demais fontes (UF, município, metas) — P2

# COMMAND ----------
# Carregadas quando os arquivos estiverem no volume. Enquanto o schema real
# não é validado, a leitura usa header + inferSchema APENAS aqui (uma vez),
# e o schema deve ser promovido para src/schemas.py assim que confirmado.
arquivos_p2 = {
    "uf":             f"{VOLUME_RAW}/uf.csv",
    "municipio":      f"{VOLUME_RAW}/municipio.csv",
    "meta_brasil":    f"{VOLUME_RAW}/meta_brasil.csv",
    "meta_uf":        f"{VOLUME_RAW}/meta_uf.csv",
    "meta_municipio": f"{VOLUME_RAW}/meta_municipio.csv",
}

for tabela, path in arquivos_p2.items():
    try:
        df = com_metadados(
            spark.read.option("header", True).option("inferSchema", True).csv(path),
            source_system=f"basedosdados_{tabela}",
        )
        (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(f"{CATALOG}.bronze.{tabela}"))
        print(f"✓ {tabela}: {df.count():,} linhas")
    except Exception as e:
        print(f"⚠ {tabela}: arquivo não encontrado — aguardando P2 ({type(e).__name__})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------
tabelas = ["avaliacao_alfabetizacao", "uf", "municipio", "meta_brasil", "meta_uf", "meta_municipio"]
for t in tabelas:
    try:
        n = spark.table(f"{CATALOG}.bronze.{t}").count()
        print(f"✓ {t}: {n:,} linhas")
    except Exception:
        print(f"✗ {t}: ainda não disponível")
