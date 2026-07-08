# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Batch (P2)
# MAGIC Lê as fontes e grava em Delta (bruto, particionado). Bronze = sem transformação.
# MAGIC
# MAGIC ## Como subir os dados (P2)
# MAGIC 1. Menu lateral: **Catalog → workspace → bronze → raw_files**
# MAGIC 2. Clique em **Upload to this volume**
# MAGIC 3. Suba cada CSV e use o caminho `/Volumes/workspace/bronze/raw_files/nome_arquivo.csv`

# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)

import uuid

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit
)

RUN_ID = str(uuid.uuid4())

CATALOG = "workspace"
VOLUME_RAW = f"/Volumes/{CATALOG}/bronze/raw_files"

# Schema explícito da Avaliação de Alfabetização
schema_avaliacao = StructType([
    StructField("ano", IntegerType(), False),
    StructField("sigla_uf", StringType(), False),
    StructField("serie", IntegerType(), False),
    StructField("rede", IntegerType(), False),
    StructField("taxa_alfabetizacao", DoubleType(), True),
    StructField("media_portugues", DoubleType(), True),
    StructField("proporcao_aluno_nivel_0", DoubleType(), True),
    StructField("proporcao_aluno_nivel_1", DoubleType(), True),
    StructField("proporcao_aluno_nivel_2", DoubleType(), True),
    StructField("proporcao_aluno_nivel_3", DoubleType(), True),
    StructField("proporcao_aluno_nivel_4", DoubleType(), True),
    StructField("proporcao_aluno_nivel_5", DoubleType(), True),
    StructField("proporcao_aluno_nivel_6", DoubleType(), True),
    StructField("proporcao_aluno_nivel_7", DoubleType(), True),
    StructField("proporcao_aluno_nivel_8", DoubleType(), True)
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Avaliação Alfabetização (SAEB)

# COMMAND ----------
df_avaliacao = (
    spark.read
        .option("header", True)
        .schema(schema_avaliacao)
        .csv(f"{VOLUME_RAW}/br_inep_avaliacao_alfabetizacao_uf.csv.gz")
)

print("Schema aplicado com sucesso:")

# _metadata.file_path substitui input_file_name(), que não é suportado no
# compute serverless com Unity Catalog.
df_avaliacao = (
    df_avaliacao
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("source_system", lit("basedosdados_inep"))
        .withColumn("pipeline_run_id", lit(RUN_ID))
        .withColumn("schema_version", lit("1.0"))
)

df_avaliacao.printSchema()
(
    df_avaliacao.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        # FinOps: tabela de ~150 linhas — particionar só criaria overhead de
        # arquivos pequenos (ver seção FinOps do README).
        .saveAsTable(f"{CATALOG}.bronze.avaliacao_alfabetizacao")
)

origem = df_avaliacao.count()

print(f"✓ avaliacao_alfabetizacao gravada: {origem:,} linhas")

destino = spark.table(
    f"{CATALOG}.bronze.avaliacao_alfabetizacao"
).count()

print(f"Origem : {origem}")
print(f"Destino: {destino}")

if origem == destino:
    print("✓ Reconciliação realizada com sucesso.")
else:
    raise Exception(
        f"Falha na reconciliação: origem={origem}, destino={destino}"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Demais fontes (P2)

# COMMAND ----------
arquivos_p2 = {
    "uf":             (f"{VOLUME_RAW}/uf.csv",             {}),
    "municipio":      (f"{VOLUME_RAW}/municipio.csv",      {}),
    "meta_brasil":    (f"{VOLUME_RAW}/meta_brasil.csv",    {}),
    "meta_uf":        (f"{VOLUME_RAW}/meta_uf.csv",        {"partitionBy": "sigla_uf"}),
    "meta_municipio": (f"{VOLUME_RAW}/meta_municipio.csv", {"partitionBy": "sigla_uf"}),
    "alunos":         (f"{VOLUME_RAW}/alunos_simulados.csv.gz", {}),
}

for tabela, (path, opts) in arquivos_p2.items():
    try:
        df = (
            spark.read
                .option("header", True)
                .option("inferSchema", True)
                .csv(path)
        )

        writer = (
            df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
        )

        if "partitionBy" in opts:
            writer = writer.partitionBy(opts["partitionBy"])

        writer.saveAsTable(f"{CATALOG}.bronze.{tabela}")

        print(f"✓ {tabela}: {df.count():,} linhas")

    except Exception as e:
        print(f"⚠ {tabela}: arquivo não encontrado — aguardando P2 ({e})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------
tabelas = [
    "avaliacao_alfabetizacao",
    "uf",
    "municipio",
    "meta_brasil",
    "meta_uf",
    "meta_municipio"
]

for tabela in tabelas:
    try:
        quantidade = spark.table(f"{CATALOG}.bronze.{tabela}").count()
        print(f"✓ {tabela}: {quantidade:,} linhas")
    except Exception as e:
        print(f"✗ {tabela}: {e}")
