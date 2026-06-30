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
CATALOG = "workspace"
VOLUME_RAW = f"/Volumes/{CATALOG}/bronze/raw_files"

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Avaliação Alfabetização (SAEB)

# COMMAND ----------
# TODO (P2): confirmar nome do arquivo após upload
df_avaliacao = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{VOLUME_RAW}/avaliacao_alfabetizacao.csv"))

(df_avaliacao.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("ano", "sigla_uf")
    .saveAsTable(f"{CATALOG}.bronze.avaliacao_alfabetizacao"))

print("✓ avaliacao_alfabetizacao gravada:", df_avaliacao.count(), "linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. UF

# COMMAND ----------
# TODO (P2): confirmar nome do arquivo após upload
df_uf = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{VOLUME_RAW}/uf.csv"))

(df_uf.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.uf"))

print("✓ uf gravada:", df_uf.count(), "linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Município

# COMMAND ----------
# TODO (P2): confirmar nome do arquivo após upload
df_municipio = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{VOLUME_RAW}/municipio.csv"))

(df_municipio.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.municipio"))

print("✓ municipio gravada:", df_municipio.count(), "linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Meta Brasil

# COMMAND ----------
# TODO (P2): confirmar nome do arquivo após upload
df_meta_brasil = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{VOLUME_RAW}/meta_brasil.csv"))

(df_meta_brasil.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.bronze.meta_brasil"))

print("✓ meta_brasil gravada:", df_meta_brasil.count(), "linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Meta UF

# COMMAND ----------
# TODO (P2): confirmar nome do arquivo após upload
df_meta_uf = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{VOLUME_RAW}/meta_uf.csv"))

(df_meta_uf.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sigla_uf")
    .saveAsTable(f"{CATALOG}.bronze.meta_uf"))

print("✓ meta_uf gravada:", df_meta_uf.count(), "linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Meta Município

# COMMAND ----------
# TODO (P2): confirmar nome do arquivo após upload
df_meta_municipio = (spark.read.option("header", True).option("inferSchema", True)
    .csv(f"{VOLUME_RAW}/meta_municipio.csv"))

(df_meta_municipio.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sigla_uf")
    .saveAsTable(f"{CATALOG}.bronze.meta_municipio"))

print("✓ meta_municipio gravada:", df_meta_municipio.count(), "linhas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------
tabelas = ["avaliacao_alfabetizacao", "uf", "municipio", "meta_brasil", "meta_uf", "meta_municipio"]
for t in tabelas:
    try:
        n = spark.table(f"{CATALOG}.bronze.{t}").count()
        print(f"✓ {t}: {n:,} linhas")
    except Exception as e:
        print(f"✗ {t}: {e}")
