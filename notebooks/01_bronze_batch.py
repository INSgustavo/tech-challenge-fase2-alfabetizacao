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
df_avaliacao = (spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{VOLUME_RAW}/br_inep_avaliacao_alfabetizacao_uf.csv.gz"))

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
# Tabelas de P2 — carregadas quando os arquivos estiverem disponíveis
arquivos_p2 = {
    "uf":             (f"{VOLUME_RAW}/uf.csv",             {}),
    "municipio":      (f"{VOLUME_RAW}/municipio.csv",      {}),
    "meta_brasil":    (f"{VOLUME_RAW}/meta_brasil.csv",    {}),
    "meta_uf":        (f"{VOLUME_RAW}/meta_uf.csv",        {"partitionBy": "sigla_uf"}),
    "meta_municipio": (f"{VOLUME_RAW}/meta_municipio.csv", {"partitionBy": "sigla_uf"}),
}

for tabela, (path, opts) in arquivos_p2.items():
    try:
        df = (spark.read.option("header", True).option("inferSchema", True).csv(path))
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
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
tabelas = ["avaliacao_alfabetizacao", "uf", "municipio", "meta_brasil", "meta_uf", "meta_municipio"]
for t in tabelas:
    try:
        n = spark.table(f"{CATALOG}.bronze.{t}").count()
        print(f"✓ {t}: {n:,} linhas")
    except Exception as e:
        print(f"✗ {t}: {e}")
