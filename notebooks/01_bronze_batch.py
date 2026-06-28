# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Batch (P2)
# MAGIC Lê as fontes e grava em Delta (bruto, particionado). Bronze = sem transformação.

# COMMAND ----------
BASE = "/FileStore/alfabetizacao"

# TODO (P2): ler o CSV de avaliação subido no DBFS
df = (spark.read.option("header", True).option("inferSchema", True)
      .csv(f"{BASE}/raw/avaliacao_alfabetizacao.csv"))

(df.write.format("delta").mode("overwrite")
   .partitionBy("ano", "sigla_uf")
   .saveAsTable("bronze.avaliacao_alfabetizacao"))

# TODO (P2): repetir para uf, municipio, meta_brasil, meta_uf, meta_municipio
print("Bronze batch gravado.")
