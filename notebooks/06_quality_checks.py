# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Qualidade de dados (P4)
# MAGIC Duplicidade, nulos, chaves de relacionamento, consistência.

# COMMAND ----------
from pyspark.sql import functions as F
s = spark.table("silver.alfabetizacao")

assert s.count() == s.dropDuplicates(["ano","sigla_uf","rede"]).count(), "duplicidade em chave"
nul = s.filter(F.col("media_portugues").isNull()).count()
print("nulos media_portugues:", nul)
# TODO (P4): validar accepted_values de rede; integridade id_municipio vs bronze.municipio
