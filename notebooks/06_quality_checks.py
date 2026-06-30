# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Qualidade de dados (P4)
# MAGIC Duplicidade, nulos, chaves de relacionamento, consistência.

# COMMAND ----------
CATALOG = "workspace"

from pyspark.sql import functions as F

s = spark.table(f"{CATALOG}.silver.alfabetizacao")

# Duplicidade
total = s.count()
dedup = s.dropDuplicates(["ano", "sigla_uf", "rede"]).count()
assert total == dedup, f"ERRO: {total - dedup} duplicatas em (ano, sigla_uf, rede)"
print(f"✓ Sem duplicatas: {total:,} linhas")

# Nulos
nulos_portugues = s.filter(F.col("media_portugues").isNull()).count()
print(f"  nulos media_portugues: {nulos_portugues:,}")

nulos_alfabetizado = s.filter(F.col("alfabetizado").isNull()).count()
print(f"  nulos alfabetizado:    {nulos_alfabetizado:,}")

# TODO (P4): validar accepted_values de rede (0, 2, 3, 5)
# TODO (P4): integridade id_municipio vs workspace.bronze.municipio
