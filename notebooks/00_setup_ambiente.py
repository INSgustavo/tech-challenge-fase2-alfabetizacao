# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup do ambiente (P1)
# MAGIC Valida o cluster, cria os schemas Delta e os caminhos no DBFS.

# COMMAND ----------
BASE = "/FileStore/alfabetizacao"
for camada in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {camada}")
    dbutils.fs.mkdirs(f"{BASE}/{camada}")
print("Ambiente OK:", spark.version)
