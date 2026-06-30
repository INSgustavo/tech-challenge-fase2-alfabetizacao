# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Serving NoSQL (P4): Gold → MongoDB

# COMMAND ----------
CATALOG = "workspace"

# TODO (P4): configurar MONGO_URI via Databricks Secret
# dbutils.secrets.get(scope="alfabetizacao", key="mongo_uri")

from pymongo import MongoClient

rows = spark.table(f"{CATALOG}.gold.indicador_municipio").toPandas().to_dict("records")

# client = MongoClient(MONGO_URI)
# db = client["alfabetizacao"]
# db.indicador_municipio.delete_many({})
# db.indicador_municipio.insert_many(rows)

print("Documentos a inserir:", len(rows))
