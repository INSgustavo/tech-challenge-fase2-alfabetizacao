# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Serving NoSQL (P4): Gold -> MongoDB (um documento por município)

# COMMAND ----------
from pymongo import MongoClient
# TODO (P4): MONGO_URI via secret; conectar
rows = spark.table("gold.indicador_municipio").toPandas().to_dict("records")
# client = MongoClient(MONGO_URI); db = client["alfabetizacao"]
# db.indicador_municipio.delete_many({}); db.indicador_municipio.insert_many(rows)
print("Documentos a inserir:", len(rows))
