# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Streaming (P3) — SIMULAÇÃO
# MAGIC Producer grava eventos numa landing; Structured Streaming lê e faz append no Bronze.
# MAGIC (Mesma API leria de Kafka em produção: `.format("kafka")`.)

# COMMAND ----------
BASE = "/FileStore/alfabetizacao"
LANDING = f"{BASE}/streaming_landing"

# --- Producer (simula "novas medições") ---
import json, time, random
# TODO (P3): gerar N eventos {ano, sigla_uf, id_municipio, taxa, ts} -> arquivos JSON na LANDING

# COMMAND ----------
# --- Consumer: Structured Streaming -> Bronze Delta ---
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
schema = (StructType()
          .add("ano", IntegerType()).add("sigla_uf", StringType())
          .add("id_municipio", StringType()).add("taxa", DoubleType()).add("ts", StringType()))

stream = (spark.readStream.schema(schema).json(LANDING))
(stream.writeStream.format("delta").outputMode("append")
   .option("checkpointLocation", f"{BASE}/_chk/streaming")
   .toTable("bronze.eventos_streaming"))
