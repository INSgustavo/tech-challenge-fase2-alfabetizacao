# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup do ambiente
# MAGIC Cria schemas, volumes e estrutura de observabilidade.

# COMMAND ----------
CATALOG = "workspace"

for schema in ["bronze", "silver", "gold", "observability"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"Schema disponível: {CATALOG}.{schema}")

# COMMAND ----------
for schema, volume in [
    ("bronze", "raw_files"),
    ("bronze", "streaming_landing"),
    ("observability", "checkpoints"),
    ("observability", "quarantine"),
]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{schema}.{volume}")
    print(f"Volume disponível: /Volumes/{CATALOG}/{schema}/{volume}/")

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.observability.pipeline_metrics (
    run_id STRING,
    task_name STRING,
    status STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    rows_read BIGINT,
    rows_written BIGINT,
    rows_rejected BIGINT,
    max_event_time TIMESTAMP,
    schema_version STRING,
    error_message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.observability.quarantine_records (
    run_id STRING,
    task_name STRING,
    rejection_reason STRING,
    payload STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA
""")

print(f"Ambiente validado com Spark {spark.version}")
