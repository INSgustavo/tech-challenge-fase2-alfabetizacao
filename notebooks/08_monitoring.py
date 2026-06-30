# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Monitoramento
# MAGIC Consolida evidências de volume e disponibilidade das tabelas.

# COMMAND ----------
CATALOG = "workspace"
import uuid
from datetime import datetime, timezone
from pyspark.sql import Row

RUN_ID = str(uuid.uuid4())
started_at = datetime.now(timezone.utc)

tables = [
    f"{CATALOG}.bronze.avaliacao_alfabetizacao",
    f"{CATALOG}.bronze.eventos_streaming",
    f"{CATALOG}.silver.medicoes_alfabetizacao",
    f"{CATALOG}.gold.indicador_municipio",
    f"{CATALOG}.gold.resumo_uf",
]

rows = []
for table in tables:
    try:
        count = spark.table(table).count()
        status = "SUCCESS"
        error = None
        print(f"✓ {table}: {count:,}")
    except Exception as exc:
        count = 0
        status = "NOT_AVAILABLE"
        error = str(exc)[:1000]
        print(f"✗ {table}: {error}")

    rows.append(Row(
        run_id=RUN_ID,
        task_name=f"monitor:{table}",
        status=status,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc),
        rows_read=count,
        rows_written=count,
        rows_rejected=0,
        max_event_time=None,
        schema_version="1.0",
        error_message=error,
    ))

spark.createDataFrame(rows).write.mode("append").saveAsTable(
    f"{CATALOG}.observability.pipeline_metrics"
)
print(f"run_id: {RUN_ID}")
