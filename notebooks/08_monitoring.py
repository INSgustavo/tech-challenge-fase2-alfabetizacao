# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Monitoramento e observabilidade (P3)
# MAGIC Consolida volume e disponibilidade das tabelas, **latência do streaming**
# MAGIC (percentis), **sistema de alertas** com thresholds e dashboard consolidado.
# MAGIC Tudo persistido em `observability.pipeline_metrics`.

# COMMAND ----------
import uuid
from datetime import datetime, timezone

from pyspark.sql import Row, functions as F

CATALOG = "workspace"
# run_id injetado pelo Workflow ({{job.run_id}}) para correlacionar as tasks.
try:
    RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
except Exception:
    RUN_ID = str(uuid.uuid4())
started_at = datetime.now(timezone.utc)

# Thresholds definidos pelo grupo
THRESHOLDS = {
    "max_latency_seconds": 300,
    "max_p95_latency_seconds": 180,
    "min_events_total": 1,
    "max_rejection_rate": 0.05,
    "max_gold_age_hours": 26,
}
alertas = []

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Volume e disponibilidade por tabela

# COMMAND ----------
TABELAS = [
    f"{CATALOG}.bronze.avaliacao_alfabetizacao",
    f"{CATALOG}.bronze.uf",
    f"{CATALOG}.bronze.municipio",
    f"{CATALOG}.bronze.meta_brasil",
    f"{CATALOG}.bronze.meta_uf",
    f"{CATALOG}.bronze.meta_municipio",
    f"{CATALOG}.bronze.alunos",
    f"{CATALOG}.bronze.eventos_streaming",
    f"{CATALOG}.silver.medicoes_alfabetizacao",
    f"{CATALOG}.silver.medicoes_aprovadas",
    f"{CATALOG}.gold.indicador_municipio",
    f"{CATALOG}.gold.resumo_uf",
    f"{CATALOG}.gold.meta_vs_resultado",
    f"{CATALOG}.gold.evolucao_temporal",
]

rows = []
for table in TABELAS:
    try:
        count = spark.table(table).count()
        status, error = "SUCCESS", None
        print(f"✓ {table}: {count:,}")
        if count == 0 and "meta" not in table:
            alertas.append(f"TABELA VAZIA: {table}")
    except Exception as exc:
        count, status, error = 0, "NOT_AVAILABLE", str(exc)[:500]
        print(f"✗ {table}")

    rows.append(Row(
        run_id=RUN_ID, task_name=f"monitor:{table}", status=status,
        started_at=started_at, finished_at=datetime.now(timezone.utc),
        rows_read=count, rows_written=count, rows_rejected=0,
        max_event_time=None, schema_version="1.0", error_message=error,
    ))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Latência do streaming — percentis (event_time → ingestão)

# COMMAND ----------
lat_stats = None
if spark.catalog.tableExists(f"{CATALOG}.bronze.eventos_streaming"):
    eventos = spark.table(f"{CATALOG}.bronze.eventos_streaming")
    latency_df = eventos.withColumn(
        "latency_seconds",
        F.unix_timestamp("_ingestion_timestamp") - F.unix_timestamp("event_time"),
    )
    lat_stats = latency_df.select(
        F.count("*").alias("total"),
        F.min("latency_seconds").alias("min"),
        F.avg("latency_seconds").alias("avg"),
        F.expr("percentile(latency_seconds, 0.5)").alias("p50"),
        F.expr("percentile(latency_seconds, 0.95)").alias("p95"),
        F.expr("percentile(latency_seconds, 0.99)").alias("p99"),
        F.max("latency_seconds").alias("max"),
        F.max("event_time").alias("ultimo_evento"),
    ).collect()[0]

    if lat_stats["total"] > 0 and lat_stats["avg"] is not None:
        print("=== LATÊNCIA DE PROCESSAMENTO ===")
        print(f"Eventos: {lat_stats['total']:,}")
        for k in ["min", "avg", "p50", "p95", "p99", "max"]:
            print(f"{k:>4}: {lat_stats[k]:.2f}s")

        if lat_stats["max"] > THRESHOLDS["max_latency_seconds"]:
            alertas.append(f"LATENCIA MAXIMA {lat_stats['max']:.0f}s > "
                           f"{THRESHOLDS['max_latency_seconds']}s")
        if lat_stats["p95"] > THRESHOLDS["max_p95_latency_seconds"]:
            alertas.append(f"P95 DE LATENCIA {lat_stats['p95']:.0f}s > "
                           f"{THRESHOLDS['max_p95_latency_seconds']}s")

        rows.append(Row(
            run_id=RUN_ID, task_name="monitor:latencia_streaming", status="SUCCESS",
            started_at=started_at, finished_at=datetime.now(timezone.utc),
            rows_read=lat_stats["total"], rows_written=0, rows_rejected=0,
            max_event_time=lat_stats["ultimo_evento"], schema_version="1.0",
            error_message=None,
        ))
    else:
        alertas.append("STREAMING SEM EVENTOS: bronze.eventos_streaming vazia")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Taxa de rejeição acumulada e frescor da Gold

# COMMAND ----------
n_quarentena = spark.table(f"{CATALOG}.observability.quarantine_records").count()
n_silver = spark.table(f"{CATALOG}.silver.medicoes_alfabetizacao").count()
taxa_rej = n_quarentena / max(n_quarentena + n_silver, 1)
print(f"Quarentena: {n_quarentena:,} · taxa de rejeição acumulada: {taxa_rej:.1%}")
if taxa_rej > THRESHOLDS["max_rejection_rate"]:
    alertas.append(f"REJEICAO ALTA: {taxa_rej:.1%} (limiar {THRESHOLDS['max_rejection_rate']:.0%})")

freshness = None
if spark.catalog.tableExists(f"{CATALOG}.gold.resumo_uf"):
    freshness = (spark.table(f"{CATALOG}.gold.resumo_uf")
                 .select(F.max("updated_at").alias("u")).collect()[0]["u"])
if freshness is None:
    alertas.append("GOLD VAZIA OU AUSENTE: resumo_uf sem updated_at")
else:
    idade_h = (datetime.now(timezone.utc)
               - freshness.replace(tzinfo=timezone.utc)).total_seconds() / 3600
    print(f"Gold atualizada há {idade_h:.1f}h")
    if idade_h > THRESHOLDS["max_gold_age_hours"]:
        alertas.append(f"GOLD DESATUALIZADA: {idade_h:.0f}h desde o último refresh")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Dashboard consolidado

# COMMAND ----------
print("═" * 70)
print("  PIPELINE ALFABETIZAÇÃO — DASHBOARD DE MÉTRICAS")
print("═" * 70)
print(f"run_id: {RUN_ID} · {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC\n")

if spark.catalog.tableExists(f"{CATALOG}.bronze.eventos_streaming"):
    eventos = spark.table(f"{CATALOG}.bronze.eventos_streaming")
    print("── VOLUME POR SOURCE ──")
    eventos.groupBy("source").count().orderBy(F.desc("count")).show(truncate=False)
    print("── TOP UFs POR VOLUME DE EVENTOS ──")
    eventos.groupBy("sigla_uf").count().orderBy(F.desc("count")).show(10, truncate=False)
    ultima_hora = eventos.filter(
        F.col("_ingestion_timestamp") >= F.expr("current_timestamp() - interval 1 hour")
    ).count()
    print(f"Throughput última hora: {ultima_hora} eventos "
          f"({ultima_hora / 60:.2f}/min)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Persistência e alertas

# COMMAND ----------
# Schema explícito: max_event_time/error_message podem ser None em todas as
# linhas e a inferência de tipos falharia (ValueError).
schema_metrics = spark.table(f"{CATALOG}.observability.pipeline_metrics").schema
spark.createDataFrame(rows, schema=schema_metrics).write.mode("append").saveAsTable(
    f"{CATALOG}.observability.pipeline_metrics"
)

print(f"✓ métricas persistidas (run_id={RUN_ID})")
if alertas:
    print("\n🚨 ALERTAS:")
    