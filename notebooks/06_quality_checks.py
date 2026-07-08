# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Quality Gate
# MAGIC Valida a Silver **antes** da publicação da Gold (contrato, seção 8).
# MAGIC
# MAGIC O que este notebook faz:
# MAGIC 1. Aplica validações **por registro** e move os reprovados para
# MAGIC    `observability.quarantine_records` com `rejection_reason`.
# MAGIC 2. Publica os registros aprovados em `silver.medicoes_aprovadas`
# MAGIC    (fonte da Gold).
# MAGIC 3. Aplica validações **sistêmicas** (volume, cobertura, duplicidade).
# MAGIC    Se alguma falhar, a task **reprova** e a Gold não é sobrescrita.
# MAGIC 4. Registra a execução em `observability.pipeline_metrics`.

# COMMAND ----------
CATALOG = "workspace"
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

# run_id pode ser injetado pelo Workflow (widget); senão, gera um novo.
try:
    RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
except Exception:
    RUN_ID = str(uuid.uuid4())
TASK = "06_quality_gate"
started_at = datetime.now(timezone.utc)

SILVER = f"{CATALOG}.silver.medicoes_alfabetizacao"
APROVADA = f"{CATALOG}.silver.medicoes_aprovadas"

# Limite mínimo de cobertura definido pelo grupo (aprovados / lidos).
COBERTURA_MIN = 0.80

UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
]

s = spark.table(SILVER)
rows_read = s.count()
print(f"Silver lida: {rows_read:,} registros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Validações por registro → motivo de rejeição
# MAGIC A primeira regra violada define o `rejection_reason` do registro.

# COMMAND ----------
rejection_reason = (
    F.when(
        F.col("ano").isNull() | F.col("sigla_uf").isNull() | F.col("id_municipio").isNull()
        | F.col("rede").isNull() | F.col("record_id").isNull(),
        F.lit("campo_critico_nulo"),
    )
    .when(~F.col("sigla_uf").rlike("^[A-Z]{2}$") | ~F.col("sigla_uf").isin(UFS_VALIDAS),
          F.lit("sigla_uf_invalida"))
    .when(~F.col("id_municipio").rlike("^[0-9]{7}$"), F.lit("id_municipio_invalido"))
    .when(~F.col("rede").isin([0, 2, 3, 5]), F.lit("rede_fora_do_dominio"))
    .when(F.col("taxa_alfabetizacao").isNotNull()
          & ~F.col("taxa_alfabetizacao").between(0.0, 1.0), F.lit("taxa_fora_do_dominio"))
    .otherwise(F.lit(None))
)

marcada = s.withColumn("rejection_reason", rejection_reason)

invalidos = marcada.filter(F.col("rejection_reason").isNotNull())
aprovados = marcada.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")

rows_rejected = invalidos.count()
rows_written = aprovados.count()
print(f"Aprovados: {rows_written:,} | Reprovados (quarentena): {rows_rejected:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Quarentena dos registros reprovados
# MAGIC Payload original preservado em JSON (contrato, seção 5).

# COMMAND ----------
if rows_rejected > 0:
    payload_cols = [c for c in s.columns]
    quarentena = (
        invalidos
        .withColumn("run_id", F.lit(RUN_ID))
        .withColumn("task_name", F.lit(TASK))
        .withColumn("payload", F.to_json(F.struct(*payload_cols)))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select("run_id", "task_name", "rejection_reason", "payload", "ingestion_timestamp")
    )
    (quarentena.write.format("delta").mode("append")
        .saveAsTable(f"{CATALOG}.observability.quarantine_records"))
    print(f"→ {rows_rejected:,} registros enviados para observability.quarantine_records")

    # Distribuição dos motivos, útil na demo
    invalidos.groupBy("rejection_reason").count().orderBy(F.desc("count")).show(truncate=False)
else:
    print("Nenhum registro reprovado por regra de linha.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Publica a Silver aprovada (fonte da Gold)

# COMMAND ----------
(aprovados.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(APROVADA))
spark.sql(f"COMMENT ON TABLE {APROVADA} IS "
          f"'Silver aprovada pelo Quality Gate (06). Fonte da Gold. Responsável: P4.'")
print(f"✓ {APROVADA} publicada com {rows_written:,} registros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Validações sistêmicas (bloqueantes)
# MAGIC Falhas aqui reprovam a task — a Gold **não** deve ser publicada.

# COMMAND ----------
record_id_unico = rows_written == aprovados.select("record_id").distinct().count()
cobertura = (rows_written / rows_read) if rows_read else 0.0

checks_sistemicos = {
    "silver_nao_vazia": rows_read > 0,
    "aprovados_maior_que_zero": rows_written > 0,
    "record_id_unico_nos_aprovados": record_id_unico,
    f"cobertura_min_{COBERTURA_MIN:.0%}": cobertura >= COBERTURA_MIN,
}

for nome, ok in checks_sistemicos.items():
    print(f"{'✓' if ok else '✗'} {nome}")
print(f"Cobertura: {cobertura:.1%}")

reprovados = [nome for nome, ok in checks_sistemicos.items() if not ok]
status = "FAILED" if reprovados else "SUCCESS"
error_message = f"Quality Gate reprovado: {', '.join(reprovados)}" if reprovados else None

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Auditoria em observability.pipeline_metrics

# COMMAND ----------
from pyspark.sql import Row

metric = Row(
    run_id=RUN_ID,
    task_name=TASK,
    status=status,
    started_at=started_at,
    finished_at=datetime.now(timezone.utc),
    rows_read=int(rows_read),
    rows_written=int(rows_written),
    rows_rejected=int(rows_rejected),
    max_event_time=None,
    schema_version="1.0",
    error_message=error_message,
)
spark.createDataFrame([metric]).write.mode("append").saveAsTable(
    f"{CATALOG}.observability.pipeline_metrics")
print(f"Auditoria registrada. run_id={RUN_ID}")

# COMMAND ----------
if reprovados:
    raise AssertionError(error_message)
print("Quality Gate APROVADO — Gold liberada.")
