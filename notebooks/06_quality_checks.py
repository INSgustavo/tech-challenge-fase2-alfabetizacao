# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Quality Gate (P4)
# MAGIC Valida a Silver **antes** da publicação da Gold.
# MAGIC
# MAGIC Estratégia em dois níveis:
# MAGIC 1. **Checks estruturais** (Silver vazia, `record_id` duplicado) → falham imediatamente;
# MAGIC 2. **Checks de domínio por registro** → registros reprovados vão para a **quarentena**
# MAGIC    com o motivo; o gate só falha se a taxa de rejeição passar do limiar (5%).

# COMMAND ----------
import uuid
from pyspark.sql import functions as F

CATALOG = "workspace"
QUARANTINE = f"{CATALOG}.observability.quarantine_records"
RUN_ID = str(uuid.uuid4())
LIMIAR_REJEICAO = 0.05

s = spark.table(f"{CATALOG}.silver.medicoes_alfabetizacao")
total = s.count()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Checks estruturais (hard fail)

# COMMAND ----------
estruturais = {
    "silver_not_empty": total > 0,
    "record_id_not_null": s.filter(F.col("record_id").isNull()).count() == 0,
    "record_id_unique": total == s.select("record_id").distinct().count(),
}
for name, passed in estruturais.items():
    print(f"{'✓' if passed else '✗'} {name}")

falhas_estruturais = [n for n, ok in estruturais.items() if not ok]
if falhas_estruturais:
    raise AssertionError(f"Quality Gate reprovado (estrutural): {', '.join(falhas_estruturais)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Checks de domínio por registro → quarentena

# COMMAND ----------
# id_municipio é validado apenas quando presente: a fonte batch tem grão UF (CONTRACT.md)
regras = {
    "uf_invalida": ~F.col("sigla_uf").rlike("^[A-Z]{2}$"),
    "municipio_invalido": F.col("id_municipio").isNotNull() & ~F.col("id_municipio").rlike("^[0-9]{7}$"),
    "rede_fora_dominio": ~F.col("rede").isin(0, 2, 3, 5),
    "taxa_fora_dominio": F.col("taxa_alfabetizacao").isNotNull()
                         & ~F.col("taxa_alfabetizacao").between(0.0, 1.0),
    "ano_fora_intervalo": ~F.col("ano").between(2000, 2100),
}

cond_reprovado = None
motivo = F.lit("")
for nome, cond in regras.items():
    cond_reprovado = cond if cond_reprovado is None else (cond_reprovado | cond)
    motivo = F.concat(motivo, F.when(cond, F.lit(nome + ";")).otherwise(F.lit("")))

reprovados = s.withColumn("rejection_reason", motivo).filter(cond_reprovado)
n_reprovados = reprovados.count()

if n_reprovados > 0:
    (reprovados
     .select(
         F.lit(RUN_ID).alias("run_id"),
         F.lit("06_quality_checks").alias("task_name"),
         F.col("rejection_reason"),
         F.to_json(F.struct("record_id", "ano", "sigla_uf", "id_municipio",
                            "rede", "taxa_alfabetizacao", "source")).alias("payload"),
         F.current_timestamp().alias("ingestion_timestamp"),
     )
     .write.mode("append").saveAsTable(QUARANTINE))

taxa_rejeicao = n_reprovados / total
print(f"Registros: {total:,} · reprovados: {n_reprovados:,} ({taxa_rejeicao:.1%}) · limiar: {LIMIAR_REJEICAO:.0%}")

# COMMAND ----------
if taxa_rejeicao > LIMIAR_REJEICAO:
    raise AssertionError(
        f"Quality Gate reprovado: rejeição de {taxa_rejeicao:.1%} acima do limiar. "
        f"Motivos em {QUARANTINE} (run_id={RUN_ID})."
    )

print(f"✓ Quality Gate aprovado (run_id={RUN_ID})")
