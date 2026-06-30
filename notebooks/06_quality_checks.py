# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Quality Gate
# MAGIC Valida a Silver antes da publicação da Gold.

# COMMAND ----------
CATALOG = "workspace"
from pyspark.sql import functions as F

s = spark.table(f"{CATALOG}.silver.medicoes_alfabetizacao")

# COMMAND ----------
checks = {
    "silver_not_empty": s.limit(1).count() == 1,
    "record_id_not_null": s.filter(F.col("record_id").isNull()).count() == 0,
    "record_id_unique": s.count() == s.select("record_id").distinct().count(),
    "uf_format": s.filter(~F.col("sigla_uf").rlike("^[A-Z]{2}$")).count() == 0,
    "municipio_format": s.filter(~F.col("id_municipio").rlike("^[0-9]{7}$")).count() == 0,
    "rede_domain": s.filter(~F.col("rede").isin([0, 2, 3, 5])).count() == 0,
    "taxa_domain": s.filter(F.col("taxa_alfabetizacao").isNotNull() & ~F.col("taxa_alfabetizacao").between(0.0, 1.0)).count() == 0,
}

for name, passed in checks.items():
    print(f"{'✓' if passed else '✗'} {name}")

failed = [name for name, passed in checks.items() if not passed]
if failed:
    raise AssertionError(f"Quality Gate reprovado: {', '.join(failed)}")

print("Quality Gate aprovado")
