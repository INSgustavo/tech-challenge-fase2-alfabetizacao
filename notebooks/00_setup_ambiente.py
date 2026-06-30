# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup do ambiente (P1)
# MAGIC Valida o ambiente, cria os schemas no Unity Catalog e os volumes para arquivos.
# MAGIC
# MAGIC **Catálogo:** workspace | **Schemas:** bronze · silver · gold

# COMMAND ----------
CATALOG = "workspace"

# Criar schemas no Unity Catalog
for camada in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{camada}")
    print(f"Schema criado: {CATALOG}.{camada}")

# COMMAND ----------
# Criar volume para arquivos brutos (landing zone do P2 e P3)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.raw_files")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.bronze.streaming_landing")

print(f"\nVolumes criados em {CATALOG}.bronze")
print(f"  - raw_files:         /Volumes/{CATALOG}/bronze/raw_files/")
print(f"  - streaming_landing: /Volumes/{CATALOG}/bronze/streaming_landing/")

# COMMAND ----------
# Validação
print(f"\nAmbiente OK: Spark {spark.version}")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
