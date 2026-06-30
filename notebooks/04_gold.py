# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold (P4) — marts com Spark SQL

# COMMAND ----------
CATALOG = "workspace"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 1: Indicador por Município

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.indicador_municipio AS
SELECT
    ano,
    sigla_uf,
    rede,
    AVG(taxa_alfabetizacao)                          AS taxa_media,
    AVG(CASE WHEN alfabetizado THEN 1.0 ELSE 0.0 END) AS pct_alfabetizado
FROM {CATALOG}.silver.alfabetizacao
GROUP BY ano, sigla_uf, rede
""")
print("✓ gold.indicador_municipio criada")

# COMMAND ----------
# TODO (P4): gold.meta_vs_resultado — comparar taxa real vs meta INEP
# TODO (P4): gold.evolucao_temporal — série 2023→2025 por UF/rede
