# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold (P4) — marts com Spark SQL

# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.indicador_municipio AS
# MAGIC SELECT ano, sigla_uf, /* id_municipio, */ rede,
# MAGIC        AVG(taxa_alfabetizacao) AS taxa_media,
# MAGIC        AVG(CASE WHEN alfabetizado THEN 1 ELSE 0 END) AS pct_alfabetizado
# MAGIC FROM silver.alfabetizacao
# MAGIC GROUP BY ano, sigla_uf, rede;
# COMMAND ----------
# TODO (P4): gold.meta_vs_resultado  e  gold.evolucao_temporal (2023->2025)
