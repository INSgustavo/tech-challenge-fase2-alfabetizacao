# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold
# MAGIC Cria marts analíticos após aprovação do Quality Gate.

# COMMAND ----------
CATALOG = "workspace"
SOURCE = f"{CATALOG}.silver.medicoes_alfabetizacao"

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.indicador_municipio AS
SELECT
    ano,
    sigla_uf,
    id_municipio,
    rede,
    rede_label,
    AVG(taxa_alfabetizacao) AS taxa_alfabetizacao_media,
    AVG(media_portugues) AS media_portugues,
    AVG(CASE WHEN alfabetizado THEN 1.0 WHEN alfabetizado = false THEN 0.0 END) AS pct_registros_alfabetizados,
    COUNT(*) AS quantidade_registros,
    MAX(processed_at) AS updated_at
FROM {SOURCE}
GROUP BY ano, sigla_uf, id_municipio, rede, rede_label
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.resumo_uf AS
SELECT
    ano,
    sigla_uf,
    rede,
    rede_label,
    AVG(taxa_alfabetizacao) AS taxa_alfabetizacao_media,
    COUNT(DISTINCT id_municipio) AS municipios_cobertos,
    MAX(processed_at) AS updated_at
FROM {SOURCE}
GROUP BY ano, sigla_uf, rede, rede_label
""")

print("Tabelas Gold criadas com grão documentado")
