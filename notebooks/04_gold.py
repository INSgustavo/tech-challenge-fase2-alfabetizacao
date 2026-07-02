# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold (P4)
# MAGIC Marts analíticos, criados **após** aprovação do Quality Gate.
# MAGIC
# MAGIC A fonte batch tem grão UF; o grão município vem do streaming (e das metas
# MAGIC municipais quando P2 disponibilizar). Cada mart declara o grão no comentário.

# COMMAND ----------
CATALOG = "workspace"
SOURCE = f"{CATALOG}.silver.medicoes_alfabetizacao"

# COMMAND ----------
# MAGIC %md
# MAGIC ## resumo_uf — grão: ano × UF × rede (fonte batch SAEB)

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.resumo_uf
COMMENT 'Grão: ano x sigla_uf x rede. Indicador Criança Alfabetizada por UF (taxa em fração 0-1).' AS
SELECT
    ano,
    sigla_uf,
    rede,
    rede_label,
    AVG(taxa_alfabetizacao) AS taxa_alfabetizacao,
    AVG(media_portugues) AS media_portugues,
    MAX(CASE WHEN media_atinge_corte THEN 1 ELSE 0 END) = 1 AS media_atinge_corte,
    COUNT(*) AS quantidade_registros,
    MAX(processed_at) AS updated_at
FROM {SOURCE}
WHERE grao = 'uf'
GROUP BY ano, sigla_uf, rede, rede_label
""")
print("✓ gold.resumo_uf")

# COMMAND ----------
# MAGIC %md
# MAGIC ## evolucao_temporal — grão: UF × rede, com variação ano a ano

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.evolucao_temporal
COMMENT 'Grão: sigla_uf x rede x ano. Evolução do indicador com variação vs ano anterior.' AS
SELECT
    sigla_uf,
    rede,
    rede_label,
    ano,
    taxa_alfabetizacao,
    LAG(taxa_alfabetizacao) OVER (PARTITION BY sigla_uf, rede ORDER BY ano) AS taxa_ano_anterior,
    taxa_alfabetizacao
      - LAG(taxa_alfabetizacao) OVER (PARTITION BY sigla_uf, rede ORDER BY ano) AS variacao,
    updated_at
FROM {CATALOG}.gold.resumo_uf
""")
print("✓ gold.evolucao_temporal")

# COMMAND ----------
# MAGIC %md
# MAGIC ## indicador_municipio — grão: ano × município × rede (medições de streaming)

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.indicador_municipio
COMMENT 'Grão: ano x id_municipio x rede. Medições municipais recebidas via streaming.' AS
SELECT
    ano,
    sigla_uf,
    id_municipio,
    rede,
    rede_label,
    AVG(taxa_alfabetizacao) AS taxa_alfabetizacao,
    COUNT(*) AS quantidade_registros,
    MAX(processed_at) AS updated_at
FROM {SOURCE}
WHERE grao = 'municipio' AND id_municipio IS NOT NULL
GROUP BY ano, sigla_uf, id_municipio, rede, rede_label
""")
print("✓ gold.indicador_municipio")

# COMMAND ----------
# MAGIC %md
# MAGIC ## meta_vs_resultado — depende das metas (P2)

# COMMAND ----------
if spark.catalog.tableExists(f"{CATALOG}.bronze.meta_uf"):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.meta_vs_resultado
    COMMENT 'Grão: ano x sigla_uf x rede. Comparação meta x resultado observado.' AS
    SELECT
        r.ano,
        r.sigla_uf,
        r.rede,
        r.rede_label,
        r.taxa_alfabetizacao,
        m.meta AS meta,
        r.taxa_alfabetizacao - m.meta AS gap_vs_meta,
        r.taxa_alfabetizacao >= m.meta AS meta_atingida,
        r.updated_at
    FROM {CATALOG}.gold.resumo_uf r
    JOIN {CATALOG}.bronze.meta_uf m
      ON r.sigla_uf = m.sigla_uf AND r.ano = m.ano
    """)
    print("✓ gold.meta_vs_resultado")
else:
    print("⚠ gold.meta_vs_resultado adiada: bronze.meta_uf ainda não carregada (P2). "
          "Ajustar nomes de colunas da meta ao carregar a fonte real.")

print("Tabelas Gold criadas com grão documentado")
