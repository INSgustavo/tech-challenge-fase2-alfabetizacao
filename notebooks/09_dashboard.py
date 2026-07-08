# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Dashboard executivo (P4)
# MAGIC Notebook analítico sobre a Gold. Cada célula gera uma visualização com
# MAGIC `display()`; no Databricks, use **"+ Add to dashboard"** em cada gráfico
# MAGIC para montar o painel executivo apresentado no vídeo.

# COMMAND ----------
CATALOG = "workspace"
from pyspark.sql import functions as F

# COMMAND ----------
# MAGIC %md
# MAGIC ## KPIs principais

# COMMAND ----------
ind = spark.table(f"{CATALOG}.gold.indicador_municipio")

kpis = ind.agg(
    F.countDistinct("id_municipio").alias("municipios_cobertos"),
    F.countDistinct("sigla_uf").alias("ufs_cobertas"),
    F.round(F.avg("taxa_alfabetizacao_media"), 4).alias("taxa_media_geral"),
    F.min("ano").alias("ano_min"),
    F.max("ano").alias("ano_max"),
)
display(kpis)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Taxa média de alfabetização por UF (rede total)
# MAGIC Sugestão de visualização: **gráfico de barras** ordenado.

# COMMAND ----------
por_uf = spark.sql(f"""
SELECT sigla_uf,
       ROUND(AVG(taxa_alfabetizacao_media), 4) AS taxa_media
FROM {CATALOG}.gold.indicador_municipio
WHERE rede = 0
GROUP BY sigla_uf
ORDER BY taxa_media DESC
""")
display(por_uf)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Evolução temporal nacional
# MAGIC Sugestão: **gráfico de linha** (eixo X = ano).

# COMMAND ----------
evolucao = spark.sql(f"""
SELECT ano,
       ROUND(AVG(taxa_alfabetizacao_media), 4) AS taxa_media_nacional
FROM {CATALOG}.gold.evolucao_temporal
WHERE rede = 0
GROUP BY ano
ORDER BY ano
""")
display(evolucao)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Meta versus resultado — % de municípios que atingiram a meta
# MAGIC (Só popula quando as tabelas de meta do P2 estiverem em Bronze.)

# COMMAND ----------
mvr = spark.table(f"{CATALOG}.gold.meta_vs_resultado")
if mvr.filter(F.col("meta_taxa").isNotNull()).limit(1).count() > 0:
    resumo_meta = (
        mvr.filter(F.col("atingiu_meta").isNotNull())
        .groupBy("ano")
        .agg(
            F.round(F.avg(F.col("atingiu_meta").cast("double")), 4).alias("pct_municipios_na_meta"),
            F.count("*").alias("municipios_avaliados"),
        )
        .orderBy("ano")
    )
    display(resumo_meta)
else:
    print("Sem dados de meta ainda (aguardando fontes do P2). Mart criado, porém vazio.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Municípios com maior queda na taxa (atenção prioritária)

# COMMAND ----------
quedas = spark.sql(f"""
SELECT ano, sigla_uf, id_municipio, taxa_alfabetizacao_media, variacao_absoluta
FROM {CATALOG}.gold.evolucao_temporal
WHERE rede = 0 AND variacao_absoluta IS NOT NULL
ORDER BY variacao_absoluta ASC
LIMIT 20
""")
display(quedas)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Saúde do pipeline (últimas execuções)

# COMMAND ----------
display(spark.sql(f"""
SELECT task_name, status, rows_read, rows_written, rows_rejected, finished_at
FROM {CATALOG}.observability.pipeline_metrics
ORDER BY finished_at DESC
LIMIT 20
"""))
