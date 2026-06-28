# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver (P4)
# MAGIC Limpeza, tratamento de nulos, padronização, normalização de chaves e INTEGRAÇÃO das bases.

# COMMAND ----------
from pyspark.sql import functions as F

b = spark.table("bronze.avaliacao_alfabetizacao")

# TODO (P4): tratar nulos de proporcao_aluno_nivel_X (~52% ausentes) — decidir e documentar
# TODO (P4): padronizar 'rede' (0/2/3/5 -> rótulos) conforme CONTRACT
silver = (b
          .withColumn("alfabetizado", F.col("media_portugues") >= F.lit(743))
          # .join(spark.table("bronze.municipio"), "id_municipio", "left")
          # .join(spark.table("bronze.meta_municipio"), ["ano","id_municipio"], "left")
         )

(silver.write.format("delta").mode("overwrite")
   .partitionBy("ano", "sigla_uf")
   .saveAsTable("silver.alfabetizacao"))
