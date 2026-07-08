# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold
# MAGIC Cria os marts analíticos após aprovação do Quality Gate (notebook 06).
# MAGIC
# MAGIC Marts publicados:
# MAGIC 1. `gold.indicador_municipio` — grão: `ano + id_municipio + rede`
# MAGIC 2. `gold.resumo_uf` — grão: `ano + sigla_uf + rede`
# MAGIC 3. `gold.meta_vs_resultado` — grão: `ano + id_municipio + rede` (resultado x meta)
# MAGIC 4. `gold.evolucao_temporal` — grão: `ano + id_municipio + rede` (variação ano a ano)

# COMMAND ----------
CATALOG = "workspace"

# A Gold lê da Silver APROVADA pelo Quality Gate (06). Se a tabela aprovada
# ainda não existir (execução isolada), cai para a Silver bruta como fallback.
APROVADA = f"{CATALOG}.silver.medicoes_aprovadas"
SOURCE = APROVADA if spark.catalog.tableExists(APROVADA) else f"{CATALOG}.silver.medicoes_alfabetizacao"
print(f"Fonte da Gold: {SOURCE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 1 — indicador por município (grão: ano + id_municipio + rede)

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.indicador_municipio
COMMENT 'Indicador de alfabetização por município. Grão: ano + id_municipio + rede. Responsável: P4.'
AS
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 2 — resumo por UF (grão: ano + sigla_uf + rede)

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.resumo_uf
COMMENT 'Resumo do indicador por UF. Grão: ano + sigla_uf + rede. Responsável: P4.'
AS
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 3 — meta versus resultado (grão: ano + id_municipio + rede)
# MAGIC Compara a taxa observada com a meta municipal. As tabelas de meta são
# MAGIC responsabilidade do P2; se ainda não existirem em Bronze, o mart é criado
# MAGIC vazio (com o schema correto) para não travar o pipeline.

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

META_TBL = f"{CATALOG}.bronze.meta_municipio"

def detectar_coluna_meta(df):
    """Escolhe, de forma defensiva, a coluna numérica que representa a meta.
    O schema da fonte de metas ainda não foi confirmado pelo P2 (ver
    docs/data_dictionary.md), então procuramos uma coluna cujo nome contenha
    'meta' e que seja numérica. Retorna o nome da coluna ou None."""
    numericos = ("double", "float", "int", "bigint", "decimal")
    candidatas = [
        c for c, t in df.dtypes
        if "meta" in c.lower() and any(t.startswith(n) for n in numericos)
    ]
    return candidatas[0] if candidatas else None

indicador = spark.table(f"{CATALOG}.gold.indicador_municipio")

if spark.catalog.tableExists(META_TBL):
    meta_raw = spark.table(META_TBL)
    col_meta = detectar_coluna_meta(meta_raw)

    if col_meta is None:
        print(f"⚠ {META_TBL} existe mas nenhuma coluna de meta numérica foi encontrada. "
              f"Colunas: {meta_raw.columns}. Ajustar detectar_coluna_meta().")
        meta_prep = None
    else:
        print(f"Coluna de meta detectada: '{col_meta}'")
        meta_prep = (
            meta_raw
            .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
            .withColumn("ano", F.col("ano").cast("int"))
            .withColumn("meta_taxa", F.col(col_meta).cast("double"))
            # normaliza para 0..1 caso a fonte traga percentual em 0..100
            .withColumn("meta_taxa", F.when(F.col("meta_taxa") > 1.0, F.col("meta_taxa") / 100.0)
                                       .otherwise(F.col("meta_taxa")))
            .select("ano", "id_municipio", "meta_taxa")
            .dropDuplicates(["ano", "id_municipio"])
        )
else:
    print(f"⚠ {META_TBL} ainda não existe (aguardando CSV do P2). Mart criado vazio.")
    meta_prep = None

if meta_prep is not None:
    meta_vs_resultado = (
        indicador
        .join(meta_prep, on=["ano", "id_municipio"], how="left")
        .withColumn("gap_meta", F.round(F.col("taxa_alfabetizacao_media") - F.col("meta_taxa"), 4))
        .withColumn("atingiu_meta",
                    F.when(F.col("meta_taxa").isNull(), None)
                     .otherwise(F.col("taxa_alfabetizacao_media") >= F.col("meta_taxa")))
        .select(
            "ano", "sigla_uf", "id_municipio", "rede", "rede_label",
            "taxa_alfabetizacao_media", "meta_taxa", "gap_meta", "atingiu_meta", "updated_at",
        )
    )
else:
    schema = T.StructType([
        T.StructField("ano", T.IntegerType()),
        T.StructField("sigla_uf", T.StringType()),
        T.StructField("id_municipio", T.StringType()),
        T.StructField("rede", T.IntegerType()),
        T.StructField("rede_label", T.StringType()),
        T.StructField("taxa_alfabetizacao_media", T.DoubleType()),
        T.StructField("meta_taxa", T.DoubleType()),
        T.StructField("gap_meta", T.DoubleType()),
        T.StructField("atingiu_meta", T.BooleanType()),
        T.StructField("updated_at", T.TimestampType()),
    ])
    meta_vs_resultado = spark.createDataFrame([], schema)

(meta_vs_resultado.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.meta_vs_resultado"))

spark.sql(f"COMMENT ON TABLE {CATALOG}.gold.meta_vs_resultado IS "
          f"'Resultado observado x meta por município. Grão: ano + id_municipio + rede. Responsável: P4.'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 4 — evolução temporal (grão: ano + id_municipio + rede)
# MAGIC Variação da taxa ano a ano por município, para análises de tendência.

# COMMAND ----------
from pyspark.sql.window import Window

w = Window.partitionBy("id_municipio", "rede").orderBy("ano")

evolucao_temporal = (
    indicador
    .select("ano", "sigla_uf", "id_municipio", "rede", "rede_label",
            "taxa_alfabetizacao_media", "updated_at")
    .withColumn("taxa_ano_anterior", F.lag("taxa_alfabetizacao_media").over(w))
    .withColumn("ano_anterior", F.lag("ano").over(w))
    .withColumn("variacao_absoluta",
                F.round(F.col("taxa_alfabetizacao_media") - F.col("taxa_ano_anterior"), 4))
    .withColumn("variacao_relativa",
                F.when(F.col("taxa_ano_anterior") > 0,
                       F.round((F.col("taxa_alfabetizacao_media") - F.col("taxa_ano_anterior"))
                               / F.col("taxa_ano_anterior"), 4)))
    .withColumn("tendencia",
                F.when(F.col("variacao_absoluta") > 0, F.lit("alta"))
                 .when(F.col("variacao_absoluta") < 0, F.lit("queda"))
                 .when(F.col("variacao_absoluta") == 0, F.lit("estavel")))
)

(evolucao_temporal.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.evolucao_temporal"))

spark.sql(f"COMMENT ON TABLE {CATALOG}.gold.evolucao_temporal IS "
          f"'Variação da taxa ano a ano por município. Grão: ano + id_municipio + rede. Responsável: P4.'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação dos marts

# COMMAND ----------
for mart in ["indicador_municipio", "resumo_uf", "meta_vs_resultado", "evolucao_temporal"]:
    n = spark.table(f"{CATALOG}.gold.{mart}").count()
    print(f"✓ gold.{mart}: {n:,} linhas")

print("Gold publicada com 4 marts e grão documentado.")
