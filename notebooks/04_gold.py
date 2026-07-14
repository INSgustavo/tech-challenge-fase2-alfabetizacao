# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold
# MAGIC Cria os marts analíticos após aprovação do Quality Gate (notebook 06).
# MAGIC
# MAGIC **A Gold lê exclusivamente da Silver aprovada** (arquitetura Medalhão:
# MAGIC nenhuma leitura direta da Bronze). Metas e dimensões já chegam integradas
# MAGIC pela Silver (notebook 03). Todos os marts carregam `fonte_dados` para
# MAGIC distinguir dado oficial do INEP de eventos do simulador.
# MAGIC
# MAGIC Marts publicados:
# MAGIC 1. `gold.indicador_municipio` — grão: `ano + id_municipio + rede`
# MAGIC 2. `gold.resumo_uf` — grão: `ano + sigla_uf + rede`
# MAGIC 3. `gold.meta_vs_resultado` — grão: `ano + território + rede` (UF e município)
# MAGIC 4. `gold.evolucao_temporal` — grão: `ano + território + rede` (variação anual)

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
# MAGIC `fonte_dados` indica se a linha vem do INEP (oficial) ou do simulador.

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.indicador_municipio
COMMENT 'Indicador de alfabetização por município. Grão: ano + id_municipio + rede. Responsável: P4.'
AS
SELECT
    ano,
    sigla_uf,
    nome_uf,
    regiao,
    id_municipio,
    nome_municipio,
    rede,
    rede_label,
    fonte_dados,
    AVG(taxa_alfabetizacao) AS taxa_alfabetizacao_media,
    AVG(media_portugues) AS media_portugues,
    AVG(CASE WHEN alfabetizado THEN 1.0 WHEN alfabetizado = false THEN 0.0 END) AS pct_registros_alfabetizados,
    COUNT(*) AS quantidade_registros,
    MAX(processed_at) AS updated_at
FROM {SOURCE}
WHERE grao = 'municipio' AND id_municipio IS NOT NULL
GROUP BY ano, sigla_uf, nome_uf, regiao, id_municipio, nome_municipio, rede, rede_label, fonte_dados
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 2 — resumo por UF (grão: ano + sigla_uf + rede)
# MAGIC Construído sobre o dado OFICIAL do INEP (grão UF), enriquecido com o
# MAGIC agregado de alunos integrado na Silver.

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.resumo_uf
COMMENT 'Resumo do indicador por UF (dado oficial INEP). Grão: ano + sigla_uf + rede. Responsável: P4.'
AS
SELECT
    ano,
    sigla_uf,
    nome_uf,
    regiao,
    rede,
    rede_label,
    fonte_dados,
    AVG(taxa_alfabetizacao) AS taxa_alfabetizacao_media,
    AVG(media_portugues) AS media_portugues,
    AVG(alunos_proficiencia_media) AS alunos_proficiencia_media,
    AVG(alunos_pct_alfabetizados) AS alunos_pct_alfabetizados,
    COUNT(DISTINCT id_municipio) AS municipios_cobertos,
    MAX(processed_at) AS updated_at
FROM {SOURCE}
GROUP BY ano, sigla_uf, nome_uf, regiao, rede, rede_label, fonte_dados
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 3 — meta versus resultado (grão: ano + território + rede)
# MAGIC As metas já foram integradas na Silver (notebook 03) via join com
# MAGIC `bronze.meta_uf` / `bronze.meta_municipio` — a Gold **não lê a Bronze**.
# MAGIC O mart cobre os dois grãos: UF (dado oficial) e município.

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.meta_vs_resultado
COMMENT 'Resultado observado x meta, por UF (oficial) e por município. Grão: ano + território + rede. Responsável: P4.'
AS
WITH base AS (
    SELECT
        ano,
        grao,
        sigla_uf,
        nome_uf,
        regiao,
        id_municipio,
        nome_municipio,
        rede,
        rede_label,
        fonte_dados,
        AVG(taxa_alfabetizacao) AS taxa_alfabetizacao_media,
        AVG(meta_taxa) AS meta_taxa,
        AVG(meta_brasil) AS meta_brasil,
        MAX(processed_at) AS updated_at
    FROM {SOURCE}
    GROUP BY ano, grao, sigla_uf, nome_uf, regiao, id_municipio, nome_municipio,
             rede, rede_label, fonte_dados
)
SELECT
    *,
    ROUND(taxa_alfabetizacao_media - meta_taxa, 4) AS gap_meta,
    CASE WHEN meta_taxa IS NULL THEN NULL
         ELSE taxa_alfabetizacao_media >= meta_taxa END AS atingiu_meta,
    CASE WHEN meta_brasil IS NULL THEN NULL
         ELSE taxa_alfabetizacao_media >= meta_brasil END AS atingiu_meta_brasil
FROM base
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 4 — evolução temporal (grão: ano + território + rede)
# MAGIC Variação da taxa ano a ano. Cobre o grão UF (série histórica oficial do
# MAGIC INEP) e o grão município (eventos + dado municipal oficial quando houver).

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

base = (
    spark.table(SOURCE)
    .groupBy("ano", "grao", "sigla_uf", "nome_uf", "regiao",
             "id_municipio", "nome_municipio", "rede", "rede_label", "fonte_dados")
    .agg(F.avg("taxa_alfabetizacao").alias("taxa_alfabetizacao_media"),
         F.max("processed_at").alias("updated_at"))
)

# chave territorial: município quando existir, senão a UF (grão oficial)
w = (Window
     .partitionBy("grao", F.coalesce("id_municipio", "sigla_uf"), "rede")
     .orderBy("ano"))

evolucao_temporal = (
    base
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
          f"'Variação da taxa ano a ano por UF e município. Grão: ano + território + rede. Responsável: P4.'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação dos marts

# COMMAND ----------
for mart in ["indicador_municipio", "resumo_uf", "meta_vs_resultado", "evolucao_temporal"]:
    df = spark.table(f"{CATALOG}.gold.{mart}")
    n = df.count()
    if "fonte_dados" in df.columns:
        oficiais = df.filter(F.col("fonte_dados") == "oficial_inep").count()
        print(f"✓ gold.{mart}: {n:,} linhas ({oficiais:,} de fonte oficial INEP)")
    else:
        print(f"✓ gold.{mart}: {n:,} linhas")

print("Gold publicada com 4 marts, grão documentado e origem do dado identificada.")
