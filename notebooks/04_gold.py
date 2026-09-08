# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 Gold
# MAGIC Cria os marts analíticos após aprovação do Quality Gate (notebook 06).
# MAGIC
# MAGIC **A Gold lê exclusivamente da Silver aprovada** (arquitetura Medalhão:
# MAGIC nenhuma leitura direta da Bronze). Metas e dimensões já chegam integradas
# MAGIC pela Silver (notebook 03). Todos os marts carregam `fonte_dados` para
# MAGIC preservar a rastreabilidade da origem oficial do INEP.
# MAGIC
# MAGIC Marts publicados:
# MAGIC 1. `gold.indicador_municipio` - grão: `ano + id_municipio + rede`
# MAGIC 2. `gold.resumo_uf` - grão: `ano + sigla_uf + rede`
# MAGIC 3. `gold.meta_vs_resultado` - grão: `ano + território + rede`, com
# MAGIC    `nivel_territorial` explícito (`uf` | `municipio`)
# MAGIC 4. `gold.evolucao_temporal` - grão: `ano + território + rede`, com
# MAGIC    `nivel_territorial` explícito (`uf` | `municipio`)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "workspace"

APROVADA = f"{CATALOG}.silver.medicoes_aprovadas"

if not spark.catalog.tableExists(APROVADA):
    raise RuntimeError(
        f"Quality Gate não aprovado: tabela {APROVADA} não encontrada. "
        "Execute o notebook 06 antes da Gold."
    )

SOURCE = APROVADA
print(f"Fonte da Gold: {SOURCE}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 1 - indicador por município (grão: ano + id_municipio + rede)
# MAGIC O mart consome exclusivamente registros no grão municipal da Silver aprovada.

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
WHERE grao = 'municipio'
  AND id_municipio IS NOT NULL
GROUP BY
    ano,
    sigla_uf,
    nome_uf,
    regiao,
    id_municipio,
    nome_municipio,
    rede,
    rede_label,
    fonte_dados
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 2 - resumo por UF (grão: ano + sigla_uf + rede)
# MAGIC Construído sobre o dado OFICIAL do INEP (grão UF), enriquecido com o
# MAGIC agregado de alunos integrado na Silver.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.resumo_uf
COMMENT 'Resumo do indicador por UF. Grão: ano + sigla_uf + rede. Responsável: P4.'
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
WHERE grao = 'uf'
  AND id_municipio IS NULL
GROUP BY
    ano,
    sigla_uf,
    nome_uf,
    regiao,
    rede,
    rede_label,
    fonte_dados
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 3 - meta versus resultado (grão: ano + território + rede)
# MAGIC As metas já foram integradas na Silver (notebook 03) via join com
# MAGIC `bronze.meta_uf` / `bronze.meta_municipio` - a Gold **não lê a Bronze**.
# MAGIC O mart cobre os dois grãos, mas expõe `nivel_territorial` explicitamente.
# MAGIC Toda consulta agregada deve filtrar `nivel_territorial` para não somar
# MAGIC UF e município na mesma análise.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.meta_vs_resultado
COMMENT 'Resultado observado x meta por UF e município. Grão: ano + território + rede. Responsável: P4.'
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
    GROUP BY
        ano,
        grao,
        sigla_uf,
        nome_uf,
        regiao,
        id_municipio,
        nome_municipio,
        rede,
        rede_label,
        fonte_dados
)
SELECT
    *,
    grao AS nivel_territorial,
    ROUND(taxa_alfabetizacao_media - meta_taxa, 4) AS gap_meta,
    CASE
        WHEN meta_taxa IS NULL THEN NULL
        ELSE taxa_alfabetizacao_media >= meta_taxa
    END AS atingiu_meta,
    CASE
        WHEN meta_brasil IS NULL THEN NULL
        ELSE taxa_alfabetizacao_media >= meta_brasil
    END AS atingiu_meta_brasil
FROM base
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Mart 4 - evolução temporal (grão: ano + território + rede)
# MAGIC Variação da taxa ano a ano. Cobre o grão UF (série histórica oficial do
# MAGIC INEP) e o grão município (eventos + dado municipal oficial quando houver).

# COMMAND ----------

base = (
    spark.table(SOURCE)
    .groupBy(
        "ano",
        "grao",
        "sigla_uf",
        "nome_uf",
        "regiao",
        "id_municipio",
        "nome_municipio",
        "rede",
        "rede_label",
        "fonte_dados",
    )
    .agg(
        F.avg("taxa_alfabetizacao").alias("taxa_alfabetizacao_media"),
        F.max("processed_at").alias("updated_at"),
    )
)

w = (
    Window
    .partitionBy(
        "grao",
        F.coalesce("id_municipio", "sigla_uf"),
        "rede",
    )
    .orderBy("ano")
)

evolucao_temporal = (
    base
    .withColumn("nivel_territorial", F.col("grao"))
    .withColumn(
        "taxa_ano_anterior",
        F.lag("taxa_alfabetizacao_media").over(w),
    )
    .withColumn(
        "ano_anterior",
        F.lag("ano").over(w),
    )
    .withColumn(
        "variacao_absoluta",
        F.round(
            F.col("taxa_alfabetizacao_media")
            - F.col("taxa_ano_anterior"),
            4,
        ),
    )
    .withColumn(
        "variacao_relativa",
        F.when(
            F.col("taxa_ano_anterior") > 0,
            F.round(
                (
                    F.col("taxa_alfabetizacao_media")
                    - F.col("taxa_ano_anterior")
                )
                / F.col("taxa_ano_anterior"),
                4,
            ),
        ),
    )
    .withColumn(
        "tendencia",
        F.when(F.col("variacao_absoluta") > 0, F.lit("alta"))
        .when(F.col("variacao_absoluta") < 0, F.lit("queda"))
        .when(F.col("variacao_absoluta") == 0, F.lit("estavel")),
    )
)

(
    evolucao_temporal.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.evolucao_temporal")
)

spark.sql(
    f"""
    COMMENT ON TABLE {CATALOG}.gold.evolucao_temporal IS
    'Variação da taxa ano a ano por UF e município. nivel_territorial obrigatório em agregações. Grão: ano + território + rede. Responsável: P4.'
    """
)


# COMMAND ----------

# MAGIC %md
# MAGIC #Mart 5 - base de modelagem no grão de aluno

# COMMAND ----------

ALUNOS_APROVADOS = f"{CATALOG}.silver.alunos_modelagem_aprovados"
GOLD_ALUNOS = f"{CATALOG}.gold.base_modelagem_aluno"

if not spark.catalog.tableExists(ALUNOS_APROVADOS):
    raise RuntimeError(
        f"{ALUNOS_APROVADOS} não existe. "
        "Execute o Quality Gate de alunos no notebook 06."
    )

alunos_aprovados = spark.table(ALUNOS_APROVADOS)

print(
    f"Fonte Mart 5: {ALUNOS_APROVADOS} "
    f"({alunos_aprovados.count():,} registros)"
)

base_modelagem_aluno = (
    alunos_aprovados
    .select(
        "record_id",
        "id_aluno",
        "ano",
        "sigla_uf",
        "nome_uf",
        "regiao",
        "id_municipio",
        "nome_municipio",
        "capital",
        "serie",
        "id_escola",
        "tp_dependencia",
        "rede",
        "rede_label",
        "presenca_lp",
        "preenchimento_lp",
        "caderno_lp",
        "peso_aluno_lp",
        "alfabetizado_oficial",
        "uf_consistente",
        "source",
        "fonte_dados",
        "schema_version",
        "processed_at",
    )
)

(
    base_modelagem_aluno.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_ALUNOS)
)

spark.sql(
    f"""
    COMMENT ON TABLE {GOLD_ALUNOS} IS
    'Base analítica oficial no grão de aluno para preparação da Fase 3. Target: alfabetizado_oficial. Proficiência excluída da base de modelagem para evitar data leakage.'
    """
)

rows_silver_alunos = alunos_aprovados.count()
rows_gold_alunos = spark.table(GOLD_ALUNOS).count()

if rows_silver_alunos != rows_gold_alunos:
    raise RuntimeError(
        "Falha de reconciliação do Mart 5: "
        f"silver={rows_silver_alunos:,} "
        f"gold={rows_gold_alunos:,}"
    )

targets_gold = (
    spark.table(GOLD_ALUNOS)
    .filter(F.col("alfabetizado_oficial").isin([0, 1]))
    .count()
)

record_ids_gold = (
    spark.table(GOLD_ALUNOS)
    .select("record_id")
    .distinct()
    .count()
)

if record_ids_gold != rows_gold_alunos:
    raise RuntimeError("Mart 5 possui record_id duplicado.")

if targets_gold != rows_gold_alunos:
    raise RuntimeError("Mart 5 possui target fora do domínio 0/1.")

print("\n=== MART 5 - BASE MODELAGEM ALUNO ===")
print(f"✓ Silver aprovada: {rows_silver_alunos:,}")
print(f"✓ Gold alunos: {rows_gold_alunos:,}")
print(f"✓ Target oficial válido: {targets_gold:,}")
print(f"✓ record_id único: {record_ids_gold:,}")
print(f"✓ Tabela: {GOLD_ALUNOS}")
print("✓ Base no grão de aluno pronta para a Fase 3.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação dos marts

# COMMAND ----------

for mart in [
    "indicador_municipio",
    "resumo_uf",
    "meta_vs_resultado",
    "evolucao_temporal",
    "base_modelagem_aluno",
]:
    df = spark.table(f"{CATALOG}.gold.{mart}")
    n = df.count()

    if "fonte_dados" in df.columns:
        oficiais = df.filter(
            F.col("fonte_dados") == "oficial_inep"
        ).count()
        print(
            f"✓ gold.{mart}: {n:,} linhas "
            f"({oficiais:,} de fonte oficial INEP)"
        )
    else:
        print(f"✓ gold.{mart}: {n:,} linhas")

if (
    spark.table(f"{CATALOG}.gold.resumo_uf")
    .filter(F.col("sigla_uf").isNull())
    .count()
    > 0
):
    raise RuntimeError("gold.resumo_uf contém linha sem UF.")

for mart in ["meta_vs_resultado", "evolucao_temporal"]:
    niveis = {
        r["nivel_territorial"]
        for r in (
            spark.table(f"{CATALOG}.gold.{mart}")
            .select("nivel_territorial")
            .distinct()
            .collect()
        )
    }

    inesperados = niveis - {"uf", "municipio"}

    if inesperados:
        raise RuntimeError(
            f"gold.{mart}: nivel_territorial inválido: {inesperados}"
        )

print(
    "Gold publicada com 5 marts, grão documentado, nível territorial explícito "
    "nos marts mistos e origem oficial identificada."
)

# COMMAND ----------

dbutils.notebook.exit(
    f"Gold publicada: base_modelagem_aluno={rows_gold_alunos:,} linhas, 5 marts"
)