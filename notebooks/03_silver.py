# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 Silver canônica
# MAGIC Normaliza chaves, **integra as seis fontes do edital** e publica o modelo
# MAGIC canônico:
# MAGIC 1. medições batch (INEP, grão UF) + streaming (grão município) - fatos;
# MAGIC 2. `bronze.municipio` e `bronze.uf` - dimensões territoriais (join);
# MAGIC 3. `bronze.meta_brasil`, `bronze.meta_uf`, `bronze.meta_municipio` - metas
# MAGIC    associadas a cada medição conforme o grão (join);
# MAGIC 4. `bronze.alunos` - agregado por ano+UF+rede como enriquecimento (join).
# MAGIC
# MAGIC Cada registro analítico sai com `fonte_dados = 'oficial_inep'`.
# MAGIC Dados simulados não participam mais da Silver oficial.

# COMMAND ----------

CATALOG = "workspace"
import sys
from pyspark.sql import functions as F

# Constantes compartilhadas em src/ (evita drift entre notebooks). Em execução
# via Databricks Repos o repositório está no sys.path do workspace; se o import
# falhar (ex.: notebook importado avulso), usa o fallback inline equivalente.
try:
    sys.path.append("..")
    from src.utils import REDE_MAP, ALFABETIZACAO_CORTE
except Exception:
    REDE_MAP = {0: "total", 2: "estadual", 3: "municipal", 5: "privada"}
    ALFABETIZACAO_CORTE = 743

rede_mapping = F.create_map([F.lit(x) for pair in REDE_MAP.items() for x in pair])

def tbl(schema, name):
    return f"{CATALOG}.{schema}.{name}"

def existe(schema, name):
    return spark.catalog.tableExists(tbl(schema, name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Fatos - batch (grão UF, dado oficial)

# COMMAND ----------

batch = spark.table(tbl("bronze", "avaliacao_alfabetizacao"))

# A fonte batch (avaliação SAEB agregada) tem grão UF - NÃO existe id_municipio
# no CSV. `taxa_alfabetizacao` chega em percentual (0-100) e é normalizada para
# 0-1 (contrato, seção 2); o streaming já chega em fração.
batch_canonical = (
    batch
    .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    .withColumn("id_municipio", F.lit(None).cast("string"))
    .withColumn("grao", F.lit("uf"))
    .withColumn("serie", F.col("serie").cast("int"))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("rede_label", rede_mapping[F.col("rede")])
    .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double") / 100.0)
    .withColumn("media_portugues", F.col("media_portugues").cast("double"))
    .withColumn("alfabetizado", F.when(F.col("media_portugues").isNotNull(),
                                       F.col("media_portugues") >= ALFABETIZACAO_CORTE))
    .withColumn("event_id", F.lit(None).cast("string"))
    .withColumn("event_time", F.lit(None).cast("timestamp"))
    .withColumn("source", F.lit("batch_inep"))
    .withColumn("fonte_dados", F.lit("oficial_inep"))
    .withColumn("schema_version", F.lit("1.0"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b. Fatos - batch municipal (dado oficial obrigatório)
# MAGIC O indicador municipal oficial já é ingerido na Bronze.
# MAGIC Não existe mais fallback para dado simulado.

# COMMAND ----------

if existe("bronze", "avaliacao_alfabetizacao_municipio"):
    batch_mun = spark.table(tbl("bronze", "avaliacao_alfabetizacao_municipio"))
    batch_mun_canonical = (
        batch_mun
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .withColumn("grao", F.lit("municipio"))
        .withColumn("serie", F.col("serie").cast("int"))
        .withColumn("rede", F.col("rede").cast("int"))
        .withColumn("rede_label", rede_mapping[F.col("rede")])
        .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double") / 100.0)
        .withColumn("media_portugues",
                    F.col("media_portugues").cast("double")
                    if "media_portugues" in batch_mun.columns else F.lit(None).cast("double"))
        .withColumn("alfabetizado", F.when(F.col("media_portugues").isNotNull(),
                                           F.col("media_portugues") >= ALFABETIZACAO_CORTE))
        .withColumn("event_id", F.lit(None).cast("string"))
        .withColumn("event_time", F.lit(None).cast("timestamp"))
        .withColumn("source", F.lit("batch_inep_municipio"))
        .withColumn("fonte_dados", F.lit("oficial_inep"))
        .withColumn("schema_version", F.lit("1.0"))
    )
    print(f"✓ dado oficial municipal encontrado: {batch_mun_canonical.count():,} registros")
else:
    raise RuntimeError(
        "bronze.avaliacao_alfabetizacao_municipio não existe. "
        "A Silver não pode usar dado simulado como fallback."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fatos - streaming (grão município, replay de dado oficial)

# COMMAND ----------

if existe("bronze", "eventos_streaming"):
    events = (
        spark.table(tbl("bronze", "eventos_streaming"))
        .filter(F.col("source") == "INEP_OFICIAL_REPLAY")
    )
    stream_canonical = (
        events
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .withColumn("grao", F.lit("municipio"))
        .withColumn("serie", F.lit(None).cast("int"))
        .withColumn("rede", F.col("rede").cast("int"))
        .withColumn("rede_label", rede_mapping[F.col("rede")])
        .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double") / 100.0)
        .withColumn("media_portugues", F.lit(None).cast("double"))
        .withColumn("alfabetizado", F.lit(None).cast("boolean"))
        .withColumn("fonte_dados", F.lit("oficial_inep"))
    )
else:
    stream_canonical = spark.createDataFrame([], batch_canonical.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. União dos fatos

# COMMAND ----------

columns = [
    "ano", "sigla_uf", "id_municipio", "grao", "serie", "rede", "rede_label",
    "media_portugues", "taxa_alfabetizacao", "alfabetizado",
    "event_id", "event_time", "source", "fonte_dados", "schema_version",
]

fatos = batch_canonical.select(*columns)
if batch_mun_canonical is not None:
    fatos = fatos.unionByName(batch_mun_canonical.select(*columns), allowMissingColumns=True)
fatos = fatos.unionByName(stream_canonical.select(*columns), allowMissingColumns=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Integração com as dimensões territoriais (município e UF)
# MAGIC Joins com `bronze.municipio` e `bronze.uf` - aqui ocorre a integração das
# MAGIC bases exigida pelo edital, e não apenas a união batch+streaming.

# COMMAND ----------

if existe("bronze", "municipio"):
    dim_mun = (
        spark.table(tbl("bronze", "municipio"))
        .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .select(
            "id_municipio",
            F.col("nome").alias("nome_municipio"),
            F.col("sigla_uf").alias("sigla_uf_dim"),
            F.col("capital").cast("int").alias("capital"),
        )
        .dropDuplicates(["id_municipio"])
    )
    fatos = (
        fatos.join(dim_mun, on="id_municipio", how="left")
        # consistência entre tabelas: a UF do evento deve bater com a UF do
        # município na dimensão (checada de novo no Quality Gate, notebook 06)
        .withColumn("uf_consistente",
                    F.when(F.col("id_municipio").isNull(), F.lit(None))
                     .otherwise(F.col("sigla_uf") == F.col("sigla_uf_dim")))
        .drop("sigla_uf_dim")
    )
else:
    fatos = (fatos
             .withColumn("nome_municipio", F.lit(None).cast("string"))
             .withColumn("capital", F.lit(None).cast("int"))
             .withColumn("uf_consistente", F.lit(None).cast("boolean")))

if existe("bronze", "uf"):
    dim_uf = (
        spark.table(tbl("bronze", "uf"))
        .select(
            F.upper(F.trim(F.col("sigla_uf"))).alias("sigla_uf"),
            F.col("nome").alias("nome_uf"),
            F.col("regiao"),
        )
        .dropDuplicates(["sigla_uf"])
    )
    fatos = fatos.join(dim_uf, on="sigla_uf", how="left")
else:
    fatos = (fatos
             .withColumn("nome_uf", F.lit(None).cast("string"))
             .withColumn("regiao", F.lit(None).cast("string")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Integração com as metas (Brasil, UF e município)
# MAGIC Cada medição recebe a meta do seu grão (`meta_taxa`) e a meta nacional de
# MAGIC referência (`meta_brasil`). Metas em 0-100 são normalizadas para 0-1.

# COMMAND ----------

def normalizar_meta(col):
    return F.when(col > 1.0, col / 100.0).otherwise(col)

if existe("bronze", "meta_municipio"):
    meta_mun = (
        spark.table(tbl("bronze", "meta_municipio"))
        .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("meta_municipio", normalizar_meta(F.col("meta").cast("double")))
        .select("ano", "id_municipio", "meta_municipio")
        .dropDuplicates(["ano", "id_municipio"])
    )
    fatos = fatos.join(meta_mun, on=["ano", "id_municipio"], how="left")
else:
    fatos = fatos.withColumn("meta_municipio", F.lit(None).cast("double"))

if existe("bronze", "meta_uf"):
    meta_uf = (
        spark.table(tbl("bronze", "meta_uf"))
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("meta_uf", normalizar_meta(F.col("meta").cast("double")))
        .select("ano", "sigla_uf", "meta_uf")
        .dropDuplicates(["ano", "sigla_uf"])
    )
    fatos = fatos.join(meta_uf, on=["ano", "sigla_uf"], how="left")
else:
    fatos = fatos.withColumn("meta_uf", F.lit(None).cast("double"))

if existe("bronze", "meta_brasil"):
    meta_br = (
        spark.table(tbl("bronze", "meta_brasil"))
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("meta_brasil", normalizar_meta(F.col("meta").cast("double")))
        .select("ano", "meta_brasil")
        .dropDuplicates(["ano"])
    )
    fatos = fatos.join(meta_br, on="ano", how="left")
else:
    fatos = fatos.withColumn("meta_brasil", F.lit(None).cast("double"))

# meta do grão da medição:
# município usa EXCLUSIVAMENTE a meta municipal oficial;
# UF usa EXCLUSIVAMENTE a meta estadual oficial.
# Se a meta oficial não existir, permanece NULL - sem herança/fallback.
fatos = fatos.withColumn(
    "meta_taxa",
    F.when(F.col("grao") == "municipio", F.col("meta_municipio"))
     .otherwise(F.col("meta_uf")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Integração com dados de alunos (agregado por ano + UF + rede)
# MAGIC Microdados de alunos agregados e associados como enriquecimento: média de
# MAGIC proficiência e % de alunos acima do corte de alfabetização (743).

# COMMAND ----------

if existe("bronze", "alunos"):

    alunos_canonical = (
        spark.table(tbl("bronze", "alunos"))
        .select(
            # ano oficial
            F.col("NU_ANO_AVALIACAO")
             .cast("int")
             .alias("ano"),

            # UF oficial
            F.upper(
                F.trim(F.col("SG_UF"))
            ).alias("sigla_uf"),

            # compatibilidade com o código de rede usado no projeto
            F.when(
                F.col("TP_DEPENDENCIA").cast("int") == 4,
                F.lit(5)
            ).otherwise(
                F.col("TP_DEPENDENCIA").cast("int")
            ).alias("rede"),

            # proficiência oficial
            F.regexp_replace(
                F.trim(F.col("VL_PROFICIENCIA_LP")),
                ",",
                "."
            )
            .cast("double")
            .alias("proficiencia_portugues")
        )
        .filter(
            F.col("ano").isNotNull()
            & F.col("sigla_uf").isNotNull()
            & F.col("rede").isNotNull()
            & F.col("proficiencia_portugues").isNotNull()
        )
    )

    agg_alunos = (
        alunos_canonical

        .groupBy(
            "ano",
            "sigla_uf",
            "rede"
        )

        .agg(
            F.avg(
                "proficiencia_portugues"
            ).alias(
                "alunos_proficiencia_media"
            ),

            F.avg(
                F.when(
                    F.col("proficiencia_portugues")
                    >= ALFABETIZACAO_CORTE,
                    1.0
                ).otherwise(0.0)
            ).alias(
                "alunos_pct_alfabetizados"
            ),

            F.count("*").alias(
                "alunos_amostra"
            )
        )
    )

    fatos = fatos.join(
        agg_alunos,
        on=[
            "ano",
            "sigla_uf",
            "rede"
        ],
        how="left"
    )

else:
    raise RuntimeError(
        "bronze.alunos não existe. "
        "A Silver exige os microdados oficiais do INEP."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Chave determinística, dedup e publicação

# COMMAND ----------

if "sigla_uf" not in fatos.columns and "SG_UF" in fatos.columns:
    fatos = fatos.withColumn(
        "sigla_uf",
        F.upper(F.trim(F.col("SG_UF")))
    )

if "ano" not in fatos.columns and "NU_ANO_AVALIACAO" in fatos.columns:
    fatos = fatos.withColumn(
        "ano",
        F.col("NU_ANO_AVALIACAO").cast("int")
    )

if "id_municipio" not in fatos.columns and "CO_MUNICIPIO" in fatos.columns:
    fatos = fatos.withColumn(
        "id_municipio",
        F.lpad(F.col("CO_MUNICIPIO").cast("string"), 7, "0")
    )

if "serie" not in fatos.columns and "TP_SERIE" in fatos.columns:
    fatos = fatos.withColumn(
        "serie",
        F.col("TP_SERIE").cast("int")
    )

if "rede" not in fatos.columns and "TP_DEPENDENCIA" in fatos.columns:
    fatos = fatos.withColumn(
        "rede",
        F.when(
            F.col("TP_DEPENDENCIA").cast("int") == 4,
            F.lit(5)
        ).otherwise(
            F.col("TP_DEPENDENCIA").cast("int")
        )
    )

if "source" not in fatos.columns:
    fatos = fatos.withColumn(
        "source",
        F.lit("oficial_inep")
    )

if "event_id" not in fatos.columns:
    fatos = fatos.withColumn(
        "event_id",
        F.lit(None).cast("string")
    )


# Validação antes de gerar o record_id
colunas_obrigatorias = [
    "ano",
    "sigla_uf",
    "id_municipio",
    "serie",
    "rede",
    "source",
    "event_id",
]

faltantes = [
    coluna
    for coluna in colunas_obrigatorias
    if coluna not in fatos.columns
]

if faltantes:
    raise RuntimeError(
        f"Silver com schema incompleto. "
        f"Colunas ausentes: {faltantes}. "
        f"Disponíveis: {fatos.columns}"
    )


# Chave determinística original
silver = (
    fatos

    .withColumn(
        "record_id",
        F.sha2(
            F.concat_ws(
                "|",

                F.col("ano").cast("string"),

                F.col("sigla_uf"),

                F.coalesce(
                    F.col("id_municipio"),
                    F.lit("uf")
                ),

                F.coalesce(
                    F.col("serie").cast("string"),
                    F.lit("na")
                ),

                F.col("rede").cast("string"),

                F.col("source"),

                F.coalesce(
                    F.col("event_id"),
                    F.lit("batch")
                )
            ),
            256
        )
    )

    .withColumn(
        "processed_at",
        F.current_timestamp()
    )

    .dropDuplicates(["record_id"])
)


# Publicação Delta
(
    silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(
            tbl("silver", "medicoes_alfabetizacao")
        )
)


# Validação
total = silver.count()

com_dim = (
    silver
    .filter(F.col("nome_uf").isNotNull())
    .count()
)

com_meta = (
    silver
    .filter(F.col("meta_taxa").isNotNull())
    .count()
)

print(
    f"✓ Silver gravada com {total:,} registros "
    f"| {com_dim:,} enriquecidos com dimensão UF "
    f"| {com_meta:,} com meta associada"
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Silver no grão de aluno para preparação da Fase 3

# COMMAND ----------

CATALOG = "workspace"

ALUNOS_BRONZE = f"{CATALOG}.bronze.alunos"
ALUNOS_SILVER = f"{CATALOG}.silver.alunos_modelagem"

DIM_MUNICIPIO = f"{CATALOG}.bronze.municipio"
DIM_UF = f"{CATALOG}.bronze.uf"

if not spark.catalog.tableExists(ALUNOS_BRONZE):
    raise RuntimeError(
        f"{ALUNOS_BRONZE} não existe. "
        "A Silver de alunos exige os microdados oficiais do INEP."
    )

alunos_raw = spark.table(ALUNOS_BRONZE)

print(f"Bronze alunos: {alunos_raw.count():,} registros")

# COMMAND ----------

# Normalização dos campos oficiais do TS_ALUNO.csv.
# Não agregamos: 1 linha continua representando 1 aluno.

alunos_base = (
    alunos_raw

    .select(
        F.col("ID_ALUNO").cast("string").alias("id_aluno"),
        F.col("NU_ANO_AVALIACAO").cast("int").alias("ano"),

        F.upper(
            F.trim(F.col("SG_UF"))
        ).alias("sigla_uf"),

        F.lpad(
            F.col("CO_MUNICIPIO").cast("string"),
            7,
            "0"
        ).alias("id_municipio"),

        F.col("NO_MUNICIPIO")
         .cast("string")
         .alias("nome_municipio_fonte"),

        F.col("TP_SERIE")
         .cast("int")
         .alias("serie"),

        F.col("ID_ESCOLA")
         .cast("string")
         .alias("id_escola"),

        F.col("TP_DEPENDENCIA")
         .cast("int")
         .alias("tp_dependencia"),

        F.col("IN_PRESENCA_LP")
         .cast("int")
         .alias("presenca_lp"),

        F.col("IN_PREENCHIMENTO_LP")
         .cast("int")
         .alias("preenchimento_lp"),

        F.col("CO_CADERNO_LP")
         .cast("string")
         .alias("caderno_lp"),

        F.regexp_replace(
            F.trim(F.col("VL_PESO_ALUNO_LP").cast("string")),
            ",",
            "."
        ).cast("double").alias("peso_aluno_lp"),

        F.regexp_replace(
            F.trim(F.col("VL_PROFICIENCIA_LP").cast("string")),
            ",",
            "."
        ).cast("double").alias("proficiencia_portugues"),

        F.col("IN_ALFABETIZADO")
         .cast("int")
         .alias("alfabetizado_oficial"),
    )

    # Convenção já utilizada pelo projeto:
    # TP_DEPENDENCIA 4 = privada -> rede 5.
    .withColumn(
        "rede",
        F.when(F.col("tp_dependencia") == 4, F.lit(5))
         .otherwise(F.col("tp_dependencia"))
         .cast("int")
    )

    .withColumn(
        "rede_label",
        rede_mapping[F.col("rede")]
    )

    # Regra de 743 preservada para validação.
    # NÃO deverá ser usada como feature do modelo da Fase 3.
    .withColumn(
        "alfabetizado_regra_743",
        F.when(
            F.col("proficiencia_portugues").isNotNull(),
            F.when(
                F.col("proficiencia_portugues") >= ALFABETIZACAO_CORTE,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )
)

# COMMAND ----------

# Enriquecimento com dimensão municipal.

dim_municipio_aluno = (
    spark.table(tbl("bronze", "municipio"))
    .select(
        F.lpad(
            F.col("id_municipio").cast("string"),
            7,
            "0"
        ).alias("id_municipio"),

        F.col("nome")
         .alias("nome_municipio"),

        F.upper(
            F.trim(F.col("sigla_uf"))
        ).alias("sigla_uf_dim"),

        F.col("capital")
         .cast("int")
         .alias("capital"),
    )
    .dropDuplicates(["id_municipio"])
)

alunos_base = (
    alunos_base

    .join(
        dim_municipio_aluno,
        on="id_municipio",
        how="left"
    )

    .withColumn(
        "uf_consistente",
        F.when(
            F.col("sigla_uf_dim").isNull(),
            F.lit(None).cast("boolean")
        ).otherwise(
            F.col("sigla_uf") == F.col("sigla_uf_dim")
        )
    )

    .drop("sigla_uf_dim")
)

# COMMAND ----------

# Enriquecimento com dimensão UF.

dim_uf_aluno = (
    spark.table(tbl("bronze", "uf"))
    .select(
        F.upper(
            F.trim(F.col("sigla_uf"))
        ).alias("sigla_uf"),

        F.col("nome")
         .alias("nome_uf"),

        F.col("regiao")
    )
    .dropDuplicates(["sigla_uf"])
)

alunos_base = alunos_base.join(
    dim_uf_aluno,
    on="sigla_uf",
    how="left"
)

# COMMAND ----------

# Identificador determinístico da observação de aluno.

alunos_silver = (
    alunos_base

    .withColumn(
        "record_id",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("ano").cast("string"),
                F.col("id_aluno"),
                F.coalesce(
                    F.col("id_escola"),
                    F.lit("sem_escola")
                )
            ),
            256
        )
    )

    .withColumn(
        "source",
        F.lit("inep_microdados_alfabetizacao_2024")
    )

    .withColumn(
        "fonte_dados",
        F.lit("oficial_inep")
    )

    .withColumn(
        "schema_version",
        F.lit("1.0")
    )

    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
)

# COMMAND ----------

# Publicação.
# Aqui NÃO fazemos dropDuplicates:
# duplicidade deve ser detectada pelo Quality Gate do notebook 06.

(
    alunos_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(ALUNOS_SILVER)
)

# COMMAND ----------

# Reconciliação mínima Bronze -> Silver.

rows_bronze_alunos = alunos_raw.count()
rows_silver_alunos = spark.table(ALUNOS_SILVER).count()

if rows_bronze_alunos != rows_silver_alunos:
    raise RuntimeError(
        "Falha de reconciliação da Silver de alunos: "
        f"bronze={rows_bronze_alunos:,} "
        f"silver={rows_silver_alunos:,}"
    )

targets_validos = (
    alunos_silver
    .filter(F.col("alfabetizado_oficial").isin([0, 1]))
    .count()
)

proficiencias_validas = (
    alunos_silver
    .filter(F.col("proficiencia_portugues").isNotNull())
    .count()
)

print("\n=== SILVER ALUNOS ===")
print(f"✓ Origem Bronze: {rows_bronze_alunos:,}")
print(f"✓ Silver alunos: {rows_silver_alunos:,}")
print(f"✓ Target oficial 0/1: {targets_validos:,}")
print(f"✓ Proficiência disponível: {proficiencias_validas:,}")
print(f"✓ Tabela: {ALUNOS_SILVER}")

# COMMAND ----------

dbutils.notebook.exit(
    f"Silver validada: alunos={rows_silver_alunos:,} linhas, "
    f"proficiência disponível={proficiencias_validas:,}"
)