# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver canônica
# MAGIC Normaliza chaves, **integra as seis fontes do edital** e publica o modelo
# MAGIC canônico:
# MAGIC 1. medições batch (INEP, grão UF) + streaming (grão município) — fatos;
# MAGIC 2. `bronze.municipio` e `bronze.uf` — dimensões territoriais (join);
# MAGIC 3. `bronze.meta_brasil`, `bronze.meta_uf`, `bronze.meta_municipio` — metas
# MAGIC    associadas a cada medição conforme o grão (join);
# MAGIC 4. `bronze.alunos` — agregado por ano+UF+rede como enriquecimento (join).
# MAGIC
# MAGIC Cada registro sai com `fonte_dados` ('oficial_inep' | 'simulado') para que
# MAGIC os consumidores saibam distinguir dado real de dado do simulador.

# COMMAND ----------
CATALOG = "workspace"
import sys
from pyspark.sql import functions as F

# Constantes compartilhadas em src/ (evita drift entre notebooks). Em execução
# via Databricks Repos o repositório está no sys.path do workspace; se o import
# falhar (ex.: notebook importado avulso), usa o fallback inline equivalente.
try:
    sys.path.append("..")
    from src.utils import REDE_MAP, ALFABETIZACAO_CORTE, ALFABETIZACAO_RULE_VERSION
except Exception:
    REDE_MAP = {0: "total", 2: "estadual", 3: "municipal", 5: "privada"}
    ALFABETIZACAO_CORTE = 743
    ALFABETIZACAO_RULE_VERSION = "1.0"

rede_mapping = F.create_map([F.lit(x) for pair in REDE_MAP.items() for x in pair])

def tbl(schema, name):
    return f"{CATALOG}.{schema}.{name}"

def existe(schema, name):
    return spark.catalog.tableExists(tbl(schema, name))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Fatos — batch (grão UF, dado oficial)

# COMMAND ----------
batch = spark.table(tbl("bronze", "avaliacao_alfabetizacao"))

# A fonte batch (avaliação SAEB agregada) tem grão UF e não possui id_municipio.
# `taxa_alfabetizacao` chega em percentual (0-100) e é normalizada para 0-1
# (contrato, seção 2); o streaming já chega em fração.
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
# MAGIC ## 1b. Fatos — batch municipal (dado oficial)
# MAGIC A Base dos Dados publica o indicador também no grão município
# MAGIC (`br_inep_indicador_crianca_alfabetizada`, tabela `municipio`), o que permite
# MAGIC alimentar os marts municipais com dado oficial e não apenas simulado.
# MAGIC
# MAGIC A fonte municipal não possui `sigla_uf`: é identificada apenas por
# MAGIC `id_municipio`. A UF é derivada aqui por join com a dimensão
# MAGIC `bronze.municipio`. Sem ela, não há como associar a meta estadual (seção 5),
# MAGIC agregar por UF na Gold nem compor o `record_id` (seção 7).

# COMMAND ----------
if existe("bronze", "avaliacao_alfabetizacao_municipio"):
    if not existe("bronze", "municipio"):
        raise RuntimeError(
            "bronze.municipio é obrigatória para derivar a sigla_uf do indicador "
            "municipal. Execute o notebook 01 antes da Silver."
        )

    # Dimensão território → UF. A sigla_uf eventualmente presente na Bronze é
    # descartada: a ingestão aplica schema por posição e a fonte não publica a
    # coluna, então o valor gravado ali não corresponde à UF.
    uf_por_municipio = (
        spark.table(tbl("bronze", "municipio"))
        .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .select("id_municipio", F.upper(F.trim(F.col("sigla_uf"))).alias("sigla_uf"))
        .dropDuplicates(["id_municipio"])
    )

    batch_mun = (
        spark.table(tbl("bronze", "avaliacao_alfabetizacao_municipio"))
        .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .drop("sigla_uf")
        .join(uf_por_municipio, on="id_municipio", how="left")
    )

    def opcional(nome, tipo):
        """Colunas que podem não existir no CSV publicado (ex.: media_portugues)."""
        return (F.col(nome) if nome in batch_mun.columns else F.lit(None)).cast(tipo)


    batch_mun_canonical = (
        batch_mun
        .withColumn("grao", F.lit("municipio"))
        .withColumn("serie", opcional("serie", "int"))
        .withColumn("rede", opcional("rede", "int"))
        .withColumn("rede_label", rede_mapping[F.col("rede")])
        .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double") / 100.0)
        .withColumn("media_portugues", opcional("media_portugues", "double"))
        .withColumn("alfabetizado", F.when(F.col("media_portugues").isNotNull(),
                                           F.col("media_portugues") >= ALFABETIZACAO_CORTE))
        .withColumn("event_id", F.lit(None).cast("string"))
        .withColumn("event_time", F.lit(None).cast("timestamp"))
        .withColumn("source", F.lit("batch_inep_municipio"))
        .withColumn("fonte_dados", F.lit("oficial_inep"))
        .withColumn("schema_version", F.lit("1.0"))
    )

    total_mun = batch_mun_canonical.count()
    sem_uf = batch_mun_canonical.filter(F.col("sigla_uf").isNull()).count()
    print(f"dado oficial municipal: {total_mun:,} registros | "
          f"{total_mun - sem_uf:,} com UF resolvida pela dimensão")
    if sem_uf:
        print(f"AVISO: {sem_uf:,} registros sem correspondência em bronze.municipio. "
              "Seguem sem UF e são acusados na checagem de integridade referencial "
              "do Quality Gate (notebook 06).")
else:
    batch_mun_canonical = None
    print("AVISO: bronze.avaliacao_alfabetizacao_municipio não existe. Os marts "
          "municipais usarão apenas eventos do simulador (fonte_dados='simulado').")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Fatos — streaming (grão município, dado simulado)

# COMMAND ----------
if existe("bronze", "eventos_streaming"):
    events = spark.table(tbl("bronze", "eventos_streaming"))
    stream_canonical = (
        events
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("id_municipio", F.lpad(F.col("id_municipio").cast("string"), 7, "0"))
        .withColumn("grao", F.lit("municipio"))
        .withColumn("serie", F.lit(None).cast("int"))
        .withColumn("rede", F.col("rede").cast("int"))
        .withColumn("rede_label", rede_mapping[F.col("rede")])
        .withColumn("media_portugues", F.lit(None).cast("double"))
        .withColumn("alfabetizado", F.lit(None).cast("boolean"))
        .withColumn("fonte_dados", F.lit("simulado"))
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
# MAGIC Joins com `bronze.municipio` e `bronze.uf` — aqui ocorre a integração das
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

# meta do grão da medição: município usa meta municipal, UF usa meta estadual
fatos = fatos.withColumn(
    "meta_taxa",
    F.when(F.col("grao") == "municipio", F.coalesce("meta_municipio", "meta_uf"))
     .otherwise(F.col("meta_uf")),
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5b. Reconciliação das metas
# MAGIC O join da seção 5 é `left`, portanto uma meta sem fato correspondente é
# MAGIC descartada sem gerar erro. Esta célula compara o total de metas na Bronze
# MAGIC com o total efetivamente associado e lista os anos órfãos.
# MAGIC
# MAGIC Metas de anos ainda sem medição publicada são órfãs esperadas. O objetivo da
# MAGIC reconciliação é tornar a perda observável, não eliminá-la.

# COMMAND ----------
# Sem cache(): PERSIST/CACHE TABLE não é suportado em compute serverless. A
# reavaliação é aceitável — são três colunas distintas sobre um volume pequeno.
chaves_fato = fatos.select("ano", "sigla_uf", "id_municipio").distinct()

def reconciliar_meta(tabela, meta_df, chaves):
    if meta_df is None:
        return
    alvo = chaves_fato.select(*chaves).distinct()
    consumidas = meta_df.join(alvo, chaves, "inner").count()
    orfas = meta_df.join(alvo, chaves, "left_anti")
    total, n_orfas = meta_df.count(), orfas.count()
    print(f"\nbronze.{tabela}: {total:,} metas | {consumidas:,} associadas a um fato "
          f"| {n_orfas:,} órfãs")
    if n_orfas:
        anos_orfaos = [r["ano"] for r in orfas.select("ano").distinct().orderBy("ano").collect()]
        print(f"  anos sem fato correspondente: {anos_orfaos}")
        print("  Anos futuros não possuem medição. Um ano com medição publicada "
              "listado acima indica falha de chave no join.")

print("Reconciliação das metas (Bronze -> Silver)")
reconciliar_meta("meta_brasil", meta_br if existe("bronze", "meta_brasil") else None, ["ano"])
reconciliar_meta("meta_uf", meta_uf if existe("bronze", "meta_uf") else None,
                 ["ano", "sigla_uf"])
reconciliar_meta("meta_municipio", meta_mun if existe("bronze", "meta_municipio") else None,
                 ["ano", "id_municipio"])

# Contrapartida: fatos sem meta associada. Os dois blocos fecham a conta nos dois
# sentidos.
sem_meta = fatos.filter(F.col("meta_taxa").isNull())
n_sem_meta = sem_meta.count()
if n_sem_meta:
    anos_sem_meta = [r["ano"] for r in sem_meta.select("ano").distinct().orderBy("ano").collect()]
    print(f"\n{n_sem_meta:,} fatos sem meta associada (anos: {anos_sem_meta}). "
          "Aparecem como 'Meta indisponível' no dashboard.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Integração com dados de alunos (agregado por ano + UF + rede)
# MAGIC Microdados de alunos agregados e associados como enriquecimento: média de
# MAGIC proficiência e % de alunos acima do corte de alfabetização (743).

# COMMAND ----------
if existe("bronze", "alunos"):
    agg_alunos = (
        spark.table(tbl("bronze", "alunos"))
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("rede", F.col("rede").cast("int"))
        .groupBy("ano", "sigla_uf", "rede")
        .agg(
            F.avg("proficiencia_portugues").alias("alunos_proficiencia_media"),
            F.avg(F.when(F.col("proficiencia_portugues") >= ALFABETIZACAO_CORTE, 1.0)
                   .otherwise(0.0)).alias("alunos_pct_alfabetizados"),
            F.count("*").alias("alunos_amostra"),
        )
    )
    fatos = fatos.join(agg_alunos, on=["ano", "sigla_uf", "rede"], how="left")
else:
    fatos = (fatos
             .withColumn("alunos_proficiencia_media", F.lit(None).cast("double"))
             .withColumn("alunos_pct_alfabetizados", F.lit(None).cast("double"))
             .withColumn("alunos_amostra", F.lit(None).cast("long")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Chave determinística, dedup e publicação

# COMMAND ----------
silver = (
    fatos
    .withColumn(
        "record_id",
        F.sha2(F.concat_ws("|",
            F.col("ano"), F.col("sigla_uf"),
            F.coalesce(F.col("id_municipio"), F.lit("uf")),
            F.coalesce(F.col("serie").cast("string"), F.lit("na")),
            F.col("rede"), F.col("source"),
            F.coalesce(F.col("event_id"), F.lit("batch"))), 256),
    )
    # Contrato, seção 4: a Silver versiona a regra de negócio junto com o dado,
    # para que o registro identifique sob qual corte foi classificado.
    .withColumn("alfabetizacao_rule_version", F.lit(ALFABETIZACAO_RULE_VERSION))
    .withColumn("processed_at", F.current_timestamp())
    .dropDuplicates(["record_id"])
)

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tbl("silver", "medicoes_alfabetizacao"))
)

total = silver.count()
com_dim = silver.filter(F.col("nome_uf").isNotNull()).count()
com_meta = silver.filter(F.col("meta_taxa").isNotNull()).count()
print(f"Silver gravada com {total:,} registros "
      f"| {com_dim:,} enriquecidos com dimensão UF "
      f"| {com_meta:,} com meta associada")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Silver de alunos — `silver.alunos_proficiencia` (grão: aluno)
# MAGIC A seção 6 consome os microdados de alunos apenas como enriquecimento agregado
# MAGIC do fato. A distribuição de proficiência em torno do corte de 743 é uma análise
# MAGIC própria e precisa ser servida pela Gold, sem que a camada de consumo leia a
# MAGIC Bronze.
# MAGIC
# MAGIC Esta tabela publica o grão de aluno canônico: chaves normalizadas, corte de
# MAGIC alfabetização aplicado por aluno (conforme o contrato, seção 4) e a versão da
# MAGIC regra registrada. A Gold agrega a distribuição a partir daqui (notebook 04).

# COMMAND ----------
if existe("bronze", "alunos"):
    alunos_silver = (
        spark.table(tbl("bronze", "alunos"))
        .withColumn("ano", F.col("ano").cast("int"))
        .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
        .withColumn("serie", F.col("serie").cast("int"))
        .withColumn("rede", F.col("rede").cast("int"))
        .withColumn("rede_label", rede_mapping[F.col("rede")])
        .withColumn("proficiencia_portugues", F.col("proficiencia_portugues").cast("double"))
        # O corte de 743 é definido por aluno (contrato, seção 4). A flag
        # `alfabetizado` da tabela de medições incide sobre uma média agregada e
        # tem interpretação distinta.
        .withColumn("alfabetizado",
                    F.when(F.col("proficiencia_portugues").isNotNull(),
                           F.col("proficiencia_portugues") >= ALFABETIZACAO_CORTE))
        .withColumn("alfabetizacao_rule_version", F.lit(ALFABETIZACAO_RULE_VERSION))
        .withColumn("source", F.lit("batch_alunos"))
        .withColumn("fonte_dados", F.lit("simulado"))
        .withColumn("schema_version", F.lit("1.0"))
        .withColumn("event_time", F.lit(None).cast("timestamp"))
        .withColumn("record_id",
                    F.sha2(F.concat_ws("|", F.col("aluno_id"), F.col("ano"),
                                       F.col("sigla_uf"), F.col("rede"),
                                       F.col("source")), 256))
        .withColumn("processed_at", F.current_timestamp())
        .select(
            "record_id", "aluno_id", "ano", "sigla_uf", "serie", "rede", "rede_label",
            "proficiencia_portugues", "alfabetizado", "alfabetizacao_rule_version",
            "source", "fonte_dados", "schema_version", "event_time", "processed_at",
        )
        .dropDuplicates(["record_id"])
    )

    (
        alunos_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tbl("silver", "alunos_proficiencia"))
    )
    spark.sql(f"COMMENT ON TABLE {tbl('silver', 'alunos_proficiencia')} IS "
              "'Proficiência por aluno (microdados SIMULADOS), corte 743 aplicado no grão "
              "de aluno. Fonte da gold.distribuicao_proficiencia.'")

    n_alunos = alunos_silver.count()
    n_alfab = alunos_silver.filter(F.col("alfabetizado")).count()
    print(f"silver.alunos_proficiencia: {n_alunos:,} alunos | "
          f"{n_alfab:,} acima do corte de {ALFABETIZACAO_CORTE} pontos")
else:
    print("AVISO: bronze.alunos não existe. silver.alunos_proficiencia não será "
          "publicada e a distribuição de proficiência ficará indisponível no dashboard.")
