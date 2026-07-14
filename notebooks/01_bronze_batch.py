# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Batch (P2)
# MAGIC Lê as fontes e grava em Delta (bruto). Bronze = sem transformação.
# MAGIC
# MAGIC ## Como subir os dados (P2)
# MAGIC 1. Menu lateral: **Catalog → workspace → bronze → raw_files**
# MAGIC 2. Clique em **Upload to this volume**
# MAGIC 3. Suba cada CSV e use o caminho `/Volumes/workspace/bronze/raw_files/nome_arquivo.csv`

# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)

import uuid

from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit
)

# run_id injetado pelo Workflow ({{job.run_id}} em workflows/job_pipeline.json)
# para correlacionar todas as tasks de uma execução; standalone gera um novo.
try:
    RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
except Exception:
    RUN_ID = str(uuid.uuid4())

CATALOG = "workspace"
VOLUME_RAW = f"/Volumes/{CATALOG}/bronze/raw_files"

# Schema explícito da Avaliação de Alfabetização
schema_avaliacao = StructType([
    StructField("ano", IntegerType(), False),
    StructField("sigla_uf", StringType(), False),
    StructField("serie", IntegerType(), False),
    StructField("rede", IntegerType(), False),
    StructField("taxa_alfabetizacao", DoubleType(), True),
    StructField("media_portugues", DoubleType(), True),
    StructField("proporcao_aluno_nivel_0", DoubleType(), True),
    StructField("proporcao_aluno_nivel_1", DoubleType(), True),
    StructField("proporcao_aluno_nivel_2", DoubleType(), True),
    StructField("proporcao_aluno_nivel_3", DoubleType(), True),
    StructField("proporcao_aluno_nivel_4", DoubleType(), True),
    StructField("proporcao_aluno_nivel_5", DoubleType(), True),
    StructField("proporcao_aluno_nivel_6", DoubleType(), True),
    StructField("proporcao_aluno_nivel_7", DoubleType(), True),
    StructField("proporcao_aluno_nivel_8", DoubleType(), True)
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Avaliação Alfabetização (SAEB)

# COMMAND ----------
df_avaliacao = (
    spark.read
        .option("header", True)
        .schema(schema_avaliacao)
        .csv(f"{VOLUME_RAW}/br_inep_avaliacao_alfabetizacao_uf.csv.gz")
)

print("Schema aplicado com sucesso:")

# _metadata.file_path substitui input_file_name(), que não é suportado no
# compute serverless com Unity Catalog.
df_avaliacao = (
    df_avaliacao
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("source_system", lit("basedosdados_inep"))
        .withColumn("pipeline_run_id", lit(RUN_ID))
        .withColumn("schema_version", lit("1.0"))
)

df_avaliacao.printSchema()
(
    df_avaliacao.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        # FinOps: tabela de ~150 linhas — particionar só criaria overhead de
        # arquivos pequenos (ver seção FinOps do README).
        # Idempotência: overwrite evita duplicar a fonte a cada reexecução;
        # o histórico de versões é preservado pelo Delta (time travel /
        # DESCRIBE HISTORY), atendendo ao requisito de histórico da Bronze.
        .saveAsTable(f"{CATALOG}.bronze.avaliacao_alfabetizacao")
)

origem = df_avaliacao.count()

print(f"✓ avaliacao_alfabetizacao gravada: {origem:,} linhas")

destino = spark.table(
    f"{CATALOG}.bronze.avaliacao_alfabetizacao"
).count()

print(f"Origem : {origem}")
print(f"Destino: {destino}")

if origem == destino:
    print("✓ Reconciliação realizada com sucesso.")
else:
    raise Exception(
        f"Falha na reconciliação: origem={origem}, destino={destino}"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Demais fontes (P2)

# COMMAND ----------
# Schemas explícitos (contrato, seção 2): evita drift silencioso de tipos que
# o inferSchema causaria entre execuções. Coerente com a prática do arquivo 1.
schema_uf = StructType([
    StructField("codigo_uf", IntegerType(), True),
    StructField("sigla_uf", StringType(), False),
    StructField("nome", StringType(), True),
    StructField("regiao", StringType(), True),
])
schema_municipio = StructType([
    StructField("id_municipio", StringType(), False),
    StructField("nome", StringType(), True),
    StructField("sigla_uf", StringType(), False),
    StructField("capital", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])
schema_meta_brasil = StructType([
    StructField("ano", IntegerType(), False),
    StructField("meta", DoubleType(), False),
    StructField("metodologia", StringType(), True),
])
schema_meta_uf = StructType([
    StructField("sigla_uf", StringType(), False),
    StructField("ano", IntegerType(), False),
    StructField("meta", DoubleType(), False),
    StructField("metodologia", StringType(), True),
])
schema_meta_municipio = StructType([
    StructField("id_municipio", StringType(), False),
    StructField("sigla_uf", StringType(), False),
    StructField("ano", IntegerType(), False),
    StructField("meta", DoubleType(), False),
    StructField("metodologia", StringType(), True),
])
schema_alunos = StructType([
    StructField("aluno_id", StringType(), False),
    StructField("ano", IntegerType(), False),
    StructField("sigla_uf", StringType(), False),
    StructField("serie", IntegerType(), True),
    StructField("rede", IntegerType(), True),
    StructField("proficiencia_portugues", DoubleType(), True),
    StructField("fonte", StringType(), True),
])

# FinOps: nenhuma dessas tabelas é particionada — todas são pequenas (27 a
# ~5.5k linhas) e particionar só criaria overhead de small files (ver README).
arquivos_p2 = {
    "uf":             (f"{VOLUME_RAW}/uf.csv",             schema_uf),
    "municipio":      (f"{VOLUME_RAW}/municipio.csv",      schema_municipio),
    "meta_brasil":    (f"{VOLUME_RAW}/meta_brasil.csv",    schema_meta_brasil),
    "meta_uf":        (f"{VOLUME_RAW}/meta_uf.csv",        schema_meta_uf),
    "meta_municipio": (f"{VOLUME_RAW}/meta_municipio.csv", schema_meta_municipio),
    "alunos":         (f"{VOLUME_RAW}/alunos_simulados.csv.gz", schema_alunos),
}

for tabela, (path, schema) in arquivos_p2.items():
    try:
        df = (
            spark.read
                .option("header", True)
                .schema(schema)
                .csv(path)
                .withColumn("ingestion_timestamp", current_timestamp())
                .withColumn("source_file", col("_metadata.file_path"))
                .withColumn("pipeline_run_id", lit(RUN_ID))
        )

        (
            df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(f"{CATALOG}.bronze.{tabela}")
        )

        print(f"✓ {tabela}: {df.count():,} linhas")

    except Exception as e:
        print(f"⚠ {tabela}: arquivo não encontrado — aguardando P2 ({e})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Indicador oficial no grão MUNICÍPIO
# MAGIC A Base dos Dados publica o Indicador Criança Alfabetizada também por
# MAGIC município (`basedosdados.br_inep_indicador_crianca_alfabetizada`, tabela
# MAGIC `municipio`), versionado em `data/raw/`. Com ele, a Silver (03) alimenta os
# MAGIC marts municipais com dado OFICIAL, não só simulado.
# MAGIC
# MAGIC **A fonte não traz `sigla_uf`** — é identificada só por `id_municipio`. A UF
# MAGIC é derivada na Silver pelo join com a dimensão `bronze.municipio` (IBGE).
# MAGIC Declarar `sigla_uf` no schema abaixo seria pior do que inútil: veja o
# MAGIC comentário da leitura.

# COMMAND ----------
# Contrato da Bronze municipal. `sigla_uf` NÃO entra aqui porque não existe na
# fonte — e as colunas proporcao_aluno_nivel_* do CSV não são usadas.
schema_avaliacao_mun = StructType([
    StructField("ano", IntegerType(), False),
    StructField("id_municipio", StringType(), False),
    StructField("serie", IntegerType(), True),
    StructField("rede", IntegerType(), True),
    StructField("taxa_alfabetizacao", DoubleType(), True),
    StructField("media_portugues", DoubleType(), True),
])

try:
    # Leitura por NOME, não por posição.
    #
    # `spark.read.schema(...).csv(...)` casa os campos pela ORDEM do arquivo, não
    # pelo nome: o `header=True` apenas descarta a primeira linha. Como este CSV
    # tem 15 colunas e não possui `sigla_uf`, declará-la no schema faria com que
    # ela recebesse o valor da coluna que estivesse naquela posição — no caso,
    # `proporcao_aluno_nivel_0`. O resultado seria uma UF corrompida (ou nula)
    # gravada na Bronze, sem erro nenhum.
    #
    # Lendo tudo como string e selecionando pelo nome do header, a ingestão fica
    # imune à ordem e a colunas extras na origem. Os tipos continuam explícitos:
    # vêm do schema acima, aplicados por `cast`.
    bruto_mun = (
        spark.read
            .option("header", True)
            .csv(f"{VOLUME_RAW}/br_inep_avaliacao_alfabetizacao_municipio.csv.gz")
    )

    obrigatorias = {"ano", "id_municipio"}
    ausentes = obrigatorias - set(bruto_mun.columns)
    if ausentes:
        raise ValueError(
            f"colunas obrigatórias ausentes no CSV municipal: {sorted(ausentes)} "
            f"(header lido: {bruto_mun.columns})"
        )

    df_mun = (
        bruto_mun
            .select(*[
                (col(campo.name) if campo.name in bruto_mun.columns else lit(None))
                .cast(campo.dataType).alias(campo.name)
                for campo in schema_avaliacao_mun.fields
            ])
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("source_system", lit("basedosdados_inep"))
            .withColumn("pipeline_run_id", lit(RUN_ID))
            .withColumn("schema_version", lit("1.0"))
    )
    (df_mun.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.bronze.avaliacao_alfabetizacao_municipio"))
    print(f"✓ avaliacao_alfabetizacao_municipio: {df_mun.count():,} linhas (dado oficial)")
except Exception as e:
    print("⚠ indicador municipal oficial não ingerido — os marts municipais ficarão "
          f"apenas com dado simulado ({type(e).__name__}: {e})")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------
tabelas = [
    "avaliacao_alfabetizacao",
    "avaliacao_alfabetizacao_municipio",
    "uf",
    "municipio",
    "meta_brasil",
    "meta_uf",
    "meta_municipio",
    "alunos",
]

for tabela in tabelas:
    try:
        quantidade = spark.table(f"{CATALOG}.bronze.{tabela}").count()
        print(f"✓ {tabela}: {quantidade:,} linhas")
    except Exception as e:
        print(f"✗ {tabela}: {e}")

# A Bronze municipal NÃO tem sigla_uf, por decisão de contrato: a fonte não a
# publica. Este check existe para impedir que alguém "conserte" isso declarando
# a coluna no schema — ela seria preenchida por posição com o valor de outra
# coluna, corrompendo a UF em silêncio. A UF é derivada na Silver (notebook 03).
TBL_MUN = f"{CATALOG}.bronze.avaliacao_alfabetizacao_municipio"
if spark.catalog.tableExists(TBL_MUN):
    if "sigla_uf" in spark.table(TBL_MUN).columns:
        raise AssertionError(
            f"{TBL_MUN} tem sigla_uf, mas a fonte não publica essa coluna — o schema "
            "explícito a preencheu por POSIÇÃO com o valor de outro campo. Remova-a "
            "do schema; a UF é derivada na Silver via join com bronze.municipio."
        )
    print("✓ avaliacao_alfabetizacao_municipio sem sigla_uf (correto — derivada na Silver)")
