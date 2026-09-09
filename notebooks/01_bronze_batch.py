# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 · Bronze Batch
# MAGIC
# MAGIC **Pra que serve:** pega os arquivos oficiais que o `00_setup_ambiente`
# MAGIC já deixou prontos no Volume e grava cada um como uma tabela Delta na
# MAGIC camada Bronze, sem mudar nada do conteúdo (sem calcular, sem
# MAGIC interpolar, sem inventar dado).
# MAGIC
# MAGIC **Pré-requisito:** já ter rodado `00_setup_ambiente` com sucesso nesse
# MAGIC workspace (é ele quem publica os arquivos no Volume).
# MAGIC
# MAGIC **O que ele cria:** as tabelas `bronze.avaliacao_alfabetizacao`,
# MAGIC `bronze.avaliacao_alfabetizacao_municipio`, `bronze.uf`,
# MAGIC `bronze.municipio`, `bronze.meta_brasil`, `bronze.meta_uf`,
# MAGIC `bronze.meta_municipio` e `bronze.alunos`.
# MAGIC
# MAGIC **Sobre a tabela `alunos`:** o microdado oficial (`TS_ALUNO.csv`) é
# MAGIC obrigatório para esta versão do pipeline. Se o arquivo não estiver no
# MAGIC Volume, o notebook falha explicitamente para evitar uma Bronze incompleta
# MAGIC que produziria tabelas Silver/Gold vazias.
# MAGIC
# MAGIC **Regra:** a Bronze não gera, interpola ou simula dados.

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

# COMMAND ----------

# run_id injetado pelo Workflow ({{job.run_id}} em workflows/job_pipeline.json)
# para correlacionar todas as tasks de uma execução; standalone gera um novo.
try:
    RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
except Exception:
    RUN_ID = str(uuid.uuid4())

CATALOG = "workspace"
VOLUME_RAW = f"/Volumes/{CATALOG}/bronze/raw_files"
SCHEMA_VERSION = "2.0"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Avaliação de alfabetização - UF
# MAGIC
# MAGIC Fonte oficial do INEP distribuída via Base dos Dados.
# MAGIC O conteúdo é ingerido sem transformação de negócio.

# COMMAND ----------

schema_avaliacao_uf = StructType([
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

df_avaliacao_uf = (
    spark.read
        .option("header", True)
        .schema(schema_avaliacao_uf)
        .csv(f"{VOLUME_RAW}/br_inep_avaliacao_alfabetizacao_uf.csv.gz")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("source_system", lit("INEP via Base dos Dados"))
        .withColumn("pipeline_run_id", lit(RUN_ID))
        .withColumn("schema_version", lit(SCHEMA_VERSION))
)

df_avaliacao_uf.printSchema()

origem_uf = df_avaliacao_uf.count()

(
    df_avaliacao_uf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.bronze.avaliacao_alfabetizacao")
)

destino_uf = spark.table(
    f"{CATALOG}.bronze.avaliacao_alfabetizacao"
).count()

if origem_uf != destino_uf:
    raise RuntimeError(
        f"Falha de reconciliação UF: origem={origem_uf}, destino={destino_uf}"
    )

print(f"✓ avaliacao_alfabetizacao: {destino_uf:,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Avaliação de alfabetização - Município
# MAGIC
# MAGIC Indicador municipal oficial extraído da planilha do INEP por
# MAGIC `scripts/gerar_fontes.py`.
# MAGIC
# MAGIC Esta fonte agora é **obrigatória**: os marts municipais não podem ser
# MAGIC alimentados por dados simulados.

# COMMAND ----------

schema_avaliacao_municipio = StructType([
    StructField("ano", IntegerType(), False),
    StructField("sigla_uf", StringType(), False),
    StructField("id_municipio", StringType(), False),
    StructField("serie", IntegerType(), True),
    StructField("rede", IntegerType(), True),
    StructField("taxa_alfabetizacao", DoubleType(), True),
    StructField("media_portugues", DoubleType(), True),
])

df_avaliacao_municipio = (
    spark.read
        .option("header", True)
        .schema(schema_avaliacao_municipio)
        .csv(f"{VOLUME_RAW}/br_inep_avaliacao_alfabetizacao_municipio.csv.gz")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("source_system", lit("INEP oficial"))
        .withColumn("pipeline_run_id", lit(RUN_ID))
        .withColumn("schema_version", lit(SCHEMA_VERSION))
)

origem_municipio = df_avaliacao_municipio.count()

(
    df_avaliacao_municipio.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.bronze.avaliacao_alfabetizacao_municipio")
)

destino_municipio = spark.table(
    f"{CATALOG}.bronze.avaliacao_alfabetizacao_municipio"
).count()

if origem_municipio != destino_municipio:
    raise RuntimeError(
        "Falha de reconciliação do indicador municipal: "
        f"origem={origem_municipio}, destino={destino_municipio}"
    )

print(
    "✓ avaliacao_alfabetizacao_municipio: "
    f"{destino_municipio:,} linhas"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dimensões territoriais e metas oficiais
# MAGIC
# MAGIC - `uf.csv` e `municipio.csv`: dimensões territoriais IBGE.
# MAGIC - `meta_brasil.csv`, `meta_uf.csv`, `meta_municipio.csv`: metas oficiais do INEP.
# MAGIC
# MAGIC Nenhuma meta é calculada nesta camada.

# COMMAND ----------

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

arquivos_batch = {
    "uf": {
        "path": f"{VOLUME_RAW}/uf.csv",
        "schema": schema_uf,
        "source_system": "IBGE",
    },
    "municipio": {
        "path": f"{VOLUME_RAW}/municipio.csv",
        "schema": schema_municipio,
        "source_system": "IBGE",
    },
    "meta_brasil": {
        "path": f"{VOLUME_RAW}/meta_brasil.csv",
        "schema": schema_meta_brasil,
        "source_system": "INEP oficial",
    },
    "meta_uf": {
        "path": f"{VOLUME_RAW}/meta_uf.csv",
        "schema": schema_meta_uf,
        "source_system": "INEP oficial",
    },
    "meta_municipio": {
        "path": f"{VOLUME_RAW}/meta_municipio.csv",
        "schema": schema_meta_municipio,
        "source_system": "INEP oficial",
    },
}

for tabela, config in arquivos_batch.items():
    path = config["path"]
    schema = config["schema"]
    source_system = config["source_system"]

    df = (
        spark.read
            .option("header", True)
            .schema(schema)
            .csv(path)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("source_system", lit(source_system))
            .withColumn("pipeline_run_id", lit(RUN_ID))
            .withColumn("schema_version", lit(SCHEMA_VERSION))
    )

    origem = df.count()

    (
        df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{CATALOG}.bronze.{tabela}")
    )

    destino = spark.table(f"{CATALOG}.bronze.{tabela}").count()

    if origem != destino:
        raise RuntimeError(
            f"Falha de reconciliação {tabela}: "
            f"origem={origem}, destino={destino}"
        )

    print(f"✓ {tabela}: {destino:,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Microdados oficiais de alunos - INEP
# MAGIC
# MAGIC Ingestão bruta do arquivo oficial `TS_ALUNO.csv`.
# MAGIC A Bronze preserva os nomes e o conteúdo da fonte; normalizações e
# MAGIC mapeamentos de negócio pertencem à Silver.
# MAGIC
# MAGIC **Não existe fallback para dados simulados.**

# COMMAND ----------

from pathlib import Path

MICRO_ALUNOS = f"{VOLUME_RAW}/microdados_inep/DADOS/TS_ALUNO.csv"
MICRODADOS_DISPONIVEIS = Path(MICRO_ALUNOS).exists()

if not MICRODADOS_DISPONIVEIS:
    raise FileNotFoundError(
        f"TS_ALUNO.csv não foi disponibilizado em {MICRO_ALUNOS}. "
        "Execute o 00_setup_ambiente.py antes do 01_bronze_batch.py."
    )
else:
    # Schema real do arquivo oficial recebido (2º ano EF, avaliação em
    # organização complementar ao Saeb) - colunas conferidas diretamente
    # no cabeçalho do TS_ALUNO.csv. Todas mantidas como string na Bronze,
    # sem conversão de tipo: preserva exatamente a representação textual
    # da fonte oficial (ex.: CO_RESPOSTA_TEXTO traz valores como "TX"/"NL",
    # não numéricos). Conversão numérica ocorre na Silver.
    #
    # Atenção: este arquivo é da edição 2023 (ID_SAEB=2023), enquanto o
    # restante do pipeline (metas, indicador municipal) é de 2024 - registrar
    # essa diferença de ano na documentação/decisões do projeto.
    colunas_ts_aluno = [
        "ID_SAEB", "ID_REGIAO", "ID_UF", "ID_MUNICIPIO", "ID_AREA",
        "ID_ESCOLA", "IN_PUBLICA", "ID_LOCALIZACAO", "ID_TURMA", "ID_SERIE",
        "ID_ALUNO", "IN_SITUACAO_CENSO", "IN_PREENCHIMENTO_LP",
        "IN_PREENCHIMENTO_MT", "IN_PRESENCA_LP", "IN_PRESENCA_MT",
        "ID_CADERNO_LP", "ID_BLOCO_1_LP", "ID_BLOCO_2_LP",
        "NU_BLOCO_1_ABERTA_LP", "NU_BLOCO_2_ABERTA_LP", "ID_CADERNO_MT",
        "ID_BLOCO_1_MT", "ID_BLOCO_2_MT", "NU_BLOCO_1_ABERTA_MT",
        "NU_BLOCO_2_ABERTA_MT", "TX_RESP_BLOCO1_LP", "TX_RESP_BLOCO2_LP",
        "CO_CONCEITO_Q1_LP", "CO_CONCEITO_Q2_LP", "CO_RESPOSTA_TEXTO",
        "CO_CONCEITO_SEQUENCIA", "CO_CONCEITO_COESAO", "CO_CONCEITO_PONTUACAO",
        "CO_CONCEITO_SEGMENTACAO", "CO_TEXTO_GRAFIA", "TX_RESP_BLOCO1_MT",
        "TX_RESP_BLOCO2_MT", "CO_CONCEITO_Q1_MT", "CO_CONCEITO_Q2_MT",
        "IN_PROFICIENCIA_LP", "IN_PROFICIENCIA_MT", "IN_AMOSTRA", "ESTRATO",
        "PESO_ALUNO_LP", "IN_ALFABETIZADO", "PROFICIENCIA_LP",
        "ERRO_PADRAO_LP", "PROFICIENCIA_LP_SAEB", "ERRO_PADRAO_LP_SAEB",
        "PESO_ALUNO_MT", "PROFICIENCIA_MT", "ERRO_PADRAO_MT",
        "PROFICIENCIA_MT_SAEB", "ERRO_PADRAO_MT_SAEB",
    ]
    schema_alunos_oficial = StructType(
        [StructField(nome, StringType(), True) for nome in colunas_ts_aluno]
    )

    df_alunos = (
        spark.read
            .option("header", True)
            .option("sep", ";")
            .option("encoding", "UTF-8")
            .schema(schema_alunos_oficial)
            .csv(MICRO_ALUNOS)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
            .withColumn("source_system", lit("INEP oficial - avaliação da alfabetização 2023"))
            .withColumn("pipeline_run_id", lit(RUN_ID))
            .withColumn("schema_version", lit(SCHEMA_VERSION))
    )

    origem_alunos = df_alunos.count()

    if origem_alunos == 0:
        raise RuntimeError(
            f"TS_ALUNO.csv foi encontrado, mas não contém registros: {MICRO_ALUNOS}"
        )

    (
        df_alunos.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{CATALOG}.bronze.alunos")
    )

    destino_alunos = spark.table(
        f"{CATALOG}.bronze.alunos"
    ).count()

    if origem_alunos != destino_alunos:
        raise RuntimeError(
            "Falha de reconciliação alunos: "
            f"origem={origem_alunos}, destino={destino_alunos}"
        )

    print(
        f"✓ alunos: {destino_alunos:,} linhas "
        "(microdados oficiais INEP)"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação da Bronze
# MAGIC
# MAGIC Todas as fontes obrigatórias desta etapa devem existir.
# MAGIC Qualquer ausência encerra a execução com erro.

# COMMAND ----------

# Permite executar somente o bloco de validação em uma sessão em que as
# tabelas já existam, sem depender do estado das células anteriores.
CATALOG = globals().get("CATALOG", "workspace")

tabelas_obrigatorias = [
    "avaliacao_alfabetizacao",
    "avaliacao_alfabetizacao_municipio",
    "uf",
    "municipio",
    "meta_brasil",
    "meta_uf",
    "meta_municipio",
]

if MICRODADOS_DISPONIVEIS:
    tabelas_obrigatorias.append("alunos")
else:
    print(
        "⚠ alunos: microdados oficiais ainda pendentes, "
        "tabela não exigida nesta execução."
    )

falhas = []

for tabela in tabelas_obrigatorias:
    try:
        quantidade = spark.table(
            f"{CATALOG}.bronze.{tabela}"
        ).count()

        if quantidade <= 0:
            falhas.append(f"{tabela}: tabela vazia")
            print(f"✗ {tabela}: tabela vazia")
        else:
            print(f"✓ {tabela}: {quantidade:,} linhas")

    except Exception as e:
        falhas.append(f"{tabela}: {type(e).__name__}: {e}")
        print(f"✗ {tabela}: {type(e).__name__}: {e}")

if falhas:
    raise RuntimeError(
        "Bronze incompleta. Falhas encontradas:\n- "
        + "\n- ".join(falhas)
    )

print("\n✓ Bronze Batch concluída com todas as fontes oficiais.")

# COMMAND ----------

alunos_msg = f"{destino_alunos:,} linhas" if destino_alunos is not None else "pendente (microdados não disponibilizados)"
dbutils.notebook.exit(
    f"Bronze validada: alunos={alunos_msg}, "
    f"{len(tabelas_obrigatorias)} tabelas obrigatórias confirmadas"
)