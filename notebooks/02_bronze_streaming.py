# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 · Bronze Streaming
# MAGIC
# MAGIC **Pra que serve:** demonstra o caminho de streaming do pipeline. Ele
# MAGIC pega os dados municipais oficiais que já estão na Bronze e "reproduz"
# MAGIC eles como se fossem eventos chegando em tempo real (arquivos JSON),
# MAGIC depois consome esses eventos com Spark Structured Streaming.
# MAGIC
# MAGIC **Pré-requisito:** `01_bronze_batch` já ter rodado (ele lê
# MAGIC `bronze.avaliacao_alfabetizacao_municipio`).
# MAGIC
# MAGIC **O que ele cria:** a tabela `bronze.eventos_streaming`, com
# MAGIC deduplicação por `event_id` e MERGE idempotente (rodar de novo não
# MAGIC duplica dado).
# MAGIC
# MAGIC O notebook preserva a estratégia original:
# MAGIC - contrato explícito;
# MAGIC - quarentena;
# MAGIC - deduplicação por `event_id`;
# MAGIC - MERGE idempotente;
# MAGIC - `AvailableNow` por decisão de FinOps (processa o que tem disponível e
# MAGIC   termina sozinho — não fica rodando pra sempre).
# MAGIC
# MAGIC **Correção da Fase 2:** os eventos válidos não possuem mais taxa inventada.
# MAGIC Eles são construídos a partir de `workspace.bronze.avaliacao_alfabetizacao_municipio`.
# MAGIC O único payload artificial é propositalmente inválido e existe apenas para
# MAGIC comprovar o Quality Gate/quarentena. Ele nunca entra na tabela válida.

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

CATALOG = "workspace"
LANDING = f"/Volumes/{CATALOG}/bronze/streaming_landing/"
CHECKPOINT = f"/Volumes/{CATALOG}/observability/checkpoints/bronze_streaming/"
TARGET = f"{CATALOG}.bronze.eventos_streaming"
QUARANTINE = f"{CATALOG}.observability.quarantine_records"
SOURCE_TABLE = f"{CATALOG}.bronze.avaliacao_alfabetizacao_municipio"

try:
    RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
except Exception:
    RUN_ID = str(uuid.uuid4())

UFS = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
    "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"
]

SCHEMA_VERSION = "2.0"
SOURCE_NAME = "INEP_OFICIAL_REPLAY"

SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("schema_version", StringType(), True),
    StructField("ano", IntegerType(), True),
    StructField("sigla_uf", StringType(), True),
    StructField("id_municipio", StringType(), True),
    StructField("rede", IntegerType(), True),
    StructField("taxa_alfabetizacao", DoubleType(), True),
    StructField("source", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Producer replay de dados oficiais
# MAGIC
# MAGIC Em vez de gerar taxas aleatórias, selecionamos registros reais da tabela
# MAGIC municipal oficial já ingerida na Bronze.
# MAGIC
# MAGIC Em produção, o produtor seria substituído por Kafka/Event Hubs ou outro
# MAGIC barramento, mantendo o mesmo contrato do consumer.

# COMMAND ----------

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise RuntimeError(
        f"Fonte oficial obrigatória ausente: {SOURCE_TABLE}. "
        "Execute primeiro o notebook 01_bronze_batch.py."
    )

df_fonte = (
    spark.table(SOURCE_TABLE)
    .select(
        "ano",
        "sigla_uf",
        "id_municipio",
        "rede",
        "taxa_alfabetizacao",
    )
    .filter(
        F.col("ano").isNotNull()
        & F.col("sigla_uf").isNotNull()
        & F.col("id_municipio").isNotNull()
        & F.col("rede").isNotNull()
        & F.col("taxa_alfabetizacao").isNotNull()
    )
    .orderBy("ano", "sigla_uf", "id_municipio", "rede")
    .limit(8)
)

linhas_oficiais = df_fonte.collect()

if len(linhas_oficiais) < 8:
    raise RuntimeError(
        "A fonte municipal oficial não possui 8 registros válidos "
        "para a demonstração do streaming."
    )


def event_id_oficial(row) -> str:
    """
    ID determinístico para garantir idempotência entre reexecuções.
    UUID5 é derivado exclusivamente das chaves/valor da medição oficial.
    """
    chave = (
        f"{row['ano']}|{row['sigla_uf']}|{row['id_municipio']}|"
        f"{row['rede']}|{row['taxa_alfabetizacao']}"
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, chave))


def evento_de_linha_oficial(row) -> dict:
    return {
        "event_id": event_id_oficial(row),
        # Momento do replay/ingestão, não data da medição original.
        "event_time": datetime.now(timezone.utc).isoformat(),
        "schema_version": SCHEMA_VERSION,
        "ano": int(row["ano"]),
        "sigla_uf": str(row["sigla_uf"]),
        "id_municipio": str(row["id_municipio"]),
        "rede": int(row["rede"]),
        # Mantém exatamente a unidade/valor presente na fonte oficial Bronze.
        "taxa_alfabetizacao": float(row["taxa_alfabetizacao"]),
        "source": SOURCE_NAME,
    }


def publicar(evento: dict, sufixo: str = ""):
    # RUN_ID entra no nome físico para que o file source reconheça cada replay
    # como novo arquivo; a idempotência lógica continua sendo pelo event_id.
    caminho = (
        f"{LANDING}"
        f"event-{RUN_ID}-{evento['event_id']}{sufixo}.json"
    )
    dbutils.fs.put(
        caminho,
        json.dumps(evento, ensure_ascii=False),
        overwrite=True,
    )


eventos = [evento_de_linha_oficial(row) for row in linhas_oficiais]

# 8 eventos válidos baseados em dados oficiais
for ev in eventos:
    publicar(ev)

# 1 duplicata proposital: mesmo event_id, para comprovar deduplicação
publicar(dict(eventos[0]), sufixo="-dup")

# 1 payload propositalmente inválido para testar contrato/quarentena.
# Parte de um registro real, mas altera apenas a UF para um valor impossível.
# Esse registro NÃO é fato analítico e nunca é aceito no TARGET.
evento_invalido = dict(eventos[1])
evento_invalido["event_id"] = str(
    uuid.uuid5(
        uuid.NAMESPACE_URL,
        f"quality-test-invalid-{eventos[1]['event_id']}",
    )
)
evento_invalido["sigla_uf"] = "XX"
evento_invalido["source"] = "TESTE_CONTRATO_INVALIDO"
publicar(evento_invalido, sufixo="-invalid")

print(
    f"→ 10 eventos publicados em {LANDING}: "
    "8 oficiais + 1 duplicado + 1 inválido de teste"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consumer - contrato + quarentena + deduplicação

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
    event_id STRING,
    event_time TIMESTAMP,
    schema_version STRING,
    ano INT,
    sigla_uf STRING,
    id_municipio STRING,
    rede INT,
    taxa_alfabetizacao DOUBLE,
    source STRING,
    _source_file STRING,
    _ingestion_timestamp TIMESTAMP,
    _pipeline_run_id STRING
) USING DELTA
COMMENT 'Replay streaming de medições oficiais INEP, deduplicado por event_id.'
""")

# Limpeza de legado: remove somente eventos válidos criados pelo simulador antigo.
# Não afeta fontes oficiais nem registros de outras origens.
spark.sql(
    f"DELETE FROM {TARGET} WHERE source = 'simulador_medicoes'"
)

# O indicador oficial está em percentual (0 a 100), e não em fração 0 a 1.
regras_validade = (
    F.col("event_id").isNotNull()
    & F.col("event_time").isNotNull()
    & F.col("schema_version").isNotNull()
    & F.col("ano").between(2000, 2100)
    & F.col("sigla_uf").isin(UFS)
    & F.col("id_municipio").rlike("^[0-9]{7}$")
    & F.col("rede").isin(0, 2, 3, 5)
    & F.col("taxa_alfabetizacao").between(0.0, 100.0)
    & (F.col("source") == SOURCE_NAME)
)


def process_batch(df, batch_id):
    session = df.sparkSession

    df = (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_pipeline_run_id", F.lit(RUN_ID))
        .dropDuplicates(["event_id"])
    )

    is_valido = F.coalesce(regras_validade, F.lit(False))
    invalidos = df.filter(~is_valido)
    validos = df.filter(is_valido)

    if invalidos.limit(1).count() > 0:
        (
            invalidos
            .select(
                F.lit(RUN_ID).alias("run_id"),
                F.lit("02_bronze_streaming").alias("task_name"),
                F.lit("contrato_violado").alias("rejection_reason"),
                F.to_json(F.struct(*SCHEMA.fieldNames())).alias("payload"),
                F.col("_ingestion_timestamp").alias("ingestion_timestamp"),
            )
            .write
            .mode("append")
            .saveAsTable(QUARANTINE)
        )

    from delta.tables import DeltaTable

    (
        DeltaTable.forName(session, TARGET)
        .alias("t")
        .merge(validos.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Execução com AvailableNow
# MAGIC
# MAGIC `AvailableNow` processa os arquivos disponíveis e encerra o cluster do
# MAGIC stream quando o backlog termina. A estratégia é mantida por FinOps:
# MAGIC não há justificativa para manter compute de streaming continuamente
# MAGIC ativo neste cenário acadêmico/batch-like.

# COMMAND ----------

query = (
    spark.readStream
    .schema(SCHEMA)
    .json(LANDING)
    .select("*", F.col("_metadata.file_path").alias("_source_file"))
    .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .foreachBatch(process_batch)
    .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validação

# COMMAND ----------

# Os event_id dos 8 eventos oficiais são determinísticos.
# Em uma reexecução idempotente, o MERGE pode inserir 0 linhas novas porque
# os mesmos 8 eventos já existem no TARGET. Portanto o Quality Gate deve
# validar a PRESENÇA dos 8 eventos esperados, e não exigir 8 inserts por run.
event_ids_esperados = [ev["event_id"] for ev in eventos]

eventos_esperados_no_target = (
    spark.table(TARGET)
    .filter(
        (F.col("source") == SOURCE_NAME)
        & F.col("event_id").isin(event_ids_esperados)
    )
    .select("event_id")
    .distinct()
    .count()
)

inseridos_neste_run = (
    spark.table(TARGET)
    .filter(F.col("_pipeline_run_id") == RUN_ID)
    .count()
)

quar_run = (
    spark.table(QUARANTINE)
    .filter(
        (F.col("run_id") == RUN_ID)
        & (F.col("task_name") == "02_bronze_streaming")
    )
    .count()
)

total_oficial = (
    spark.table(TARGET)
    .filter(F.col("source") == SOURCE_NAME)
    .count()
)

print(f"✓ eventos oficiais esperados presentes no TARGET: {eventos_esperados_no_target}/8")
print(f"✓ novos eventos inseridos neste run: {inseridos_neste_run}")
print(f"✓ eventos inválidos deste run na quarentena: {quar_run}")
print(f"✓ eventos oficiais acumulados em {TARGET}: {total_oficial}")

if eventos_esperados_no_target != 8:
    raise RuntimeError(
        "Quality Gate streaming falhou: "
        f"esperado encontrar 8 event_id oficiais no TARGET, "
        f"encontrados {eventos_esperados_no_target}."
    )

if quar_run < 1:
    raise RuntimeError(
        "Quality Gate streaming falhou: o payload inválido de teste "
        "não chegou à quarentena."
    )

print(
    "\n✓ Streaming validado: fonte oficial, contrato ativo, "
    "deduplicação/idempotência comprovada e payload inválido em quarentena."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Observação de arquitetura
# MAGIC
# MAGIC Este notebook demonstra o caminho de streaming sem transformar uma
# MAGIC simulação em fonte de verdade. A fonte analítica permanece o INEP.
# MAGIC O replay apenas adapta registros oficiais ao contrato de evento.
# MAGIC
# MAGIC Em produção:
# MAGIC
# MAGIC `INEP / sistema produtor → Kafka/Event Hubs → Structured Streaming → Bronze`

# COMMAND ----------

total_eventos = spark.table(TARGET).count()
dbutils.notebook.exit(f"eventos_streaming: {total_eventos:,} linhas")