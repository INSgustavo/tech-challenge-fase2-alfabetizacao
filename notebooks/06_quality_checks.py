# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 · Quality Gate
# MAGIC
# MAGIC **Pra que serve:** é o "fiscal" do pipeline. Confere se os dados da
# MAGIC Silver são confiáveis o suficiente pra virar Gold. Se não forem, ele
# MAGIC **bloqueia** a Gold de propósito — isso é o comportamento correto, não
# MAGIC é bug.
# MAGIC
# MAGIC **Pré-requisito:** `03_silver.py` já ter rodado.
# MAGIC
# MAGIC **Se ele reprovar** (mensagem tipo "Quality Gate reprovado" /
# MAGIC "cobertura mínima"): olhe a tabela de quarentena
# MAGIC (`observability.quarantine_records`) pra ver o motivo exato de cada
# MAGIC linha rejeitada, com a coluna `rejection_reason`.
# MAGIC
# MAGIC O que este notebook faz:
# MAGIC 1. Valida **integridade referencial** contra as dimensões (`bronze.municipio`
# MAGIC    e `bronze.uf`) e a consistência entre tabelas (UF × município).
# MAGIC 2. Aplica validações **por registro** e move os reprovados para
# MAGIC    `observability.quarantine_records` com `rejection_reason`.
# MAGIC 3. Publica os registros aprovados em `silver.medicoes_aprovadas`
# MAGIC    (fonte da Gold).
# MAGIC 4. Aplica validações **sistêmicas** (volume, cobertura, duplicidade).
# MAGIC    Se alguma falhar, a task **reprova** e a Gold não é sobrescrita.
# MAGIC 5. Registra a execução em `observability.pipeline_metrics`.

# COMMAND ----------

CATALOG = "workspace"
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

# run_id pode ser injetado pelo Workflow (widget); senão, gera um novo.
try:
    RUN_ID = dbutils.widgets.get("run_id") or str(uuid.uuid4())
except Exception:
    RUN_ID = str(uuid.uuid4())
TASK = "06_quality_gate"
started_at = datetime.now(timezone.utc)

SILVER = f"{CATALOG}.silver.medicoes_alfabetizacao"
APROVADA = f"{CATALOG}.silver.medicoes_aprovadas"

# Limite mínimo de cobertura definido pelo grupo (aprovados / lidos).
COBERTURA_MIN = 0.80

UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
]

s = spark.table(SILVER)
rows_read = s.count()
print(f"Silver lida: {rows_read:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Integridade referencial contra as dimensões
# MAGIC Valida as chaves de relacionamento exigidas pelo edital:
# MAGIC - `id_municipio` deve existir em `bronze.municipio` (grão municipal);
# MAGIC - `sigla_uf` deve existir em `bronze.uf`;
# MAGIC - consistência entre tabelas: a UF do registro deve bater com a UF do
# MAGIC   município na dimensão (flag `uf_consistente` calculada na Silver).

# COMMAND ----------

DIM_MUN = f"{CATALOG}.bronze.municipio"
DIM_UF = f"{CATALOG}.bronze.uf"

if spark.catalog.tableExists(DIM_MUN):
    ids_validos = (spark.table(DIM_MUN)
                   .select(F.lpad(F.col("id_municipio").cast("string"), 7, "0")
                           .alias("id_municipio"))
                   .distinct()
                   .withColumn("_fk_municipio_ok", F.lit(True)))
    s = s.join(ids_validos, on="id_municipio", how="left")
else:
    print(f"⚠ {DIM_MUN} não existe - check de FK municipal não aplicado.")
    s = s.withColumn("_fk_municipio_ok", F.lit(True))

if spark.catalog.tableExists(DIM_UF):
    ufs_dim = (spark.table(DIM_UF)
               .select(F.upper(F.trim(F.col("sigla_uf"))).alias("sigla_uf"))
               .distinct()
               .withColumn("_fk_uf_ok", F.lit(True)))
    s = s.join(ufs_dim, on="sigla_uf", how="left")
else:
    print(f"⚠ {DIM_UF} não existe - check de FK de UF não aplicado.")
    s = s.withColumn("_fk_uf_ok", F.lit(True))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validações por registro → motivo de rejeição
# MAGIC A primeira regra violada define o `rejection_reason` do registro.

# COMMAND ----------

# id_municipio é obrigatório apenas no grão municipal (streaming): a fonte
# batch da avaliação tem grão UF e não traz município (contrato, seção 3).
rejection_reason = (
    F.when(
        F.col("ano").isNull() | F.col("sigla_uf").isNull()
        | F.col("rede").isNull() | F.col("record_id").isNull(),
        F.lit("campo_critico_nulo"),
    )
    .when((F.col("grao") == "municipio") & F.col("id_municipio").isNull(),
          F.lit("municipio_nulo_no_grao_municipal"))
    .when(~F.col("sigla_uf").rlike("^[A-Z]{2}$") | ~F.col("sigla_uf").isin(UFS_VALIDAS),
          F.lit("sigla_uf_invalida"))
    .when(F.col("id_municipio").isNotNull()
          & ~F.col("id_municipio").rlike("^[0-9]{7}$"), F.lit("id_municipio_invalido"))
    .when(~F.col("rede").isin([0, 2, 3, 5]), F.lit("rede_fora_do_dominio"))
    .when(F.col("taxa_alfabetizacao").isNotNull()
          & ~F.col("taxa_alfabetizacao").between(0.0, 1.0), F.lit("taxa_fora_do_dominio"))
    # integridade referencial (edital: validação de chaves de relacionamento)
    .when(F.col("id_municipio").isNotNull() & F.col("_fk_municipio_ok").isNull(),
          F.lit("municipio_inexistente_na_dimensao"))
    .when(F.col("_fk_uf_ok").isNull(), F.lit("uf_inexistente_na_dimensao"))
    # consistência entre tabelas: UF do registro x UF do município na dimensão
    .when(F.col("uf_consistente") == False,  # noqa: E712 - coluna booleana nullável
          F.lit("uf_incompativel_com_municipio"))
    .otherwise(F.lit(None))
)

marcada = s.withColumn("rejection_reason", rejection_reason)

invalidos = marcada.filter(F.col("rejection_reason").isNotNull())
aprovados = (marcada.filter(F.col("rejection_reason").isNull())
             .drop("rejection_reason", "_fk_municipio_ok", "_fk_uf_ok"))

rows_rejected = invalidos.count()
rows_written = aprovados.count()
print(f"Aprovados: {rows_written:,} | Reprovados (quarentena): {rows_rejected:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Quarentena dos registros reprovados
# MAGIC Payload original preservado em JSON (contrato, seção 5).

# COMMAND ----------

if rows_rejected > 0:
    payload_cols = [c for c in s.columns if not c.startswith("_fk_")]
    quarentena = (
        invalidos
        .withColumn("run_id", F.lit(RUN_ID))
        .withColumn("task_name", F.lit(TASK))
        .withColumn("payload", F.to_json(F.struct(*payload_cols)))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select("run_id", "task_name", "rejection_reason", "payload", "ingestion_timestamp")
    )
    (quarentena.write.format("delta").mode("append")
        .saveAsTable(f"{CATALOG}.observability.quarantine_records"))
    print(f"→ {rows_rejected:,} registros enviados para observability.quarantine_records")

    # Distribuição dos motivos, útil na demo
    invalidos.groupBy("rejection_reason").count().orderBy(F.desc("count")).show(truncate=False)
else:
    print("Nenhum registro reprovado por regra de linha.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Publica a Silver aprovada (fonte da Gold)

# COMMAND ----------

# 3. Validações sistêmicas bloqueantes

record_id_unico = (
    rows_written
    == aprovados.select("record_id").distinct().count()
)

cobertura = (
    rows_written / rows_read
    if rows_read
    else 0.0
)

checks_sistemicos = {
    "silver_nao_vazia": rows_read > 0,
    "aprovados_maior_que_zero": rows_written > 0,
    "record_id_unico_nos_aprovados": record_id_unico,
    f"cobertura_min_{COBERTURA_MIN:.0%}": cobertura >= COBERTURA_MIN,
}

print("\n=== QUALITY GATE SISTÊMICO ===")

for nome, ok in checks_sistemicos.items():
    print(f"{'✓' if ok else '✗'} {nome}")

print(f"Cobertura: {cobertura:.1%}")

reprovados = [
    nome
    for nome, ok in checks_sistemicos.items()
    if not ok
]

# COMMAND ----------

# 4. Se houver falha sistêmica, registra auditoria e interrompe
# sem alterar a última Silver aprovada.

from pyspark.sql import Row

if reprovados:

    status = "FAILED"

    error_message = (
        "Quality Gate reprovado: "
        + ", ".join(reprovados)
    )

    metric = Row(
        run_id=RUN_ID,
        task_name=TASK,
        status=status,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc),
        rows_read=int(rows_read),

        # Nenhuma nova versão aprovada foi publicada.
        rows_written=0,

        rows_rejected=int(rows_rejected),
        max_event_time=None,
        schema_version="1.0",
        error_message=error_message,
    )

    schema_metrics = spark.table(
        f"{CATALOG}.observability.pipeline_metrics"
    ).schema

    (
        spark.createDataFrame(
            [metric],
            schema=schema_metrics
        )
        .write
        .mode("append")
        .saveAsTable(
            f"{CATALOG}.observability.pipeline_metrics"
        )
    )

    print(f"✗ {error_message}")
    print("✗ Silver aprovada NÃO foi sobrescrita.")
    print(f"Auditoria registrada. run_id={RUN_ID}")

    raise AssertionError(error_message)

# COMMAND ----------

# 5. Publicação da Silver aprovada
# Só chega aqui se todos os checks sistêmicos passaram.

(
    aprovados.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(APROVADA)
)

spark.sql(
    f"""
    COMMENT ON TABLE {APROVADA} IS
    'Silver aprovada pelo Quality Gate (06).
     Fonte da Gold. Responsável: P4.'
    """
)

print(
    f"✓ {APROVADA} publicada com "
    f"{rows_written:,} registros"
)

# COMMAND ----------

# 6. Auditoria da execução aprovada

metric = Row(
    run_id=RUN_ID,
    task_name=TASK,
    status="SUCCESS",
    started_at=started_at,
    finished_at=datetime.now(timezone.utc),
    rows_read=int(rows_read),
    rows_written=int(rows_written),
    rows_rejected=int(rows_rejected),
    max_event_time=None,
    schema_version="1.0",
    error_message=None,
)

schema_metrics = spark.table(
    f"{CATALOG}.observability.pipeline_metrics"
).schema

(
    spark.createDataFrame(
        [metric],
        schema=schema_metrics
    )
    .write
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.observability.pipeline_metrics"
    )
)

print(f"✓ Auditoria registrada. run_id={RUN_ID}")
print("✓ Quality Gate APROVADO - Gold liberada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validações sistêmicas (bloqueantes)
# MAGIC Falhas aqui reprovam a task - a Gold **não** deve ser publicada.

# COMMAND ----------

record_id_unico = rows_written == aprovados.select("record_id").distinct().count()
cobertura = (rows_written / rows_read) if rows_read else 0.0

checks_sistemicos = {
    "silver_nao_vazia": rows_read > 0,
    "aprovados_maior_que_zero": rows_written > 0,
    "record_id_unico_nos_aprovados": record_id_unico,
    f"cobertura_min_{COBERTURA_MIN:.0%}": cobertura >= COBERTURA_MIN,
}

for nome, ok in checks_sistemicos.items():
    print(f"{'✓' if ok else '✗'} {nome}")
print(f"Cobertura: {cobertura:.1%}")

reprovados = [nome for nome, ok in checks_sistemicos.items() if not ok]
status = "FAILED" if reprovados else "SUCCESS"
error_message = f"Quality Gate reprovado: {', '.join(reprovados)}" if reprovados else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Auditoria em observability.pipeline_metrics

# COMMAND ----------

from pyspark.sql import Row

metric = Row(
    run_id=RUN_ID,
    task_name=TASK,
    status=status,
    started_at=started_at,
    finished_at=datetime.now(timezone.utc),
    rows_read=int(rows_read),
    rows_written=int(rows_written),
    rows_rejected=int(rows_rejected),
    max_event_time=None,
    schema_version="1.0",
    error_message=error_message,
)
# Schema explícito da própria tabela: max_event_time/error_message são None no
# caminho feliz e a inferência de tipos falharia (CANNOT_DETERMINE_TYPE).
schema_metrics = spark.table(f"{CATALOG}.observability.pipeline_metrics").schema
spark.createDataFrame([metric], schema=schema_metrics).write.mode("append").saveAsTable(
    f"{CATALOG}.observability.pipeline_metrics")
print(f"Auditoria registrada. run_id={RUN_ID}")

# COMMAND ----------

if reprovados:
    raise AssertionError(error_message)
print("Quality Gate APROVADO - Gold liberada.")

# COMMAND ----------

# 7. Quality Gate da Silver no grão de aluno

import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql import Row

CATALOG = "workspace"

ALUNOS_SILVER = f"{CATALOG}.silver.alunos_modelagem"
ALUNOS_APROVADOS = f"{CATALOG}.silver.alunos_modelagem_aprovados"

DIM_MUN_ALUNOS = f"{CATALOG}.bronze.municipio"
DIM_UF_ALUNOS = f"{CATALOG}.bronze.uf"

COBERTURA_MIN_ALUNOS = 0.80
ANOS_VALIDOS_ALUNOS = [2023, 2024]

UFS_VALIDAS_ALUNOS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]

# Mantém o mesmo run_id da execução principal quando disponível.
try:
    RUN_ID_ALUNOS = RUN_ID
except NameError:
    RUN_ID_ALUNOS = str(uuid.uuid4())

TASK_ALUNOS = "06_quality_gate_alunos"
started_at_alunos = datetime.now(timezone.utc)

if not spark.catalog.tableExists(ALUNOS_SILVER):
    raise RuntimeError(
        f"{ALUNOS_SILVER} não existe. "
        "Execute o 03_silver.py antes do Quality Gate."
    )

alunos_qg = spark.table(ALUNOS_SILVER)
rows_read_alunos = alunos_qg.count()

print("\n=== QUALITY GATE ALUNOS ===")
print(f"Silver alunos lida: {rows_read_alunos:,} registros")

# COMMAND ----------

# Integridade referencial de UF.
# (município NÃO é checado contra a dimensão: o TS_ALUNO oficial traz
# id_municipio anonimizado/mascarado - proteção LGPD nos microdados
# individuais - e nunca vai bater com o código real do IBGE. UF continua
# confiável e é a única geografia checada nesse nível.)

ufs_validas_dim = (
    spark.table(DIM_UF_ALUNOS)
    .select(
        F.upper(
            F.trim(F.col("sigla_uf"))
        ).alias("sigla_uf")
    )
    .distinct()
    .withColumn("_fk_uf_ok", F.lit(True))
)

alunos_qg = alunos_qg.join(
    ufs_validas_dim,
    on="sigla_uf",
    how="left"
)

# COMMAND ----------

# Validações por registro.

rejection_reason_aluno = (
    F.when(
        F.col("id_aluno").isNull()
        | F.col("ano").isNull()
        | F.col("sigla_uf").isNull()
        | F.col("id_municipio").isNull()
        | F.col("record_id").isNull()
        | F.col("alfabetizado_oficial").isNull(),
        F.lit("campo_critico_nulo")
    )

    .when(
        ~F.col("ano").isin(ANOS_VALIDOS_ALUNOS),
        F.lit("ano_invalido")
    )

    .when(
        ~F.col("sigla_uf").isin(UFS_VALIDAS_ALUNOS),
        F.lit("sigla_uf_invalida")
    )

    .when(
        ~F.col("id_municipio").rlike("^[0-9]{7}$"),
        F.lit("id_municipio_invalido")
    )

    .when(
        ~F.col("alfabetizado_oficial").isin([0, 1]),
        F.lit("target_alfabetizacao_invalido")
    )

    .when(
        F.col("_fk_uf_ok").isNull(),
        F.lit("uf_inexistente_na_dimensao")
    )

    .when(
        F.col("fonte_dados") != "oficial_inep",
        F.lit("fonte_nao_oficial")
    )

    .otherwise(F.lit(None))
)

marcada_alunos = alunos_qg.withColumn(
    "rejection_reason",
    rejection_reason_aluno
)

invalidos_alunos = marcada_alunos.filter(
    F.col("rejection_reason").isNotNull()
)

aprovados_alunos = (
    marcada_alunos
    .filter(F.col("rejection_reason").isNull())
    .drop(
        "rejection_reason",
        "_fk_municipio_ok",
        "_fk_uf_ok"
    )
)

rows_rejected_alunos = invalidos_alunos.count()
rows_written_alunos = aprovados_alunos.count()

print(
    f"Aprovados: {rows_written_alunos:,} | "
    f"Reprovados: {rows_rejected_alunos:,}"
)

# COMMAND ----------

# Quarentena dos registros inválidos.

if rows_rejected_alunos > 0:

    payload_cols_alunos = [
        c
        for c in alunos_qg.columns
        if not c.startswith("_fk_")
    ]

    quarentena_alunos = (
        invalidos_alunos

        .withColumn(
            "run_id",
            F.lit(RUN_ID_ALUNOS)
        )

        .withColumn(
            "task_name",
            F.lit(TASK_ALUNOS)
        )

        .withColumn(
            "payload",
            F.to_json(
                F.struct(*payload_cols_alunos)
            )
        )

        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp()
        )

        .select(
            "run_id",
            "task_name",
            "rejection_reason",
            "payload",
            "ingestion_timestamp"
        )
    )

    (
        quarentena_alunos.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            f"{CATALOG}.observability.quarantine_records"
        )
    )

    invalidos_alunos.groupBy(
        "rejection_reason"
    ).count().orderBy(
        F.desc("count")
    ).show(truncate=False)

# COMMAND ----------

# Checks sistêmicos bloqueantes.

record_id_unico_alunos = (
    rows_written_alunos
    == aprovados_alunos.select("record_id").distinct().count()
)

cobertura_alunos = (
    rows_written_alunos / rows_read_alunos
    if rows_read_alunos
    else 0.0
)

checks_alunos = {
    "silver_alunos_nao_vazia":
        rows_read_alunos > 0,

    "alunos_aprovados_maior_que_zero":
        rows_written_alunos > 0,

    "record_id_aluno_unico":
        record_id_unico_alunos,

    f"cobertura_alunos_min_{COBERTURA_MIN_ALUNOS:.0%}":
        cobertura_alunos >= COBERTURA_MIN_ALUNOS,
}

print("\n=== QUALITY GATE SISTÊMICO - ALUNOS ===")

for nome, ok in checks_alunos.items():
    print(f"{'✓' if ok else '✗'} {nome}")

print(f"Cobertura alunos: {cobertura_alunos:.1%}")

falhas_alunos = [
    nome
    for nome, ok in checks_alunos.items()
    if not ok
]

# COMMAND ----------

# Falhou: audita e NÃO sobrescreve a última tabela aprovada.

if falhas_alunos:

    error_message_alunos = (
        "Quality Gate alunos reprovado: "
        + ", ".join(falhas_alunos)
    )

    metric_alunos = Row(
        run_id=RUN_ID_ALUNOS,
        task_name=TASK_ALUNOS,
        status="FAILED",
        started_at=started_at_alunos,
        finished_at=datetime.now(timezone.utc),
        rows_read=int(rows_read_alunos),
        rows_written=0,
        rows_rejected=int(rows_rejected_alunos),
        max_event_time=None,
        schema_version="1.0",
        error_message=error_message_alunos,
    )

    schema_metrics = spark.table(
        f"{CATALOG}.observability.pipeline_metrics"
    ).schema

    (
        spark.createDataFrame(
            [metric_alunos],
            schema=schema_metrics
        )
        .write
        .mode("append")
        .saveAsTable(
            f"{CATALOG}.observability.pipeline_metrics"
        )
    )

    print(f"✗ {error_message_alunos}")
    print("✗ Silver alunos aprovada NÃO foi sobrescrita.")

    raise AssertionError(error_message_alunos)

# COMMAND ----------

# Passou: somente agora publica a Silver aprovada de alunos.

(
    aprovados_alunos.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(ALUNOS_APROVADOS)
)

spark.sql(
    f"""
    COMMENT ON TABLE {ALUNOS_APROVADOS} IS
    'Base de alunos oficial aprovada pelo Quality Gate.
     Grão: aluno. Fonte da Gold de modelagem da Fase 3.'
    """
)

print(
    f"✓ {ALUNOS_APROVADOS} publicada com "
    f"{rows_written_alunos:,} registros"
)

# COMMAND ----------

# Auditoria de sucesso.

metric_alunos = Row(
    run_id=RUN_ID_ALUNOS,
    task_name=TASK_ALUNOS,
    status="SUCCESS",
    started_at=started_at_alunos,
    finished_at=datetime.now(timezone.utc),
    rows_read=int(rows_read_alunos),
    rows_written=int(rows_written_alunos),
    rows_rejected=int(rows_rejected_alunos),
    max_event_time=None,
    schema_version="1.0",
    error_message=None,
)

schema_metrics = spark.table(
    f"{CATALOG}.observability.pipeline_metrics"
).schema

(
    spark.createDataFrame(
        [metric_alunos],
        schema=schema_metrics
    )
    .write
    .mode("append")
    .saveAsTable(
        f"{CATALOG}.observability.pipeline_metrics"
    )
)

print(
    f"✓ Auditoria alunos registrada. "
    f"run_id={RUN_ID_ALUNOS}"
)

print(
    "✓ Quality Gate ALUNOS APROVADO - "
    "base de modelagem liberada para Gold."
)

dbutils.notebook.exit(
    f"Quality Gate aprovado: cobertura territorial={cobertura:.1%}, "
    f"cobertura alunos={cobertura_alunos:.1%}"
)