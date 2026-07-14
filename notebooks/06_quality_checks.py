# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Quality Gate
# MAGIC Valida a Silver **antes** da publicação da Gold (contrato, seção 8).
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
import sys
import uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F

# Corte de alfabetização importado de src/, para que o Gate valide a regra contra
# a mesma constante aplicada pela Silver.
try:
    sys.path.append("..")
    from src.utils import ALFABETIZACAO_CORTE
except Exception:
    ALFABETIZACAO_CORTE = 743

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
    print(f"AVISO: {DIM_MUN} não existe. Check de FK municipal não aplicado.")
    s = s.withColumn("_fk_municipio_ok", F.lit(True))

if spark.catalog.tableExists(DIM_UF):
    ufs_dim = (spark.table(DIM_UF)
               .select(F.upper(F.trim(F.col("sigla_uf"))).alias("sigla_uf"))
               .distinct()
               .withColumn("_fk_uf_ok", F.lit(True)))
    s = s.join(ufs_dim, on="sigla_uf", how="left")
else:
    print(f"AVISO: {DIM_UF} não existe. Check de FK de UF não aplicado.")
    s = s.withColumn("_fk_uf_ok", F.lit(True))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Validações por registro e motivo de rejeição
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
    # A meta é validada no mesmo domínio do resultado, já que a Gold compara as
    # duas. Uma meta em percentual (0-100) que escape da normalização da Silver
    # produziria um gap incorreto sem gerar erro.
    .when(F.col("meta_taxa").isNotNull()
          & ~F.col("meta_taxa").between(0.0, 1.0), F.lit("meta_fora_do_dominio"))
    # `grao` e `fonte_dados` são usados como critério de filtro pelos marts e pelo
    # dashboard. Um valor fora do domínio não interrompe o pipeline, mas altera o
    # resultado das agregações.
    .when(F.col("grao").isNull() | ~F.col("grao").isin(["uf", "municipio"]),
          F.lit("grao_invalido"))
    .when(F.col("fonte_dados").isNull()
          | ~F.col("fonte_dados").isin(["oficial_inep", "simulado"]),
          F.lit("fonte_dados_invalida"))
    # integridade referencial (edital: validação de chaves de relacionamento)
    .when(F.col("id_municipio").isNotNull() & F.col("_fk_municipio_ok").isNull(),
          F.lit("municipio_inexistente_na_dimensao"))
    .when(F.col("_fk_uf_ok").isNull(), F.lit("uf_inexistente_na_dimensao"))
    # consistência entre tabelas: UF do registro x UF do município na dimensão
    .when(F.col("uf_consistente") == False,  # noqa: E712 — coluna booleana nullável
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
    print(f"{rows_rejected:,} registros enviados para observability.quarantine_records")

    # Distribuição dos motivos de rejeição.
    invalidos.groupBy("rejection_reason").count().orderBy(F.desc("count")).show(truncate=False)
else:
    print("Nenhum registro reprovado por regra de linha.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Publica a Silver aprovada (fonte da Gold)

# COMMAND ----------
(aprovados.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(APROVADA))
spark.sql(f"COMMENT ON TABLE {APROVADA} IS "
          f"'Silver aprovada pelo Quality Gate (06). Fonte da Gold.'")
print(f"{APROVADA} publicada com {rows_written:,} registros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3b. Quality Gate da Silver de alunos (`silver.alunos_proficiencia`)
# MAGIC Segue o mesmo padrão da tabela de medições: os reprovados vão para a
# MAGIC quarentena e os aprovados são publicados em `silver.alunos_aprovados`, fonte
# MAGIC do mart `gold.distribuicao_proficiencia`. O Definition of Done da Gold
# MAGIC (contrato, seção 11) exige Quality Gate aprovado para toda tabela que alimenta
# MAGIC um mart.

# COMMAND ----------
ALUNOS_SILVER = f"{CATALOG}.silver.alunos_proficiencia"
ALUNOS_APROVADOS = f"{CATALOG}.silver.alunos_aprovados"

# Domínio da escala de proficiência do Saeb. Valores fora deste intervalo indicam
# erro de unidade ou de parsing, não desempenho extremo.
PROFICIENCIA_MIN, PROFICIENCIA_MAX = 0.0, 1000.0

alunos_rejeitados = 0
if spark.catalog.tableExists(ALUNOS_SILVER):
    a = spark.table(ALUNOS_SILVER)
    alunos_lidos = a.count()

    motivo_aluno = (
        F.when(F.col("record_id").isNull() | F.col("ano").isNull()
               | F.col("sigla_uf").isNull() | F.col("aluno_id").isNull(),
               F.lit("aluno_campo_critico_nulo"))
        .when(~F.col("sigla_uf").isin(UFS_VALIDAS), F.lit("aluno_sigla_uf_invalida"))
        .when(~F.col("rede").isin([0, 2, 3, 5]), F.lit("aluno_rede_fora_do_dominio"))
        .when(F.col("proficiencia_portugues").isNotNull()
              & ~F.col("proficiencia_portugues").between(PROFICIENCIA_MIN, PROFICIENCIA_MAX),
              F.lit("aluno_proficiencia_fora_do_dominio"))
        # Coerência da regra de negócio: `alfabetizado` deve corresponder ao corte
        # aplicado à proficiência do próprio registro. Divergência indica que a regra
        # foi alterada sem versionamento, situação que o contrato (seção 4) pretende
        # evitar ao exigir `alfabetizacao_rule_version`.
        .when(F.col("proficiencia_portugues").isNotNull()
              & (F.col("alfabetizado")
                 != (F.col("proficiencia_portugues") >= ALFABETIZACAO_CORTE)),
              F.lit("aluno_flag_incoerente_com_o_corte"))
        .when(F.col("alfabetizacao_rule_version").isNull(),
              F.lit("aluno_regra_nao_versionada"))
        .otherwise(F.lit(None))
    )

    a_marcada = a.withColumn("rejection_reason", motivo_aluno)
    a_invalidos = a_marcada.filter(F.col("rejection_reason").isNotNull())
    a_aprovados = a_marcada.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")

    alunos_rejeitados = a_invalidos.count()
    alunos_aprovados_n = a_aprovados.count()

    if alunos_rejeitados:
        (a_invalidos
         .withColumn("run_id", F.lit(RUN_ID))
         .withColumn("task_name", F.lit(TASK))
         .withColumn("payload", F.to_json(F.struct(*a.columns)))
         .withColumn("ingestion_timestamp", F.current_timestamp())
         .select("run_id", "task_name", "rejection_reason", "payload", "ingestion_timestamp")
         .write.format("delta").mode("append")
         .saveAsTable(f"{CATALOG}.observability.quarantine_records"))
        a_invalidos.groupBy("rejection_reason").count().orderBy(F.desc("count")).show(truncate=False)

    (a_aprovados.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(ALUNOS_APROVADOS))
    spark.sql(f"COMMENT ON TABLE {ALUNOS_APROVADOS} IS "
              "'Alunos aprovados pelo Quality Gate (06). Fonte da "
              "gold.distribuicao_proficiencia.'")

    alunos_id_unico = alunos_aprovados_n == a_aprovados.select("record_id").distinct().count()
    print(f"{ALUNOS_APROVADOS}: {alunos_aprovados_n:,} aprovados | "
          f"{alunos_rejeitados:,} em quarentena (de {alunos_lidos:,} lidos)")
else:
    print(f"AVISO: {ALUNOS_SILVER} não existe. Gate de alunos não aplicado.")
    alunos_id_unico = True

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Validações sistêmicas (bloqueantes)
# MAGIC Falhas aqui reprovam a task — a Gold **não** deve ser publicada.

# COMMAND ----------
record_id_unico = rows_written == aprovados.select("record_id").distinct().count()
cobertura = (rows_written / rows_read) if rows_read else 0.0

# Integridade das metas: se a Bronze publica meta para um par (ano, UF), o fato
# correspondente deve tê-la recebido na Silver. Um `meta_taxa` nulo nesse caso
# indica falha de chave no join, e não ausência de meta na fonte.
#
# O join usa (ano, sigla_uf) e não apenas o ano porque DF e RR não constam da fonte
# do INEP e, portanto, não possuem meta. Exigir meta para essas UFs reprovaria o
# Gate por uma lacuna da fonte.
META_UF = f"{CATALOG}.bronze.meta_uf"
if spark.catalog.tableExists(META_UF):
    chaves_com_meta = (
        spark.table(META_UF)
        .select(F.col("ano").cast("int").alias("ano"),
                F.upper(F.trim(F.col("sigla_uf"))).alias("sigla_uf"))
        .distinct()
    )
    metas_perdidas = (
        aprovados.join(chaves_com_meta, on=["ano", "sigla_uf"], how="inner")
        .filter(F.col("meta_taxa").isNull())
    )
    n_metas_perdidas = metas_perdidas.count()
    if n_metas_perdidas:
        print(f"FALHA: {n_metas_perdidas:,} fatos perderam a meta no join da Silver:")
        (metas_perdidas.groupBy("ano", "sigla_uf", "grao").count()
         .orderBy(F.desc("count")).show(10, truncate=False))
else:
    print(f"AVISO: {META_UF} não existe. Check de integridade das metas não aplicado.")
    n_metas_perdidas = 0

checks_sistemicos = {
    "silver_nao_vazia": rows_read > 0,
    "aprovados_maior_que_zero": rows_written > 0,
    "record_id_unico_nos_aprovados": record_id_unico,
    f"cobertura_min_{COBERTURA_MIN:.0%}": cobertura >= COBERTURA_MIN,
    "nenhuma_meta_perdida_no_join": n_metas_perdidas == 0,
    "record_id_unico_nos_alunos_aprovados": alunos_id_unico,
}

for nome, ok in checks_sistemicos.items():
    print(f"[{'OK' if ok else 'FALHA'}] {nome}")
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
    # Inclui os alunos reprovados, para que a auditoria reflita o total enviado à
    # quarentena pela task e o número reconcilie com a tabela.
    rows_rejected=int(rows_rejected + alunos_rejeitados),
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
print("Quality Gate APROVADO — Gold liberada.")
