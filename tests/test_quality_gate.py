"""
Testes do Quality Gate da Silver.

Não alteram o pipeline. Validam:
- regras de linha;
- integridade referencial;
- unicidade;
- cobertura mínima;
- correspondência entre o conjunto válido e silver.medicoes_aprovadas.
"""

import pytest


CATALOG = "workspace"
SILVER = f"{CATALOG}.silver.medicoes_alfabetizacao"
APROVADA = f"{CATALOG}.silver.medicoes_aprovadas"
DIM_MUN = f"{CATALOG}.bronze.municipio"
DIM_UF = f"{CATALOG}.bronze.uf"

COBERTURA_MIN = 0.80

UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
]


def _spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("PySpark não disponível neste ambiente.")

    session = SparkSession.getActiveSession()
    if session is None:
        pytest.skip("Teste de integração requer uma sessão Spark ativa.")
    return session


def _require_table(spark, table_name):
    assert spark.catalog.tableExists(table_name), (
        f"Tabela obrigatória ausente: {table_name}"
    )


def _silver_com_integridade_referencial(spark):
    """Reproduz apenas as marcações de FK usadas pelo notebook 06."""
    from pyspark.sql import functions as F

    s = spark.table(SILVER)

    ids_validos = (
        spark.table(DIM_MUN)
        .select(
            F.lpad(
                F.col("id_municipio").cast("string"),
                7,
                "0",
            ).alias("id_municipio")
        )
        .distinct()
        .withColumn("_fk_municipio_ok", F.lit(True))
    )

    ufs_dim = (
        spark.table(DIM_UF)
        .select(
            F.upper(F.trim(F.col("sigla_uf"))).alias("sigla_uf")
        )
        .distinct()
        .withColumn("_fk_uf_ok", F.lit(True))
    )

    return (
        s.join(ids_validos, on="id_municipio", how="left")
        .join(ufs_dim, on="sigla_uf", how="left")
    )


def _rejection_reason():
    """
    Mesmas regras de linha do notebook 06_quality_checks.py.
    O teste funciona como contrato de não-regressão dessas regras.
    """
    from pyspark.sql import functions as F

    return (
        F.when(
            F.col("ano").isNull()
            | F.col("sigla_uf").isNull()
            | F.col("rede").isNull()
            | F.col("record_id").isNull(),
            F.lit("campo_critico_nulo"),
        )
        .when(
            (F.col("grao") == "municipio")
            & F.col("id_municipio").isNull(),
            F.lit("municipio_nulo_no_grao_municipal"),
        )
        .when(
            ~F.col("sigla_uf").rlike("^[A-Z]{2}$")
            | ~F.col("sigla_uf").isin(UFS_VALIDAS),
            F.lit("sigla_uf_invalida"),
        )
        .when(
            F.col("id_municipio").isNotNull()
            & ~F.col("id_municipio").rlike("^[0-9]{7}$"),
            F.lit("id_municipio_invalido"),
        )
        .when(
            ~F.col("rede").isin([0, 2, 3, 5]),
            F.lit("rede_fora_do_dominio"),
        )
        .when(
            F.col("taxa_alfabetizacao").isNotNull()
            & ~F.col("taxa_alfabetizacao").between(0.0, 1.0),
            F.lit("taxa_fora_do_dominio"),
        )
        .when(
            F.col("id_municipio").isNotNull()
            & F.col("_fk_municipio_ok").isNull(),
            F.lit("municipio_inexistente_na_dimensao"),
        )
        .when(
            F.col("_fk_uf_ok").isNull(),
            F.lit("uf_inexistente_na_dimensao"),
        )
        .when(
            F.col("uf_consistente") == False,  # noqa: E712
            F.lit("uf_incompativel_com_municipio"),
        )
        .otherwise(F.lit(None))
    )


def test_quality_gate_tabelas_obrigatorias_existem():
    spark = _spark()

    for table in [SILVER, APROVADA, DIM_MUN, DIM_UF]:
        _require_table(spark, table)


def test_quality_gate_aprovada_nao_esta_vazia():
    spark = _spark()
    _require_table(spark, APROVADA)

    assert spark.table(APROVADA).limit(1).count() == 1, (
        "silver.medicoes_aprovadas está vazia."
    )


def test_quality_gate_record_id_unico_nos_aprovados():
    from pyspark.sql import functions as F

    spark = _spark()
    _require_table(spark, APROVADA)

    aprovados = spark.table(APROVADA)
    total = aprovados.count()
    distintos = aprovados.select("record_id").distinct().count()
    nulos = aprovados.filter(F.col("record_id").isNull()).count()

    assert nulos == 0, "Existem record_id nulos entre os aprovados."
    assert total == distintos, (
        f"record_id duplicado nos aprovados: total={total}, distintos={distintos}"
    )


def test_quality_gate_cobertura_minima_80_porcento():
    spark = _spark()
    _require_table(spark, SILVER)
    _require_table(spark, APROVADA)

    rows_read = spark.table(SILVER).count()
    rows_written = spark.table(APROVADA).count()

    cobertura = rows_written / rows_read if rows_read else 0.0

    assert rows_read > 0, "Silver de entrada está vazia."
    assert cobertura >= COBERTURA_MIN, (
        f"Cobertura abaixo do mínimo: {cobertura:.1%} < {COBERTURA_MIN:.0%}"
    )


def test_quality_gate_aprovados_nao_possuem_violacao_de_regra():
    from pyspark.sql import functions as F

    spark = _spark()

    for table in [APROVADA, DIM_MUN, DIM_UF]:
        _require_table(spark, table)

    # Executa as mesmas marcações de integridade diretamente sobre a tabela aprovada.
    aprovados = spark.table(APROVADA)

    ids_validos = (
        spark.table(DIM_MUN)
        .select(
            F.lpad(
                F.col("id_municipio").cast("string"),
                7,
                "0",
            ).alias("id_municipio")
        )
        .distinct()
        .withColumn("_fk_municipio_ok", F.lit(True))
    )

    ufs_dim = (
        spark.table(DIM_UF)
        .select(
            F.upper(F.trim(F.col("sigla_uf"))).alias("sigla_uf")
        )
        .distinct()
        .withColumn("_fk_uf_ok", F.lit(True))
    )

    marcada = (
        aprovados
        .join(ids_validos, on="id_municipio", how="left")
        .join(ufs_dim, on="sigla_uf", how="left")
        .withColumn("rejection_reason", _rejection_reason())
    )

    invalidos = marcada.filter(
        F.col("rejection_reason").isNotNull()
    ).count()

    assert invalidos == 0, (
        f"A tabela aprovada contém {invalidos} registro(s) que violam o Quality Gate."
    )


def test_quality_gate_publica_exatamente_o_conjunto_valido():
    """
    Compara os record_id calculados como válidos a partir da Silver com os
    record_id efetivamente publicados em silver.medicoes_aprovadas.
    """
    from pyspark.sql import functions as F

    spark = _spark()

    for table in [SILVER, APROVADA, DIM_MUN, DIM_UF]:
        _require_table(spark, table)

    marcada = _silver_com_integridade_referencial(spark).withColumn(
        "rejection_reason",
        _rejection_reason(),
    )

    esperados = (
        marcada
        .filter(F.col("rejection_reason").isNull())
        .select("record_id")
        .distinct()
    )

    publicados = (
        spark.table(APROVADA)
        .select("record_id")
        .distinct()
    )

    faltando = esperados.join(
        publicados,
        on="record_id",
        how="left_anti",
    ).count()

    indevidos = publicados.join(
        esperados,
        on="record_id",
        how="left_anti",
    ).count()

    assert faltando == 0, (
        f"{faltando} registro(s) válido(s) da Silver não foram publicados."
    )
    assert indevidos == 0, (
        f"{indevidos} registro(s) publicado(s) não passam nas regras do Gate."
    )


def test_quality_gate_aprovada_e_subconjunto_da_silver():
    spark = _spark()
    _require_table(spark, SILVER)
    _require_table(spark, APROVADA)

    silver_ids = spark.table(SILVER).select("record_id").distinct()
    aprovados_ids = spark.table(APROVADA).select("record_id").distinct()

    estranhos = aprovados_ids.join(
        silver_ids,
        on="record_id",
        how="left_anti",
    ).count()

    assert estranhos == 0, (
        f"Existem {estranhos} record_id na aprovada que não existem na Silver."
    )
