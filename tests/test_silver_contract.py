"""
Testes de contrato da camada Silver.

Objetivo: validar as correções pedidas pelo professor sem alterar a lógica
do pipeline. Os testes de integração usam as tabelas já produzidas no
Databricks; quando não existe sessão Spark ativa, são pulados.
"""

import pytest


CATALOG = "workspace"
SILVER = f"{CATALOG}.silver.medicoes_alfabetizacao"

FONTES_OFICIAIS_OBRIGATORIAS = [
    f"{CATALOG}.bronze.avaliacao_alfabetizacao",
    f"{CATALOG}.bronze.avaliacao_alfabetizacao_municipio",
    f"{CATALOG}.bronze.uf",
    f"{CATALOG}.bronze.municipio",
    f"{CATALOG}.bronze.meta_brasil",
    f"{CATALOG}.bronze.meta_uf",
    f"{CATALOG}.bronze.meta_municipio",
    f"{CATALOG}.bronze.alunos",
]


def _spark():
    """Obtém a sessão Spark ativa do Databricks."""
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


def test_fontes_oficiais_obrigatorias_estao_disponiveis():
    spark = _spark()

    ausentes = [
        table
        for table in FONTES_OFICIAIS_OBRIGATORIAS
        if not spark.catalog.tableExists(table)
    ]

    assert not ausentes, (
        "Fontes oficiais obrigatórias ausentes: " + ", ".join(ausentes)
    )


def test_bronze_alunos_possui_schema_oficial_inep():
    spark = _spark()
    table = f"{CATALOG}.bronze.alunos"
    _require_table(spark, table)

    alunos = spark.table(table)

    colunas_oficiais = {
        "NU_ANO_AVALIACAO",
        "CO_UF",
        "SG_UF",
        "ID_ALUNO",
        "TP_SERIE",
        "ID_ESCOLA",
        "TP_DEPENDENCIA",
        "CO_MUNICIPIO",
        "NO_MUNICIPIO",
        "IN_PRESENCA_LP",
        "IN_PREENCHIMENTO_LP",
        "CO_CADERNO_LP",
        "VL_PESO_ALUNO_LP",
        "VL_PROFICIENCIA_LP",
        "IN_ALFABETIZADO",
    }

    faltantes = colunas_oficiais.difference(alunos.columns)

    assert not faltantes, (
        "bronze.alunos não está com o schema oficial esperado do INEP. "
        f"Colunas ausentes: {sorted(faltantes)}"
    )
    assert alunos.limit(1).count() == 1, "bronze.alunos está vazia."


def test_silver_nao_contem_fonte_simulada():
    from pyspark.sql import functions as F

    spark = _spark()
    _require_table(spark, SILVER)

    silver = spark.table(SILVER)

    assert "fonte_dados" in silver.columns

    simulados = silver.filter(
        F.lower(F.coalesce(F.col("fonte_dados"), F.lit(""))).contains("simul")
    ).count()

    assert simulados == 0, (
        f"A Silver contém {simulados} registro(s) marcado(s) como simulado."
    )


def test_taxa_alfabetizacao_silver_esta_normalizada():
    from pyspark.sql import functions as F

    spark = _spark()
    _require_table(spark, SILVER)

    silver = spark.table(SILVER)

    invalidos = silver.filter(
        F.col("taxa_alfabetizacao").isNotNull()
        & ~F.col("taxa_alfabetizacao").between(0.0, 1.0)
    ).count()

    assert invalidos == 0, (
        f"Existem {invalidos} taxa(s) fora do domínio 0..1 na Silver."
    )


def test_registro_municipal_tem_id_municipio():
    from pyspark.sql import functions as F

    spark = _spark()
    _require_table(spark, SILVER)

    silver = spark.table(SILVER)

    invalidos = silver.filter(
        (F.col("grao") == "municipio")
        & F.col("id_municipio").isNull()
    ).count()

    assert invalidos == 0, (
        f"Existem {invalidos} registro(s) municipais sem id_municipio."
    )


def test_alfabetizado_derivado_respeita_corte_743():
    """
    Quando existe media_portugues, a classificação da Silver deve ser
    exatamente media_portugues >= 743.
    """
    from pyspark.sql import functions as F

    spark = _spark()
    _require_table(spark, SILVER)

    silver = spark.table(SILVER)

    divergencias = silver.filter(
        F.col("media_portugues").isNotNull()
        & (
            F.col("alfabetizado")
            != (F.col("media_portugues") >= F.lit(743.0))
        )
    ).count()

    assert divergencias == 0, (
        f"Existem {divergencias} classificação(ões) divergentes do corte 743."
    )


def test_record_id_e_unico():
    from pyspark.sql import functions as F

    spark = _spark()
    _require_table(spark, SILVER)

    silver = spark.table(SILVER)

    total = silver.count()
    distintos = silver.select("record_id").distinct().count()
    nulos = silver.filter(F.col("record_id").isNull()).count()

    assert nulos == 0, "Existem record_id nulos na Silver."
    assert total == distintos, (
        f"record_id não é único: total={total}, distintos={distintos}"
    )


def test_record_id_reproduz_formula_deterministica_da_silver():
    """
    Recalcula a chave com a mesma fórmula da Silver e verifica que não existe
    divergência. Isso protege contra alteração silenciosa do contrato da chave.
    """
    from pyspark.sql import functions as F

    spark = _spark()
    _require_table(spark, SILVER)

    silver = spark.table(SILVER)

    recalculado = F.sha2(
        F.concat_ws(
            "|",
            F.col("ano"),
            F.col("sigla_uf"),
            F.coalesce(F.col("id_municipio"), F.lit("uf")),
            F.coalesce(F.col("serie").cast("string"), F.lit("na")),
            F.col("rede"),
            F.col("source"),
            F.coalesce(F.col("event_id"), F.lit("batch")),
        ),
        256,
    )

    divergencias = silver.filter(
        F.col("record_id") != recalculado
    ).count()

    assert divergencias == 0, (
        f"Existem {divergencias} record_id fora da fórmula determinística."
    )


def test_meta_municipal_nao_herda_meta_da_uf():
    """
    Regra corrigida: registro no grão município só pode ter meta_taxa quando
    existe meta oficial para aquele município/ano. Não existe fallback para UF.
    """
    from pyspark.sql import functions as F

    spark = _spark()

    meta_table = f"{CATALOG}.bronze.meta_municipio"
    _require_table(spark, SILVER)
    _require_table(spark, meta_table)

    silver_municipal = (
        spark.table(SILVER)
        .filter(F.col("grao") == "municipio")
        .select(
            F.col("ano").cast("int").alias("ano"),
            F.lpad(F.col("id_municipio").cast("string"), 7, "0").alias("id_municipio"),
            "meta_taxa",
        )
    )

    metas_oficiais = (
        spark.table(meta_table)
        .select(
            F.col("ano").cast("int").alias("ano"),
            F.lpad(F.col("id_municipio").cast("string"), 7, "0").alias("id_municipio"),
            F.when(
                F.col("meta").cast("double") > 1.0,
                F.col("meta").cast("double") / 100.0,
            )
            .otherwise(F.col("meta").cast("double"))
            .alias("_meta_municipal_oficial"),
        )
        .dropDuplicates(["ano", "id_municipio"])
    )

    comparacao = silver_municipal.join(
        metas_oficiais,
        on=["ano", "id_municipio"],
        how="left",
    )

    herdadas = comparacao.filter(
        F.col("meta_taxa").isNotNull()
        & F.col("_meta_municipal_oficial").isNull()
    ).count()

    divergentes = comparacao.filter(
        F.col("meta_taxa").isNotNull()
        & F.col("_meta_municipal_oficial").isNotNull()
        & (
            F.abs(
                F.col("meta_taxa") - F.col("_meta_municipal_oficial")
            ) > F.lit(1e-9)
        )
    ).count()

    assert herdadas == 0, (
        f"Existem {herdadas} meta(s) municipal(is) sem meta oficial correspondente. "
        "Possível fallback indevido para meta da UF."
    )
    assert divergentes == 0, (
        f"Existem {divergentes} meta(s) municipal(is) divergentes da fonte oficial."
    )
