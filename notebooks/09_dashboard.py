# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 09: Command Center da Alfabetização
# MAGIC Dashboard executivo em HTML/CSS/JS construído a partir da camada Gold,
# MAGIC com filtros de ano, rede e UF aplicados no navegador, sem nova consulta
# MAGIC ao Databricks a cada troca.

# COMMAND ----------
from datetime import datetime, timezone
from string import Template

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "workspace"

# Tabelas centrais
T_RESUMO_UF = f"{CATALOG}.gold.resumo_uf"
T_IND_MUN = f"{CATALOG}.gold.indicador_municipio"
T_META_RESULTADO = f"{CATALOG}.gold.meta_vs_resultado"
T_EVOLUCAO = f"{CATALOG}.gold.evolucao_temporal"
T_META_BRASIL = f"{CATALOG}.bronze.meta_brasil"
T_META_UF = f"{CATALOG}.bronze.meta_uf"
T_ALUNOS = f"{CATALOG}.bronze.alunos"
T_EVENTOS = f"{CATALOG}.bronze.eventos_streaming"
T_METRICAS = f"{CATALOG}.observability.pipeline_metrics"
T_QUARENTENA = f"{CATALOG}.observability.quarantine_records"
T_SILVER = f"{CATALOG}.silver.medicoes_alfabetizacao"

# =============================================================================
# MODO PIPELINE: Quando executado via dbutils.notebook.run(), apenas valida
# que as tabelas necessárias existem e retorna sucesso.
# Para executar o dashboard completo com visualizações, comente as linhas abaixo.
# =============================================================================
tabelas_dashboard = [T_RESUMO_UF, T_IND_MUN, T_META_RESULTADO]
for tabela in tabelas_dashboard:
    if not spark.catalog.tableExists(tabela):
        raise RuntimeError(f"Tabela obrigatória ausente: {tabela}")
    count = spark.table(tabela).count()
    print(f"✓ {tabela}: {count:,} registros")

print("✓ Dashboard validado com sucesso")
print("ℹ  Gerando dashboard HTML completo...")
# dbutils.notebook.exit("OK")  # Comentado para gerar HTML completo

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Modo headless
# MAGIC Detecta se o notebook está rodando via dbutils.notebook.run (dentro do
# MAGIC pipeline) ou de forma interativa, para pular visualizações pesadas quando
# MAGIC chamado pelo runner.

# COMMAND ----------

# Modo headless: quando executado via dbutils.notebook.run(), pula visualizações
# e apenas valida que as tabelas necessárias existem
import sys

try:
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    HEADLESS_MODE = True
except:
    HEADLESS_MODE = False

print(f"Modo de execução: {'HEADLESS (via pipeline)' if HEADLESS_MODE else 'INTERATIVO'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Funções auxiliares
# MAGIC Helpers de formatação e verificação de tabela usados no resto do notebook.

# COMMAND ----------

from typing import Optional, List

def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def require_tables(tables: List[str]) -> None:
    missing = [table for table in tables if not table_exists(table)]
    if missing:
        raise RuntimeError(
            "Execute os notebooks anteriores antes do dashboard. "
            f"Tabelas obrigatórias ausentes: {', '.join(missing)}"
        )


def numeric_column_like(df: DataFrame, token: str) -> Optional[str]:
    """Localiza uma coluna numérica cujo nome contenha o token informado."""
    numeric_prefixes = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")
    candidates = [
        column
        for column, dtype in df.dtypes
        if token.lower() in column.lower() and dtype.lower().startswith(numeric_prefixes)
    ]
    return candidates[0] if candidates else None


def normalize_rate(column: F.Column) -> F.Column:
    """Normaliza percentuais 0-100 para frações 0-1 sem alterar valores já normalizados."""
    return F.when(column > 1.0, column / F.lit(100.0)).otherwise(column)


def safe_count(table_name: str) -> int:
    return spark.table(table_name).count() if table_exists(table_name) else 0


def scalar(df: DataFrame, column: str, default=None):
    rows = df.select(column).limit(1).collect()
    return rows[0][column] if rows and rows[0][column] is not None else default


def fmt_int(value) -> str:
    return f"{int(value or 0):,}".replace(",", ".")


def fmt_num(value, decimals: int = 1) -> str:
    """Formata número em pt-BR (troca apenas o separador decimal)."""
    return f"{value:.{decimals}f}".replace(".", ",")


def fmt_pct(value, decimals: int = 1) -> str:
    return "N/D" if value is None else f"{fmt_num(value * 100, decimals)}%"


def fmt_pp(value, decimals: int = 1) -> str:
    if value is None:
        return "N/D"
    num = fmt_num(value * 100, decimals)
    return f"+{num} p.p." if value > 0 else f"{num} p.p."


require_tables([T_RESUMO_UF, T_IND_MUN, T_META_RESULTADO])

resumo_uf = spark.table(T_RESUMO_UF)
indicador_municipio = spark.table(T_IND_MUN)
meta_vs_resultado = spark.table(T_META_RESULTADO)

print("✓ Dashboard: tabelas validadas com sucesso")
print(f"  - {T_RESUMO_UF}: {resumo_uf.count()} registros")
print(f"  - {T_IND_MUN}: {indicador_municipio.count()} registros")
print(f"  - {T_META_RESULTADO}: {meta_vs_resultado.count()} registros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Anos disponíveis
# MAGIC Lê os anos reais presentes em gold.resumo_uf, em vez de anos fixos no
# MAGIC código, para os filtros do dashboard sempre baterem com o dado de verdade.

# COMMAND ----------

available_years = [
    str(row["ano"])
    for row in resumo_uf.select("ano").distinct().orderBy(F.desc("ano")).collect()
]
if not available_years:
    raise RuntimeError("gold.resumo_uf está vazia. Execute novamente a construção da Gold.")

available_ufs = [
    row["sigla_uf"]
    for row in resumo_uf.select("sigla_uf")
    .where(F.col("sigla_uf").isNotNull())
    .distinct()
    .orderBy("sigla_uf")
    .collect()
]

for widget_name in ["ano_dashboard", "rede_dashboard", "uf_dashboard"]:
    try:
        dbutils.widgets.remove(widget_name)
    except Exception:
        pass

dbutils.widgets.dropdown(
    "ano_dashboard",
    available_years[0],
    available_years,
    "Ano de referência",
)
dbutils.widgets.dropdown(
    "rede_dashboard",
    "Rede pública",
    ["Rede pública", "Total", "Estadual", "Municipal", "Privada"],
    "Rede de ensino",
)
dbutils.widgets.dropdown(
    "uf_dashboard",
    "Todas",
    ["Todas"] + available_ufs,
    "Recorte territorial",
)

ANO = int(dbutils.widgets.get("ano_dashboard"))
REDE_SELECIONADA = dbutils.widgets.get("rede_dashboard")
UF_SELECIONADA = dbutils.widgets.get("uf_dashboard")

REDE_CODES = {
    "Rede pública": [2, 3],
    "Total": [0],
    "Estadual": [2],
    "Municipal": [3],
    "Privada": [5],
}
rede_codes = REDE_CODES[REDE_SELECIONADA]

print(f"Filtros ativos: ano={ANO} | rede={REDE_SELECIONADA} | UF={UF_SELECIONADA}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Dimensão territorial
# MAGIC Mapa de UF para região, independente de variações de schema da fonte.

# COMMAND ----------

# Dimensão regional independente de variações de schema da fonte de UF.
REGIAO_UF = {
    "AC": "Norte", "AP": "Norte", "AM": "Norte", "PA": "Norte",
    "RO": "Norte", "RR": "Norte", "TO": "Norte",
    "AL": "Nordeste", "BA": "Nordeste", "CE": "Nordeste", "MA": "Nordeste",
    "PB": "Nordeste", "PE": "Nordeste", "PI": "Nordeste", "RN": "Nordeste", "SE": "Nordeste",
    "DF": "Centro-Oeste", "GO": "Centro-Oeste", "MT": "Centro-Oeste", "MS": "Centro-Oeste",
    "ES": "Sudeste", "MG": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "RS": "Sul", "SC": "Sul",
}
regiao_dim = spark.createDataFrame(
    [(uf, regiao) for uf, regiao in REGIAO_UF.items()],
    ["sigla_uf", "regiao"],
)

# Base filtrada por rede. Para "Rede pública", média das redes estadual e municipal disponíveis.
uf_ano = (
    resumo_uf
    .filter((F.col("ano") == ANO) & F.col("rede").isin(rede_codes))
    .groupBy("sigla_uf")
    .agg(
        F.avg("taxa_alfabetizacao_media").alias("taxa_resultado"),
        F.max("updated_at").alias("updated_at"),
    )
    .join(regiao_dim, "sigla_uf", "left")
)

if UF_SELECIONADA != "Todas":
    uf_ano = uf_ano.filter(F.col("sigla_uf") == UF_SELECIONADA)

# Ano anterior disponível, usado para calcular tendência.
previous_years = [int(year) for year in available_years if int(year) < ANO]
ANO_ANTERIOR = max(previous_years) if previous_years else None

if ANO_ANTERIOR is not None:
    uf_anterior = (
        resumo_uf
        .filter((F.col("ano") == ANO_ANTERIOR) & F.col("rede").isin(rede_codes))
        .groupBy("sigla_uf")
        .agg(F.avg("taxa_alfabetizacao_media").alias("taxa_anterior"))
    )
else:
    uf_anterior = spark.createDataFrame(
        [],
        T.StructType([
            T.StructField("sigla_uf", T.StringType()),
            T.StructField("taxa_anterior", T.DoubleType()),
        ]),
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Metas normalizadas
# MAGIC Identifica defensivamente a coluna numérica de meta em cada fonte oficial.

# COMMAND ----------

# Metas normalizadas. O código identifica defensivamente a coluna numérica de meta.
meta_uf_norm = None
if table_exists(T_META_UF):
    raw_meta_uf = spark.table(T_META_UF)
    meta_column = numeric_column_like(raw_meta_uf, "meta")
    if meta_column and {"ano", "sigla_uf"}.issubset(raw_meta_uf.columns):
        meta_uf_norm = (
            raw_meta_uf
            .select(
                F.col("ano").cast("int").alias("ano"),
                F.upper(F.trim(F.col("sigla_uf"))).alias("sigla_uf"),
                normalize_rate(F.col(meta_column).cast("double")).alias("meta_uf"),
            )
            .dropDuplicates(["ano", "sigla_uf"])
        )

if meta_uf_norm is None:
    meta_uf_norm = spark.createDataFrame(
        [],
        T.StructType([
            T.StructField("ano", T.IntegerType()),
            T.StructField("sigla_uf", T.StringType()),
            T.StructField("meta_uf", T.DoubleType()),
        ]),
    )

meta_brasil_norm = None
if table_exists(T_META_BRASIL):
    raw_meta_brasil = spark.table(T_META_BRASIL)
    meta_column = numeric_column_like(raw_meta_brasil, "meta")
    if meta_column and "ano" in raw_meta_brasil.columns:
        meta_brasil_norm = (
            raw_meta_brasil
            .select(
                F.col("ano").cast("int").alias("ano"),
                normalize_rate(F.col(meta_column).cast("double")).alias("meta_brasil"),
            )
            .dropDuplicates(["ano"])
        )

if meta_brasil_norm is None:
    meta_brasil_norm = spark.createDataFrame(
        [],
        T.StructType([
            T.StructField("ano", T.IntegerType()),
            T.StructField("meta_brasil", T.DoubleType()),
        ]),
    )

# Ranking enriquecido com meta, variação e score de prioridade.
ranking_ufs = (
    uf_ano
    .join(uf_anterior, "sigla_uf", "left")
    .join(
        meta_uf_norm.filter(F.col("ano") == ANO).drop("ano"),
        "sigla_uf",
        "left",
    )
    .withColumn("variacao", F.col("taxa_resultado") - F.col("taxa_anterior"))
    .withColumn("gap_meta", F.col("taxa_resultado") - F.col("meta_uf"))
    .withColumn(
        "status_meta",
        F.when(F.col("meta_uf").isNull(), F.lit("Meta indisponível"))
        .when(F.col("gap_meta") >= 0, F.lit("Na trajetória"))
        .when(F.col("gap_meta") >= -0.05, F.lit("Atenção"))
        .otherwise(F.lit("Prioridade")),
    )
    .withColumn(
        "score_prioridade",
        F.round(
            F.greatest(F.lit(0.0), -F.coalesce(F.col("gap_meta"), F.lit(0.0))) * 70
            + F.greatest(F.lit(0.0), -F.coalesce(F.col("variacao"), F.lit(0.0))) * 30,
            2,
        ),
    )
    .withColumn("taxa_pct", F.round(F.col("taxa_resultado") * 100, 1))
    .withColumn("meta_pct", F.round(F.col("meta_uf") * 100, 1))
    .withColumn("gap_pp", F.round(F.col("gap_meta") * 100, 1))
    .withColumn("variacao_pp", F.round(F.col("variacao") * 100, 1))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. KPIs educacionais e municipais
# MAGIC Calcula os indicadores agregados usados nos cartões de resumo da visão
# MAGIC geral (taxa média, municípios na meta, UFs na meta).

# COMMAND ----------

# KPIs educacionais
kpi_educacao = uf_ano.agg(
    F.avg("taxa_resultado").alias("taxa_media_ufs"),
    F.countDistinct("sigla_uf").alias("ufs_com_dados"),
    F.max("updated_at").alias("gold_updated_at"),
).collect()[0]

taxa_media_ufs = kpi_educacao["taxa_media_ufs"]
ufs_com_dados = kpi_educacao["ufs_com_dados"]
gold_updated_at = kpi_educacao["gold_updated_at"]

meta_nacional = scalar(
    meta_brasil_norm.filter(F.col("ano") == ANO),
    "meta_brasil",
)
gap_nacional = (
    taxa_media_ufs - meta_nacional
    if taxa_media_ufs is not None and meta_nacional is not None
    else None
)

meta_stats = ranking_ufs.filter(F.col("meta_uf").isNotNull()).agg(
    F.sum(F.when(F.col("gap_meta") >= 0, 1).otherwise(0)).alias("ufs_na_meta"),
    F.count("*").alias("ufs_com_meta"),
).collect()[0]
ufs_na_meta = meta_stats["ufs_na_meta"] or 0
ufs_com_meta = meta_stats["ufs_com_meta"] or 0
pct_ufs_na_meta = ufs_na_meta / ufs_com_meta if ufs_com_meta else None

# KPIs municipais
# nivel_territorial filtra só município: meta_vs_resultado mistura UF e
# município na mesma tabela (correção pedida pelo professor), então toda
# agregação precisa declarar explicitamente qual grão está usando.
municipal_filtrado = meta_vs_resultado.filter(
    (F.col("ano") == ANO)
    & F.col("rede").isin(rede_codes)
    & (F.col("nivel_territorial") == "municipio")
)
if UF_SELECIONADA != "Todas":
    municipal_filtrado = municipal_filtrado.filter(F.col("sigla_uf") == UF_SELECIONADA)

municipal_stats = municipal_filtrado.agg(
    F.countDistinct("id_municipio").alias("municipios_monitorados"),
    F.avg(F.col("atingiu_meta").cast("double")).alias("pct_municipios_meta"),
).collect()[0]
municipios_monitorados = municipal_stats["municipios_monitorados"] or 0
pct_municipios_meta = municipal_stats["pct_municipios_meta"]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Ranking territorial
# MAGIC Tabela completa de UFs ordenada por resultado, pronta para exibição.

# COMMAND ----------

ranking_dashboard = ranking_ufs.select(
    "sigla_uf",
    "regiao",
    "taxa_pct",
    "meta_pct",
    "gap_pp",
    "variacao_pp",
    "status_meta",
    "score_prioridade",
).orderBy(F.desc("taxa_pct"))

display(ranking_dashboard)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Matriz de prioridade
# MAGIC Cruza resultado atual com evolução em relação ao ano anterior.

# COMMAND ----------

matriz_prioridade = (
    ranking_ufs
    .filter(F.col("taxa_anterior").isNotNull())
    .select(
        "sigla_uf",
        "regiao",
        "taxa_pct",
        "variacao_pp",
        "gap_pp",
        "status_meta",
        "score_prioridade",
    )
    .orderBy(F.desc("score_prioridade"))
)

display(matriz_prioridade)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Trajetória histórica
# MAGIC Série de resultado observado ao longo dos anos, para comparar com a meta.

# COMMAND ----------

resultado_historico = (
    resumo_uf
    .filter(F.col("rede").isin([0, 2, 3, 5]))
    .groupBy("ano", "rede_label")
    .agg(F.avg("taxa_alfabetizacao_media").alias("valor"))
    .select(
        "ano",
        F.concat(F.lit("Resultado · "), F.initcap("rede_label")).alias("serie"),
        F.round(F.col("valor") * 100, 1).alias("valor_pct"),
        F.lit("Resultado observado").alias("tipo"),
    )
)

meta_historica = (
    meta_brasil_norm
    .select(
        "ano",
        F.lit("Meta nacional").alias("serie"),
        F.round(F.col("meta_brasil") * 100, 1).alias("valor_pct"),
        F.lit("Meta").alias("tipo"),
    )
)

trajetoria_2030 = resultado_historico.unionByName(meta_historica).orderBy("ano", "serie")
display(trajetoria_2030)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Desigualdade regional
# MAGIC Compara resultado por região entre rede pública e privada.

# COMMAND ----------

desigualdade_regional = (
    resumo_uf

    # Seleciona somente as colunas necessárias.
    # Isso evita conflito com a coluna "regiao" já existente na Gold.
    .select(
        "ano",
        "sigla_uf",
        "rede",
        "taxa_alfabetizacao_media"
    )

    # Compara apenas redes de ensino.
    # rede=0 é o agregado Total e não entra nesta análise.
    .filter(
        (F.col("ano") == ANO)
        & F.col("rede").isin([2, 3, 5])
    )

    .withColumn(
        "sigla_uf",
        F.upper(F.trim(F.col("sigla_uf")))
    )

    .withColumn(
        "rede_ensino",
        F.when(F.col("rede") == 2, F.lit("Estadual"))
         .when(F.col("rede") == 3, F.lit("Municipal"))
         .when(F.col("rede") == 5, F.lit("Privada"))
    )

    .join(
        regiao_dim,
        on="sigla_uf",
        how="left"
    )

    .filter(
        F.col("regiao").isNotNull()
    )

    .groupBy(
        "regiao",
        "rede_ensino"
    )

    .agg(
        F.round(
            F.avg("taxa_alfabetizacao_media") * 100,
            1
        ).alias("taxa_pct"),

        F.countDistinct(
            "sigla_uf"
        ).alias("ufs_com_dados")
    )

    .orderBy(
        "regiao",
        "rede_ensino"
    )
)

display(desigualdade_regional)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 11. Municípios prioritários
# MAGIC Lista os municípios mais distantes da meta, com nome quando disponível.

# COMMAND ----------

municipio_dim = None

if table_exists(f"{CATALOG}.bronze.municipio"):
    raw_municipio = spark.table(f"{CATALOG}.bronze.municipio")

    name_candidates = [
        "nome_municipio",
        "municipio",
        "nome"
    ]

    municipality_name = next(
        (
            c for c in name_candidates
            if c in raw_municipio.columns
        ),
        None
    )

    if municipality_name and "id_municipio" in raw_municipio.columns:
        municipio_dim = (
            raw_municipio
            .select(
                F.lpad(
                    F.col("id_municipio").cast("string"),
                    7,
                    "0"
                ).alias("id_municipio"),

                F.col(municipality_name)
                .cast("string")
                .alias("nome_municipio")
            )
            .dropDuplicates(["id_municipio"])
        )


municipios_prioritarios = (
    municipal_filtrado

    .filter(
        F.col("meta_taxa").isNotNull()
    )

    .withColumn(
        "taxa_pct",
        F.round(
            F.col("taxa_alfabetizacao_media") * 100,
            1
        )
    )

    .withColumn(
        "meta_pct",
        F.round(
            F.col("meta_taxa") * 100,
            1
        )
    )

    .withColumn(
        "gap_pp",
        F.round(
            F.col("gap_meta") * 100,
            1
        )
    )

    .withColumn(
        "severidade",
        F.when(
            F.col("gap_meta") >= 0,
            "Na meta"
        )
        .when(
            F.col("gap_meta") >= -0.05,
            "Atenção"
        )
        .when(
            F.col("gap_meta") >= -0.10,
            "Alta"
        )
        .otherwise("Crítica")
    )
)


if municipio_dim is not None:

    # Evita duas colunas nome_municipio após o join.
    municipios_prioritarios = (
        municipios_prioritarios
        .drop("nome_municipio")
        .join(
            municipio_dim,
            on="id_municipio",
            how="left"
        )
    )

elif "nome_municipio" not in municipios_prioritarios.columns:

    municipios_prioritarios = (
        municipios_prioritarios
        .withColumn(
            "nome_municipio",
            F.lit(None).cast("string")
        )
    )


municipios_prioritarios = (
    municipios_prioritarios
    .select(
        "sigla_uf",
        "id_municipio",
        "nome_municipio",
        "rede_label",
        "taxa_pct",
        "meta_pct",
        "gap_pp",
        "severidade",
        "updated_at"
    )
    .orderBy(
        F.asc("gap_pp")
    )
    .limit(25)
)

display(municipios_prioritarios)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 12. Pulso do streaming
# MAGIC Volume de eventos e latência de ingestão por janela de hora.

# COMMAND ----------

if table_exists(T_EVENTOS):
    eventos = spark.table(T_EVENTOS)

    if {"event_time", "_ingestion_timestamp"}.issubset(eventos.columns):
        streaming_pulse = (
            eventos
            .withColumn("janela", F.date_trunc("hour", F.col("_ingestion_timestamp")))
            .withColumn(
                "latency_seconds",
                F.unix_timestamp("_ingestion_timestamp") - F.unix_timestamp("event_time"),
            )
            .groupBy("janela")
            .agg(
                F.count("*").alias("eventos"),
                F.round(F.avg("latency_seconds"), 1).alias("latencia_media_s"),
                F.round(
                    F.expr("percentile_approx(latency_seconds, 0.95)"), 1
                ).alias("latencia_p95_s"),
                F.countDistinct("id_municipio").alias("municipios"),
            )
            .orderBy("janela")
        )
        display(streaming_pulse)

        latest_stream = (
            eventos
            .select(
                "event_time",
                "_ingestion_timestamp",
                "sigla_uf",
                "id_municipio",
                "rede",
                "taxa_alfabetizacao",
                "source",
            )
            .orderBy(F.desc("_ingestion_timestamp"))
            .limit(20)
        )
        display(latest_stream)
    else:
        print("⚠ A tabela de streaming existe, mas não possui event_time e _ingestion_timestamp.")
else:
    print("⚠ bronze.eventos_streaming ainda não existe. Execute o notebook 02.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 13. Distribuição de alunos e corte de 743 pontos
# MAGIC Classifica os alunos por faixa de proficiência usando a regra oficial.

# COMMAND ----------

if table_exists(T_ALUNOS):
    alunos_raw = spark.table(T_ALUNOS)

    # Compatibilidade com o schema bruto oficial do INEP.
    # A análise abaixo continua usando os mesmos nomes canônicos do dashboard.
    if {
        "NU_ANO_AVALIACAO",
        "SG_UF",
        "TP_DEPENDENCIA",
        "VL_PROFICIENCIA_LP",
    }.issubset(alunos_raw.columns):
        alunos = (
            alunos_raw
            .select(
                F.col("NU_ANO_AVALIACAO").cast("int").alias("ano"),
                F.upper(F.trim(F.col("SG_UF"))).alias("sigla_uf"),
                F.when(
                    F.col("TP_DEPENDENCIA").cast("int") == 4,
                    F.lit(5)
                ).otherwise(
                    F.col("TP_DEPENDENCIA").cast("int")
                ).alias("rede"),
                F.regexp_replace(
                    F.trim(F.col("VL_PROFICIENCIA_LP")),
                    ",",
                    "."
                ).cast("double").alias("proficiencia_portugues"),
            )
            .filter(F.col("proficiencia_portugues").isNotNull())
        )
    else:
        alunos = alunos_raw

    if "proficiencia_portugues" in alunos.columns:
        alunos_ano = alunos.filter(F.col("ano") == ANO) if "ano" in alunos.columns else alunos

        if UF_SELECIONADA != "Todas" and "sigla_uf" in alunos_ano.columns:
            alunos_ano = alunos_ano.filter(F.col("sigla_uf") == UF_SELECIONADA)

        faixas_alunos = (
            alunos_ano
            .withColumn(
                "faixa_proficiencia",
                F.when(F.col("proficiencia_portugues") < 650, "1 · Abaixo de 650")
                .when(F.col("proficiencia_portugues") < 700, "2 · 650 a 699")
                .when(F.col("proficiencia_portugues") < 743, "3 · 700 a 742")
                .when(F.col("proficiencia_portugues") < 800, "4 · 743 a 799")
                .otherwise("5 · 800 ou mais"),
            )
            .withColumn(
                "classificacao",
                F.when(F.col("proficiencia_portugues") >= 743, "Alfabetizado")
                .otherwise("Abaixo do corte"),
            )
            .groupBy("faixa_proficiencia", "classificacao")
            .agg(
                F.count("*").alias("alunos"),
                F.round(F.avg("proficiencia_portugues"), 1).alias("proficiencia_media"),
            )
            .orderBy("faixa_proficiencia")
        )
        display(faixas_alunos)

        resumo_alunos = (
            alunos_ano
            .groupBy("ano", "rede")
            .agg(
                F.count("*").alias("alunos"),
                F.round(
                    F.avg(
                        F.when(
                            F.col("proficiencia_portugues") >= 743,
                            1.0
                        ).otherwise(0.0)
                    ) * 100,
                    1,
                ).alias("pct_alfabetizados"),
                F.round(
                    F.avg("proficiencia_portugues"),
                    1
                ).alias("proficiencia_media"),
            )
            .withColumn(
                "rede_label",
                F.when(F.col("rede") == 0, "total")
                .when(F.col("rede") == 2, "estadual")
                .when(F.col("rede") == 3, "municipal")
                .when(F.col("rede") == 5, "privada")
                .otherwise(F.concat(F.lit("rede "), F.col("rede"))),
            )
            .select(
                "ano",
                "rede_label",
                "alunos",
                "pct_alfabetizados",
                "proficiencia_media",
            )
            .orderBy("ano", F.desc("pct_alfabetizados"))
        )
        display(resumo_alunos)
    else:
        print("⚠ bronze.alunos não contém proficiência utilizável.")
else:
    print("⚠ bronze.alunos ainda não existe. A visão de 743 pontos ficará indisponível.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 14. Saúde operacional do pipeline
# MAGIC Métricas das últimas execuções: duração, linhas lidas, gravadas e rejeitadas.

# COMMAND ----------

if table_exists(T_METRICAS):
    metricas_dashboard = (
        spark.table(T_METRICAS)
        .withColumn(
            "duracao_s",
            F.round(
                F.unix_timestamp("finished_at") - F.unix_timestamp("started_at"),
                1,
            ),
        )
        .withColumn(
            "taxa_rejeicao_pct",
            F.round(
                F.when(
                    (
                        F.coalesce(F.col("rows_written"), F.lit(0))
                        + F.coalesce(F.col("rows_rejected"), F.lit(0))
                    ) > 0,
                    F.coalesce(F.col("rows_rejected"), F.lit(0))
                    / (
                        F.coalesce(F.col("rows_written"), F.lit(0))
                        + F.coalesce(F.col("rows_rejected"), F.lit(0))
                    )
                    * 100,
                ).otherwise(0.0),
                2,
            ),
        )
        .withColumn(
            "status_normalizado",
            F.when(F.upper(F.col("status")).isin("SUCCESS", "SUCCEEDED"), "Saudável")
            .when(F.upper(F.col("status")).isin("RUNNING", "PENDING"), "Em execução")
            .otherwise("Falha/Atenção"),
        )
        .select(
            "finished_at",
            "task_name",
            "status_normalizado",
            "rows_read",
            "rows_written",
            "rows_rejected",
            "taxa_rejeicao_pct",
            "duracao_s",
            "max_event_time",
            "schema_version",
        )
        .orderBy(F.desc("finished_at"))
        .limit(30)
    )
    display(metricas_dashboard)
else:
    print("⚠ observability.pipeline_metrics ainda não existe. Execute o notebook 08.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 15. Painel de decisão
# MAGIC Traduz o status de cada UF em uma recomendação de ação.

# COMMAND ----------

action_board = (
    ranking_ufs
    .withColumn(
        "recomendacao",
        F.when(F.col("status_meta") == "Prioridade", "Plano intensivo e diagnóstico territorial")
        .when(F.col("status_meta") == "Atenção", "Monitoramento mensal e intervenção focalizada")
        .when(F.col("status_meta") == "Na trajetória", "Preservar avanço e compartilhar práticas")
        .otherwise("Completar dados de meta antes da decisão"),
    )
    .select(
        "sigla_uf",
        "regiao",
        "taxa_pct",
        "meta_pct",
        "gap_pp",
        "variacao_pp",
        "score_prioridade",
        "status_meta",
        "recomendacao",
    )
    .orderBy(F.desc("score_prioridade"), F.asc("gap_pp"))
)

display(action_board)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 16. Paleta e helpers de gráficos SVG
# MAGIC Cores e funções reutilizáveis para montar os gráficos nativos a seguir.

# COMMAND ----------

# Paleta e moldura compartilhadas
C_BG = "linear-gradient(135deg,#07111f,#101a35)"
C_TEXT, C_MUTED = "#eef5ff", "#b7c5d9"
C_CYAN, C_GREEN, C_AMBER, C_RED, C_VIOLET = "#34d7e7", "#2de2a0", "#ffc857", "#ff6b7a", "#8b7cff"
STATUS_COLORS = {
    "Na trajetória": C_GREEN, "Atenção": C_AMBER,
    "Prioridade": C_RED, "Meta indisponível": C_MUTED,
}


def chart_box(title: str, subtitle: str, body: str) -> str:
    return (
        f'<div style="width:100%;background:{C_BG};border:1px solid rgba(255,255,255,.15);'
        f'border-radius:18px;padding:22px 24px;margin:8px 0;color:{C_TEXT};'
        f'font-family:Inter,\'Segoe UI\',sans-serif;box-shadow:0 14px 38px rgba(0,0,0,.28);overflow:hidden">'
        f'<div style="font-size:20px;font-weight:850;letter-spacing:-.02em;color:#ffffff">{title}</div>'
        f'<div style="font-size:13px;color:{C_MUTED};margin:5px 0 18px;line-height:1.45">{subtitle}</div>'
        f'{body}</div>'
    )


def legend(items) -> str:
    dots = "".join(
        f'<span style="display:inline-flex;align-items:center;gap:7px;margin:4px 16px 4px 0;'
        f'font-size:12px;color:{C_MUTED};white-space:nowrap"><span style="width:10px;height:10px;'
        f'border-radius:99px;background:{color};display:inline-block;box-shadow:0 0 0 2px rgba(255,255,255,.06)"></span>{label}</span>'
        for label, color in items
    )
    return f'<div style="display:flex;flex-wrap:wrap;align-items:center;margin-top:14px">{dots}</div>'


def svg_line_chart(series, x_values, y_min, y_max, width=1040, height=360, pad=58) -> str:
    x_lo, x_hi = min(x_values), max(x_values)

    def sx(x):
        return pad + (x - x_lo) / ((x_hi - x_lo) or 1) * (width - 2 * pad)

    def sy(y):
        return height - pad - (y - y_min) / ((y_max - y_min) or 1) * (height - 2 * pad)

    p = [f'<svg viewBox="0 0 {width} {height}" style="width:100%;height:auto;display:block;overflow:visible">']
    for i in range(5):
        yv = y_min + (y_max - y_min) * i / 4
        p.append(f'<line x1="{pad}" y1="{sy(yv):.1f}" x2="{width - pad}" y2="{sy(yv):.1f}" '
                 f'stroke="rgba(255,255,255,.15)" stroke-width="1"/>')
        p.append(f'<text x="{pad - 10}" y="{sy(yv) + 4:.1f}" fill="{C_MUTED}" font-size="12" '
                 f'font-weight="600" text-anchor="end">{yv:.0f}%</text>')
    for x in x_values:
        p.append(f'<text x="{sx(x):.1f}" y="{height - pad + 24}" fill="{C_MUTED}" '
                 f'font-size="12" font-weight="600" text-anchor="middle">{x}</text>')
    for _name, color, pts, dash in series:
        line = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in pts)
        p.append(f'<polyline points="{line}" fill="none" stroke="{color}" stroke-width="4" '
                 f'stroke-dasharray="{dash}" stroke-linecap="round" stroke-linejoin="round"/>')
        for x, y in pts:
            p.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="5" fill="{color}" stroke="#07111f" stroke-width="2"/>')
    p.append("</svg>")
    return "".join(p)


def donut(pct: float | None, label: str, color: str) -> str:
    if pct is None:
        pct = 0.0
    circ = 2 * 3.14159 * 44
    filled = circ * min(max(pct, 0), 1)
    return (
        f'<div style="text-align:center;min-width:155px;padding:5px 8px">'
        f'<svg viewBox="0 0 110 110" style="width:118px;height:118px">'
        f'<circle cx="55" cy="55" r="44" fill="none" stroke="rgba(255,255,255,.12)" stroke-width="12"/>'
        f'<circle cx="55" cy="55" r="44" fill="none" stroke="{color}" stroke-width="12" '
        f'stroke-linecap="round" stroke-dasharray="{filled:.1f} {circ:.1f}" '
        f'transform="rotate(-90 55 55)"/>'
        f'<text x="55" y="62" fill="{C_TEXT}" font-size="22" font-weight="850" '
        f'text-anchor="middle">{pct * 100:.0f}%</text></svg>'
        f'<div style="font-size:12px;color:{C_MUTED};margin-top:7px;max-width:170px;line-height:1.35">{label}</div></div>'
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 17. Gráfico 1: donuts de progresso
# MAGIC Progresso geral e ranking com marcador de meta, renderizado via displayHTML.

# COMMAND ----------

# ---- Gráfico 1 · Donuts de progresso + ranking com marcador de meta ----
rk = ranking_ufs.orderBy(F.desc("taxa_pct")).collect()

donuts = (
    '<div style="display:flex;gap:18px;flex-wrap:wrap;justify-content:center;align-items:flex-start">'
    + donut(pct_ufs_na_meta, "UFs na trajetória da meta", C_GREEN)
    + donut(pct_municipios_meta, "Municípios monitorados na meta", C_CYAN)
    + donut(taxa_media_ufs, f"Resultado médio · {REDE_SELECIONADA} {ANO}", C_VIOLET)
    + (donut(meta_nacional, f"Meta nacional {ANO}", C_AMBER) if meta_nacional else "")
    + "</div>"
)
progresso_chart_html = chart_box(
    "Progresso rumo a 2030",
    "Visão executiva dos principais indicadores do recorte selecionado",
    donuts,
)
displayHTML(progresso_chart_html)

bars = []
for r in rk:
    taxa = r["taxa_pct"] or 0.0
    color = STATUS_COLORS.get(r["status_meta"], C_MUTED)
    marker = ""
    if r["meta_pct"] is not None:
        marker = (f'<div style="position:absolute;left:{min(r["meta_pct"], 100):.1f}%;top:-4px;'
                  f'bottom:-4px;width:2px;background:{C_TEXT};opacity:.95" '
                  f'title="meta {r["meta_pct"]}%"></div>')
    gap_txt = f'{r["gap_pp"]:+.1f} pp'.replace(".", ",") if r["gap_pp"] is not None else "N/D"
    bars.append(
        f'<div style="display:flex;align-items:center;gap:10px;margin:7px 0">'
        f'<div style="width:34px;font-size:12px;font-weight:800;color:{C_TEXT}">{r["sigla_uf"]}</div>'
        f'<div style="flex:1;position:relative;height:18px;background:rgba(255,255,255,.10);'
        f'border-radius:99px">{marker}'
        f'<div style="position:absolute;left:0;top:0;bottom:0;width:{min(taxa, 100):.1f}%;'
        f'background:{color};border-radius:99px;opacity:.95"></div></div>'
        f'<div style="width:56px;font-size:12px;font-weight:800;text-align:right;color:{C_TEXT}">'
        f'{str(taxa).replace(".", ",")}%</div>'
        f'<div style="width:72px;font-size:11px;color:{C_MUTED};text-align:right">{gap_txt}</div>'
        f'</div>'
    )
ranking_html = "".join(bars) + legend(
    [(s, c) for s, c in STATUS_COLORS.items()] + [("│ marcador = meta da UF", C_TEXT)]
)
ranking_chart_html = chart_box(
    f"Ranking das UFs · Indicador Criança Alfabetizada · {REDE_SELECIONADA} {ANO}",
    "Barra = resultado · marcador branco = meta do ano · cor = status da trajetória",
    ranking_html,
)
displayHTML(ranking_chart_html)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 18. Gráfico 2: trajetória até 2030

# COMMAND ----------

# ---- Gráfico 2 · Trajetória até 2030 (resultado observado x meta nacional) ----
hist = resultado_historico.collect()
metas_l = meta_brasil_norm.orderBy("ano").collect()

series_map = {}
for r in hist:
    series_map.setdefault(r["serie"], []).append((int(r["ano"]), float(r["valor_pct"])))
palette = [C_CYAN, C_GREEN, C_VIOLET, C_AMBER]
series = [
    (name, palette[i % len(palette)], sorted(pts), "")
    for i, (name, pts) in enumerate(sorted(series_map.items()))
]
if metas_l:
    series.append((
        "Meta nacional", C_RED,
        [(int(m["ano"]), float(m["meta_brasil"]) * 100) for m in metas_l], "7 5",
    ))

all_years = sorted({x for _n, _c, pts, _d in series for x, _y in pts})
all_vals = [y for _n, _c, pts, _d in series for _x, y in pts]
svg = svg_line_chart(series, all_years, max(min(all_vals) - 5, 0), min(max(all_vals) + 5, 100))
trajetoria_chart_html = chart_box(
    "Jornada até 2030 · resultado observado x meta nacional",
    "Linhas sólidas = resultado por rede · tracejada vermelha = meta nacional disponível",
    svg + legend([(n, c) for n, c, _p, _d in series]),
)
displayHTML(trajetoria_chart_html)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 19. Gráfico 3: matriz de prioridade

# COMMAND ----------

# ---- Gráfico 3 · Matriz de prioridade (dispersão desempenho x evolução) ----
mp = matriz_prioridade.collect()
matriz_chart_html = ""
if mp:
    W, H, PAD = 1040, 410, 64
    xs = [float(r["taxa_pct"]) for r in mp]
    ys = [float(r["variacao_pp"]) for r in mp]
    x_lo, x_hi = min(xs) - 4, max(xs) + 4
    y_lo, y_hi = min(ys) - 2, max(ys) + 2

    def px(v):
        return PAD + (v - x_lo) / ((x_hi - x_lo) or 1) * (W - 2 * PAD)

    def py(v):
        return H - PAD - (v - y_lo) / ((y_hi - y_lo) or 1) * (H - 2 * PAD)

    pts = [f'<svg viewBox="0 0 {W} {H}" style="width:100%;height:auto;display:block;overflow:visible">']
    for i in range(5):
        xv = x_lo + (x_hi - x_lo) * i / 4
        pts.append(f'<text x="{px(xv):.0f}" y="{H - PAD + 24}" fill="{C_MUTED}" font-size="12" '
                   f'font-weight="600" text-anchor="middle">{xv:.0f}%</text>')
    if y_lo < 0 < y_hi:
        pts.append(f'<line x1="{PAD}" y1="{py(0):.0f}" x2="{W - PAD}" y2="{py(0):.0f}" '
                   f'stroke="rgba(255,255,255,.35)" stroke-dasharray="5 5"/>')
        pts.append(f'<text x="{W - PAD}" y="{py(0) - 8:.0f}" fill="{C_MUTED}" font-size="11" '
                   f'text-anchor="end">estabilidade</text>')
    for r in mp:
        color = STATUS_COLORS.get(r["status_meta"], C_MUTED)
        raio = 7 + min(float(r["score_prioridade"] or 0), 12)
        pts.append(f'<circle cx="{px(float(r["taxa_pct"])):.0f}" cy="{py(float(r["variacao_pp"])):.0f}" '
                   f'r="{raio:.0f}" fill="{color}" fill-opacity=".82" stroke="#eef5ff" stroke-opacity=".24"/>')
        pts.append(f'<text x="{px(float(r["taxa_pct"])):.0f}" '
                   f'y="{py(float(r["variacao_pp"])) - raio - 5:.0f}" fill="{C_TEXT}" '
                   f'font-size="12" font-weight="800" text-anchor="middle">{r["sigla_uf"]}</text>')
    pts.append(f'<text x="{W / 2:.0f}" y="{H - 12}" fill="{C_MUTED}" font-size="12" '
               f'font-weight="700" text-anchor="middle">Resultado {ANO} (%)</text>')
    pts.append(f'<text x="18" y="{H / 2:.0f}" fill="{C_MUTED}" font-size="12" '
               f'font-weight="700" transform="rotate(-90 18 {H / 2:.0f})" text-anchor="middle">Variação vs ano anterior (p.p.)</text>')
    pts.append("</svg>")
    matriz_chart_html = chart_box(
        "Matriz de prioridade · desempenho x evolução",
        "Tamanho da bolha = score de prioridade · quadrante inferior-esquerdo = agir primeiro",
        "".join(pts) + legend(list(STATUS_COLORS.items())),
    )
    displayHTML(matriz_chart_html)
else:
    print("Sem ano anterior no recorte para montar a matriz.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 20. Gráfico 4: distribuição de alunos e corte 743

# COMMAND ----------

# ---- Gráfico 4 · Distribuição dos alunos e a linha de corte 743 (INEP oficial) ----
alunos_chart_html = ""
if table_exists(T_ALUNOS):
    dist = (
        alunos
        .filter(F.col("ano") == ANO)
        .withColumn("faixa", (F.floor(F.col("proficiencia_portugues") / 25) * 25).cast("int"))
        .groupBy("faixa").count().orderBy("faixa").collect()
    )
    if dist:
        max_n = max(r["count"] for r in dist)
        cols = []
        for r in dist:
            alto = r["faixa"] >= 743 - 12
            color = C_GREEN if r["faixa"] >= 750 else (C_AMBER if alto else "rgba(255,255,255,.32)")
            h = max(r["count"] / max_n * 190, 4)
            cols.append(
                f'<div style="flex:1;min-width:16px;display:flex;flex-direction:column;justify-content:flex-end;'
                f'align-items:center;gap:5px" title="{r["faixa"]}-{r["faixa"] + 24}: {r["count"]} alunos">'
                f'<div style="width:82%;height:{h:.0f}px;background:{color};'
                f'border-radius:6px 6px 0 0"></div>'
                f'<div style="font-size:10px;color:{C_MUTED}">{r["faixa"]}</div></div>'
            )
        corte_pos = None
        faixas_x = [r["faixa"] for r in dist]
        if faixas_x:
            span = (max(faixas_x) + 25) - min(faixas_x)
            corte_pos = (743 - min(faixas_x)) / span * 100
        marcador = (
            f'<div style="position:absolute;left:{corte_pos:.1f}%;top:0;bottom:24px;width:2px;'
            f'background:{C_RED};box-shadow:0 0 0 1px rgba(0,0,0,.25)"></div>'
            f'<div style="position:absolute;left:{corte_pos:.1f}%;top:-6px;transform:translateX(8px);'
            f'font-size:11px;font-weight:850;color:{C_RED}">corte 743 · alfabetizado →</div>'
        ) if corte_pos is not None else ""
        body = (
            f'<div style="position:relative;padding-top:22px;overflow-x:auto">{marcador}'
            f'<div style="display:flex;align-items:flex-end;gap:2px;height:225px;min-width:760px">'
            + "".join(cols) + "</div></div>"
            + legend([("≥ 750 (alfabetizado)", C_GREEN),
                      ("faixa do corte", C_AMBER),
                      ("abaixo do corte", "rgba(255,255,255,.32)")])
        )
        alunos_chart_html = chart_box(
            f"Distribuição de proficiência dos alunos · {ANO} (INEP oficial)",
            "Histograma por faixa de 25 pontos na escala Saeb · a linha vermelha representa a regra dos 743 pontos",
            body,
        )
        displayHTML(alunos_chart_html)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 21. Gráfico 5: pulso do streaming

# COMMAND ----------

# ---- Gráfico 5 · Pulso do streaming ----
streaming_chart_html = ""
if table_exists(T_EVENTOS):
    pulso = (
        spark.table(T_EVENTOS)
        .withColumn("janela", F.date_format(F.date_trunc("hour", "_ingestion_timestamp"), "dd/MM HH'h'"))
        .groupBy("janela")
        .agg(F.count("*").alias("eventos"),
             F.round(F.avg(F.unix_timestamp("_ingestion_timestamp")
                           - F.unix_timestamp("event_time")), 1).alias("lat"))
        .orderBy("janela").collect()
    )
    if pulso:
        max_e = max(r["eventos"] for r in pulso)
        cols_stream = "".join(
            f'<div style="flex:1;min-width:70px;max-width:110px;display:flex;flex-direction:column;'
            f'justify-content:flex-end;align-items:center;gap:7px">'
            f'<div style="font-size:12px;font-weight:850;color:{C_TEXT}">{r["eventos"]}</div>'
            f'<div style="width:70%;height:{max(r["eventos"] / max_e * 165, 5):.0f}px;'
            f'background:linear-gradient(180deg,{C_CYAN},{C_VIOLET});border-radius:8px 8px 2px 2px"></div>'
            f'<div style="font-size:10px;color:{C_MUTED}">{r["janela"]}</div>'
            f'<div style="font-size:10px;color:{C_MUTED}">lat {r["lat"]}s</div></div>'
            for r in pulso
        )
        streaming_chart_html = chart_box(
            "Pulso do streaming · eventos por hora de ingestão",
            "Evidência visual da ingestão híbrida: volume e latência média por janela",
            f'<div style="display:flex;align-items:flex-end;gap:10px;height:240px;'
            f'justify-content:center;overflow-x:auto">{cols_stream}</div>',
        )
        displayHTML(streaming_chart_html)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 22. Inventário de features para IA (Fase 3)
# MAGIC Lista o que já está disponível na Gold para evoluir para predição na
# MAGIC próxima fase.

# COMMAND ----------

# Inventário simples de features disponíveis para comunicar a prontidão analítica.
feature_rows = [
    Row(bloco="Resultado educacional", feature="taxa_alfabetizacao_media",
        uso="Target ou variável de tendência", disponibilidade="Gold"),
    Row(bloco="Meta", feature="gap_meta",
        uso="Priorização e distância da política pública", disponibilidade="Gold"),
    Row(bloco="Temporal", feature="variacao_absoluta",
        uso="Momentum, regressão e anomalias", disponibilidade="Gold"),
    Row(bloco="Territorial", feature="UF, município e região",
        uso="Segmentação espacial", disponibilidade="Gold + dimensão"),
    Row(bloco="Rede", feature="rede_label",
        uso="Análise de desigualdade", disponibilidade="Gold"),
    Row(bloco="Operacional", feature="latência e frescor",
        uso="Confiabilidade do dado usado pelo modelo", disponibilidade="Observability"),
    Row(bloco="Enriquecimento futuro", feature="renda, infraestrutura, FUNDEB, vulnerabilidade",
        uso="Explicação e ganho preditivo", disponibilidade="Recomendado"),
]
display(spark.createDataFrame(feature_rows))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 23. Encerramento da preparação analítica
# MAGIC Confirma que tudo que o Command Center precisa já está calculado.

# COMMAND ----------

print("✓ Command Center atualizado.")
print("✓ Filtros, capa, rankings, trajetória, streaming, qualidade e IA preparados.")
print("→ Use '+ Add to dashboard' nos resultados que farão parte do vídeo executivo.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 24. Setup do Command Center HTML
# MAGIC Declara as tabelas adicionais usadas só na versão HTML e os helpers de
# MAGIC contagem/formatação.

# COMMAND ----------

import json
import pandas as pd
from html import escape
from datetime import datetime, timezone
from pathlib import Path

T_AVALIACAO_UF = f"{CATALOG}.bronze.avaliacao_alfabetizacao"
T_AVALIACAO_MUN = f"{CATALOG}.bronze.avaliacao_alfabetizacao_municipio"
T_UF = f"{CATALOG}.bronze.uf"
T_MUNICIPIO = f"{CATALOG}.bronze.municipio"
T_META_MUNICIPIO = f"{CATALOG}.bronze.meta_municipio"

T_SILVER_APROVADA = f"{CATALOG}.silver.medicoes_aprovadas"
T_SILVER_ALUNOS = f"{CATALOG}.silver.alunos_modelagem"
T_SILVER_ALUNOS_APROVADOS = f"{CATALOG}.silver.alunos_modelagem_aprovados"

T_GOLD_BASE_ALUNO = f"{CATALOG}.gold.base_modelagem_aluno"

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gold.dashboard")

DASHBOARD_DIR = "/Volumes/workspace/gold/dashboard"
DASHBOARD_HTML = f"{DASHBOARD_DIR}/command_center_alfabetizacao.html"


def count_or_zero(table_name: str) -> int:
    if not spark.catalog.tableExists(table_name):
        return 0
    return spark.table(table_name).count()


def fmt_count(value: int) -> str:
    return f"{int(value or 0):,}".replace(",", ".")


# COMMAND ----------
# MAGIC %md
# MAGIC ## 25. Inventário real das fontes
# MAGIC Origem, arquivo e grão de cada entidade usada no pipeline, sem números
# MAGIC inventados.

# COMMAND ----------

source_inventory = [
    {
        "origem": "INEP",
        "entidade": "Indicador de alfabetização por UF",
        "arquivo": "resultados_e_metas_ufs_2024_2.xlsx",
        "grao": "ano + UF + série + rede",
        "tabela": T_AVALIACAO_UF,
        "registros": count_or_zero(T_AVALIACAO_UF),
        "papel": "Resultado territorial por UF",
    },
    {
        "origem": "INEP",
        "entidade": "Indicador de alfabetização por município",
        "arquivo": "resultados_e_metas_municipios_2024.xlsx",
        "grao": "ano + município + rede",
        "tabela": T_AVALIACAO_MUN,
        "registros": count_or_zero(T_AVALIACAO_MUN),
        "papel": "Resultado territorial municipal",
    },
    {
        "origem": "IBGE",
        "entidade": "Unidades da Federação",
        "arquivo": "estados.csv",
        "grao": "1 linha por UF",
        "tabela": T_UF,
        "registros": count_or_zero(T_UF),
        "papel": "Dimensão territorial de UF",
    },
    {
        "origem": "IBGE",
        "entidade": "Municípios",
        "arquivo": "municipios.csv",
        "grao": "1 linha por município",
        "tabela": T_MUNICIPIO,
        "registros": count_or_zero(T_MUNICIPIO),
        "papel": "Dimensão territorial municipal",
    },
    {
        "origem": "INEP",
        "entidade": "Metas Brasil",
        "arquivo": "meta_brasil.csv",
        "grao": "ano",
        "tabela": T_META_BRASIL,
        "registros": count_or_zero(T_META_BRASIL),
        "papel": "Trajetória nacional de metas",
    },
    {
        "origem": "INEP",
        "entidade": "Metas por UF",
        "arquivo": "meta_uf.csv",
        "grao": "ano + UF",
        "tabela": T_META_UF,
        "registros": count_or_zero(T_META_UF),
        "papel": "Meta territorial de UF",
    },
    {
        "origem": "INEP",
        "entidade": "Metas por município",
        "arquivo": "meta_municipio.csv",
        "grao": "ano + município",
        "tabela": T_META_MUNICIPIO,
        "registros": count_or_zero(T_META_MUNICIPIO),
        "papel": "Meta territorial municipal",
    },
    {
        "origem": "INEP",
        "entidade": "Microdados de alunos",
        "arquivo": "microdados_inep/DADOS/TS_ALUNO.csv",
        "grao": "1 linha por aluno",
        "tabela": T_ALUNOS,
        "registros": count_or_zero(T_ALUNOS),
        "papel": "Regra dos 743 pontos e base da Fase 3",
    },
    {
        "origem": "INEP oficial replay",
        "entidade": "Eventos de streaming",
        "arquivo": "replay da fonte municipal oficial",
        "grao": "1 evento por medição selecionada",
        "tabela": T_EVENTOS,
        "registros": count_or_zero(T_EVENTOS),
        "papel": "Demonstração do caminho streaming",
    },
]

gold_inventory = [
    ("gold.indicador_municipio", count_or_zero(T_IND_MUN), "ano + município + rede"),
    ("gold.resumo_uf", count_or_zero(T_RESUMO_UF), "ano + UF + rede"),
    ("gold.meta_vs_resultado", count_or_zero(T_META_RESULTADO), "ano + território + rede"),
    ("gold.evolucao_temporal", count_or_zero(T_EVOLUCAO), "ano + território + rede"),
    ("gold.base_modelagem_aluno", count_or_zero(T_GOLD_BASE_ALUNO), "1 linha por aluno"),
]

silver_territorial_aprovada = count_or_zero(T_SILVER_APROVADA)
silver_alunos_aprovados = count_or_zero(T_SILVER_ALUNOS_APROVADOS)
alunos_gold_count = count_or_zero(T_GOLD_BASE_ALUNO)


def source_inventory_html():
    rows = []
    for item in source_inventory:
        status = "Disponível" if item["registros"] > 0 else "Ausente"
        badge = "success" if item["registros"] > 0 else "danger"
        rows.append(
            "<tr>"
            f"<td class='cell-key'>{escape(item['origem'])}</td>"
            f"<td>{escape(item['entidade'])}</td>"
            f"<td>{escape(item['arquivo'])}</td>"
            f"<td>{escape(item['grao'])}</td>"
            f"<td><code>{escape(item['tabela'])}</code></td>"
            f"<td class='num'>{fmt_count(item['registros'])}</td>"
            f"<td>{escape(item['papel'])}</td>"
            f"<td><span class='badge {badge}'>{status}</span></td>"
            "</tr>"
        )

    return (
        "<div class='table-shell'><div class='table-scroll'>"
        "<table class='data-table'>"
        "<thead><tr>"
        "<th>Origem</th><th>Entidade</th><th>Arquivo / derivação</th>"
        "<th>Grão</th><th>Tabela</th><th>Registros</th><th>Uso</th><th>Status</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
        f"<div class='table-footer'>{len(rows)} fontes/entidades mapeadas</div></div>"
    )


def gold_inventory_html():
    rows = []
    for table_name, count, grain in gold_inventory:
        rows.append(
            "<tr>"
            f"<td class='cell-key'><code>workspace.{escape(table_name)}</code></td>"
            f"<td>{escape(grain)}</td>"
            f"<td class='num'>{fmt_count(count)}</td>"
            "<td><span class='badge success'>Publicada</span></td>"
            "</tr>"
        )

    return (
        "<div class='table-shell'><div class='table-scroll'>"
        "<table class='data-table'>"
        "<thead><tr><th>Mart</th><th>Grão</th><th>Registros</th><th>Status</th></tr></thead>"
        "<tbody>" + "".join(rows) + "</tbody></table></div></div>"
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## 26. Dados leves embarcados no HTML
# MAGIC Monta os arrays JSON que alimentam os filtros do lado do navegador, sem
# MAGIC precisar de nova consulta ao Databricks a cada troca de filtro.

# COMMAND ----------

MAX_FILTER_YEARS = 2

# Antes: FILTER_YEARS vinha fixo em [2024, 2023]. Se a tabela fosse reprocessada
# com outros anos, os arrays enviados ao dashboard ficavam vazios sem gerar erro
# (DATA.resumo = [] no JS), derrubando todos os painéis em silêncio.
# Agora os anos vêm direto do dado real, sempre atualizados.
FILTER_YEARS = sorted(
    (int(year) for year in available_years),
    reverse=True,
)[:MAX_FILTER_YEARS]

if not FILTER_YEARS:
    raise RuntimeError(
        "Não foi possível determinar os anos de referência a partir de gold.resumo_uf."
    )

available_filter_years = FILTER_YEARS

ufs_html = [
    row["sigla_uf"]
    for row in resumo_uf
    .select("sigla_uf")
    .where(F.col("sigla_uf").isNotNull())
    .distinct()
    .orderBy("sigla_uf")
    .collect()
]

resumo_html_rows = [
    {
        "ano": int(row["ano"]),
        "sigla_uf": row["sigla_uf"],
        "rede": int(row["rede"]) if row["rede"] is not None else None,
        "taxa": float(row["taxa_alfabetizacao_media"])
        if row["taxa_alfabetizacao_media"] is not None
        else None,
    }
    for row in resumo_uf
    .select("ano", "sigla_uf", "rede", "taxa_alfabetizacao_media")
    .filter(F.col("ano").isin(FILTER_YEARS))
    .collect()
]

meta_uf_html_rows = [
    {
        "ano": int(row["ano"]),
        "sigla_uf": row["sigla_uf"],
        "meta": float(row["meta_uf"]) if row["meta_uf"] is not None else None,
    }
    for row in meta_uf_norm.select("ano", "sigla_uf", "meta_uf").collect()
]

meta_brasil_html_rows = [
    {
        "ano": int(row["ano"]),
        "meta": float(row["meta_brasil"]) if row["meta_brasil"] is not None else None,
    }
    for row in meta_brasil_norm.select("ano", "meta_brasil").collect()
]

municipio_columns = meta_vs_resultado.columns
municipio_name_expr = (
    F.col("nome_municipio")
    if "nome_municipio" in municipio_columns
    else F.lit(None).cast("string")
)

municipio_html_rows = [
    {
        "ano": int(row["ano"]),
        "sigla_uf": row["sigla_uf"],
        "id_municipio": row["id_municipio"],
        "nome_municipio": row["nome_municipio"],
        "rede": int(row["rede"]) if row["rede"] is not None else None,
        "rede_label": row["rede_label"],
        "taxa": float(row["taxa_alfabetizacao_media"])
        if row["taxa_alfabetizacao_media"] is not None
        else None,
        "meta": float(row["meta_taxa"]) if row["meta_taxa"] is not None else None,
        "gap": float(row["gap_meta"]) if row["gap_meta"] is not None else None,
        "atingiu_meta": bool(row["atingiu_meta"])
        if row["atingiu_meta"] is not None
        else None,
    }
    for row in meta_vs_resultado
    .filter(
        (F.col("nivel_territorial") == "municipio")
        & F.col("ano").isin(FILTER_YEARS)
    )
    .select(
        "ano",
        "sigla_uf",
        "id_municipio",
        municipio_name_expr.alias("nome_municipio"),
        "rede",
        "rede_label",
        "taxa_alfabetizacao_media",
        "meta_taxa",
        "gap_meta",
        "atingiu_meta",
    )
    .collect()
]

stream_html_rows = []

if table_exists(T_EVENTOS):
    eventos_html_df = spark.table(T_EVENTOS)

    fields = set(eventos_html_df.columns)

    if {"ano", "sigla_uf", "rede"}.issubset(fields):
        select_cols = [
            F.col("ano").cast("int").alias("ano"),
            F.upper(F.trim(F.col("sigla_uf"))).alias("sigla_uf"),
            F.col("rede").cast("int").alias("rede"),
        ]

        if "event_time" in fields:
            select_cols.append(F.col("event_time").cast("timestamp").alias("event_time"))
        else:
            select_cols.append(F.lit(None).cast("timestamp").alias("event_time"))

        if "_ingestion_timestamp" in fields:
            select_cols.append(
                F.col("_ingestion_timestamp").cast("timestamp").alias("ingestion_time")
            )
        else:
            select_cols.append(F.lit(None).cast("timestamp").alias("ingestion_time"))

        for row in eventos_html_df.select(*select_cols).collect():
            event_time = row["event_time"]
            ingestion_time = row["ingestion_time"]

            latency = None
            if event_time is not None and ingestion_time is not None:
                latency = (ingestion_time - event_time).total_seconds()

            stream_html_rows.append(
                {
                    "ano": int(row["ano"]) if row["ano"] is not None else None,
                    "sigla_uf": row["sigla_uf"],
                    "rede": int(row["rede"]) if row["rede"] is not None else None,
                    "event_time": event_time.isoformat() if event_time else None,
                    "ingestion_time": ingestion_time.isoformat() if ingestion_time else None,
                    "latency": latency,
                }
            )

student_dist_rows = []

if table_exists(T_SILVER_ALUNOS_APROVADOS):
    alunos_aprovados_dashboard = spark.table(T_SILVER_ALUNOS_APROVADOS)

    if {
        "ano",
        "sigla_uf",
        "rede",
        "proficiencia_portugues",
    }.issubset(alunos_aprovados_dashboard.columns):
        dist_df = (
            alunos_aprovados_dashboard
            .filter(
                F.col("proficiencia_portugues").isNotNull()
                & F.col("ano").isin(FILTER_YEARS)
            )
            .withColumn(
                "faixa",
                (F.floor(F.col("proficiencia_portugues") / 25) * 25).cast("int"),
            )
            .groupBy("ano", "sigla_uf", "rede", "faixa")
            .agg(F.count("*").alias("alunos"))
        )

        student_dist_rows = [
            {
                "ano": int(row["ano"]),
                "sigla_uf": row["sigla_uf"],
                "rede": int(row["rede"]) if row["rede"] is not None else None,
                "faixa": int(row["faixa"]),
                "alunos": int(row["alunos"]),
            }
            for row in dist_df.collect()
        ]

FILTER_DATA_JSON = json.dumps(
    {
        "filter_years": available_filter_years,
        "ufs": ufs_html,
        "resumo": resumo_html_rows,
        "meta_uf": meta_uf_html_rows,
        "meta_brasil": meta_brasil_html_rows,
        "municipios": municipio_html_rows,
        "streaming": stream_html_rows,
        "student_dist": student_dist_rows,
        "base_modelagem_aluno": alunos_gold_count,
        "silver_territorial_aprovada": silver_territorial_aprovada,
        "silver_alunos_aprovados": silver_alunos_aprovados,
    },
    ensure_ascii=False,
)


# COMMAND ----------
# MAGIC %md
# MAGIC ## 27. Tabela de qualidade dos dados
# MAGIC Gera o HTML da tabela de frescor, completude e rejeições.

# COMMAND ----------

def dataframe_to_html(
    df,
    max_rows=50,
    columns=None,
    labels=None,
    badge_columns=None,
):
    if df is None:
        return "<div class='empty-state'>Dados indisponíveis.</div>"

    pdf = df.limit(max_rows).toPandas()

    if pdf.empty:
        return "<div class='empty-state'>Sem dados disponíveis.</div>"

    labels = labels or {}
    badge_columns = set(badge_columns or [])

    if columns:
        selected = [column for column in columns if column in pdf.columns]
        pdf = pdf[selected]

    header = "".join(
        f"<th>{escape(labels.get(column, column.replace('_', ' ').title()))}</th>"
        for column in pdf.columns
    )

    body = []

    for _, row in pdf.iterrows():
        cells = []

        for column in pdf.columns:
            value = row[column]

            if pd.isna(value):
                formatted = "N/D"
            elif hasattr(value, "strftime"):
                formatted = value.strftime("%d/%m/%Y %H:%M")
            elif column in {"rows_read", "rows_written", "rows_rejected"}:
                formatted = fmt_count(value)
            elif column in {"taxa_rejeicao_pct"}:
                formatted = f"{float(value):.1f}%".replace(".", ",")
            elif column in {"duracao_s"}:
                formatted = f"{float(value):.1f} s".replace(".", ",")
            else:
                formatted = escape(str(value))

            if column in badge_columns and not pd.isna(value):
                text_value = str(value)
                lower = text_value.lower()

                if "success" in lower or "saud" in lower:
                    badge = "success"
                elif "fail" in lower or "error" in lower:
                    badge = "danger"
                else:
                    badge = "warning"

                formatted = f"<span class='badge {badge}'>{escape(text_value)}</span>"

            cells.append(f"<td>{formatted}</td>")

        body.append("<tr>" + "".join(cells) + "</tr>")

    return (
        "<div class='table-shell'><div class='table-scroll'>"
        "<table class='data-table'><thead><tr>"
        + header
        + "</tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table></div>"
        f"<div class='table-footer'>{len(pdf)} registro(s) exibidos</div></div>"
    )


if "metricas_dashboard" in globals():
    html_saude = dataframe_to_html(
        metricas_dashboard,
        max_rows=30,
        columns=[
            "finished_at",
            "task_name",
            "status_normalizado",
            "rows_read",
            "rows_written",
            "rows_rejected",
            "taxa_rejeicao_pct",
            "duracao_s",
            "max_event_time",
        ],
        labels={
            "finished_at": "Execução",
            "task_name": "Componente",
            "status_normalizado": "Status",
            "rows_read": "Lidos",
            "rows_written": "Gravados",
            "rows_rejected": "Rejeitados",
            "taxa_rejeicao_pct": "Rejeição",
            "duracao_s": "Duração",
            "max_event_time": "Último evento",
        },
        badge_columns=["status_normalizado"],
    )
else:
    html_saude = "<div class='empty-state'>Métricas operacionais indisponíveis.</div>"


SOURCE_INVENTORY_HTML = source_inventory_html()
GOLD_INVENTORY_HTML = gold_inventory_html()

# Tabelas completas dos marts que até aqui só existiam como display()
# nativo do Databricks - trazidas pra dentro do Command Center HTML.
if "ranking_dashboard" in globals():
    RANKING_TABLE_HTML = dataframe_to_html(
        ranking_dashboard,
        max_rows=27,
        labels={
            "sigla_uf": "UF",
            "regiao": "Região",
            "taxa_pct": "Resultado (%)",
            "meta_pct": "Meta (%)",
            "gap_pp": "Gap (p.p.)",
            "variacao_pp": "Variação (p.p.)",
            "status_meta": "Status",
            "score_prioridade": "Score",
        },
        badge_columns=["status_meta"],
    )
else:
    RANKING_TABLE_HTML = "<div class='empty-state'>Ranking indisponível.</div>"

if "matriz_prioridade" in globals():
    MATRIZ_TABLE_HTML = dataframe_to_html(
        matriz_prioridade,
        max_rows=27,
        labels={
            "sigla_uf": "UF",
            "regiao": "Região",
            "taxa_pct": "Resultado (%)",
            "variacao_pp": "Variação (p.p.)",
            "gap_pp": "Gap (p.p.)",
            "status_meta": "Status",
            "score_prioridade": "Score",
        },
        badge_columns=["status_meta"],
    )
else:
    MATRIZ_TABLE_HTML = "<div class='empty-state'>Matriz de prioridade indisponível.</div>"

if "trajetoria_2030" in globals():
    JORNADA_TABLE_HTML = dataframe_to_html(
        trajetoria_2030,
        max_rows=60,
        labels={
            "ano": "Ano",
            "serie": "Série",
            "valor_pct": "Valor (%)",
            "tipo": "Tipo",
        },
        badge_columns=["tipo"],
    )
else:
    JORNADA_TABLE_HTML = "<div class='empty-state'>Trajetória indisponível.</div>"

if "desigualdade_regional" in globals():
    DESIGUALDADE_TABLE_HTML = dataframe_to_html(
        desigualdade_regional,
        max_rows=40,
        labels={
            "regiao": "Região",
            "rede_ensino": "Rede",
            "taxa_pct": "Resultado (%)",
            "ufs_com_dados": "UFs com dados",
        },
    )
else:
    DESIGUALDADE_TABLE_HTML = "<div class='empty-state'>Desigualdade regional indisponível.</div>"

if "municipios_prioritarios" in globals():
    MUNICIPIOS_TABLE_HTML = dataframe_to_html(
        municipios_prioritarios,
        max_rows=25,
        labels={
            "sigla_uf": "UF",
            "id_municipio": "Código IBGE",
            "nome_municipio": "Município",
            "rede_label": "Rede",
            "taxa_pct": "Resultado (%)",
            "meta_pct": "Meta (%)",
            "gap_pp": "Gap (p.p.)",
            "severidade": "Severidade",
            "updated_at": "Atualizado em",
        },
        badge_columns=["severidade"],
    )
else:
    MUNICIPIOS_TABLE_HTML = "<div class='empty-state'>Municípios prioritários indisponível.</div>"

if "action_board" in globals():
    DECISAO_TABLE_HTML = dataframe_to_html(
        action_board,
        max_rows=27,
        labels={
            "sigla_uf": "UF",
            "regiao": "Região",
            "taxa_pct": "Resultado (%)",
            "meta_pct": "Meta (%)",
            "gap_pp": "Gap (p.p.)",
            "variacao_pp": "Variação (p.p.)",
            "score_prioridade": "Score",
            "status_meta": "Status",
            "recomendacao": "Recomendação",
        },
        badge_columns=["status_meta"],
    )
else:
    DECISAO_TABLE_HTML = "<div class='empty-state'>Painel de decisão indisponível.</div>"

generated_at = datetime.now(timezone.utc).strftime("%d/%m/%Y %H:%M UTC")

# Saúde operacional nas últimas 20 métricas registradas pelo pipeline.
pipeline_health = "SEM MÉTRICAS"
pipeline_health_class = "neutral"
if table_exists(T_METRICAS):
    recent_metrics = spark.table(T_METRICAS).orderBy(F.desc("finished_at")).limit(20)
    failed_tasks = recent_metrics.filter(
        ~F.upper(F.col("status")).isin("SUCCESS", "SUCCEEDED")
    ).count()
    if failed_tasks == 0 and recent_metrics.count() > 0:
        pipeline_health = "SAUDÁVEL"
        pipeline_health_class = "success"
    elif failed_tasks > 0:
        pipeline_health = "ATENÇÃO"
        pipeline_health_class = "warning"

pipeline_health_value = (
    pipeline_health
    if "pipeline_health" in globals()
    else "SEM MÉTRICAS"
)

pipeline_health_class_value = (
    pipeline_health_class
    if "pipeline_health_class" in globals()
    else "neutral"
)


# COMMAND ----------
# MAGIC %md
# MAGIC ## 28. Template HTML/CSS/JS do Command Center
# MAGIC String única com a página inteira: estilo, marcação de todas as abas e o
# MAGIC JavaScript que aplica os filtros a partir do JSON embarcado.

# COMMAND ----------

html_final = r"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Command Center · Alfabetização no Brasil</title>

<style>
:root {
    --bg: #07101d;
    --bg-deep: #040a12;
    --surface: #0b1728;
    --surface-2: #101f34;
    --surface-3: #142741;
    --line: rgba(255,255,255,.085);
    --line-strong: rgba(255,255,255,.14);
    --text: #f4f8ff;
    --muted: #8fa2bd;
    --cyan: #43d8e7;
    --violet: #8d7dff;
    --green: #43dda6;
    --amber: #f4c761;
    --red: #ff7181;
    --blue: #67a9ff;
    --pink: #ed8fc4;
    --sidebar: 242px;
}

* {
    box-sizing: border-box;
}

html {
    scroll-behavior: smooth;
}

body {
    margin: 0;
    min-height: 100vh;
    color: var(--text);
    background:
        radial-gradient(circle at 82% -8%, rgba(67,216,231,.08), transparent 26%),
        radial-gradient(circle at 10% 5%, rgba(141,125,255,.07), transparent 28%),
        var(--bg-deep);
    font-family: Inter, "Segoe UI", Roboto, Arial, sans-serif;
}

code {
    color: #9eeaf2;
    font-family: "SFMono-Regular", Consolas, monospace;
    font-size: 9px;
}

button, select {
    font: inherit;
}

.app {
    min-height: 100vh;
}

.sidebar {
    position: fixed;
    inset: 0 auto 0 0;
    width: var(--sidebar);
    padding: 20px 14px;
    overflow-y: auto;
    z-index: 30;
    background: rgba(6,16,29,.97);
    border-right: 1px solid var(--line);
    backdrop-filter: blur(16px);
}

.brand {
    padding: 3px 10px 17px;
    border-bottom: 1px solid var(--line);
}

.brand-kicker {
    color: var(--cyan);
    font-size: 9px;
    font-weight: 900;
    letter-spacing: .16em;
    text-transform: uppercase;
}

.brand-title {
    margin-top: 6px;
    font-size: 23px;
    line-height: 1;
    font-weight: 900;
    letter-spacing: -.035em;
}

.brand-subtitle {
    margin-top: 8px;
    color: var(--muted);
    font-size: 10px;
    line-height: 1.45;
}

.nav-label {
    margin: 17px 10px 6px;
    color: #627592;
    font-size: 8px;
    font-weight: 900;
    letter-spacing: .14em;
    text-transform: uppercase;
}

.sidebar nav {
    display: grid;
    gap: 2px;
}

.nav-button {
    width: 100%;
    display: flex;
    align-items: center;
    gap: 9px;
    padding: 9px 10px;
    border: 1px solid transparent;
    border-radius: 9px;
    background: transparent;
    color: #9dafc7;
    text-align: left;
    cursor: pointer;
    transition: .15s ease;
    font-size: 10px;
}

.nav-button:hover {
    color: #fff;
    background: rgba(255,255,255,.04);
}

.nav-button.active {
    color: #fff;
    background: linear-gradient(90deg, rgba(67,216,231,.13), rgba(141,125,255,.05));
    border-color: rgba(67,216,231,.16);
}

.nav-icon {
    width: 23px;
    height: 23px;
    display: inline-grid;
    place-items: center;
    flex: 0 0 auto;
    border-radius: 7px;
    color: var(--cyan);
    background: rgba(255,255,255,.055);
    font-size: 8px;
    font-weight: 900;
}

.sidebar-footer {
    margin-top: 18px;
    padding: 12px 10px;
    border: 1px solid var(--line);
    border-radius: 10px;
    color: #7487a2;
    background: rgba(255,255,255,.022);
    font-size: 8px;
    line-height: 1.55;
}

.main {
    margin-left: var(--sidebar);
    min-height: 100vh;
}

.topbar {
    position: sticky;
    top: 0;
    z-index: 20;
    min-height: 72px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 18px;
    padding: 12px 24px;
    background: rgba(4,10,18,.88);
    border-bottom: 1px solid var(--line);
    backdrop-filter: blur(18px);
}

.page-kicker {
    color: var(--cyan);
    font-size: 8px;
    font-weight: 900;
    letter-spacing: .14em;
    text-transform: uppercase;
}

.page-title {
    margin-top: 3px;
    font-size: 20px;
    font-weight: 900;
    letter-spacing: -.03em;
}

.top-status {
    display: flex;
    align-items: flex-end;
    justify-content: flex-end;
    gap: 8px;
    flex-wrap: wrap;
}

.filters {
    display: flex;
    align-items: flex-end;
    justify-content: flex-end;
    gap: 7px;
    flex-wrap: wrap;
}

.filter-group {
    display: grid;
    gap: 3px;
}

.filter-group label {
    padding-left: 2px;
    color: #6f829f;
    font-size: 7px;
    font-weight: 900;
    letter-spacing: .08em;
    text-transform: uppercase;
}

.filter-select {
    height: 31px;
    min-width: 105px;
    padding: 0 28px 0 10px;
    border: 1px solid var(--line);
    border-radius: 8px;
    outline: none;
    background: #0b192c;
    color: #f5f8ff;
    font-size: 9px;
    font-weight: 800;
    cursor: pointer;
}

.filter-select:hover,
.filter-select:focus {
    border-color: rgba(67,216,231,.35);
}

.flow-chip {
    min-height: 31px;
    display: inline-flex;
    align-items: center;
    padding: 5px 10px;
    border: 1px solid var(--line);
    border-radius: 8px;
    background: rgba(255,255,255,.025);
    color: #8fa2bd;
    font-size: 8px;
    white-space: nowrap;
}

.health-chip {
    min-height: 31px;
    display: inline-flex;
    align-items: center;
    padding: 5px 9px;
    border: 1px solid var(--line);
    border-radius: 8px;
    background: rgba(255,255,255,.03);
    font-size: 8px;
    font-weight: 900;
}

.health-chip.success {
    color: #7cf0c5;
    background: rgba(67,221,166,.08);
    border-color: rgba(67,221,166,.20);
}

.health-chip.warning {
    color: #ffd878;
    background: rgba(244,199,97,.08);
    border-color: rgba(244,199,97,.20);
}

.health-chip.neutral {
    color: #a9b9ce;
}

.content {
    width: min(1500px, calc(100% - 38px));
    margin: 0 auto;
    padding: 22px 0 54px;
}

.tab-panel {
    display: none;
    animation: fade .15s ease;
}

.tab-panel.active {
    display: block;
}

@keyframes fade {
    from { opacity: 0; transform: translateY(2px); }
    to { opacity: 1; transform: translateY(0); }
}

.overview-head {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: 24px;
    margin-bottom: 16px;
}

.overview-eyebrow {
    color: var(--cyan);
    font-size: 8px;
    font-weight: 900;
    letter-spacing: .15em;
    text-transform: uppercase;
}

.overview-title h1 {
    margin: 6px 0 6px;
    max-width: 880px;
    font-size: clamp(27px, 3vw, 42px);
    line-height: 1;
    letter-spacing: -.05em;
}

.overview-title p {
    margin: 0;
    max-width: 850px;
    color: var(--muted);
    font-size: 11px;
    line-height: 1.55;
}

.source-pill {
    flex: 0 0 auto;
    padding: 8px 11px;
    border-radius: 999px;
    border: 1px solid rgba(67,221,166,.20);
    background: rgba(67,221,166,.07);
    color: #7cefc6;
    font-size: 8px;
    font-weight: 900;
}

.kpi-grid {
    display: grid;
    grid-template-columns: repeat(6, minmax(0, 1fr));
    gap: 9px;
    margin-bottom: 14px;
}

.kpi-card {
    position: relative;
    overflow: hidden;
    min-height: 105px;
    padding: 14px;
    border: 1px solid var(--line);
    border-radius: 13px;
    background: linear-gradient(145deg, rgba(15,29,49,.96), rgba(9,20,36,.96));
}

.kpi-card::before {
    content: "";
    position: absolute;
    left: 0;
    top: 0;
    width: 3px;
    height: 100%;
    background: var(--tone);
}

.tone-cyan { --tone: var(--cyan); }
.tone-violet { --tone: var(--violet); }
.tone-green { --tone: var(--green); }
.tone-blue { --tone: var(--blue); }
.tone-amber { --tone: var(--amber); }
.tone-pink { --tone: var(--pink); }

.kpi-label {
    color: #8397b2;
    font-size: 8px;
    font-weight: 900;
    letter-spacing: .09em;
    text-transform: uppercase;
}

.kpi-value {
    margin-top: 8px;
    font-size: 24px;
    line-height: 1;
    font-weight: 900;
    letter-spacing: -.04em;
}

.kpi-note {
    margin-top: 8px;
    color: var(--muted);
    font-size: 8px;
    line-height: 1.35;
}

.grid-2 {
    display: grid;
    grid-template-columns: minmax(0, 1.6fr) minmax(300px, .75fr);
    gap: 13px;
}

.grid-3 {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 12px;
}

.panel {
    margin-bottom: 13px;
    border: 1px solid var(--line);
    border-radius: 15px;
    background: rgba(10,22,39,.84);
    overflow: hidden;
}

.panel-header {
    padding: 15px 17px 0;
}

.panel-title {
    font-size: 15px;
    font-weight: 900;
    letter-spacing: -.02em;
}

.panel-subtitle {
    margin-top: 4px;
    color: var(--muted);
    font-size: 9px;
    line-height: 1.45;
}

.panel-body {
    padding: 12px 15px 15px;
}

.section-head {
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
    gap: 16px;
    margin: 3px 0 12px;
}

.section-head h2 {
    margin: 0;
    font-size: 20px;
    letter-spacing: -.035em;
}

.section-head p {
    margin: 4px 0 0;
    color: var(--muted);
    font-size: 10px;
}

.flow-grid {
    display: grid;
    grid-template-columns: repeat(6, minmax(120px, 1fr));
    gap: 9px;
    align-items: stretch;
}

.flow-node {
    position: relative;
    min-height: 104px;
    padding: 13px;
    border: 1px solid var(--line);
    border-radius: 12px;
    background: rgba(255,255,255,.025);
}

.flow-node:not(:last-child)::after {
    content: "→";
    position: absolute;
    right: -10px;
    top: 42%;
    z-index: 2;
    color: var(--cyan);
    font-size: 17px;
    font-weight: 900;
}

.flow-node strong {
    display: block;
    font-size: 10px;
}

.flow-node span {
    display: block;
    margin-top: 5px;
    color: var(--muted);
    font-size: 8px;
    line-height: 1.4;
}

.flow-node .layer {
    margin-bottom: 6px;
    color: var(--cyan);
    font-size: 7px;
    font-weight: 900;
    letter-spacing: .10em;
    text-transform: uppercase;
}

.mini-card {
    min-height: 122px;
    padding: 15px;
    border: 1px solid var(--line);
    border-radius: 13px;
    background: linear-gradient(145deg, rgba(15,29,49,.95), rgba(9,20,36,.95));
}

.mini-card .tag {
    display: inline-flex;
    padding: 4px 7px;
    border-radius: 999px;
    background: rgba(67,216,231,.08);
    color: var(--cyan);
    font-size: 7px;
    font-weight: 900;
    letter-spacing: .08em;
    text-transform: uppercase;
}

.mini-card h3 {
    margin: 10px 0 5px;
    font-size: 15px;
}

.mini-card p {
    margin: 0;
    color: var(--muted);
    font-size: 9px;
    line-height: 1.48;
}

.callout {
    padding: 14px;
    border: 1px solid rgba(244,199,97,.18);
    border-radius: 12px;
    background: linear-gradient(145deg, rgba(244,199,97,.07), rgba(141,125,255,.035));
}

.callout strong {
    color: #f5d77f;
}

.table-shell {
    border: 1px solid var(--line);
    border-radius: 12px;
    overflow: hidden;
    background: rgba(8,18,32,.68);
}

.table-scroll {
    overflow: auto;
    max-height: 620px;
}

.data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 9px;
}

.data-table th {
    position: sticky;
    top: 0;
    z-index: 2;
    padding: 9px 10px;
    color: #79dfe9;
    background: #0b192c;
    border-bottom: 1px solid var(--line-strong);
    text-align: left;
    font-size: 7px;
    font-weight: 900;
    letter-spacing: .065em;
    text-transform: uppercase;
    white-space: nowrap;
}

.data-table td {
    padding: 9px 10px;
    color: #dce6f4;
    border-bottom: 1px solid rgba(255,255,255,.045);
    vertical-align: middle;
}

.data-table tbody tr:nth-child(even) td {
    background: rgba(255,255,255,.012);
}

.data-table tbody tr:hover td {
    background: rgba(67,216,231,.045);
}

.cell-key {
    color: #fff !important;
    font-weight: 800;
}

.num {
    text-align: right;
    font-variant-numeric: tabular-nums;
}

.badge {
    display: inline-flex;
    padding: 4px 7px;
    border-radius: 999px;
    border: 1px solid var(--line);
    font-size: 7px;
    font-weight: 900;
    white-space: nowrap;
}

.badge.success {
    color: #7cf0c5;
    background: rgba(67,221,166,.09);
    border-color: rgba(67,221,166,.18);
}

.badge.warning {
    color: #f8d77c;
    background: rgba(244,199,97,.09);
    border-color: rgba(244,199,97,.18);
}

.badge.danger {
    color: #ff9aa6;
    background: rgba(255,113,129,.09);
    border-color: rgba(255,113,129,.18);
}

.table-footer {
    padding: 8px 10px;
    color: #6f829f;
    background: rgba(255,255,255,.018);
    border-top: 1px solid var(--line);
    font-size: 8px;
}

.empty-state {
    padding: 22px;
    border: 1px dashed var(--line-strong);
    border-radius: 11px;
    color: var(--muted);
    background: rgba(255,255,255,.018);
    font-size: 9px;
    text-align: center;
}

.chart-shell {
    padding: 4px 2px 8px;
}

.chart-legend {
    display: flex;
    gap: 13px;
    flex-wrap: wrap;
    margin-top: 10px;
    color: var(--muted);
    font-size: 8px;
}

.legend-dot {
    width: 8px;
    height: 8px;
    display: inline-block;
    margin-right: 5px;
    border-radius: 999px;
}

.footer {
    margin-top: 17px;
    display: flex;
    justify-content: space-between;
    gap: 15px;
    color: #60738f;
    font-size: 8px;
}

@media (max-width: 1280px) {
    .kpi-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
    }

    .flow-grid {
        grid-template-columns: repeat(3, minmax(0, 1fr));
    }

    .flow-node::after {
        display: none;
    }

    .grid-3 {
        grid-template-columns: 1fr;
    }
}

@media (max-width: 950px) {
    :root {
        --sidebar: 0px;
    }

    .sidebar {
        position: relative;
        width: 100%;
        padding: 13px;
        border-right: 0;
        border-bottom: 1px solid var(--line);
    }

    .brand {
        padding-bottom: 10px;
    }

    .nav-label,
    .sidebar-footer {
        display: none;
    }

    .sidebar nav {
        display: flex;
        gap: 5px;
        overflow-x: auto;
        padding-top: 9px;
    }

    .nav-button {
        width: auto;
        flex: 0 0 auto;
    }

    .main {
        margin-left: 0;
    }

    .topbar {
        position: relative;
        align-items: flex-start;
        flex-direction: column;
        padding: 13px 17px;
    }

    .top-status,
    .filters {
        justify-content: flex-start;
    }

    .content {
        width: min(96%, 1500px);
    }

    .overview-head {
        align-items: flex-start;
        flex-direction: column;
    }

    .grid-2 {
        grid-template-columns: 1fr;
    }
}

@media (max-width: 600px) {
    .kpi-grid,
    .flow-grid {
        grid-template-columns: 1fr;
    }
}
</style>

<style>
.sidebar,
.nav-label,
.sidebar-footer,
.flow-info,
.flow-chip {
    display: none !important;
}

.main {
    margin-left: 0 !important;
    min-height: 100vh;
}

.topbar {
    position: sticky;
    top: 0;
    z-index: 40;
    display: block;
    min-height: auto;
    padding: 0;
    background: rgba(6,15,27,.97);
    border-bottom: 1px solid var(--line);
    backdrop-filter: blur(18px);
}

.top-nav-wrap {
    width: 100%;
    border-bottom: 1px solid var(--line);
}

.top-nav {
    display: flex;
    flex-wrap: wrap;
    align-items: stretch;
    gap: 2px;
    padding: 8px 18px 0;
}

.top-tab {
    flex: 0 0 auto;
    padding: 11px 15px 10px;
    border: 0;
    border-bottom: 3px solid transparent;
    background: transparent;
    color: #9aabc2;
    font-size: 11px;
    font-weight: 800;
    cursor: pointer;
    white-space: nowrap;
}

.top-tab:hover {
    color: #fff;
    background: rgba(255,255,255,.025);
}

.top-tab.active {
    color: #fff;
    border-bottom-color: var(--cyan);
}

.dashboard-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 18px;
    padding: 16px 22px 8px;
}

.page-kicker {
    color: var(--cyan);
    font-size: 10px;
    font-weight: 900;
    letter-spacing: .11em;
    text-transform: uppercase;
}

.page-title {
    margin-top: 4px;
    font-size: 28px;
    font-weight: 900;
    letter-spacing: -.04em;
}

.filters-row {
    display: flex;
    align-items: flex-end;
    gap: 10px;
    flex-wrap: wrap;
    padding: 0 22px 15px;
}

.filter-group {
    min-width: 150px;
}

.filter-select {
    width: 100%;
    min-width: 150px;
    height: 37px;
    font-size: 11px;
}

.content {
    width: calc(100% - 42px);
    max-width: 1560px;
    margin: 0 auto;
    padding-top: 22px;
}

@media (max-width: 900px) {
    .dashboard-head {
        align-items: flex-start;
        flex-direction: column;
    }

    .filters-row {
        align-items: stretch;
        flex-direction: column;
    }

    .filter-group,
    .filter-select {
        width: 100%;
    }

    .content {
        width: calc(100% - 24px);
    }
}
</style>


<style>
.top-nav {
    justify-content: flex-start;
}

.top-tab {
    min-width: 0;
    padding-left: 18px;
    padding-right: 18px;
}

#ranking .grid-2 {
    grid-template-columns: repeat(2, minmax(0, 1fr));
}

@media (max-width: 1000px) {
    #ranking .grid-2 {
        grid-template-columns: 1fr;
    }
}
</style>

</head>

<body>
<div class="app">

<div class="main">


<header class="topbar">

    

<div class="top-nav-wrap">
    <nav class="top-nav">
        <button class="top-tab active" onclick="openTab('visao-geral', this)">
            Visão Geral
        </button>

        <button class="top-tab" onclick="openTab('ranking', this)">
            Ranking territorial
        </button>

        <button class="top-tab" onclick="openTab('matriz', this)">
            Matriz de prioridade
        </button>

        <button class="top-tab" onclick="openTab('municipios', this)">
            Municípios prioritários
        </button>

        <button class="top-tab" onclick="openTab('desigualdade', this)">
            Desigualdade
        </button>

        <button class="top-tab" onclick="openTab('jornada', this)">
            Jornada 2030
        </button>

        <button class="top-tab" onclick="openTab('alunos', this)">
            Proficiência dos alunos
        </button>

        <button class="top-tab" onclick="openTab('qualidade', this)">
            Qualidade e proveniência
        </button>

        <button class="top-tab" onclick="openTab('decisao', this)">
            Painel de decisão
        </button>
    </nav>
</div>


<div class="dashboard-head">
        <div>
            <div class="page-kicker">Tech Challenge · Fase 2 · Command Center</div>
            <div class="page-title">Alfabetização no Brasil</div>
        </div>

        <div class="health-chip __HEALTH_CLASS__">
            ● __PIPELINE_HEALTH__
        </div>
    </div>

    <div class="filters-row">
        <div class="filter-group">
            <label for="filtro-ano">Ano</label>
            <select id="filtro-ano" class="filter-select" onchange="applyFilters()">
                __ANO_OPTIONS__
            </select>
        </div>

        <div class="filter-group">
            <label for="filtro-rede">Rede</label>
            <select id="filtro-rede" class="filter-select" onchange="applyFilters()">
                <option value="publica">Rede pública</option>
                <option value="todas" selected>Rede pública e privada</option>
                <option value="estadual">Estadual</option>
                <option value="municipal">Municipal</option>
                <option value="privada">Rede privada</option>
            </select>
        </div>

        <div class="filter-group">
            <label for="filtro-uf">UF</label>
            <select id="filtro-uf" class="filter-select" onchange="applyFilters()">
                <option value="Todas" selected>Todas</option>
                __UF_OPTIONS__
            </select>
        </div>
    </div>

</header>


<main class="content">


<section id="visao-geral" class="tab-panel active">

    <div class="overview-head">
        <div class="overview-title">

            <div id="overview-context" class="overview-eyebrow">
                Visão consolidada · carregando...
            </div>

            <h1>Onde estamos, quem precisa de prioridade e para onde vamos</h1>

        </div>
    </div>

    <div id="kpi-grid" class="kpi-grid"></div>

    <div id="progresso-dinamico" style="display:none"></div>

    <section class="panel">
        <div class="panel-header">
            <div class="panel-title">Progresso educacional</div>
            <div class="panel-subtitle">
                Resultado consolidado, meta nacional, UFs e municípios na trajetória.
            </div>
        </div>

        <div id="progresso-visao-geral" class="panel-body"></div>
    </section>

</section>


<section id="ranking" class="tab-panel">

    <div class="section-head">
        <div>
            <h2>Ranking territorial</h2>
            <p>
                Indicador Criança Alfabetizada por UF, comparando Rede pública e Rede privada.
            </p>
        </div>
    </div>

    <div class="grid-2">

        <section class="panel">
            <div class="panel-header">
                <div class="panel-title">
                    Indicador Criança Alfabetizada · Rede pública
                </div>
                <div class="panel-subtitle">
                    Ranking das UFs para redes estadual e municipal.
                </div>
            </div>

            <div id="ranking-publica" class="panel-body"></div>
        </section>

        <section class="panel">
            <div class="panel-header">
                <div class="panel-title">
                    Indicador Criança Alfabetizada · Rede privada
                </div>
                <div class="panel-subtitle">
                    Ranking das UFs para a rede privada.
                </div>
            </div>

            <div id="ranking-privada" class="panel-body"></div>
        </section>

    </div>

    <section class="panel">
        <div class="panel-header">
            <div class="panel-title">
                Ranking completo todas as UFs
            </div>
            <div class="panel-subtitle">
                Tabela integral do mart <code>gold.resumo_uf</code>, sem filtro de rede.
            </div>
        </div>

        __RANKING_TABLE_HTML__
    </section>

</section>


<section id="jornada" class="tab-panel">

    <div class="section-head">
        <div>
            <h2>Jornada até 2030</h2>
            <p>
                Resultado observado e meta nacional ao longo da trajetória.
            </p>
        </div>
    </div>

    <section class="panel">
        <div class="panel-header">
            <div class="panel-title">
                Resultado observado x meta nacional
            </div>
            <div class="panel-subtitle">
                Histórico disponível para os anos apurados na base e metas oficiais dos anos seguintes.
            </div>
        </div>

        <div id="jornada-dinamica" class="panel-body"></div>
    </section>

    <section class="panel">
        <div class="panel-header">
            <div class="panel-title">
                Trajetória em números
            </div>
        </div>

        <div id="jornada-tabela" class="panel-body"></div>
    </section>

    <section class="panel">
        <div class="panel-header">
            <div class="panel-title">
                Tabela completa resultado observado e metas por ano
            </div>
        </div>

        __JORNADA_TABLE_HTML__
    </section>

</section>


<section id="matriz" class="tab-panel">

    <div class="section-head">
        <div>
            <h2>Matriz de prioridade</h2>
            <p>
                Desempenho x evolução das UFs no recorte selecionado.
            </p>
        </div>
    </div>

    <section class="panel">
        <div id="matriz-dinamica" class="panel-body"></div>
    </section>

    <section class="panel">
        <div class="panel-header">
            <div class="panel-title">
                Tabela completa matriz de prioridade
            </div>
        </div>

        __MATRIZ_TABLE_HTML__
    </section>

</section>





<section id="desigualdade" class="tab-panel">

    <div class="section-head">
        <div>
            <h2>Desigualdade por região e rede</h2>
            <p>
                Comparação entre Rede pública e Rede privada por região no ano selecionado.
            </p>
        </div>
    </div>

    <section class="panel">
        <div id="desigualdade-dinamica" class="panel-body"></div>
    </section>

    <section class="panel">
        <div class="panel-header">
            <div class="panel-title">
                Tabela completa desigualdade por região e rede
            </div>
        </div>

        __DESIGUALDADE_TABLE_HTML__
    </section>

</section>


<section id="municipios" class="tab-panel">

    <div class="section-head">
        <div>
            <h2>Municípios que exigem prioridade</h2>
            <p>
                Municípios com maior distância em relação às metas oficiais no recorte selecionado.
            </p>
        </div>
    </div>

    <section class="panel">
        <div id="municipios-dinamico" class="panel-body"></div>
    </section>

    <section class="panel">
        <div class="panel-header">
            <div class="panel-title">
                Tabela completa 25 municípios mais distantes da meta
            </div>
        </div>

        __MUNICIPIOS_TABLE_HTML__
    </section>

</section>


<section id="decisao" class="tab-panel">

    <div class="section-head">
        <div>
            <h2>Painel de decisão</h2>
            <p>
                Priorização por distância da meta e evolução. Não substitui análise causal.
            </p>
        </div>
    </div>

    <section class="panel">
        <div id="decisao-dinamica" class="panel-body"></div>
    </section>

    <section class="panel">
        <div class="panel-header">
            <div class="panel-title">
                Tabela completa recomendação por UF
            </div>
        </div>

        __DECISAO_TABLE_HTML__
    </section>

</section>


<section id="alunos" class="tab-panel">

    <div class="section-head">
        <div>
            <h2>Distribuição de proficiência dos alunos · 2024</h2>
            <p>
                Microdados oficiais do INEP e referência educacional de 743 pontos.
            </p>
        </div>
    </div>

    <div class="grid-3">

        <div class="mini-card">
            <span class="tag">INEP oficial</span>
            <h3>2.120.560 alunos</h3>
            <p>
                Microdados individuais utilizados na construção da base analítica.
            </p>
        </div>

        <div class="mini-card">
            <span class="tag">Referência</span>
            <h3>743 pontos</h3>
            <p>
                Corte educacional utilizado como evidência de qualidade e análise.
            </p>
        </div>

        <div class="mini-card">
            <span class="tag">Fase 3</span>
            <h3>alfabetizado_oficial</h3>
            <p>
                Target oficial preservado para a classificação supervisionada.
            </p>
        </div>

    </div>

    <section class="panel" style="margin-top:14px">
        <div id="alunos-dinamico" class="panel-body"></div>
    </section>

</section>


<section id="qualidade" class="tab-panel">

    <div class="section-head">
        <div>
            <h2>Qualidade e proveniência dos dados</h2>
            <p>
                De onde vêm os dados, o que a Gold publica e a saúde operacional do pipeline 
                nada aqui é simulado, tudo vem de contagens reais nas tabelas.
            </p>
        </div>
    </div>

    <div class="grid-3">

        <div class="mini-card">
            <span class="tag">Silver</span>
            <h3>__SILVER_TERRITORIAL__</h3>
            <p>
                Medições territoriais (UF + município) aprovadas pelo Quality Gate.
            </p>
        </div>

        <div class="mini-card">
            <span class="tag">Silver</span>
            <h3>__SILVER_ALUNOS__</h3>
            <p>
                Registros de alunos aprovados pelo Quality Gate, no grão individual.
            </p>
        </div>

        <div class="mini-card">
            <span class="tag">Gold</span>
            <h3>__ALUNOS_GOLD__</h3>
            <p>
                Registros publicados em <code>gold.base_modelagem_aluno</code>, prontos para a Fase 3.
            </p>
        </div>

    </div>

    <section class="panel" style="margin-top:14px">
        <div class="panel-header">
            <div class="panel-title">
                Inventário real das fontes
            </div>
            <div class="panel-subtitle">
                Origem, arquivo/derivação e grão de cada entidade usada no pipeline.
            </div>
        </div>

        __SOURCE_INVENTORY_HTML__
    </section>

    <section class="panel" style="margin-top:14px">
        <div class="panel-header">
            <div class="panel-title">
                Marts publicados na Gold
            </div>
        </div>

        __GOLD_INVENTORY_HTML__
    </section>

    <section class="panel" style="margin-top:14px">
        <div class="panel-header">
            <div class="panel-title">
                Saúde operacional do pipeline
            </div>
            <div class="panel-subtitle">
                Últimas execuções: linhas lidas/gravadas/rejeitadas, duração e status.
            </div>
        </div>

        __QUALITY_HTML__
    </section>

</section>


<footer class="footer">
    <span>Tech Challenge · Pipeline Medalhão · Databricks</span>
    <span>Atualizado em __GENERATED_AT__</span>
</footer>

</main>
</div>
</div>


<script>
(function () {
const DATA = __FILTER_DATA_JSON__;

const REDE_CODES = {
    publica: [2, 3],
    todas: [2, 3, 5],
    estadual: [2],
    municipal: [3],
    privada: [5]
};

const REDE_CODES_BREAKDOWN = {
    publica: [2, 3],
    todas: [2, 3, 5],
    estadual: [2],
    municipal: [3],
    privada: [5]
};

const REDE_LABEL = {
    0: "Total",
    2: "Estadual",
    3: "Municipal",
    5: "Privada"
};

const REGIAO_UF = {
    AC:"Norte", AP:"Norte", AM:"Norte", PA:"Norte", RO:"Norte", RR:"Norte", TO:"Norte",
    AL:"Nordeste", BA:"Nordeste", CE:"Nordeste", MA:"Nordeste", PB:"Nordeste",
    PE:"Nordeste", PI:"Nordeste", RN:"Nordeste", SE:"Nordeste",
    DF:"Centro-Oeste", GO:"Centro-Oeste", MT:"Centro-Oeste", MS:"Centro-Oeste",
    ES:"Sudeste", MG:"Sudeste", RJ:"Sudeste", SP:"Sudeste",
    PR:"Sul", RS:"Sul", SC:"Sul"
};

function avg(values) {
    const valid = values.filter(v => v !== null && v !== undefined && Number.isFinite(v));
    if (!valid.length) return null;
    return valid.reduce((a, b) => a + b, 0) / valid.length;
}

function median(values) {
    const valid = values.filter(v => v !== null && v !== undefined && Number.isFinite(v)).sort((a,b)=>a-b);
    if (!valid.length) return null;
    const mid = Math.floor(valid.length / 2);
    return valid.length % 2 ? valid[mid] : (valid[mid-1] + valid[mid]) / 2;
}

function fmtPct(value) {
    if (value === null || value === undefined || !Number.isFinite(value)) return "N/D";
    return (value * 100).toFixed(1).replace(".", ",") + "%";
}

function fmtPP(value) {
    if (value === null || value === undefined || !Number.isFinite(value)) return "N/D";
    const pp = value * 100;
    const prefix = pp > 0 ? "+" : "";
    return prefix + pp.toFixed(1).replace(".", ",") + " p.p.";
}

function fmtInt(value) {
    return new Intl.NumberFormat("pt-BR").format(value || 0);
}

function currentFilters() {
    return {
        ano: parseInt(document.getElementById("filtro-ano").value, 10),
        rede: document.getElementById("filtro-rede").value,
        uf: document.getElementById("filtro-uf").value
    };
}

function selectedCodes(rede) {
    return REDE_CODES[rede] || [2,3];
}

function selectedBreakdownCodes(rede) {
    return REDE_CODES_BREAKDOWN[rede] || [2,3];
}

function resumoFor(filters, ano = filters.ano) {
    const codes = selectedCodes(filters.rede);

    return DATA.resumo.filter(r =>
        r.ano === ano &&
        codes.includes(r.rede) &&
        (filters.uf === "Todas" || r.sigla_uf === filters.uf)
    );
}

function municipiosFor(filters) {
    const codes = selectedCodes(filters.rede);

    return DATA.municipios.filter(r =>
        r.ano === filters.ano &&
        codes.includes(r.rede) &&
        (filters.uf === "Todas" || r.sigla_uf === filters.uf)
    );
}

function metaBrasil(ano) {
    const row = DATA.meta_brasil.find(r => r.ano === ano);
    return row ? row.meta : null;
}

function metaUF(ano, uf) {
    const row = DATA.meta_uf.find(r => r.ano === ano && r.sigla_uf === uf);
    return row ? row.meta : null;
}

function previousObservedYear(ano) {
    const years = [...new Set(DATA.resumo.map(r => r.ano))]
        .filter(y => y < ano)
        .sort((a,b) => b-a);

    return years.length ? years[0] : null;
}

function buildKpis(filters) {
    const resumo = resumoFor(filters);
    const municipios = municipiosFor(filters);

    const taxaMedia = avg(resumo.map(r => r.taxa));
    const meta = metaBrasil(filters.ano);
    const gap = taxaMedia !== null && meta !== null ? taxaMedia - meta : null;

    const ufs = [...new Set(resumo.map(r => r.sigla_uf).filter(Boolean))];

    let ufsNaMeta = 0;
    let ufsComMeta = 0;

    ufs.forEach(uf => {
        const taxaUf = avg(resumo.filter(r => r.sigla_uf === uf).map(r => r.taxa));
        const metaUf = metaUF(filters.ano, uf);

        if (taxaUf !== null && metaUf !== null) {
            ufsComMeta += 1;
            if (taxaUf >= metaUf) ufsNaMeta += 1;
        }
    });

    const municipiosIds = [...new Set(municipios.map(r => r.id_municipio).filter(Boolean))];
    const municipiosComMeta = municipios.filter(r => r.atingiu_meta !== null);

    const pctMunicipiosMeta = municipiosComMeta.length
        ? municipiosComMeta.filter(r => r.atingiu_meta === true).length / municipiosComMeta.length
        : null;

    const redeLabel = document.getElementById("filtro-rede").selectedOptions[0].text;
    const ufLabel = filters.uf === "Todas" ? "Todas as UFs" : filters.uf;

    document.getElementById("kpi-grid").innerHTML = `
        <article class="kpi-card tone-cyan">
            <div class="kpi-label">Resultado médio</div>
            <div class="kpi-value">${fmtPct(taxaMedia)}</div>
            <div class="kpi-note">${redeLabel} · ${filters.ano} · ${ufLabel}</div>
        </article>

        <article class="kpi-card tone-violet">
            <div class="kpi-label">Meta nacional</div>
            <div class="kpi-value">${fmtPct(meta)}</div>
            <div class="kpi-note">Gap ${fmtPP(gap)}</div>
        </article>

        <article class="kpi-card tone-green">
            <div class="kpi-label">UFs na trajetória</div>
            <div class="kpi-value">${ufsNaMeta}/${ufsComMeta}</div>
            <div class="kpi-note">${ufsComMeta ? fmtPct(ufsNaMeta / ufsComMeta) : "N/D"} das UFs</div>
        </article>

        <article class="kpi-card tone-blue">
            <div class="kpi-label">Municípios</div>
            <div class="kpi-value">${fmtInt(municipiosIds.length)}</div>
            <div class="kpi-note">${fmtPct(pctMunicipiosMeta)} na meta</div>
        </article>

        <article class="kpi-card tone-amber">
            <div class="kpi-label">Base Fase 3</div>
            <div class="kpi-value">${fmtInt(DATA.base_modelagem_aluno)}</div>
            <div class="kpi-note">Gold no grão de aluno</div>
        </article>

        <article class="kpi-card tone-pink">
            <div class="kpi-label">Referência</div>
            <div class="kpi-value">${filters.ano}</div>
            <div class="kpi-note">${DATA.filter_years.slice().sort().join(" + ")} disponíveis</div>
        </article>
    `;
}

function buildProgress(filters) {
    const resumo = resumoFor(filters);
    const municipios = municipiosFor(filters);

    const taxaMedia = avg(resumo.map(r => r.taxa));
    const meta = metaBrasil(filters.ano);
    const gap = taxaMedia !== null && meta !== null ? taxaMedia - meta : null;

    const ufs = [...new Set(resumo.map(r => r.sigla_uf).filter(Boolean))];

    let ufsNaMeta = 0;
    let ufsComMeta = 0;

    ufs.forEach(uf => {
        const taxaUf = avg(resumo.filter(r => r.sigla_uf === uf).map(r => r.taxa));
        const metaUf = metaUF(filters.ano, uf);

        if (taxaUf !== null && metaUf !== null) {
            ufsComMeta += 1;
            if (taxaUf >= metaUf) ufsNaMeta += 1;
        }
    });

    const pctUf = ufsComMeta ? ufsNaMeta / ufsComMeta : null;

    const municipiosComMeta = municipios.filter(r => r.atingiu_meta !== null);
    const pctMun = municipiosComMeta.length
        ? municipiosComMeta.filter(r => r.atingiu_meta === true).length / municipiosComMeta.length
        : null;

    const progress = (label, value, target, gradient) => {
        const pct = value === null ? 0 : Math.max(0, Math.min(value * 100, 100));
        const marker = target === null ? "" :
            `<span style="position:absolute;left:${Math.max(0,Math.min(target*100,100))}%;
             top:-3px;bottom:-3px;width:2px;background:#fff;z-index:2"></span>`;

        return `
        <div style="margin:13px 0">
            <div style="display:flex;justify-content:space-between;gap:12px;margin-bottom:6px">
                <strong style="font-size:9px">${label}</strong>
                <span style="font-size:9px;color:#a9bad0">${fmtPct(value)}</span>
            </div>

            <div style="height:8px;background:rgba(255,255,255,.07);border-radius:99px;position:relative">
                ${marker}
                <span style="display:block;height:100%;width:${pct}%;border-radius:99px;background:${gradient}"></span>
            </div>
        </div>`;
    };

    document.getElementById("progresso-dinamico").innerHTML = `
        ${progress("Resultado médio", taxaMedia, meta, "linear-gradient(90deg,#43d8e7,#8d7dff)")}
        ${progress("UFs na trajetória da meta", pctUf, null, "linear-gradient(90deg,#43dda6,#43d8e7)")}
        ${progress("Municípios na meta", pctMun, null, "linear-gradient(90deg,#67a9ff,#43d8e7)")}

        <div style="display:flex;justify-content:space-between;gap:10px;padding-top:10px;
                    margin-top:10px;border-top:1px solid rgba(255,255,255,.07);
                    color:#8fa2bd;font-size:8px">
            <span>Meta nacional: <strong style="color:#fff">${fmtPct(meta)}</strong></span>
            <span>Gap: <strong style="color:#fff">${fmtPP(gap)}</strong></span>
        </div>
    `;
}

function buildRanking(filters) {
    const resumo = resumoFor(filters);
    const ufs = [...new Set(resumo.map(r => r.sigla_uf).filter(Boolean))];

    const rows = ufs.map(uf => {
        const taxa = avg(resumo.filter(r => r.sigla_uf === uf).map(r => r.taxa));
        const meta = metaUF(filters.ano, uf);

        return {
            uf,
            taxa,
            meta,
            gap: taxa !== null && meta !== null ? taxa - meta : null
        };
    })
    .filter(r => r.taxa !== null)
    .sort((a,b) => b.taxa - a.taxa);

    if (!rows.length) {
        document.getElementById("ranking-dinamico").innerHTML =
            "<div class='empty-state'>Sem dados para o recorte selecionado.</div>";
        return;
    }

    const html = rows.map(r => {
        const taxaPct = Math.max(0, Math.min(r.taxa * 100, 100));
        const metaPct = r.meta !== null
            ? Math.max(0, Math.min(r.meta * 100, 100))
            : null;

        return `
        <div style="display:grid;grid-template-columns:35px 1fr 58px 72px;gap:9px;align-items:center;margin:7px 0">
            <strong style="font-size:9px">${r.uf}</strong>

            <div style="height:11px;border-radius:99px;background:rgba(255,255,255,.08);position:relative">
                ${metaPct !== null
                    ? `<span style="position:absolute;left:${metaPct}%;top:-3px;bottom:-3px;width:2px;background:#fff;z-index:2"></span>`
                    : ""
                }

                <span style="display:block;height:100%;width:${taxaPct}%;
                             border-radius:99px;background:linear-gradient(90deg,#43d8e7,#8d7dff)"></span>
            </div>

            <strong style="font-size:9px;text-align:right">${fmtPct(r.taxa)}</strong>
            <span style="font-size:8px;color:#8fa2bd;text-align:right">${fmtPP(r.gap)}</span>
        </div>`;
    }).join("");

    document.getElementById("ranking-dinamico").innerHTML = html;
}

function buildMatrix(filters) {
    const previousYear = previousObservedYear(filters.ano);
    const container = document.getElementById("matriz-dinamica");

    if (previousYear === null) {
        container.innerHTML =
            "<div class='empty-state'>Não existe ano anterior no conjunto observado para calcular evolução.</div>";
        return;
    }

    const current = resumoFor(filters, filters.ano);
    const previous = resumoFor(filters, previousYear);

    const ufs = [...new Set(current.map(r => r.sigla_uf).filter(Boolean))];

    const points = ufs.map(uf => {
        const atual = avg(current.filter(r => r.sigla_uf === uf).map(r => r.taxa));
        const anterior = avg(previous.filter(r => r.sigla_uf === uf).map(r => r.taxa));
        const meta = metaUF(filters.ano, uf);

        return {
            uf,
            atual,
            anterior,
            variacao: atual !== null && anterior !== null ? atual - anterior : null,
            gap: atual !== null && meta !== null ? atual - meta : null
        };
    }).filter(r => r.atual !== null && r.variacao !== null);

    if (!points.length) {
        container.innerHTML =
            "<div class='empty-state'>Sem pares de anos para o recorte selecionado.</div>";
        return;
    }

    const width = 900;
    const height = 360;
    const pad = 54;

    const xs = points.map(p => p.atual * 100);
    const ys = points.map(p => p.variacao * 100);

    const xMin = Math.floor(Math.min(...xs) - 3);
    const xMax = Math.ceil(Math.max(...xs) + 3);
    const yAbs = Math.max(2, Math.ceil(Math.max(...ys.map(v => Math.abs(v))) + 1));
    const yMin = -yAbs;
    const yMax = yAbs;

    const px = v => pad + (v - xMin) / Math.max(xMax - xMin, 1) * (width - 2 * pad);
    const py = v => height - pad - (v - yMin) / Math.max(yMax - yMin, 1) * (height - 2 * pad);

    let svg = `<svg viewBox="0 0 ${width} ${height}" style="width:100%;height:auto;display:block">`;

    svg += `<line x1="${pad}" y1="${py(0)}" x2="${width-pad}" y2="${py(0)}"
                  stroke="rgba(255,255,255,.25)" stroke-dasharray="5 5"/>`;

    points.forEach(p => {
        const x = px(p.atual * 100);
        const y = py(p.variacao * 100);

        let color = "#43dda6";
        if (p.gap !== null && p.gap < -0.05) color = "#ff7181";
        else if (p.gap !== null && p.gap < 0) color = "#f4c761";

        svg += `
            <circle cx="${x}" cy="${y}" r="6" fill="${color}" opacity=".92">
                <title>${p.uf} · resultado ${fmtPct(p.atual)} · evolução ${fmtPP(p.variacao)}</title>
            </circle>
            <text x="${x+8}" y="${y-7}" fill="#dbe7f6" font-size="10" font-weight="700">${p.uf}</text>
        `;
    });

    svg += `
        <text x="${width/2}" y="${height-10}" fill="#8fa2bd" font-size="10" text-anchor="middle">
            Resultado atual (%)
        </text>
        <text x="14" y="${height/2}" fill="#8fa2bd" font-size="10"
              transform="rotate(-90 14 ${height/2})" text-anchor="middle">
            Evolução vs ${previousYear} (p.p.)
        </text>
    `;

    svg += "</svg>";

    container.innerHTML = `
        <div class="chart-shell">${svg}</div>
        <div class="chart-legend">
            <span><i class="legend-dot" style="background:#43dda6"></i>Na trajetória</span>
            <span><i class="legend-dot" style="background:#f4c761"></i>Atenção</span>
            <span><i class="legend-dot" style="background:#ff7181"></i>Prioridade</span>
        </div>
    `;
}

function buildJourney(filters) {
    const observedYears = [...new Set(DATA.resumo.map(r => r.ano))].sort((a,b) => a-b);

    const observed = observedYears.map(ano => {
        const rows = resumoFor(filters, ano);
        return {
            ano,
            valor: avg(rows.map(r => r.taxa))
        };
    }).filter(r => r.valor !== null);

    const metas = DATA.meta_brasil
        .filter(r => r.meta !== null)
        .sort((a,b) => a.ano - b.ano);

    const allYears = [...new Set([
        ...observed.map(r => r.ano),
        ...metas.map(r => r.ano)
    ])].sort((a,b) => a-b);

    if (!allYears.length) {
        document.getElementById("jornada-dinamica").innerHTML =
            "<div class='empty-state'>Sem dados para montar a jornada.</div>";
        return;
    }

    const values = [
        ...observed.map(r => r.valor * 100),
        ...metas.map(r => r.meta * 100)
    ];

    const width = 940;
    const height = 330;
    const pad = 55;

    const minYear = Math.min(...allYears);
    const maxYear = Math.max(...allYears);

    const minValue = Math.max(0, Math.floor(Math.min(...values) - 5));
    const maxValue = Math.min(100, Math.ceil(Math.max(...values) + 5));

    const px = year =>
        pad + (year - minYear) / Math.max(maxYear - minYear, 1) * (width - 2 * pad);

    const py = value =>
        height - pad - (value - minValue) / Math.max(maxValue - minValue, 1) * (height - 2 * pad);

    let svg = `<svg viewBox="0 0 ${width} ${height}" style="width:100%;height:auto;display:block">`;

    for (let i = 0; i < 5; i++) {
        const value = minValue + (maxValue - minValue) * i / 4;
        const y = py(value);

        svg += `
            <line x1="${pad}" y1="${y}" x2="${width-pad}" y2="${y}"
                  stroke="rgba(255,255,255,.06)"/>
            <text x="${pad-10}" y="${y+4}" fill="#7186a3" font-size="9" text-anchor="end">
                ${value.toFixed(0)}%
            </text>
        `;
    }

    allYears.forEach(year => {
        const x = px(year);
        svg += `
            <text x="${x}" y="${height-pad+22}" fill="#8fa2bd" font-size="9" text-anchor="middle">
                ${year}
            </text>
        `;
    });

    if (metas.length) {
        const pathMeta = metas
            .map((r, i) => `${i ? "L" : "M"} ${px(r.ano)} ${py(r.meta*100)}`)
            .join(" ");

        svg += `<path d="${pathMeta}" fill="none" stroke="#f4c761" stroke-width="3"
                      stroke-dasharray="7 5"/>`;

        metas.forEach(r => {
            svg += `<circle cx="${px(r.ano)}" cy="${py(r.meta*100)}" r="4" fill="#f4c761"/>`;
        });
    }

    if (observed.length) {
        const pathObserved = observed
            .map((r, i) => `${i ? "L" : "M"} ${px(r.ano)} ${py(r.valor*100)}`)
            .join(" ");

        svg += `<path d="${pathObserved}" fill="none" stroke="#43d8e7" stroke-width="4"/>`;

        observed.forEach(r => {
            const active = r.ano === filters.ano;
            svg += `
                <circle cx="${px(r.ano)}" cy="${py(r.valor*100)}"
                        r="${active ? 7 : 5}"
                        fill="${active ? "#8d7dff" : "#43d8e7"}"/>
            `;
        });
    }

    svg += "</svg>";

    document.getElementById("jornada-dinamica").innerHTML = `
        <div class="chart-shell">${svg}</div>
        <div class="chart-legend">
            <span><i class="legend-dot" style="background:#43d8e7"></i>Resultado observado</span>
            <span><i class="legend-dot" style="background:#8d7dff"></i>Ano selecionado</span>
            <span><i class="legend-dot" style="background:#f4c761"></i>Meta nacional</span>
        </div>
    `;

    const yearsForTable = [...new Set([
        ...observed.map(r => r.ano),
        ...metas.map(r => r.ano)
    ])].sort((a,b) => a-b);

    const body = yearsForTable.map(ano => {
        const observedRow = observed.find(r => r.ano === ano);
        const metaRow = metas.find(r => r.ano === ano);

        const result = observedRow ? observedRow.valor : null;
        const meta = metaRow ? metaRow.meta : null;
        const gap = result !== null && meta !== null ? result - meta : null;

        return `
        <tr>
            <td class="cell-key">${ano}</td>
            <td>${fmtPct(result)}</td>
            <td>${fmtPct(meta)}</td>
            <td>${fmtPP(gap)}</td>
            <td>${result !== null ? "Resultado observado" : "Meta futura"}</td>
        </tr>`;
    }).join("");

    document.getElementById("jornada-tabela").innerHTML = `
        <div class="table-shell">
            <div class="table-scroll">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>Ano</th>
                            <th>Resultado</th>
                            <th>Meta nacional</th>
                            <th>Gap</th>
                            <th>Referência</th>
                        </tr>
                    </thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
        </div>
    `;
}

function buildInequality(filters) {
    const codes = selectedBreakdownCodes(filters.rede);

    const rows = DATA.resumo.filter(r =>
        r.ano === filters.ano &&
        codes.includes(r.rede) &&
        (filters.uf === "Todas" || r.sigla_uf === filters.uf)
    );

    const groups = {};

    rows.forEach(r => {
        const regiao = REGIAO_UF[r.sigla_uf] || "Sem região";
        const rede = REDE_LABEL[r.rede] || `Rede ${r.rede}`;
        const key = `${regiao}|${rede}`;

        if (!groups[key]) {
            groups[key] = {
                regiao,
                rede,
                values: [],
                ufs: new Set()
            };
        }

        if (r.taxa !== null) groups[key].values.push(r.taxa);
        if (r.sigla_uf) groups[key].ufs.add(r.sigla_uf);
    });

    const data = Object.values(groups)
        .map(g => ({
            regiao: g.regiao,
            rede: g.rede,
            taxa: avg(g.values),
            ufs: g.ufs.size
        }))
        .sort((a,b) =>
            a.regiao.localeCompare(b.regiao) ||
            a.rede.localeCompare(b.rede)
        );

    if (!data.length) {
        document.getElementById("desigualdade-dinamica").innerHTML =
            "<div class='empty-state'>Sem dados para o recorte selecionado.</div>";
        return;
    }

    const body = data.map(r => `
        <tr>
            <td class="cell-key">${r.regiao}</td>
            <td class="cell-key">${r.rede}</td>
            <td>${fmtPct(r.taxa)}</td>
            <td>${r.ufs}</td>
        </tr>
    `).join("");

    document.getElementById("desigualdade-dinamica").innerHTML = `
        <div class="table-shell">
            <div class="table-scroll">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>Região</th>
                            <th>Rede</th>
                            <th>Alfabetização</th>
                            <th>UFs com dados</th>
                        </tr>
                    </thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
        </div>
    `;
}

function severity(gap) {
    if (gap === null || gap === undefined) return "Sem meta";
    if (gap >= 0) return "Na meta";
    if (gap >= -0.05) return "Atenção";
    if (gap >= -0.10) return "Alta";
    return "Crítica";
}

function severityClass(label) {
    if (label === "Na meta" || label === "Na trajetória") return "success";
    if (label === "Atenção" || label === "Alta") return "warning";
    if (label === "Crítica" || label === "Prioridade") return "danger";
    return "warning";
}

function buildMunicipios(filters) {
    const rows = municipiosFor(filters)
        .filter(r => r.meta !== null && r.gap !== null)
        .sort((a,b) => a.gap - b.gap)
        .slice(0, 25);

    if (!rows.length) {
        document.getElementById("municipios-dinamico").innerHTML =
            "<div class='empty-state'>Sem municípios para o recorte selecionado.</div>";
        return;
    }

    const body = rows.map(r => {
        const sev = severity(r.gap);

        return `
        <tr>
            <td class="cell-key">${r.sigla_uf || "N/D"}</td>
            <td class="cell-key">${r.nome_municipio || r.id_municipio || "N/D"}</td>
            <td>${r.rede_label || "N/D"}</td>
            <td>${fmtPct(r.taxa)}</td>
            <td>${fmtPct(r.meta)}</td>
            <td>${fmtPP(r.gap)}</td>
            <td><span class="badge ${severityClass(sev)}">${sev}</span></td>
        </tr>`;
    }).join("");

    document.getElementById("municipios-dinamico").innerHTML = `
        <div class="table-shell">
            <div class="table-scroll">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>UF</th>
                            <th>Município</th>
                            <th>Rede</th>
                            <th>Resultado</th>
                            <th>Meta</th>
                            <th>Gap</th>
                            <th>Prioridade</th>
                        </tr>
                    </thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
            <div class="table-footer">${rows.length} registro(s) exibidos</div>
        </div>
    `;
}

function buildStreaming(filters) {
    const codes = filters.rede === "todas"
        ? [0,2,3,5]
        : selectedBreakdownCodes(filters.rede);

    const rows = DATA.streaming.filter(r =>
        r.ano === filters.ano &&
        codes.includes(r.rede) &&
        (filters.uf === "Todas" || r.sigla_uf === filters.uf)
    );

    if (!rows.length) {
        document.getElementById("streaming-dinamico").innerHTML =
            "<div class='empty-state'>Não há eventos de replay oficial para esse recorte.</div>";
        return;
    }

    const latencies = rows
        .map(r => r.latency)
        .filter(v => v !== null && Number.isFinite(v))
        .sort((a,b) => a-b);

    const media = avg(latencies);
    const p95 = latencies.length
        ? latencies[Math.min(latencies.length - 1, Math.floor(latencies.length * .95))]
        : null;

    const municipios = new Set(
        rows.map(r => r.sigla_uf).filter(Boolean)
    ).size;

    const body = rows.map(r => `
        <tr>
            <td>${r.ano ?? "N/D"}</td>
            <td>${r.sigla_uf || "N/D"}</td>
            <td>${REDE_LABEL[r.rede] || r.rede || "N/D"}</td>
            <td>${r.event_time ? new Date(r.event_time).toLocaleString("pt-BR") : "N/D"}</td>
            <td>${r.ingestion_time ? new Date(r.ingestion_time).toLocaleString("pt-BR") : "N/D"}</td>
            <td>${r.latency !== null ? r.latency.toFixed(1).replace(".", ",") + " s" : "N/D"}</td>
        </tr>
    `).join("");

    document.getElementById("streaming-dinamico").innerHTML = `
        <div class="grid-3" style="margin-bottom:12px">
            <div class="mini-card">
                <span class="tag">Eventos</span>
                <h3>${fmtInt(rows.length)}</h3>
                <p>Eventos oficiais no recorte selecionado.</p>
            </div>
            <div class="mini-card">
                <span class="tag">Latência média</span>
                <h3>${media !== null ? media.toFixed(1).replace(".", ",") + " s" : "N/D"}</h3>
                <p>Diferença entre event_time e ingestão.</p>
            </div>
            <div class="mini-card">
                <span class="tag">Latência p95</span>
                <h3>${p95 !== null ? p95.toFixed(1).replace(".", ",") + " s" : "N/D"}</h3>
                <p>Cauda de latência observada no replay.</p>
            </div>
        </div>

        <div class="table-shell">
            <div class="table-scroll">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>Ano</th>
                            <th>UF</th>
                            <th>Rede</th>
                            <th>Evento</th>
                            <th>Ingestão</th>
                            <th>Latência</th>
                        </tr>
                    </thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
        </div>
    `;
}

function buildStudents(filters) {
    const container = document.getElementById("alunos-dinamico");

    const codes = filters.rede === "todas"
        ? [...new Set(DATA.student_dist.map(r => r.rede).filter(v => v !== null))]
        : selectedBreakdownCodes(filters.rede);

    const rows = DATA.student_dist.filter(r =>
        r.ano === filters.ano &&
        codes.includes(r.rede) &&
        (filters.uf === "Todas" || r.sigla_uf === filters.uf)
    );

    if (!rows.length) {
        container.innerHTML =
            "<div class='empty-state'>Os microdados individuais usados na base de modelagem estão disponíveis para a referência 2024. O recorte selecionado não possui distribuição individual.</div>";
        return;
    }

    const grouped = {};

    rows.forEach(r => {
        if (!grouped[r.faixa]) grouped[r.faixa] = 0;
        grouped[r.faixa] += r.alunos;
    });

    const data = Object.entries(grouped)
        .map(([faixa, alunos]) => ({faixa: Number(faixa), alunos}))
        .sort((a,b) => a.faixa - b.faixa);

    const maxN = Math.max(...data.map(r => r.alunos));
    const total = data.reduce((sum, r) => sum + r.alunos, 0);
    const alfabetizados = data
        .filter(r => r.faixa >= 750)
        .reduce((sum, r) => sum + r.alunos, 0);

    const bars = data.map(r => {
        const h = Math.max(5, (r.alunos / maxN) * 210);
        const color = r.faixa >= 750
            ? "#43dda6"
            : r.faixa >= 725
            ? "#f4c761"
            : "rgba(255,255,255,.28)";

        return `
        <div style="flex:1;min-width:18px;display:flex;flex-direction:column;
                    justify-content:flex-end;align-items:center;gap:5px"
             title="${r.faixa}-${r.faixa+24}: ${fmtInt(r.alunos)} alunos">
            <div style="width:78%;height:${h}px;background:${color};
                        border-radius:5px 5px 0 0"></div>
            <div style="font-size:7px;color:#7e91aa">${r.faixa}</div>
        </div>`;
    }).join("");

    container.innerHTML = `
        <div class="grid-3" style="margin-bottom:12px">
            <div class="mini-card">
                <span class="tag">Registros com proficiência</span>
                <h3>${fmtInt(total)}</h3>
                <p>Distribuição do recorte ativo.</p>
            </div>
            <div class="mini-card">
                <span class="tag">Corte</span>
                <h3>743</h3>
                <p>Regra auxiliar documentada no contrato.</p>
            </div>
            <div class="mini-card">
                <span class="tag">Target Fase 3</span>
                <h3>alfabetizado_oficial</h3>
                <p>O target não é reconstruído a partir da proficiência.</p>
            </div>
        </div>

        <div style="position:relative;padding:12px 4px 0">
            <div style="display:flex;height:250px;gap:2px;align-items:flex-end;border-bottom:1px solid rgba(255,255,255,.12)">
                ${bars}
            </div>

            <div style="margin-top:10px;color:#8fa2bd;font-size:8px">
                Cinza = abaixo do corte · amarelo = faixa que contém 743 · verde = ≥ 750
            </div>
        </div>
    `;
}

function buildDecision(filters) {
    const current = resumoFor(filters);
    const previousYear = previousObservedYear(filters.ano);
    const previous = previousYear !== null
        ? resumoFor(filters, previousYear)
        : [];

    const ufs = [...new Set(current.map(r => r.sigla_uf).filter(Boolean))];

    const rows = ufs.map(uf => {
        const taxa = avg(current.filter(r => r.sigla_uf === uf).map(r => r.taxa));
        const meta = metaUF(filters.ano, uf);
        const anterior = previousYear !== null
            ? avg(previous.filter(r => r.sigla_uf === uf).map(r => r.taxa))
            : null;

        const gap = taxa !== null && meta !== null ? taxa - meta : null;
        const variacao = taxa !== null && anterior !== null ? taxa - anterior : null;

        let score = 0;

        if (gap !== null && gap < 0) score += (-gap) * 70;
        if (variacao !== null && variacao < 0) score += (-variacao) * 30;

        let status = "Meta indisponível";

        if (gap !== null) {
            if (gap >= 0) status = "Na trajetória";
            else if (gap >= -0.05) status = "Atenção";
            else status = "Prioridade";
        }

        let recomendacao = "Completar dados de meta antes da decisão";

        if (status === "Prioridade") {
            recomendacao = "Plano intensivo e diagnóstico territorial";
        } else if (status === "Atenção") {
            recomendacao = "Monitoramento e intervenção focalizada";
        } else if (status === "Na trajetória") {
            recomendacao = "Preservar avanço e compartilhar práticas";
        }

        return {
            uf,
            regiao: REGIAO_UF[uf] || "N/D",
            taxa,
            meta,
            gap,
            variacao,
            score,
            status,
            recomendacao
        };
    })
    .filter(r => r.taxa !== null)
    .sort((a,b) => b.score - a.score);

    if (!rows.length) {
        document.getElementById("decisao-dinamica").innerHTML =
            "<div class='empty-state'>Sem dados para o recorte selecionado.</div>";
        return;
    }

    const body = rows.map(r => `
        <tr>
            <td class="cell-key">${r.uf}</td>
            <td>${r.regiao}</td>
            <td>${fmtPct(r.taxa)}</td>
            <td>${fmtPct(r.meta)}</td>
            <td>${fmtPP(r.gap)}</td>
            <td>${fmtPP(r.variacao)}</td>
            <td>${r.score.toFixed(2).replace(".", ",")}</td>
            <td><span class="badge ${severityClass(r.status)}">${r.status}</span></td>
            <td>${r.recomendacao}</td>
        </tr>
    `).join("");

    document.getElementById("decisao-dinamica").innerHTML = `
        <div class="table-shell">
            <div class="table-scroll">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>UF</th>
                            <th>Região</th>
                            <th>Resultado</th>
                            <th>Meta</th>
                            <th>Gap</th>
                            <th>Evolução</th>
                            <th>Score</th>
                            <th>Situação</th>
                            <th>Ação sugerida</th>
                        </tr>
                    </thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
        </div>
    `;
}


function buildFixedRanking(filters, networkType, containerId) {

    const codes = networkType === "publica"
        ? [2, 3]
        : [5];

    const rowsBase = DATA.resumo.filter(r =>
        r.ano === filters.ano &&
        codes.includes(r.rede) &&
        (filters.uf === "Todas" || r.sigla_uf === filters.uf)
    );

    const ufs = [...new Set(
        rowsBase
        .map(r => r.sigla_uf)
        .filter(Boolean)
    )];

    const rows = ufs
        .map(uf => {

            const taxa = avg(
                rowsBase
                .filter(r => r.sigla_uf === uf)
                .map(r => r.taxa)
            );

            const meta = metaUF(filters.ano, uf);

            return {
                uf,
                taxa,
                meta,
                gap:
                    taxa !== null && meta !== null
                    ? taxa - meta
                    : null
            };
        })
        .filter(r => r.taxa !== null)
        .sort((a, b) => b.taxa - a.taxa);

    const container = document.getElementById(containerId);

    if (!rows.length) {
        container.innerHTML =
            "<div class='empty-state'>Sem dados para o recorte selecionado.</div>";
        return;
    }

    const html = rows.map(r => {

        const taxaPct = Math.max(
            0,
            Math.min(r.taxa * 100, 100)
        );

        const metaPct =
            r.meta !== null
            ? Math.max(0, Math.min(r.meta * 100, 100))
            : null;

        return `
        <div style="
            display:grid;
            grid-template-columns:38px 1fr 62px 76px;
            gap:10px;
            align-items:center;
            margin:9px 0
        ">

            <strong style="font-size:11px">
                ${r.uf}
            </strong>

            <div style="
                height:13px;
                border-radius:99px;
                background:rgba(255,255,255,.08);
                position:relative
            ">

                ${
                    metaPct !== null
                    ? `
                    <span style="
                        position:absolute;
                        left:${metaPct}%;
                        top:-3px;
                        bottom:-3px;
                        width:2px;
                        background:#fff;
                        z-index:2
                    "></span>
                    `
                    : ""
                }

                <span style="
                    display:block;
                    height:100%;
                    width:${taxaPct}%;
                    border-radius:99px;
                    background:linear-gradient(
                        90deg,
                        #42d9e8,
                        #8c7cff
                    )
                "></span>

            </div>

            <strong style="
                font-size:10px;
                text-align:right
            ">
                ${fmtPct(r.taxa)}
            </strong>

            <span style="
                color:#8fa2bd;
                font-size:9px;
                text-align:right
            ">
                ${fmtPP(r.gap)}
            </span>

        </div>
        `;
    }).join("");

    container.innerHTML = html;
}


function buildInequalityPublicPrivate(filters) {

    const rows = DATA.resumo.filter(r =>
        r.ano === filters.ano &&
        [2, 3, 5].includes(r.rede) &&
        (filters.uf === "Todas" || r.sigla_uf === filters.uf)
    );

    const regions = {};

    rows.forEach(r => {
        const regiao = REGIAO_UF[r.sigla_uf] || "Sem região";

        if (!regions[regiao]) {
            regions[regiao] = {
                publica: [],
                privada: []
            };
        }

        if ([2, 3].includes(r.rede) && r.taxa !== null) {
            regions[regiao].publica.push(r.taxa);
        }

        if (r.rede === 5 && r.taxa !== null) {
            regions[regiao].privada.push(r.taxa);
        }
    });

    const data = Object.entries(regions)
        .map(([regiao, values]) => ({
            regiao,
            publica: avg(values.publica),
            privada: avg(values.privada)
        }))
        .filter(r => r.publica !== null || r.privada !== null)
        .sort((a, b) => a.regiao.localeCompare(b.regiao));

    const container = document.getElementById("desigualdade-dinamica");

    if (!data.length) {
        container.innerHTML =
            "<div class='empty-state'>Sem dados para o recorte selecionado.</div>";
        return;
    }

    const body = data.map(r => {
        const gap =
            r.publica !== null && r.privada !== null
            ? r.publica - r.privada
            : null;

        return `
        <tr>
            <td class="cell-key">${r.regiao}</td>
            <td>${fmtPct(r.publica)}</td>
            <td>${fmtPct(r.privada)}</td>
            <td>${fmtPP(gap)}</td>
        </tr>
        `;
    }).join("");

    container.innerHTML = `
        <div class="table-shell">
            <div class="table-scroll">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>Região</th>
                            <th>Rede pública</th>
                            <th>Rede privada</th>
                            <th>Diferença pública x privada</th>
                        </tr>
                    </thead>
                    <tbody>${body}</tbody>
                </table>
            </div>
        </div>
    `;
}


function mirrorProgressToOverview() {
    const source = document.getElementById("progresso-dinamico");
    const target = document.getElementById("progresso-visao-geral");

    if (source && target) {
        target.innerHTML = source.innerHTML;
    }
}

function applyFilters() {
    const filters = currentFilters();

    buildKpis(filters);
    buildProgress(filters);

    buildFixedRanking(
        filters,
        "publica",
        "ranking-publica"
    );

    buildFixedRanking(
        filters,
        "privada",
        "ranking-privada"
    );

    buildJourney(filters);
    buildMatrix(filters);
    buildInequalityPublicPrivate(filters);
    buildMunicipios(filters);
    buildDecision(filters);
    buildStudents(filters);

    const redeLabel =
        document
        .getElementById("filtro-rede")
        .selectedOptions[0]
        .text;

    const ufLabel =
        filters.uf === "Todas"
        ? "Todas as UFs"
        : filters.uf;

    document
        .getElementById("overview-context")
        .textContent =
        `Visão consolidada · ${filters.ano} · ${redeLabel} · ${ufLabel}`;

    mirrorProgressToOverview();
    resizeFrame();
}

function openTab(tabId, button) {
    document.querySelectorAll(".tab-panel").forEach(tab => {
        tab.classList.remove("active");
    });

    document.querySelectorAll(".top-tab").forEach(btn => {
        btn.classList.remove("active");
    });

    document.getElementById(tabId).classList.add("active");
    button.classList.add("active");

    window.scrollTo({
        top: 0,
        behavior: "smooth"
    });

    resizeFrame();
}

function resizeFrame() {
    // O displayHTML do Databricks renderiza dentro de um iframe com altura
    // fixa e pequena por padrão. Sem isso, o dashboard inteiro (com a
    // sidebar fixa e min-height:100vh) fica espremido e cortado, já que
    // 100vh passa a valer a altura minúscula do iframe, não da tela.
    if (window.frameElement) {
        const h = Math.max(
            document.documentElement.scrollHeight,
            document.body.scrollHeight
        );
        window.frameElement.style.height = h + "px";
    }
}

document.addEventListener("DOMContentLoaded", function () {
    applyFilters();
    resizeFrame();
    // Recalcula depois de fontes/gráficos assentarem, e mais uma vez após
    // um pequeno atraso para pegar qualquer reflow tardio.
    window.addEventListener("load", resizeFrame);
    setTimeout(resizeFrame, 300);
});

// Expostas no escopo global porque são chamadas por atributos inline
// (onclick/onchange) no HTML, fora do escopo desta IIFE.
window.openTab = openTab;
window.applyFilters = applyFilters;
})();
</script>

</body>
</html>
"""

# COMMAND ----------
# MAGIC %md
# MAGIC ## 29. Opções dinâmicas, publicação e exibição
# MAGIC Gera as opções de UF e ano a partir dos dados reais, substitui os
# MAGIC placeholders do template, grava o HTML no Volume e exibe o resultado.

# COMMAND ----------

uf_options = "".join(
    f'<option value="{escape(uf)}">{escape(uf)}</option>'
    for uf in ufs_html
)

ano_options = "".join(
    f'<option value="{ano}"{" selected" if i == 0 else ""}>{ano}</option>'
    for i, ano in enumerate(available_filter_years)
)

html_final = (
    html_final
    .replace("__FILTER_DATA_JSON__", FILTER_DATA_JSON)
    .replace("__UF_OPTIONS__", uf_options)
    .replace("__ANO_OPTIONS__", ano_options)
    .replace("__SOURCE_INVENTORY_HTML__", SOURCE_INVENTORY_HTML)
    .replace("__GOLD_INVENTORY_HTML__", GOLD_INVENTORY_HTML)
    .replace("__QUALITY_HTML__", html_saude)
    .replace("__RANKING_TABLE_HTML__", RANKING_TABLE_HTML)
    .replace("__MATRIZ_TABLE_HTML__", MATRIZ_TABLE_HTML)
    .replace("__JORNADA_TABLE_HTML__", JORNADA_TABLE_HTML)
    .replace("__DESIGUALDADE_TABLE_HTML__", DESIGUALDADE_TABLE_HTML)
    .replace("__MUNICIPIOS_TABLE_HTML__", MUNICIPIOS_TABLE_HTML)
    .replace("__DECISAO_TABLE_HTML__", DECISAO_TABLE_HTML)
    .replace("__PIPELINE_HEALTH__", escape(str(pipeline_health_value)))
    .replace("__HEALTH_CLASS__", escape(str(pipeline_health_class_value)))
    .replace("__GENERATED_AT__", generated_at)
    .replace("__ALUNOS_GOLD__", fmt_count(alunos_gold_count))
    .replace("__SILVER_TERRITORIAL__", fmt_count(silver_territorial_aprovada))
    .replace("__SILVER_ALUNOS__", fmt_count(silver_alunos_aprovados))
)

dashboard_path = Path(DASHBOARD_HTML)
dashboard_path.parent.mkdir(parents=True, exist_ok=True)
dashboard_path.write_text(html_final, encoding="utf-8")

if not dashboard_path.is_file() or dashboard_path.stat().st_size == 0:
    raise RuntimeError(f"O dashboard não foi gravado corretamente: {DASHBOARD_HTML}")

dashboard_size_kb = dashboard_path.stat().st_size / 1024
dashboard_link = DASHBOARD_HTML.replace("/Volumes", "dbfs:/Volumes", 1)

print(f"✓ Command Center completo: {DASHBOARD_HTML}")
print(f"✓ Arquivo HTML validado: {dashboard_size_kb:.1f} KB")
print(f"→ Abra o artefato publicado: {dashboard_link}")
print("✓ Fontes, pipeline, território, jornada 2030, desigualdade, municípios,")
print("  streaming, alunos/743, qualidade, decisão e IA/Fase 3 incluídos.")
print(f"✓ Gold base_modelagem_aluno: {alunos_gold_count:,} registros.")

displayHTML(
    f"<p><strong>Dashboard publicado:</strong> "
    f"<a href='{dashboard_link}' target='_blank'>{DASHBOARD_HTML}</a> "
    f"({dashboard_size_kb:.1f} KB)</p>"
)
displayHTML(html_final)