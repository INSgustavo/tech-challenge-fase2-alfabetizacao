# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Command Center da Alfabetização
# MAGIC
# MAGIC Dashboard executivo da camada Gold, desenhado para responder quatro perguntas de liderança:
# MAGIC
# MAGIC 1. **Onde estamos?** Resultado atual, cobertura e desigualdade territorial.
# MAGIC 2. **Quem precisa de prioridade?** UFs e municípios mais distantes da meta.
# MAGIC 3. **Para onde vamos?** Evolução histórica e trajetória até 2030.
# MAGIC 4. **A plataforma é confiável?** Streaming, qualidade, frescor e saúde operacional.
# MAGIC
# MAGIC O painel combina uma capa executiva em HTML/CSS com datasets preparados para
# MAGIC visualizações nativas do Databricks. Use **+ Add to dashboard** nos resultados indicados.

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

# COMMAND ----------
# MAGIC %md
# MAGIC ## 0. Funções defensivas e validação do ambiente

# COMMAND ----------
def table_exists(table_name: str) -> bool:
    return spark.catalog.tableExists(table_name)


def require_tables(tables: list[str]) -> None:
    missing = [table for table in tables if not table_exists(table)]
    if missing:
        raise RuntimeError(
            "Execute os notebooks anteriores antes do dashboard. "
            f"Tabelas obrigatórias ausentes: {', '.join(missing)}"
        )


def numeric_column_like(df: DataFrame, token: str) -> str | None:
    """Localiza uma coluna numérica cujo nome contenha o token informado."""
    numeric_prefixes = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal")
    candidates = [
        column
        for column, dtype in df.dtypes
        if token.lower() in column.lower() and dtype.lower().startswith(numeric_prefixes)
    ]
    return candidates[0] if candidates else None


def normalize_rate(column: F.Column) -> F.Column:
    """Normaliza percentuais 0–100 para frações 0–1 sem alterar valores já normalizados."""
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
    return "—" if value is None else f"{fmt_num(value * 100, decimals)}%"


def fmt_pp(value, decimals: int = 1) -> str:
    if value is None:
        return "—"
    num = fmt_num(value * 100, decimals)
    return f"+{num} p.p." if value > 0 else f"{num} p.p."


require_tables([T_RESUMO_UF, T_IND_MUN, T_META_RESULTADO])

resumo_uf = spark.table(T_RESUMO_UF)
indicador_municipio = spark.table(T_IND_MUN)
meta_vs_resultado = spark.table(T_META_RESULTADO)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Filtros executivos
# MAGIC Os filtros controlam as principais visões do notebook e podem ser usados durante a apresentação.

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
# MAGIC ## 2. Capa executiva — visão de 30 segundos
# MAGIC Esta célula é o elemento de abertura do vídeo. Ela combina indicadores educacionais,
# MAGIC cobertura, streaming e saúde operacional em uma única narrativa.

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
municipal_filtrado = meta_vs_resultado.filter(
    (F.col("ano") == ANO) & F.col("rede").isin(rede_codes)
)
if UF_SELECIONADA != "Todas":
    municipal_filtrado = municipal_filtrado.filter(F.col("sigla_uf") == UF_SELECIONADA)

municipal_stats = municipal_filtrado.agg(
    F.countDistinct("id_municipio").alias("municipios_monitorados"),
    F.avg(F.col("atingiu_meta").cast("double")).alias("pct_municipios_meta"),
).collect()[0]
municipios_monitorados = municipal_stats["municipios_monitorados"] or 0
pct_municipios_meta = municipal_stats["pct_municipios_meta"]

# KPIs de streaming
stream_events = 0
stream_p95 = None
last_event = None
if table_exists(T_EVENTOS):
    eventos = spark.table(T_EVENTOS)
    eventos_ano = eventos.filter(F.col("ano") == ANO) if "ano" in eventos.columns else eventos
    if UF_SELECIONADA != "Todas" and "sigla_uf" in eventos_ano.columns:
        eventos_ano = eventos_ano.filter(F.col("sigla_uf") == UF_SELECIONADA)
    stream_events = eventos_ano.count()

    if {"event_time", "_ingestion_timestamp"}.issubset(eventos_ano.columns):
        latency = eventos_ano.withColumn(
            "latency_seconds",
            F.unix_timestamp("_ingestion_timestamp") - F.unix_timestamp("event_time"),
        )
        latency_stats = latency.agg(
            F.expr("percentile_approx(latency_seconds, 0.95)").alias("p95"),
            F.max("event_time").alias("last_event"),
        ).collect()[0]
        stream_p95 = latency_stats["p95"]
        last_event = latency_stats["last_event"]

# KPIs de qualidade
quarantine_count = safe_count(T_QUARENTENA)
silver_count = safe_count(T_SILVER)
rejection_rate = quarantine_count / max(quarantine_count + silver_count, 1)

# Saúde operacional nas últimas 20 métricas
pipeline_health = "SEM MÉTRICAS"
pipeline_health_class = "neutral"
failed_tasks = 0
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

# Frescor da Gold
freshness_label = "—"
if gold_updated_at is not None:
    gold_ts = gold_updated_at
    if gold_ts.tzinfo is None:
        gold_ts = gold_ts.replace(tzinfo=timezone.utc)
    age_hours = max((datetime.now(timezone.utc) - gold_ts).total_seconds() / 3600, 0)
    freshness_label = f"{fmt_num(age_hours)} h"

# Destaques narrativos
best = (
    ranking_ufs.orderBy(F.desc("taxa_resultado"))
    .select("sigla_uf", "taxa_resultado").limit(1).collect()
)
worst = (
    ranking_ufs.orderBy(F.asc("taxa_resultado"))
    .select("sigla_uf", "taxa_resultado", "gap_meta").limit(1).collect()
)
best_uf = best[0]["sigla_uf"] if best else "—"
best_rate = best[0]["taxa_resultado"] if best else None
worst_uf = worst[0]["sigla_uf"] if worst else "—"
worst_rate = worst[0]["taxa_resultado"] if worst else None
worst_gap = worst[0]["gap_meta"] if worst else None

if gap_nacional is None:
    headline = "Resultado consolidado disponível; metas ainda incompletas para o recorte selecionado."
elif gap_nacional >= 0:
    headline = f"O resultado médio está {fmt_pp(gap_nacional)} acima da meta do período."
else:
    headline = f"O resultado médio está {fmt_pp(abs(gap_nacional))} abaixo da meta do período."

if worst_gap is not None and worst_gap < 0:
    distancia = fmt_num(abs(worst_gap) * 100)
    action_text = f"{worst_uf} exige prioridade: distância de {distancia} p.p. para a meta."
else:
    action_text = f"{worst_uf} apresenta o menor resultado do recorte e deve permanecer monitorada."

hero_html = Template(r"""
<div class="wow-shell">
  <style>
    .wow-shell {
      --bg1:#07111f; --bg2:#101a35; --cyan:#34d7e7; --violet:#8b7cff;
      --green:#2de2a0; --amber:#ffc857; --red:#ff6b7a; --text:#eef5ff; --muted:#9eb0c7;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system,
        BlinkMacSystemFont, "Segoe UI", sans-serif;
      color:var(--text); background:
        radial-gradient(circle at 10% 10%, rgba(52,215,231,.20), transparent 30%),
        radial-gradient(circle at 90% 20%, rgba(139,124,255,.20), transparent 34%),
        linear-gradient(135deg,var(--bg1),var(--bg2));
      border:1px solid rgba(255,255,255,.12); border-radius:24px; padding:28px;
      position:relative; overflow:hidden; box-shadow:0 24px 70px rgba(0,0,0,.35);
    }
    .wow-shell:before {
      content:""; position:absolute; inset:-50%; opacity:.25;
      background:linear-gradient(115deg,transparent 35%,
        rgba(255,255,255,.10) 50%,transparent 65%);
      animation:sweep 8s linear infinite; pointer-events:none;
    }
    @keyframes sweep {
      from{transform:translateX(-35%) rotate(8deg)}
      to{transform:translateX(35%) rotate(8deg)}
    }
    .top {
      display:flex; justify-content:space-between; gap:18px;
      align-items:flex-start; position:relative; z-index:1;
    }
    .eyebrow {
      font-size:12px; text-transform:uppercase; letter-spacing:.18em;
      color:var(--cyan); font-weight:800;
    }
    h1 {margin:8px 0 6px; font-size:34px; line-height:1.08; letter-spacing:-.035em;}
    .subtitle {color:var(--muted); font-size:14px; max-width:760px; line-height:1.55;}
    .health {
      padding:8px 12px; border-radius:999px; font-size:12px; font-weight:800;
      white-space:nowrap; border:1px solid rgba(255,255,255,.16)
    }
    .health.success {background:rgba(45,226,160,.14); color:#7bf0c4}
    .health.warning {background:rgba(255,200,87,.14); color:#ffd77e}
    .health.neutral {background:rgba(158,176,199,.12); color:#c7d4e5}
    .grid {
      display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:12px;
      margin-top:24px; position:relative; z-index:1;
    }
    .card {
      background:rgba(255,255,255,.07); border:1px solid rgba(255,255,255,.11);
      border-radius:18px; padding:16px; min-height:112px; backdrop-filter:blur(10px);
    }
    .card:hover {
      transform:translateY(-2px); transition:.2s ease;
      border-color:rgba(52,215,231,.45)
    }
    .label {
      font-size:11px; text-transform:uppercase; letter-spacing:.08em;
      color:var(--muted); font-weight:700;
    }
    .value {
      font-size:29px; line-height:1; font-weight:850; margin:12px 0 8px;
      letter-spacing:-.03em;
    }
    .hint {font-size:11px; color:var(--muted); line-height:1.35;}
    .insights {
      display:grid; grid-template-columns:1.5fr 1fr 1fr; gap:12px;
      margin-top:12px; position:relative; z-index:1;
    }
    .insight {
      background:rgba(4,10,23,.38); border:1px solid rgba(255,255,255,.09);
      border-radius:16px; padding:15px;
    }
    .insight strong {display:block; color:white; margin-bottom:5px; font-size:14px;}
    .insight span {color:var(--muted); font-size:12px; line-height:1.45;}
    .accent {color:var(--cyan)!important}
    .positive{color:var(--green)!important}
    .attention{color:var(--amber)!important}
    .footer {
      display:flex; justify-content:space-between; align-items:center; gap:12px;
      margin-top:14px; color:var(--muted); font-size:11px; position:relative; z-index:1;
    }
    @media(max-width:1000px){
      .grid{grid-template-columns:repeat(2,1fr)}
      .insights{grid-template-columns:1fr}
      .top{flex-direction:column}
    }
  </style>

  <div class="top">
    <div>
      <div class="eyebrow">Tech Challenge · Command Center Educacional</div>
      <h1>Alfabetização no Brasil</h1>
      <div class="subtitle">Leitura executiva da camada Gold com resultado, meta,
        desigualdade, streaming e confiabilidade operacional.</div>
    </div>
    <div class="health $health_class">● $pipeline_health</div>
  </div>

  <div class="grid">
    <div class="card"><div class="label">Resultado médio das UFs</div>
      <div class="value accent">$taxa_media</div><div class="hint">$rede · ano $ano</div></div>
    <div class="card"><div class="label">Meta nacional</div>
      <div class="value">$meta_nacional</div>
      <div class="hint">Gap consolidado: $gap_nacional</div></div>
    <div class="card"><div class="label">UFs na trajetória</div>
      <div class="value positive">$ufs_meta</div>
      <div class="hint">$pct_ufs_meta das UFs com meta disponível</div></div>
    <div class="card"><div class="label">Municípios monitorados</div>
      <div class="value">$municipios</div>
      <div class="hint">Na meta municipal: $pct_municipios</div></div>
    <div class="card"><div class="label">Eventos de streaming</div>
      <div class="value">$events</div>
      <div class="hint">Latência p95: $latency · Gold: $freshness</div></div>
  </div>

  <div class="insights">
    <div class="insight"><strong>$headline</strong><span>$action_text</span></div>
    <div class="insight"><strong>Maior resultado: $best_uf</strong>
      <span>$best_rate no recorte selecionado.</span></div>
    <div class="insight"><strong>Cobertura transparente</strong>
      <span>$ufs_data UFs com dados. O painel não presume cobertura ausente.</span></div>
  </div>

  <div class="footer">
    <span>Filtro ativo: $ano · $rede · $uf</span>
    <span>Qualidade: $rejection rejeitados · $failed falhas recentes</span>
  </div>
</div>
""").safe_substitute(
    health_class=pipeline_health_class,
    pipeline_health=pipeline_health,
    taxa_media=fmt_pct(taxa_media_ufs),
    rede=REDE_SELECIONADA,
    ano=ANO,
    meta_nacional=fmt_pct(meta_nacional),
    gap_nacional=fmt_pp(gap_nacional),
    ufs_meta=f"{ufs_na_meta}/{ufs_com_meta}" if ufs_com_meta else "—",
    pct_ufs_meta=fmt_pct(pct_ufs_na_meta),
    municipios=fmt_int(municipios_monitorados),
    pct_municipios=fmt_pct(pct_municipios_meta),
    events=fmt_int(stream_events),
    latency=(f"{stream_p95:.0f}s" if stream_p95 is not None else "—"),
    freshness=freshness_label,
    headline=headline,
    action_text=action_text,
    best_uf=best_uf,
    best_rate=fmt_pct(best_rate),
    ufs_data=fmt_int(ufs_com_dados),
    uf=UF_SELECIONADA,
    rejection=fmt_pct(rejection_rate),
    failed=fmt_int(failed_tasks),
)

displayHTML(hero_html)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Ranking territorial e desigualdade entre UFs
# MAGIC **Visualização recomendada:** barras horizontais.
# MAGIC
# MAGIC - Eixo Y: `sigla_uf`
# MAGIC - Eixo X: `taxa_pct`
# MAGIC - Cor: `status_meta`
# MAGIC - Ordenação: `taxa_pct` decrescente

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
# MAGIC ## 4. Matriz de prioridade — desempenho x evolução
# MAGIC **Visualização recomendada:** gráfico de dispersão.
# MAGIC
# MAGIC - X: `taxa_pct`
# MAGIC - Y: `variacao_pp`
# MAGIC - Cor: `status_meta`
# MAGIC - Tamanho: `score_prioridade`
# MAGIC - Rótulo: `sigla_uf`
# MAGIC
# MAGIC O quadrante inferior esquerdo concentra localidades com baixo resultado e piora recente.

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
# MAGIC ## 5. Jornada histórica e trajetória das metas até 2030
# MAGIC **Visualização recomendada:** linhas.
# MAGIC
# MAGIC - X: `ano`
# MAGIC - Y: `valor_pct`
# MAGIC - Série: `serie`

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
# MAGIC ## 6. Desigualdade por região e rede de ensino
# MAGIC **Visualização recomendada:** barras agrupadas ou heatmap.
# MAGIC
# MAGIC Esta visão permite discutir equidade, mostrando como território e dependência
# MAGIC administrativa se combinam na formação do resultado.

# COMMAND ----------
desigualdade_regional = (
    resumo_uf
    .filter((F.col("ano") == ANO) & F.col("rede").isin([0, 2, 3, 5]))
    .join(regiao_dim, "sigla_uf", "left")
    .groupBy("regiao", "rede_label")
    .agg(
        F.round(F.avg("taxa_alfabetizacao_media") * 100, 1).alias("taxa_pct"),
        F.countDistinct("sigla_uf").alias("ufs_com_dados"),
    )
    .orderBy("regiao", F.desc("taxa_pct"))
)

display(desigualdade_regional)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Municípios que exigem intervenção prioritária
# MAGIC **Visualização recomendada:** tabela com formatação condicional ou barras de `gap_pp`.
# MAGIC
# MAGIC A lista usa somente registros com meta municipal disponível e prioriza o maior déficit.

# COMMAND ----------
municipio_dim = None
if table_exists(f"{CATALOG}.bronze.municipio"):
    raw_municipio = spark.table(f"{CATALOG}.bronze.municipio")
    name_candidates = ["nome_municipio", "municipio", "nome"]
    municipality_name = next((c for c in name_candidates if c in raw_municipio.columns), None)
    if municipality_name and "id_municipio" in raw_municipio.columns:
        municipio_dim = (
            raw_municipio
            .select(
                F.lpad(F.col("id_municipio").cast("string"), 7, "0").alias("id_municipio"),
                F.col(municipality_name).cast("string").alias("nome_municipio"),
            )
            .dropDuplicates(["id_municipio"])
        )

municipios_prioritarios = (
    municipal_filtrado
    .filter(F.col("meta_taxa").isNotNull())
    .withColumn("taxa_pct", F.round(F.col("taxa_alfabetizacao_media") * 100, 1))
    .withColumn("meta_pct", F.round(F.col("meta_taxa") * 100, 1))
    .withColumn("gap_pp", F.round(F.col("gap_meta") * 100, 1))
    .withColumn(
        "severidade",
        F.when(F.col("gap_meta") >= 0, "Na meta")
        .when(F.col("gap_meta") >= -0.05, "Atenção")
        .when(F.col("gap_meta") >= -0.10, "Alta")
        .otherwise("Crítica"),
    )
)

if municipio_dim is not None:
    municipios_prioritarios = municipios_prioritarios.join(
        municipio_dim, "id_municipio", "left"
    )
else:
    municipios_prioritarios = municipios_prioritarios.withColumn(
        "nome_municipio", F.lit(None).cast("string")
    )

municipios_prioritarios = municipios_prioritarios.select(
    "sigla_uf",
    "id_municipio",
    "nome_municipio",
    "rede_label",
    "taxa_pct",
    "meta_pct",
    "gap_pp",
    "severidade",
    "updated_at",
).orderBy(F.asc("gap_pp")).limit(25)

display(municipios_prioritarios)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. Pulso do streaming — dados em tempo quase real
# MAGIC **Visualização recomendada:** linha ou área para o volume por janela de tempo.
# MAGIC
# MAGIC Esta seção é a evidência visual da ingestão híbrida exigida pelo desafio.

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
# MAGIC ## 9. Regra dos 743 pontos — visão no grão de aluno
# MAGIC **Visualização recomendada:** barras empilhadas por faixa de proficiência.
# MAGIC
# MAGIC A linha de corte de 743 pontos separa estudantes classificados como alfabetizados.
# MAGIC Os dados desta seção são simulados e devem ser apresentados dessa forma no vídeo.

# COMMAND ----------
if table_exists(T_ALUNOS):
    alunos = spark.table(T_ALUNOS)
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
                        F.when(F.col("proficiencia_portugues") >= 743, 1.0).otherwise(0.0)
                    ) * 100,
                    1,
                ).alias("pct_alfabetizados"),
                F.round(F.avg("proficiencia_portugues"), 1).alias("proficiencia_media"),
            )
            .withColumn(
                "rede_label",
                F.when(F.col("rede") == 0, "total")
                .when(F.col("rede") == 2, "estadual")
                .when(F.col("rede") == 3, "municipal")
                .when(F.col("rede") == 5, "privada")
                .otherwise(F.concat(F.lit("rede "), F.col("rede"))),
            )
            .select("ano", "rede_label", "alunos", "pct_alfabetizados", "proficiencia_media")
            .orderBy("ano", F.desc("pct_alfabetizados"))
        )
        display(resumo_alunos)
    else:
        print("⚠ bronze.alunos não contém a coluna proficiencia_portugues.")
else:
    print("⚠ bronze.alunos ainda não existe. A visão de 743 pontos ficará indisponível.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Saúde operacional, qualidade e observabilidade
# MAGIC **Visualização recomendada:** tabela com cores por `status_normalizado`.
# MAGIC
# MAGIC A seção demonstra falhas, volume, rejeições, duração e frescor das últimas execuções.

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
# MAGIC ## 11. Painel de decisão — onde agir primeiro
# MAGIC **Visualização recomendada:** tabela executiva com formatação condicional.
# MAGIC
# MAGIC Este ranking não substitui análise causal. Ele organiza a atenção da liderança usando
# MAGIC distância da meta e tendência recente, transformando o dashboard em instrumento de decisão.

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
# MAGIC ## 12. Galeria visual — gráficos renderizados automaticamente
# MAGIC Os gráficos abaixo são gerados em SVG/HTML na hora, no mesmo estilo da capa —
# MAGIC **sem nenhuma configuração manual de visualização**. São os frames do vídeo.

# COMMAND ----------
# Paleta e moldura compartilhadas
C_BG = "linear-gradient(135deg,#07111f,#101a35)"
C_TEXT, C_MUTED = "#eef5ff", "#9eb0c7"
C_CYAN, C_GREEN, C_AMBER, C_RED, C_VIOLET = "#34d7e7", "#2de2a0", "#ffc857", "#ff6b7a", "#8b7cff"
STATUS_COLORS = {
    "Na trajetória": C_GREEN, "Atenção": C_AMBER,
    "Prioridade": C_RED, "Meta indisponível": C_MUTED,
}


def chart_box(title: str, subtitle: str, body: str) -> str:
    return (
        f'<div style="background:{C_BG};border:1px solid rgba(255,255,255,.12);'
        f'border-radius:20px;padding:22px 26px;margin:8px 0;color:{C_TEXT};'
        f'font-family:Inter,\'Segoe UI\',sans-serif;box-shadow:0 18px 50px rgba(0,0,0,.30)">'
        f'<div style="font-size:19px;font-weight:800;letter-spacing:-.02em">{title}</div>'
        f'<div style="font-size:12px;color:{C_MUTED};margin:4px 0 18px">{subtitle}</div>'
        f'{body}</div>'
    )


def legend(items) -> str:
    dots = "".join(
        f'<span style="display:inline-flex;align-items:center;gap:6px;margin-right:16px;'
        f'font-size:12px;color:{C_MUTED}"><span style="width:10px;height:10px;'
        f'border-radius:99px;background:{color};display:inline-block"></span>{label}</span>'
        for label, color in items
    )
    return f'<div style="margin-top:12px">{dots}</div>'


def svg_line_chart(series, x_values, y_min, y_max, width=940, height=330, pad=52) -> str:
    x_lo, x_hi = min(x_values), max(x_values)

    def sx(x):
        return pad + (x - x_lo) / ((x_hi - x_lo) or 1) * (width - 2 * pad)

    def sy(y):
        return height - pad - (y - y_min) / ((y_max - y_min) or 1) * (height - 2 * pad)

    p = [f'<svg viewBox="0 0 {width} {height}" style="width:100%;max-width:{width}px">']
    for i in range(5):
        yv = y_min + (y_max - y_min) * i / 4
        p.append(f'<line x1="{pad}" y1="{sy(yv):.1f}" x2="{width - pad}" y2="{sy(yv):.1f}" '
                 f'stroke="rgba(255,255,255,.08)"/>')
        p.append(f'<text x="{pad - 8}" y="{sy(yv) + 4:.1f}" fill="{C_MUTED}" font-size="11" '
                 f'text-anchor="end">{yv:.0f}%</text>')
    for x in x_values:
        p.append(f'<text x="{sx(x):.1f}" y="{height - pad + 20}" fill="{C_MUTED}" '
                 f'font-size="11" text-anchor="middle">{x}</text>')
    for _name, color, pts, dash in series:
        line = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in pts)
        p.append(f'<polyline points="{line}" fill="none" stroke="{color}" stroke-width="3" '
                 f'stroke-dasharray="{dash}" stroke-linecap="round"/>')
        for x, y in pts:
            p.append(f'<circle cx="{sx(x):.1f}" cy="{sy(y):.1f}" r="4" fill="{color}"/>')
    p.append("</svg>")
    return "".join(p)


def donut(pct: float | None, label: str, color: str) -> str:
    if pct is None:
        pct = 0.0
    circ = 2 * 3.14159 * 44
    filled = circ * min(max(pct, 0), 1)
    return (
        f'<div style="text-align:center;min-width:150px">'
        f'<svg viewBox="0 0 110 110" style="width:110px">'
        f'<circle cx="55" cy="55" r="44" fill="none" stroke="rgba(255,255,255,.10)" stroke-width="12"/>'
        f'<circle cx="55" cy="55" r="44" fill="none" stroke="{color}" stroke-width="12" '
        f'stroke-linecap="round" stroke-dasharray="{filled:.1f} {circ:.1f}" '
        f'transform="rotate(-90 55 55)"/>'
        f'<text x="55" y="61" fill="{C_TEXT}" font-size="20" font-weight="800" '
        f'text-anchor="middle">{pct * 100:.0f}%</text></svg>'
        f'<div style="font-size:12px;color:{C_MUTED};margin-top:6px;max-width:150px">{label}</div></div>'
    )

# COMMAND ----------
# ---- Gráfico 1 · Donuts de progresso + ranking com marcador de meta ----
rk = ranking_ufs.orderBy(F.desc("taxa_pct")).collect()

donuts = (
    '<div style="display:flex;gap:26px;flex-wrap:wrap;justify-content:center">'
    + donut(pct_ufs_na_meta, "UFs na trajetória da meta", C_GREEN)
    + donut(pct_municipios_meta, "Municípios monitorados na meta", C_CYAN)
    + donut(taxa_media_ufs, f"Resultado médio · {REDE_SELECIONADA} {ANO}", C_VIOLET)
    + (donut(meta_nacional, f"Meta nacional {ANO}", C_AMBER) if meta_nacional else "")
    + "</div>"
)
displayHTML(chart_box("Progresso rumo a 2030", "Visão de gauges — abertura da seção de resultados", donuts))

bars = []
for r in rk:
    taxa = r["taxa_pct"] or 0.0
    color = STATUS_COLORS.get(r["status_meta"], C_MUTED)
    marker = ""
    if r["meta_pct"] is not None:
        marker = (f'<div style="position:absolute;left:{min(r["meta_pct"], 100):.1f}%;top:-3px;'
                  f'bottom:-3px;width:2px;background:{C_TEXT};opacity:.85" '
                  f'title="meta {r["meta_pct"]}%"></div>')
    gap_txt = f'{r["gap_pp"]:+.1f} pp'.replace(".", ",") if r["gap_pp"] is not None else "—"
    bars.append(
        f'<div style="display:flex;align-items:center;gap:10px;margin:5px 0">'
        f'<div style="width:34px;font-size:12px;font-weight:700;color:{C_MUTED}">{r["sigla_uf"]}</div>'
        f'<div style="flex:1;position:relative;height:16px;background:rgba(255,255,255,.07);'
        f'border-radius:99px">{marker}'
        f'<div style="position:absolute;left:0;top:0;bottom:0;width:{min(taxa, 100):.1f}%;'
        f'background:{color};border-radius:99px;opacity:.9"></div></div>'
        f'<div style="width:52px;font-size:12px;font-weight:700;text-align:right">'
        f'{str(taxa).replace(".", ",")}%</div>'
        f'<div style="width:70px;font-size:11px;color:{C_MUTED};text-align:right">{gap_txt}</div>'
        f'</div>'
    )
ranking_html = "".join(bars) + legend(
    [(s, c) for s, c in STATUS_COLORS.items()] + [("│ marcador = meta da UF", C_TEXT)]
)
displayHTML(chart_box(
    f"Ranking das UFs — Indicador Criança Alfabetizada · {REDE_SELECIONADA} {ANO}",
    "Barra = resultado · marcador branco = meta do ano · cor = status da trajetória",
    ranking_html,
))

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
displayHTML(chart_box(
    "Jornada até 2030 — resultado observado x meta nacional",
    "Linhas sólidas = resultado por rede · tracejada vermelha = meta (interpolação documentada no CONTRACT)",
    svg + legend([(n, c) for n, c, _p, _d in series]),
))

# COMMAND ----------
# ---- Gráfico 3 · Matriz de prioridade (dispersão desempenho x evolução) ----
mp = matriz_prioridade.collect()
if mp:
    W, H, PAD = 940, 380, 56
    xs = [float(r["taxa_pct"]) for r in mp]
    ys = [float(r["variacao_pp"]) for r in mp]
    x_lo, x_hi = min(xs) - 4, max(xs) + 4
    y_lo, y_hi = min(ys) - 2, max(ys) + 2

    def px(v):
        return PAD + (v - x_lo) / ((x_hi - x_lo) or 1) * (W - 2 * PAD)

    def py(v):
        return H - PAD - (v - y_lo) / ((y_hi - y_lo) or 1) * (H - 2 * PAD)

    pts = [f'<svg viewBox="0 0 {W} {H}" style="width:100%;max-width:{W}px">']
    if y_lo < 0 < y_hi:
        pts.append(f'<line x1="{PAD}" y1="{py(0):.0f}" x2="{W - PAD}" y2="{py(0):.0f}" '
                   f'stroke="rgba(255,255,255,.25)" stroke-dasharray="4 4"/>')
        pts.append(f'<text x="{W - PAD}" y="{py(0) - 6:.0f}" fill="{C_MUTED}" font-size="10" '
                   f'text-anchor="end">estabilidade</text>')
    for r in mp:
        color = STATUS_COLORS.get(r["status_meta"], C_MUTED)
        raio = 6 + min(float(r["score_prioridade"] or 0), 12)
        pts.append(f'<circle cx="{px(float(r["taxa_pct"])):.0f}" cy="{py(float(r["variacao_pp"])):.0f}" '
                   f'r="{raio:.0f}" fill="{color}" fill-opacity=".75" stroke="{color}"/>')
        pts.append(f'<text x="{px(float(r["taxa_pct"])):.0f}" '
                   f'y="{py(float(r["variacao_pp"])) - raio - 4:.0f}" fill="{C_TEXT}" '
                   f'font-size="11" font-weight="700" text-anchor="middle">{r["sigla_uf"]}</text>')
    pts.append(f'<text x="{W / 2:.0f}" y="{H - 12}" fill="{C_MUTED}" font-size="11" '
               f'text-anchor="middle">Resultado {ANO} (%)</text>')
    pts.append(f'<text x="16" y="{H / 2:.0f}" fill="{C_MUTED}" font-size="11" '
               f'transform="rotate(-90 16 {H / 2:.0f})" text-anchor="middle">Variação vs ano anterior (p.p.)</text>')
    pts.append("</svg>")
    displayHTML(chart_box(
        "Matriz de prioridade — desempenho x evolução",
        "Tamanho da bolha = score de prioridade · quadrante inferior-esquerdo = agir primeiro",
        "".join(pts) + legend(list(STATUS_COLORS.items())),
    ))
else:
    print("Sem ano anterior no recorte para montar a matriz.")

# COMMAND ----------
# ---- Gráfico 4 · Distribuição dos alunos e a linha de corte 743 (SIMULADO) ----
if table_exists(T_ALUNOS):
    dist = (
        spark.table(T_ALUNOS)
        .filter(F.col("ano") == ANO)
        .withColumn("faixa", (F.floor(F.col("proficiencia_portugues") / 25) * 25).cast("int"))
        .groupBy("faixa").count().orderBy("faixa").collect()
    )
    if dist:
        max_n = max(r["count"] for r in dist)
        cols = []
        for r in dist:
            alto = r["faixa"] >= 743 - 12  # faixa que contém o corte fica acima
            color = C_GREEN if r["faixa"] >= 750 else (C_AMBER if alto else "rgba(255,255,255,.28)")
            h = max(r["count"] / max_n * 170, 3)
            cols.append(
                f'<div style="flex:1;display:flex;flex-direction:column;justify-content:flex-end;'
                f'align-items:center;gap:4px" title="{r["faixa"]}–{r["faixa"] + 24}: {r["count"]} alunos">'
                f'<div style="width:82%;height:{h:.0f}px;background:{color};'
                f'border-radius:6px 6px 0 0"></div>'
                f'<div style="font-size:9px;color:{C_MUTED}">{r["faixa"]}</div></div>'
            )
        corte_pos = None
        faixas_x = [r["faixa"] for r in dist]
        if faixas_x:
            span = (max(faixas_x) + 25) - min(faixas_x)
            corte_pos = (743 - min(faixas_x)) / span * 100
        marcador = (
            f'<div style="position:absolute;left:{corte_pos:.1f}%;top:0;bottom:22px;width:2px;'
            f'background:{C_RED}"></div>'
            f'<div style="position:absolute;left:{corte_pos:.1f}%;top:-4px;transform:translateX(8px);'
            f'font-size:11px;font-weight:800;color:{C_RED}">corte 743 · alfabetizado →</div>'
        ) if corte_pos is not None else ""
        body = (
            f'<div style="position:relative;padding-top:18px">{marcador}'
            f'<div style="display:flex;align-items:flex-end;gap:2px;height:200px">'
            + "".join(cols) + "</div></div>"
            + legend([("≥ 750 (alfabetizado)", C_GREEN),
                      ("faixa do corte", C_AMBER),
                      ("abaixo do corte", "rgba(255,255,255,.28)")])
        )
        displayHTML(chart_box(
            f"Distribuição de proficiência dos alunos · {ANO} (dados SIMULADOS)",
            "Histograma por faixa de 25 pontos na escala Saeb — a linha vermelha é a regra oficial dos 743 pontos",
            body,
        ))

# COMMAND ----------
# ---- Gráfico 5 · Pulso do streaming ----
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
        cols = "".join(
            f'<div style="flex:1;max-width:90px;display:flex;flex-direction:column;'
            f'justify-content:flex-end;align-items:center;gap:6px">'
            f'<div style="font-size:11px;font-weight:800">{r["eventos"]}</div>'
            f'<div style="width:70%;height:{max(r["eventos"] / max_e * 150, 4):.0f}px;'
            f'background:linear-gradient(180deg,{C_CYAN},{C_VIOLET});border-radius:8px 8px 0 0"></div>'
            f'<div style="font-size:10px;color:{C_MUTED}">{r["janela"]}</div>'
            f'<div style="font-size:9px;color:{C_MUTED}">lat {r["lat"]}s</div></div>'
            for r in pulso
        )
        displayHTML(chart_box(
            "Pulso do streaming — eventos por hora de ingestão",
            "Evidência visual da ingestão híbrida: volume e latência média por janela",
            f'<div style="display:flex;align-items:flex-end;gap:8px;height:220px;'
            f'justify-content:center">{cols}</div>',
        ))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 13. Potencial de inteligência artificial
# MAGIC
# MAGIC A camada Gold já permite evoluir para três aplicações:
# MAGIC
# MAGIC 1. **Predição:** estimar a taxa futura de alfabetização por município.
# MAGIC 2. **Clusterização:** agrupar municípios por perfil de vulnerabilidade educacional.
# MAGIC 3. **Detecção de anomalias:** identificar alterações incompatíveis com o histórico.
# MAGIC
# MAGIC O uso responsável exige enriquecimento socioeconômico, validação temporal,
# MAGIC explicabilidade e cuidado para que o modelo apoie políticas públicas sem
# MAGIC automatizar decisões sensíveis.

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
# MAGIC ## 14. Como montar o dashboard no Databricks
# MAGIC
# MAGIC Adicione ao dashboard, nesta ordem:
# MAGIC
# MAGIC 1. **Capa executiva**
# MAGIC 2. **Ranking territorial**
# MAGIC 3. **Matriz de prioridade**
# MAGIC 4. **Jornada histórica e metas**
# MAGIC 5. **Municípios prioritários**
# MAGIC 6. **Pulso do streaming**
# MAGIC 7. **Regra dos 743 pontos**
# MAGIC 8. **Saúde operacional**
# MAGIC 9. **Painel de decisão**
# MAGIC
# MAGIC Layout sugerido: capa em largura total; KPIs e ranking na primeira dobra; tendência e
# MAGIC matriz na segunda; streaming, qualidade e IA na última seção.

# COMMAND ----------
print("✓ Command Center atualizado.")
print("✓ Filtros, capa, rankings, trajetória, streaming, qualidade e IA preparados.")
print("→ Use '+ Add to dashboard' nos resultados que farão parte do vídeo executivo.")
