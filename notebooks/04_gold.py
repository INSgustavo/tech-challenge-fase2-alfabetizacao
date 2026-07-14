# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold
# MAGIC Cria os marts analíticos após aprovação do Quality Gate (notebook 06).
# MAGIC
# MAGIC **A Gold lê exclusivamente da Silver aprovada** (arquitetura Medalhão:
# MAGIC nenhuma leitura direta da Bronze). Metas e dimensões já chegam integradas
# MAGIC pela Silver (notebook 03). Todos os marts carregam `fonte_dados` para
# MAGIC distinguir dado oficial do INEP de eventos do simulador.
# MAGIC
# MAGIC Marts publicados:
# MAGIC 1. `gold.indicador_municipio` — grão: `ano + id_municipio + rede`
# MAGIC 2. `gold.resumo_uf` — grão: `ano + sigla_uf + rede`
# MAGIC 3. `gold.meta_vs_resultado` — grão: `ano + território + rede` (UF e município)
# MAGIC 4. `gold.evolucao_temporal` — grão: `ano + território + rede` (variação anual)

# COMMAND ----------
CATALOG = "workspace"

# A Gold lê da Silver APROVADA pelo Quality Gate (06). Se a tabela aprovada
# ainda não existir (execução isolada), cai para a Silver bruta como fallback.
APROVADA = f"{CATALOG}.silver.medicoes_aprovadas"
SOURCE = APROVADA if spark.catalog.tableExists(APROVADA) else f"{CATALOG}.silver.medicoes_alfabetizacao"
print(f"Fonte da Gold: {SOURCE}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 1 — indicador por município (grão: ano + id_municipio + rede)
# MAGIC `fonte_dados` indica se a linha vem do INEP (oficial) ou do simulador.
# MAGIC
# MAGIC Um município pode ter as duas linhas no mesmo ano. Ambas são preservadas,
# MAGIC e `fonte_preferencial` elege a oficial. **O serving (notebook 05) depende
# MAGIC disso**: o upsert no MongoDB usa a chave `ano + id_municipio + rede`, que é
# MAGIC mais estreita que o grão desta tabela — sem o filtro, as duas linhas colidem
# MAGIC na mesma chave e uma sobrescreve a outra de forma não determinística.

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.indicador_municipio
COMMENT 'Indicador de alfabetização por município. Grão: ano + id_municipio + rede + fonte. Use fonte_preferencial=true para uma linha por município. Responsável: P4.'
AS
WITH base AS (
    SELECT
        ano,
        sigla_uf,
        nome_uf,
        regiao,
        id_municipio,
        nome_municipio,
        rede,
        rede_label,
        fonte_dados,
        AVG(taxa_alfabetizacao) AS taxa_alfabetizacao_media,
        AVG(media_portugues) AS media_portugues,
        AVG(CASE WHEN alfabetizado THEN 1.0 WHEN alfabetizado = false THEN 0.0 END) AS pct_registros_alfabetizados,
        COUNT(*) AS quantidade_registros,
        MAX(processed_at) AS updated_at
    FROM {SOURCE}
    WHERE grao = 'municipio' AND id_municipio IS NOT NULL
    GROUP BY ano, sigla_uf, nome_uf, regiao, id_municipio, nome_municipio,
             rede, rede_label, fonte_dados
),
priorizada AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ano, id_municipio, rede
            ORDER BY CASE WHEN fonte_dados = 'oficial_inep' THEN 0 ELSE 1 END,
                     fonte_dados
        ) AS posicao_fonte
    FROM base
)
SELECT
    * EXCEPT (posicao_fonte),
    posicao_fonte = 1 AS fonte_preferencial
FROM priorizada
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 2 — resumo por UF (grão: ano + sigla_uf + rede)
# MAGIC Construído **exclusivamente sobre o dado OFICIAL do INEP** (grão UF),
# MAGIC enriquecido com o agregado de alunos integrado na Silver.
# MAGIC
# MAGIC O recorte `grao = 'uf'` é essencial: sem ele, os eventos simulados de grão
# MAGIC município entram no resumo estadual e a taxa deixa de ser comparável à meta
# MAGIC oficial. `municipios_cobertos` é uma métrica de cobertura e por isso vem de
# MAGIC fora do agregado — as linhas de grão UF não têm `id_municipio`.

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.resumo_uf
COMMENT 'Resumo do indicador por UF (dado oficial INEP, grão uf). Grão: ano + sigla_uf + rede. Responsável: P4.'
AS
WITH oficial AS (
    SELECT
        ano,
        sigla_uf,
        nome_uf,
        regiao,
        rede,
        rede_label,
        fonte_dados,
        AVG(taxa_alfabetizacao) AS taxa_alfabetizacao_media,
        AVG(media_portugues) AS media_portugues,
        AVG(alunos_proficiencia_media) AS alunos_proficiencia_media,
        AVG(alunos_pct_alfabetizados) AS alunos_pct_alfabetizados,
        MAX(processed_at) AS updated_at
    FROM {SOURCE}
    WHERE grao = 'uf' AND fonte_dados = 'oficial_inep'
    GROUP BY ano, sigla_uf, nome_uf, regiao, rede, rede_label, fonte_dados
),
-- Quantos municípios da UF têm medição no período, independentemente da origem.
-- É cobertura do pipeline, não insumo do indicador: não entra na taxa.
cobertura AS (
    SELECT
        ano,
        sigla_uf,
        rede,
        COUNT(DISTINCT id_municipio) AS municipios_cobertos
    FROM {SOURCE}
    WHERE grao = 'municipio' AND id_municipio IS NOT NULL
    GROUP BY ano, sigla_uf, rede
)
SELECT
    o.*,
    COALESCE(c.municipios_cobertos, 0) AS municipios_cobertos
FROM oficial o
LEFT JOIN cobertura c
       ON o.ano = c.ano AND o.sigla_uf = c.sigla_uf AND o.rede = c.rede
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 3 — meta versus resultado (grão: ano + território + rede)
# MAGIC As metas já foram integradas na Silver (notebook 03) via join com
# MAGIC `bronze.meta_uf` / `bronze.meta_municipio` — a Gold **não lê a Bronze**.
# MAGIC O mart cobre os dois grãos: UF (dado oficial) e município.
# MAGIC
# MAGIC Um mesmo território pode ter **duas linhas** no mesmo ano: a oficial do INEP
# MAGIC e a do simulador de eventos. As duas são preservadas (rastreabilidade), mas
# MAGIC `fonte_preferencial` marca qual delas o consumidor deve usar — o oficial
# MAGIC ganha do simulado. Sem essa marca, quem tirasse média das linhas misturaria
# MAGIC dado real com dado simulado, que é o vício que o Mart 2 tinha.

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.meta_vs_resultado
COMMENT 'Resultado observado x meta, por UF (oficial) e por município. Grão: ano + território + rede + fonte. Use fonte_preferencial=true para uma linha por território. Responsável: P4.'
AS
WITH base AS (
    SELECT
        ano,
        grao,
        sigla_uf,
        nome_uf,
        regiao,
        id_municipio,
        nome_municipio,
        rede,
        rede_label,
        fonte_dados,
        AVG(taxa_alfabetizacao) AS taxa_alfabetizacao_media,
        AVG(meta_taxa) AS meta_taxa,
        AVG(meta_brasil) AS meta_brasil,
        MAX(processed_at) AS updated_at
    FROM {SOURCE}
    GROUP BY ano, grao, sigla_uf, nome_uf, regiao, id_municipio, nome_municipio,
             rede, rede_label, fonte_dados
),
priorizada AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ano, grao, COALESCE(id_municipio, sigla_uf), rede
            ORDER BY CASE WHEN fonte_dados = 'oficial_inep' THEN 0 ELSE 1 END,
                     fonte_dados
        ) AS posicao_fonte
    FROM base
)
SELECT
    * EXCEPT (posicao_fonte),
    posicao_fonte = 1 AS fonte_preferencial,
    ROUND(taxa_alfabetizacao_media - meta_taxa, 4) AS gap_meta,
    CASE WHEN meta_taxa IS NULL THEN NULL
         ELSE taxa_alfabetizacao_media >= meta_taxa END AS atingiu_meta,
    CASE WHEN meta_brasil IS NULL THEN NULL
         ELSE taxa_alfabetizacao_media >= meta_brasil END AS atingiu_meta_brasil
FROM priorizada
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 4 — evolução temporal (grão: ano + território + rede)
# MAGIC Variação da taxa ano a ano. Cobre o grão UF (série histórica oficial do
# MAGIC INEP) e o grão município (eventos + dado municipal oficial quando houver).

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

base = (
    spark.table(SOURCE)
    .groupBy("ano", "grao", "sigla_uf", "nome_uf", "regiao",
             "id_municipio", "nome_municipio", "rede", "rede_label", "fonte_dados")
    .agg(F.avg("taxa_alfabetizacao").alias("taxa_alfabetizacao_media"),
         F.max("processed_at").alias("updated_at"))
)

# chave territorial: município quando existir, senão a UF (grão oficial)
w = (Window
     .partitionBy("grao", F.coalesce("id_municipio", "sigla_uf"), "rede")
     .orderBy("ano"))

evolucao_temporal = (
    base
    .withColumn("taxa_ano_anterior", F.lag("taxa_alfabetizacao_media").over(w))
    .withColumn("ano_anterior", F.lag("ano").over(w))
    .withColumn("variacao_absoluta",
                F.round(F.col("taxa_alfabetizacao_media") - F.col("taxa_ano_anterior"), 4))
    .withColumn("variacao_relativa",
                F.when(F.col("taxa_ano_anterior") > 0,
                       F.round((F.col("taxa_alfabetizacao_media") - F.col("taxa_ano_anterior"))
                               / F.col("taxa_ano_anterior"), 4)))
    .withColumn("tendencia",
                F.when(F.col("variacao_absoluta") > 0, F.lit("alta"))
                 .when(F.col("variacao_absoluta") < 0, F.lit("queda"))
                 .when(F.col("variacao_absoluta") == 0, F.lit("estavel")))
)

(evolucao_temporal.write.format("delta")
    .mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.evolucao_temporal"))

spark.sql(f"COMMENT ON TABLE {CATALOG}.gold.evolucao_temporal IS "
          f"'Variação da taxa ano a ano por UF e município. Grão: ano + território + rede. Responsável: P4.'")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Mart 5 — distribuição de proficiência (grão: ano + UF + rede + faixa)
# MAGIC Agrega `silver.alunos_proficiencia` (grão de aluno) nas faixas usadas pelo
# MAGIC painel. Existe para que o dashboard **não precise ler a Bronze** para montar
# MAGIC a curva em torno do corte de 743.
# MAGIC
# MAGIC Duas faixas convivem de propósito: `faixa_pontos` (blocos de 25 pontos, para
# MAGIC o histograma) e `faixa_label` (bandas de leitura executiva). A segunda **não**
# MAGIC é derivável da primeira — o corte de 743 cai no meio do bloco 725–749 —, por
# MAGIC isso as duas entram no grão.
# MAGIC
# MAGIC Dado SIMULADO: `fonte_dados` acompanha o registro até o painel.

# COMMAND ----------
# Mesma regra do mart principal: a Gold consome a Silver APROVADA pelo Gate (06).
# Sem a aprovada (execução isolada), cai para a Silver bruta.
ALUNOS_APROVADOS = f"{CATALOG}.silver.alunos_aprovados"
ALUNOS_BRUTA = f"{CATALOG}.silver.alunos_proficiencia"
ALUNOS_SILVER = (ALUNOS_APROVADOS if spark.catalog.tableExists(ALUNOS_APROVADOS)
                 else ALUNOS_BRUTA)
print(f"Fonte do mart de distribuição: {ALUNOS_SILVER}")

if spark.catalog.tableExists(ALUNOS_SILVER):
    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.gold.distribuicao_proficiencia
    COMMENT 'Distribuição de proficiência dos alunos por faixa (dados SIMULADOS). Grão: ano + sigla_uf + rede + faixa. Responsável: P4.'
    AS
    SELECT
        ano,
        sigla_uf,
        rede,
        rede_label,
        CAST(FLOOR(proficiencia_portugues / 25) * 25 AS INT) AS faixa_pontos,
        CASE
            WHEN proficiencia_portugues < 650 THEN '1 · Abaixo de 650'
            WHEN proficiencia_portugues < 700 THEN '2 · 650 a 699'
            WHEN proficiencia_portugues < 743 THEN '3 · 700 a 742'
            WHEN proficiencia_portugues < 800 THEN '4 · 743 a 799'
            ELSE '5 · 800 ou mais'
        END AS faixa_label,
        fonte_dados,
        COUNT(*) AS alunos,
        SUM(CASE WHEN alfabetizado THEN 1 ELSE 0 END) AS alunos_alfabetizados,
        AVG(proficiencia_portugues) AS proficiencia_media,
        MAX(processed_at) AS updated_at
    FROM {ALUNOS_SILVER}
    WHERE proficiencia_portugues IS NOT NULL
    GROUP BY ano, sigla_uf, rede, rede_label, fonte_dados,
             CAST(FLOOR(proficiencia_portugues / 25) * 25 AS INT),
             CASE
                 WHEN proficiencia_portugues < 650 THEN '1 · Abaixo de 650'
                 WHEN proficiencia_portugues < 700 THEN '2 · 650 a 699'
                 WHEN proficiencia_portugues < 743 THEN '3 · 700 a 742'
                 WHEN proficiencia_portugues < 800 THEN '4 · 743 a 799'
                 ELSE '5 · 800 ou mais'
             END
    """)
    print("✓ gold.distribuicao_proficiencia publicada")
else:
    print(f"⚠ {ALUNOS_SILVER} não existe — mart de distribuição não publicado "
          "(rode o notebook 03 com bronze.alunos disponível).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validação dos marts

# COMMAND ----------
for mart in ["indicador_municipio", "resumo_uf", "meta_vs_resultado", "evolucao_temporal",
             "distribuicao_proficiencia"]:
    if not spark.catalog.tableExists(f"{CATALOG}.gold.{mart}"):
        print(f"⚠ gold.{mart} não publicada")
        continue
    df = spark.table(f"{CATALOG}.gold.{mart}")
    n = df.count()
    if "fonte_dados" in df.columns:
        oficiais = df.filter(F.col("fonte_dados") == "oficial_inep").count()
        print(f"✓ gold.{mart}: {n:,} linhas ({oficiais:,} de fonte oficial INEP)")
    else:
        print(f"✓ gold.{mart}: {n:,} linhas")

print("Gold publicada com 4 marts, grão documentado e origem do dado identificada.")
