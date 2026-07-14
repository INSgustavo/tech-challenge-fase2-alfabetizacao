# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Aplicação em IA — MLflow
# MAGIC Modelo de regressão que estima a taxa de alfabetização de um município a
# MAGIC partir de atributos estruturais (ano, UF, rede). Compara um baseline (média
# MAGIC global), o modelo e a média de grupo, registrando as três execuções no MLflow.
# MAGIC
# MAGIC `media_portugues` e `pct_registros_alfabetizados` são derivados da mesma
# MAGIC medição do alvo e não são usados como features, para evitar vazamento. As
# MAGIC demais limitações estão no model card, ao final do notebook.

# COMMAND ----------
CATALOG = "workspace"

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Montagem do dataset (Gold para pandas)

# COMMAND ----------
# `fonte_preferencial` garante uma linha por município. A Gold mantém a medição
# oficial e a simulada lado a lado; treinar sobre as duas duplicaria o mesmo
# território no dataset, aumentando o peso dos municípios com dupla origem e
# permitindo que a mesma informação apareça no treino e no teste.
pdf = (
    spark.table(f"{CATALOG}.gold.indicador_municipio")
    .filter(F.col("fonte_preferencial"))
    .select("ano", "sigla_uf", "rede", "taxa_alfabetizacao_media")
    .toPandas()
    .dropna(subset=["taxa_alfabetizacao_media"])
)
print(f"Registros disponíveis para treino: {len(pdf):,}")

FEATURES_CAT = ["sigla_uf", "rede"]
FEATURES_NUM = ["ano"]
TARGET = "taxa_alfabetizacao_media"

X = pdf[FEATURES_CAT + FEATURES_NUM]
y = pdf[TARGET]

# Split — com fallback se houver poucos registros
if len(pdf) >= 10:
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)
else:
    print("AVISO: poucos registros. Treino e avaliação no mesmo conjunto, apenas para demonstração.")
    X_train, X_test, y_train, y_test = X, X, y, y

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Funções auxiliares

# COMMAND ----------
def avaliar(model, X_te, y_te):
    pred = model.predict(X_te)
    return {
        "mae": float(mean_absolute_error(y_te, pred)),
        "rmse": float(np.sqrt(mean_squared_error(y_te, pred))),
        "r2": float(r2_score(y_te, pred)) if len(y_te) > 1 else float("nan"),
    }

preprocess = ColumnTransformer(
    transformers=[("cat", OneHotEncoder(handle_unknown="ignore"), FEATURES_CAT)],
    remainder="passthrough",
)

mlflow.set_experiment(f"/Shared/alfabetizacao_taxa_municipio")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Baseline — DummyRegressor (prevê a média)

# COMMAND ----------
with mlflow.start_run(run_name="baseline_media") as run_base:
    baseline = Pipeline([("prep", preprocess), ("model", DummyRegressor(strategy="mean"))])
    baseline.fit(X_train, y_train)
    metrics_base = avaliar(baseline, X_test, y_test)

    mlflow.log_param("modelo", "DummyRegressor(mean)")
    mlflow.log_param("n_treino", len(X_train))
    mlflow.log_metrics(metrics_base)
    mlflow.sklearn.log_model(baseline, "model")
    print("Baseline:", metrics_base)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Modelo — RandomForestRegressor

# COMMAND ----------
with mlflow.start_run(run_name="random_forest") as run_rf:
    rf = Pipeline([
        ("prep", preprocess),
        ("model", RandomForestRegressor(n_estimators=200, max_depth=8, random_state=42)),
    ])
    rf.fit(X_train, y_train)
    metrics_rf = avaliar(rf, X_test, y_test)

    mlflow.log_param("modelo", "RandomForestRegressor")
    mlflow.log_param("n_estimators", 200)
    mlflow.log_param("max_depth", 8)
    mlflow.log_param("n_treino", len(X_train))
    mlflow.log_metrics(metrics_rf)
    mlflow.sklearn.log_model(rf, "model")
    print("RandomForest:", metrics_rf)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4b. Referência — média de grupo (ano + UF + rede)
# MAGIC Com features apenas no nível de UF, rede e ano, o limite superior de qualquer
# MAGIC modelo é prever a média do grupo, já que não há informação para distinguir
# MAGIC municípios dentro de um mesmo grupo. Esta referência mede esse limite: se o
# MAGIC modelo não a supera, não está agregando capacidade preditiva.

# COMMAND ----------
with mlflow.start_run(run_name="referencia_media_grupo"):
    medias_grupo = y_train.groupby(
        [X_train["ano"], X_train["sigla_uf"], X_train["rede"]]
    ).mean()
    media_global = y_train.mean()
    pred_grupo = np.array([
        medias_grupo.get((ano, uf, rede), media_global)
        for ano, uf, rede in zip(X_test["ano"], X_test["sigla_uf"], X_test["rede"])
    ])
    metrics_grupo = {
        "mae": float(mean_absolute_error(y_test, pred_grupo)),
        "rmse": float(np.sqrt(mean_squared_error(y_test, pred_grupo))),
        "r2": float(r2_score(y_test, pred_grupo)) if len(y_test) > 1 else float("nan"),
    }
    mlflow.log_param("modelo", "media_grupo_ano_uf_rede")
    mlflow.log_param("n_treino", len(X_train))
    mlflow.log_metrics(metrics_grupo)
    print("Média de grupo:", metrics_grupo)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Comparação entre baseline, modelo e referência

# COMMAND ----------
comparacao = pd.DataFrame([
    {"modelo": "baseline_media", **metrics_base},
    {"modelo": "random_forest", **metrics_rf},
    {"modelo": "media_grupo_ano_uf_rede", **metrics_grupo},
])
print(comparacao.to_string(index=False))

melhor = comparacao.loc[comparacao["mae"].idxmin(), "modelo"]
print(f"\nMelhor por MAE: {melhor}")
if metrics_grupo["mae"] <= metrics_rf["mae"]:
    print("A média de grupo empata ou supera o Random Forest. Com as features "
          "atuais, o modelo não agrega capacidade preditiva sobre uma agregação "
          "simples. O ganho depende de features no grão de município (ver model card).")

# Dispersão intra-grupo: parcela da variação que as features atuais não explicam.
intra = pdf.groupby(["ano", "sigla_uf", "rede"])[TARGET].std().dropna()
print(f"\nDesvio-padrão médio dentro de cada grupo (ano, UF, rede): "
      f"{intra.mean()*100:.1f} p.p. Corresponde à variação entre municípios do mesmo "
      "grupo, não observável pelo modelo atual.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Model card (limitações e riscos) — logado como artefato

# COMMAND ----------
model_card = f"""# Model Card — Previsão da taxa de alfabetização por município

## Objetivo
Estimar `taxa_alfabetizacao_media` de um município a partir de atributos
estruturais (ano, UF, rede de ensino).

## Dados
- Fonte: `workspace.gold.indicador_municipio` (derivada da Silver aprovada).
- Registros de treino: {len(X_train)} | teste: {len(X_test)}.

## Features
- Categóricas: {FEATURES_CAT} (one-hot).
- Numéricas: {FEATURES_NUM}.
- Excluídas por risco de vazamento: `media_portugues` e
  `pct_registros_alfabetizados`, ambas derivadas do próprio alvo.

## Métricas (conjunto de teste)
- Baseline (média global): MAE={metrics_base['mae']:.4f} | RMSE={metrics_base['rmse']:.4f} | R2={metrics_base['r2']:.4f}
- RandomForest:            MAE={metrics_rf['mae']:.4f} | RMSE={metrics_rf['rmse']:.4f} | R2={metrics_rf['r2']:.4f}
- Média de grupo (teto):   MAE={metrics_grupo['mae']:.4f} | RMSE={metrics_grupo['rmse']:.4f} | R2={metrics_grupo['r2']:.4f}
- Melhor por MAE: {melhor}

## Interpretação
- Com features apenas no nível de UF, rede e ano, o limite superior do modelo é a
  média de grupo: não há informação para distinguir municípios dentro do mesmo
  grupo, onde se concentra a maior parte da variação (desvio-padrão intra-grupo na
  ordem de dezenas de pontos percentuais).
- Caso a média de grupo empate ou supere o RandomForest, o modelo não agrega
  capacidade preditiva. O entregável, nesse cenário, é a infraestrutura de
  experimentação (pipeline, MLflow, comparação com referência), não um preditor
  aplicável a municípios.

## Limitações e riscos
- Sem features municipais (Censo Escolar, IBGE, FUNDEB), o modelo estima o mesmo
  valor para todos os municípios de um mesmo (ano, UF, rede).
- O split é aleatório, enquanto o uso pretendido é projetar períodos futuros. A
  validação adequada é temporal: treinar em um ano e avaliar no seguinte.
- Viés territorial: UFs com poucos municípios ficam sub-representadas.

## Próximos passos
- Enriquecer com IBGE e Censo Escolar e reavaliar contra a média de grupo.
- Substituir o split aleatório por validação temporal (2023 -> 2024).
- Avaliar modelos adicionais (GradientBoosting) após o enriquecimento. Antes disso,
  nenhum algoritmo supera o limite imposto pelas features disponíveis.
"""

card_path = "/tmp/model_card.md"
with open(card_path, "w", encoding="utf-8") as f:
    f.write(model_card)

with mlflow.start_run(run_name="model_card"):
    mlflow.log_artifact(card_path)
    mlflow.log_param("melhor_modelo", melhor)

print(model_card)
