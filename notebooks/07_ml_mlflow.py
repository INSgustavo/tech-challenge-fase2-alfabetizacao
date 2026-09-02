# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "scikit-learn",
#   "mlflow",
# ]
# ///
# MAGIC %md
# MAGIC
# MAGIC # 07 — Aplicação em IA (P4) — MLflow
# MAGIC
# MAGIC  Modelo de regressão que prevê a **taxa de alfabetização** de um município a  partir de atributos estruturais (ano, UF, rede). Compara um **baseline**  (média) com um modelo real e registra tudo no MLflow.
# MAGIC
# MAGIC **Observação honesta:** 
# MAGIC `media_portugues` e `pct_registros_alfabetizados` são derivados da mesma medição do alvo, então **não** são usados como features (seria vazamento). Ver limitações no model card no fim do notebook.

# COMMAND ----------

# DBTITLE 1,Cell 2
# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "scikit-learn",
#   "mlflow",
# ]
# ///

# COMMAND ----------
CATALOG = "workspace"

import logging

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

# O cluster bloqueia a chamada extraContext usada pelo MLflow para
# resolver tags automáticas de contexto (Py4JSecurityException). É
# inofensivo, mas polui a saída — silenciado no nível ERROR.
logging.getLogger("mlflow.tracking.context.registry").setLevel(logging.ERROR)

# COMMAND ----------
pdf = (
    spark.table(f"{CATALOG}.gold.indicador_municipio")
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
    print("⚠ Poucos registros: treinando e avaliando no mesmo conjunto (apenas demonstração).")
    X_train, X_test, y_train, y_test = X, X, y, y

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
with mlflow.start_run(run_name="baseline_media") as run_base:
    baseline = Pipeline([("prep", preprocess), ("model", DummyRegressor(strategy="mean"))])
    baseline.fit(X_train, y_train)
    metrics_base = avaliar(baseline, X_test, y_test)

    mlflow.log_param("modelo", "DummyRegressor(mean)")
    mlflow.log_param("n_treino", len(X_train))
    mlflow.log_metrics(metrics_base)

    input_example = X_train.head(5).copy()
    input_example["ano"] = input_example["ano"].astype("float64")
    input_example["rede"] = input_example["rede"].astype("float64")
    mlflow.sklearn.log_model(
        baseline,
        "model",
        input_example=input_example,
        skops_trusted_types=["sklearn.compose._column_transformer._RemainderColsList"],
    )
    print("Baseline:", metrics_base)

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

    input_example_rf = X_train.head(5).copy()
    input_example_rf["ano"] = input_example_rf["ano"].astype("float64")
    input_example_rf["rede"] = input_example_rf["rede"].astype("float64")
    mlflow.sklearn.log_model(
        rf,
        "model",
        input_example=input_example_rf,
        skops_trusted_types=["sklearn.compose._column_transformer._RemainderColsList"],
    )
    print("RandomForest:", metrics_rf)

# COMMAND ----------
comparacao = pd.DataFrame([
    {"modelo": "baseline_media", **metrics_base},
    {"modelo": "random_forest", **metrics_rf},
])
print(comparacao.to_string(index=False))

melhor = "random_forest" if metrics_rf["mae"] <= metrics_base["mae"] else "baseline_media"
print(f"\nMelhor modelo por MAE: {melhor}")

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
- **Excluídas de propósito** (vazamento): `media_portugues`,
  `pct_registros_alfabetizados` — derivadas do próprio alvo.

## Métricas (conjunto de teste)
- Baseline (média): MAE={metrics_base['mae']:.4f} | RMSE={metrics_base['rmse']:.4f} | R2={metrics_base['r2']:.4f}
- RandomForest:      MAE={metrics_rf['mae']:.4f} | RMSE={metrics_rf['rmse']:.4f} | R2={metrics_rf['r2']:.4f}
- Melhor por MAE: {melhor}

## Limitações e riscos
- Amostra pequena e majoritariamente em grão de UF na fonte atual: o modelo é
  uma **prova de conceito**, não deve subsidiar decisões reais ainda.
- Sem enriquecimento socioeconômico (IBGE/Censo/FUNDEB), o poder preditivo é
  limitado a sinais estruturais.
- Risco de viés territorial: UFs com poucos municípios ficam sub-representadas.

## Próximos passos
- Enriquecer com IBGE/Censo Escolar e reavaliar.
- Testar modelos adicionais (GradientBoosting) e validação cruzada.
"""

card_path = "/tmp/model_card.md"
with open(card_path, "w", encoding="utf-8") as f:
    f.write(model_card)

with mlflow.start_run(run_name="model_card"):
    mlflow.log_artifact(card_path)
    mlflow.log_param("melhor_modelo", melhor)

print(model_card)
# resolver tags automáticas de contexto (Py4JSecurityException). É
# inofensivo, mas polui a saída — silenciado no nível ERROR.
logging.getLogger("mlflow.tracking.context.registry").setLevel(logging.ERROR)

# COMMAND ----------

# DBTITLE 1,Retorno para pipeline runner
# Retorna sucesso para o pipeline runner
dbutils.notebook.exit(melhor)