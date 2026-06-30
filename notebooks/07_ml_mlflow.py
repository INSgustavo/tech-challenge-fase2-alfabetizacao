# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Aplicação em IA (P4) — MLflow
# MAGIC Regressão prevendo taxa por município OU clustering de vulnerabilidade.

# COMMAND ----------
CATALOG = "workspace"

import mlflow

# TODO (P4): montar features da Gold + enriquecimento; treinar; logar no MLflow
df = spark.table(f"{CATALOG}.gold.indicador_municipio")

with mlflow.start_run(run_name="baseline_alfabetizacao"):
    mlflow.log_param("modelo", "TODO")
    mlflow.log_param("catalog", CATALOG)
    mlflow.log_param("n_registros", df.count())
    print("Run iniciado no MLflow")
