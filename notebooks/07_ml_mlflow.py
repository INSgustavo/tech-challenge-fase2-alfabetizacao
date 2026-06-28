# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Aplicação em IA (P4) — MLflow
# MAGIC Regressão prevendo taxa por município OU clustering de vulnerabilidade.

# COMMAND ----------
import mlflow
# TODO (P4): montar features da Gold + enriquecimento; treinar; logar no MLflow
with mlflow.start_run(run_name="baseline"):
    mlflow.log_param("modelo", "TODO")
