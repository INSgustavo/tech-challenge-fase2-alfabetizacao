# Evidências de execução - Databricks Free Edition

32 prints da execução real do pipeline, nomeados na ordem do fluxo.

| Prints | Etapa | O que comprovam |
|---|---|---|
| 01-03 | Setup do ambiente e upload dos dados | Schemas/volumes criados no Unity Catalog e CSVs carregados em `bronze.raw_files` |
| 04-31 | Execução do pipeline notebook a notebook | Bronze batch (reconciliação origem×destino), streaming com dedup e quarentena, Silver integrada, Quality Gate aprovado, marts Gold populados, serving no MongoDB, experimento no MLflow e métricas de monitoramento |
| 32 | Workflow ponta a ponta | Grafo do job `pipeline-alfabetizacao` com todas as tasks verdes, na ordem Bronze → Silver → Quality → Gold → Serving/MLflow → Monitoring |
