# Pipeline Híbrida — Indicador Criança Alfabetizada

Pipeline de dados **híbrida (batch + streaming)** sobre o Indicador Criança Alfabetizada
(INEP / Base dos Dados), em **Arquitetura Medalhão / Lakehouse** (Bronze · Silver · Gold),
construída no **Databricks Free Edition** com **PySpark + Delta Lake**.

> Tech Challenge — Fase 2 (FIAP Pós Tech) · Entrega: **14/07**

![Arquitetura](docs/architecture.png)

## Stack (alinhada às disciplinas da Fase 2)
- **Plataforma:** Databricks Free Edition (DBFS)
- **Processamento:** PySpark + Spark SQL
- **Armazenamento / Medalhão:** Delta Lake (Bronze/Silver/Gold)
- **Streaming:** Spark Structured Streaming (simula a integração via Kafka)
- **Serving NoSQL:** MongoDB (um documento por município)
- **IA:** MLflow
- **Orquestração:** Databricks Workflows (Jobs) + Repos (Git)

## Camadas
| Camada | Conteúdo | Notebook |
|--------|----------|----------|
| Bronze | dados brutos (batch + streaming), Delta, sem transformação | `01_bronze_batch`, `02_bronze_streaming` |
| Silver | limpo, tipado, integrado, flag alfabetizado (≥743) | `03_silver` |
| Gold | marts: indicador/município, meta vs. resultado, evolução | `04_gold` |
| Serving | Gold → MongoDB (documentos) | `05_serving_mongodb` |

## Como rodar (Databricks)
1. Criar conta no Databricks Free Edition e importar este repo via **Repos**.
2. Subir o CSV de amostra para o DBFS (`/FileStore/alfabetizacao/`).
3. Executar os notebooks na ordem `00 → 08`, ou rodar o **Workflow** `workflows/job_pipeline.json`.

## Seções do README (a preencher)
<!-- TODO: contexto · arquitetura · fluxo · tecnologias+justificativa · trade-offs · monitoramento · FinOps · IA -->
