# Execucao do pipeline no Databricks

Este documento mostra como executar o pipeline completo usando o Databricks Workflows.

## Pre-requisitos

Antes de criar o Job, confirme:

- acesso ao Databricks Free Edition;
- acesso ao repositorio GitHub;
- compute serverless disponivel;
- planilhas oficiais presentes em `data/source/`:
  - `resultados_e_metas_ufs_2024_2.xlsx`;
  - `resultados_e_metas_municipios_2024.xlsx`;
- arquivo oficial `TS_ALUNO.csv` enviado para o Volume:

```text
/Volumes/workspace/bronze/raw_files/microdados_inep/DADOS/TS_ALUNO.csv
```

O arquivo `TS_ALUNO.csv` precisa estar no Volume antes de iniciar o Job. Ele nao e gerado pelo pipeline.

## Criar o Workflow

1. Acesse **Workflows** no Databricks.
2. Crie um novo Job usando o arquivo `workflows/job_pipeline.json`.
3. Confirme a origem Git:

```text
Repositorio: https://github.com/INSgustavo/tech-challenge-fase2-alfabetizacao
Branch: main
```

4. Selecione compute serverless para a execucao.
5. Configure um e-mail valido para notificacoes de falha, substituindo o valor de exemplo do JSON.
6. Mantenha a agenda pausada para evitar execucoes automaticas na Free Edition.

## Executar o pipeline

Use **Run now** para iniciar uma execucao manual. O Workflow executa as tasks na seguinte ordem:

```text
setup
  |
  +--> bronze_batch
  |
  +--> bronze_streaming
          |
          v
        silver
          |
          v
        quality
          |
          v
        gold
        /    \
   serving  mlflow
        \    /
       monitoring
          |
          v
       dashboard
```

A ordem principal e:

```text
00_setup_ambiente
01_bronze_batch + 02_bronze_streaming
03_silver
06_quality_checks
04_gold
05_serving_mongodb + 07_ml_mlflow
08_monitoring
09_dashboard
```

O `run_id` do Job e enviado automaticamente para todas as tasks. Isso permite consultar a execucao completa em `workspace.observability.pipeline_metrics`.

## Validacoes esperadas

Depois da execucao, confirme:

- schemas `bronze`, `silver`, `gold` e `observability` criados no catalogo `workspace`;
- dados presentes nas tabelas Bronze;
- streaming processado com checkpoint e deduplicacao;
- tabelas Silver preenchidas;
- task `quality` concluida com sucesso;
- marts Gold publicados;
- metricas registradas em `workspace.observability.pipeline_metrics`;
- dashboard carregando as tabelas Gold e de observabilidade.

A task `gold` depende da task `quality`. Se o Quality Gate falhar, a Gold nao deve ser publicada.

## MongoDB e MLflow

A task `serving` publica os indicadores no MongoDB Atlas usando `upsert`. Ela depende de uma connection string configurada em um secret do Databricks.

Se o MongoDB nao estiver configurado, a etapa de serving pode ser ignorada conforme o comportamento do notebook. O restante do pipeline usa Delta Lake e continua sendo validado normalmente.

A task `mlflow` registra o baseline, o modelo Random Forest, metricas e o model card no MLflow do Databricks.

## Quando uma task falhar

1. Abra o detalhe da execucao no Workflow.
2. Consulte os logs da task que falhou.
3. Verifique se a task anterior terminou com sucesso.
4. Confira os dados de entrada e o Volume correspondente.
5. Consulte a quarentena:

```sql
SELECT *
FROM workspace.observability.quarantine_records
ORDER BY ingestion_timestamp DESC;
```

6. Corrija a origem ou a configuracao e execute novamente com **Run now**.

Nao execute a Gold manualmente quando o Quality Gate estiver reprovado.
