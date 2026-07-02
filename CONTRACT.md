# CONTRACT.md — Contrato comum do pipeline

Este documento define as regras compartilhadas entre ingestão, transformação, qualidade e consumo. Mudanças de schema ou regra de negócio precisam ser revisadas pelo time.

## 1. Namespaces no Unity Catalog

- **Catálogo:** `workspace`
- **Schemas:**
  - `workspace.bronze`
  - `workspace.silver`
  - `workspace.gold`
  - `workspace.observability`
- **Volumes:**
  - `/Volumes/workspace/bronze/raw_files/`
  - `/Volumes/workspace/bronze/streaming_landing/`
  - `/Volumes/workspace/observability/checkpoints/`
  - `/Volumes/workspace/observability/quarantine/`

## 2. Convenções

- tabelas e colunas em `snake_case`, sem acento;
- identificadores como `string` para preservar zeros à esquerda;
- timestamps em UTC;
- percentuais normalizados entre `0` e `1` na Silver — a fonte batch entrega
  `taxa_alfabetizacao` em percentual (0–100) e é **dividida por 100** na Silver;
  o evento de streaming já chega em fração (0–1);
- nenhuma credencial ou URI com segredo no código;
- toda tabela gerada deve possuir comentário e responsável identificável no PR.

## 3. Chaves

- `sigla_uf`: duas letras maiúsculas;
- `id_municipio`: código IBGE com sete dígitos, armazenado como `string`; **nulo quando a
  fonte tem grão UF** (caso da avaliação SAEB agregada) — a coluna `grao` (`uf` | `municipio`)
  identifica o nível territorial;
- `serie`: série avaliada (a fonte atual cobre apenas o 2º ano);
- `rede`: código original da fonte;
- `record_id`: hash determinístico da chave de negócio e da origem;
- chave analítica mínima: `ano + id_municipio + rede`;
- eventos: `event_id` obrigatório e único.

## 4. Domínios

### Rede

| código | rótulo |
|---:|---|
| 0 | total |
| 2 | estadual |
| 3 | municipal |
| 5 | privada |

> O de-para deve ser validado contra o dicionário da fonte utilizada. Caso a fonte divirja, este contrato deve ser alterado antes da transformação.

### Regra do corte de proficiência (v1.1)

O corte oficial de **743 pontos** na escala Saeb é definido **por aluno**. Na fonte agregada,
`taxa_alfabetizacao` **já é** o Indicador Criança Alfabetizada (percentual de alunos ≥ 743).
Por isso:

- o indicador oficial do projeto é `taxa_alfabetizacao` (normalizada 0–1 na Silver);
- `media_atinge_corte = media_portugues >= 743` é apenas um **sinal auxiliar** sobre a média
  agregada — não deve ser lido como "a UF/rede é alfabetizada";
- a Silver mantém `media_portugues` e `alfabetizacao_rule_version = "1.1"`.

## 5. Contrato do streaming

| campo | tipo | obrigatório | regra |
|---|---|---:|---|
| `event_id` | string UUID | sim | único |
| `event_time` | timestamp UTC | sim | não pode estar excessivamente no futuro |
| `schema_version` | string | sim | versão suportada pelo consumer |
| `ano` | int | sim | intervalo documentado |
| `sigla_uf` | string | sim | domínio de UFs |
| `id_municipio` | string | sim | sete dígitos |
| `rede` | int | sim | domínio aceito |
| `taxa_alfabetizacao` | double | sim | entre 0 e 1 |
| `source` | string | sim | produtor identificado |

Eventos inválidos são enviados para quarentena com `rejection_reason`, `ingestion_timestamp` e payload original.

## 6. Metadados técnicos

Toda tabela Bronze deve conter, quando aplicável:

- `_ingestion_timestamp`;
- `_source_file`;
- `_source_system`;
- `_pipeline_run_id`;
- `_schema_version`;
- `_payload_hash`.

A Silver deve conter `record_id`, `source`, `event_time` e `processed_at`.

## 7. Idempotência

- batch: `record_id` determinístico e gravação por `MERGE` ou sobrescrita controlada da partição;
- streaming: deduplicação por `event_id` e checkpoint fora da landing zone;
- serving: `upsert` por chave do documento;
- reexecução do mesmo `run_id` não deve duplicar registros.

## 8. Quality Gate

Critérios bloqueantes antes da Gold:

- campos críticos não nulos;
- chave de negócio válida;
- ausência de `record_id` duplicado;
- taxa no domínio esperado;
- integridade referencial mínima;
- cobertura não inferior ao limite definido pelo grupo;
- tabela Silver com volume maior que zero.

Falhas de registro vão para quarentena. Falhas sistêmicas ou de cobertura reprovam a task.

## 9. Auditoria

Tabela: `workspace.observability.pipeline_metrics`

Campos mínimos:

- `run_id`;
- `task_name`;
- `status`;
- `started_at`;
- `finished_at`;
- `rows_read`;
- `rows_written`;
- `rows_rejected`;
- `max_event_time`;
- `schema_version`;
- `error_message`.

## 10. Tabelas por notebook

| Notebook | Lê de | Escreve em |
|---|---|---|
| 00_setup | configuração | schemas, volumes, auditoria |
| 01_bronze_batch | raw files | `bronze.*` |
| 02_bronze_streaming | streaming landing | `bronze.eventos_streaming` |
| 03_silver | Bronze batch e streaming | `silver.medicoes_alfabetizacao` |
| 06_quality | Silver | métricas e quarentena |
| 04_gold | Silver aprovada | `gold.*` |
| 05_serving | Gold | MongoDB |
| 07_ml | Gold | MLflow |
| 08_monitoring | logs e tabelas | `observability.pipeline_metrics` |

## 11. Definition of Done

### Bronze

- dado original preservado;
- metadados técnicos presentes;
- contagem reconciliada com a fonte;
- execução repetida sem duplicidade indevida.

### Silver

- schema explícito;
- chaves normalizadas;
- batch e streaming integrados no modelo canônico;
- inválidos segregados;
- regras de negócio versionadas.

### Gold

- grão documentado;
- métricas validadas;
- consultas de exemplo no README ou runbook;
- Quality Gate aprovado.

### Serving e ML

- segredo fora do código;
- escrita idempotente;
- experimento reproduzível;
- limitações documentadas.
