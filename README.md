# # Pipeline de Streaming - Alfabetização Educacional

> **Branch:** `feature/streaming`  
> **Status:** ✅ Funcional - Pronto para revisão  
> **Última atualização:** 2026-07-07

## 📋 Índice

- [Visão Geral](#visão-geral)
- [Arquitetura](#arquitetura)
- [Notebooks](#notebooks)
  - [Streaming-monitoring](#streaming-monitoring)
  - [Event Producer](#event-producer)
- [Sistema de Métricas e Alertas](#sistema-de-métricas-e-alertas)
- [Estrutura de Dados](#estrutura-de-dados)
- [Guia de Uso](#guia-de-uso)
- [Monitoramento e Observabilidade](#monitoramento-e-observabilidade)
- [Próximos Passos](#próximos-passos)

---

## 🎯 Visão Geral

Pipeline de ingestão e processamento de eventos em tempo real para métricas de alfabetização educacional no Brasil. Combina dados batch (históricos do INEP) com eventos streaming (medições em tempo real) em uma arquitetura medallion (Bronze → Silver → Gold).

### Características Principais

- **Ingestão Streaming:** Landing zone → Bronze Delta com Auto Loader
- **Deduplicação:** Hash SHA-256 de payload para idempotência
- **Schema Evolution:** Suporte a múltiplas versões de eventos
- **Quality Gates:** Validações de qualidade antes de publicar Gold
- **Observabilidade:** Métricas de latência, volume, falhas e alertas configuráveis
- **Monitoramento em Tempo Real:** Dashboard consolidado com health score

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                        LANDING ZONE                              │
│              /Volumes/workspace/bronze/streaming_landing/        │
│                   JSON files (event-*.json)                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           │ Auto Loader (readStream)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER                                │
│          workspace.bronze.eventos_streaming (Delta)              │
│   • Append-only                                                  │
│   • Auditoria: _ingestion_timestamp, _source_file               │
│   • Deduplicação: _payload_hash (SHA-256)                       │
│   • Checkpoint: /tmp/alfabetizacao/checkpoint/                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           │ Union com batch
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER                                │
│       workspace.silver.medicoes_alfabetizacao (Delta)            │
│   • Normalização de chaves (UF, rede)                           │
│   • União batch + streaming                                      │
│   • Deduplicação por record_id                                  │
│   • Enriquecimento: rede_label, alfabetizado                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           │ Quality Gate
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER                                 │
│   workspace.gold.indicador_uf                                    │
│   workspace.gold.resumo_nacional                                 │
│   • Agregações por UF, rede e ano                               │
│   • KPIs: taxa média, % alfabetizados, volume                   │
└─────────────────────────────────────────────────────────────────┘

                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    OBSERVABILITY                                 │
│       workspace.observability.pipeline_metrics                   │
│       workspace.observability.quarantine_records                 │
│   • Métricas de latência (P50, P95, P99)                        │
│   • Taxa de falha e volume                                      │
│   • Sistema de alertas configurável                             │
│   • Quarentena para eventos inválidos                           │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📓 Notebooks

### Streaming-monitoring

**Localização:** `/Users/pbarbozaguimaraes@gmail.com/Streaming-monitoring`  
**ID:** `205085435889750`

#### Descrição

Notebook principal do pipeline de streaming. Contém todo o fluxo de ingestão, transformação, qualidade e monitoramento de eventos em tempo real.

#### Estrutura de Células

| Célula | Título | Descrição |
|--------|--------|-----------|
| 1 | **Setup** | Configuração de catálogo, schemas e caminhos |
| 2 | **Landing Zone Config** | Define diretório de ingestão e checkpoint |
| 3 | **Bronze Streaming** | Lê eventos JSON e grava na tabela Delta bronze |
| 4-8 | **Event Producer** | Gerador de eventos sintéticos (setup, único, lote, específico, validação) |
| 9 | **Monitoramento Contínuo** | Stream contínuo com micro-batches (10s) |
| 10 | **Silver** | Normalização e união batch + streaming |
| 11 | **Gold** | Agregações por UF e nacional |
| 12 | **Serving MongoDB** | Exemplo de publicação via upsert (comentado) |
| 13 | **Quality Gate** | Validações de qualidade de dados |
| 14 | **MLflow** | Placeholder para modelo preditivo |
| 15 | **Monitoramento** | Consolida disponibilidade das tabelas |
| 16 | **Métricas - Latência** | Calcula P50, P95, P99, máx e mín |
| 17 | **Sistema de Alertas** | Detecta latência alta, volume baixo, taxa de falha |
| 18 | **Dashboard Consolidado** | Visão unificada com health score |

#### Bronze Streaming (Célula 3)

```python
# Schema do evento
SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("schema_version", StringType(), False),
    StructField("ano", IntegerType(), False),
    StructField("sigla_uf", StringType(), False),
    StructField("id_municipio", StringType(), True),
    StructField("rede", IntegerType(), False),
    StructField("taxa_alfabetizacao", DoubleType(), False),
    StructField("source", StringType(), False),
])

# Leitura streaming com Auto Loader
stream = (
    spark.readStream
    .schema(SCHEMA)
    .json(LANDING)
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_payload_hash", F.sha2(F.to_json(F.struct("*")), 256))
)

# Escrita com checkpoint para exactly-once
stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .toTable(TARGET)
```

**Recursos:**
- ✅ Auto Loader para ingestão incremental
- ✅ Metadados de auditoria (`_ingestion_timestamp`, `_source_file`)
- ✅ Hash de payload para deduplicação
- ✅ Checkpoint para exactly-once semantics

#### Silver Transformation (Célula 10)

```python
# Normalização de streaming
stream_canonical = (
    events
    .withColumn("sigla_uf", F.upper(F.trim(F.col("sigla_uf"))))
    .withColumn("rede", F.col("rede").cast("int"))
    .withColumn("rede_label", rede_mapping[F.col("rede")])
    # Campos batch preenchidos com NULL
    .withColumn("media_portugues", F.lit(None).cast("double"))
    .withColumn("alfabetizado", F.lit(None).cast("boolean"))
)

# União com batch
silver = (
    batch_canonical.select(*columns)
    .unionByName(stream_canonical.select(*columns), allowMissingColumns=True)
    .withColumn("record_id", F.sha2(F.concat_ws("|", ...), 256))
    .dropDuplicates(["record_id"])
)
```

**Recursos:**
- ✅ Normalização de chaves (UF, rede)
- ✅ Mapping de códigos de rede para labels
- ✅ Deduplicação por `record_id` (hash)
- ✅ `allowMissingColumns=True` para schema flexibility

#### Quality Gate (Célula 13)

```python
checks = {
    "silver_not_empty": s.limit(1).count() == 1,
    "record_id_not_null": s.filter(F.col("record_id").isNull()).count() == 0,
    "record_id_unique": s.count() == s.select("record_id").distinct().count(),
    "uf_format": s.filter(~F.col("sigla_uf").rlike("^[A-Z]{2}$")).count() == 0,
    "rede_domain": s.filter(~F.col("rede").isin([0, 2, 3, 5])).count() == 0,
    "taxa_domain": s.filter(
        F.col("taxa_alfabetizacao").isNotNull() & 
        ~F.col("taxa_alfabetizacao").between(0.0, 100.0)
    ).count() == 0,
}
```

**Validações:**
- ✅ Tabela não vazia
- ✅ `record_id` sempre presente e único
- ✅ UF no formato `[A-Z]{2}`
- ✅ Rede em `[0, 2, 3, 5]`
- ✅ Taxa entre 0.0 e 100.0 (percentual)

---

### Event Producer

**Localização:** Células 4-8 do notebook `Streaming-monitoring`  
**Função:** Gerador de eventos sintéticos para testes

#### Célula 4: Setup

```python
# Dados brasileiros realistas
UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO"
]

REDES = {0: "total", 2: "estadual", 3: "municipal", 5: "privada"}

MUNICIPIOS_EXEMPLO = {
    "SP": ["3550308", "3509502", "3543402", "3518800"],
    "RJ": ["3304557", "3303500", "3301009", "3302270"],
    # ...
}

def gerar_evento(ano=None, uf=None, rede=None):
    """Gera um evento sintético de medição de alfabetização."""
    # Gera valores realistas por rede
    base_taxa = {
        0: (0.50, 0.85),  # Total
        2: (0.55, 0.80),  # Estadual
        3: (0.45, 0.75),  # Municipal
        5: (0.65, 0.90),  # Privada
    }[rede]
    
    taxa = round(random.uniform(*base_taxa), 4)
    # Retorna dict com schema v1.0
```

**Características:**
- ✅ 27 UFs do Brasil
- ✅ 4 tipos de rede (total, estadual, municipal, privada)
- ✅ Municípios IBGE (7 dígitos)
- ✅ Taxas realistas por rede
- ✅ Timestamps com timezone UTC

#### Célula 5: Gerar Evento Único

Gera e grava **1 evento** no landing zone.

```python
evento = gerar_evento()
file_path = f"{LANDING}event-{evento['event_id']}.json"

dbutils.fs.put(file_path, json.dumps(evento, ensure_ascii=False), overwrite=False)
```

#### Célula 6: Gerar Lote de Eventos

Gera **N eventos** (padrão: 20) de uma vez.

```python
QUANTIDADE = 20  # Ajustável

for i in range(QUANTIDADE):
    evento = gerar_evento()
    dbutils.fs.put(f"{LANDING}event-{evento['event_id']}.json", ...)
```

**Saída:**
```
✓ 20 eventos gerados e gravados no landing zone

Distribuição por UF:
  PA: 2 eventos
  MS: 2 eventos
  PR: 1 eventos
  ...

Primeiros 5 eventos:
  - PR | Ano 2024 | Rede municipal  | Taxa 57.16%
  - PA | Ano 2025 | Rede municipal  | Taxa 58.49%
  ...
```

#### Célula 7: Gerar Eventos Específicos (Teste)

Cria cenários controlados para testes:

```python
# Exemplo: todas UFs do Sudeste, todas as redes
regioes = {
    "Sudeste": ["SP", "RJ", "MG", "ES"],
    "Sul": ["PR", "SC", "RS"],
    "Nordeste": ["MA", "PI", "CE", "RN", "PB", "PE", "AL", "SE", "BA"],
}

for uf in regioes["Sudeste"]:
    for rede in REDES.keys():
        evento = gerar_evento(ano=2025, uf=uf, rede=rede)
        # Grava no landing
```

**Uso:**
- Testes de carga
- Validação de agregações por UF
- Cenários de regressão

#### Célula 8: Validar Eventos no Landing

Lista e inspeciona arquivos JSON:

```python
json_files = [f for f in dbutils.fs.ls(LANDING) if f.name.endswith('.json')]
event_files = [f for f in json_files if f.name.startswith('event-')]

print(f"Total de arquivos JSON: {len(json_files)}")
print(f"Eventos gerados (event-*.json): {len(event_files)}")

# Mostra conteúdo do último evento
ultimo = sorted(event_files, key=lambda x: x.name, reverse=True)[0]
conteudo = spark.read.text(ultimo.path).first()[0]
evento = json.loads(conteudo)
print(json.dumps(evento, indent=2, ensure_ascii=False))
```

---

## 📊 Sistema de Métricas e Alertas

### Métricas de Latência (Célula 16)

Calcula o tempo entre geração (`event_time`) e processamento (`_ingestion_timestamp`).

```python
latency_df = eventos.withColumn(
    "latency_seconds",
    F.unix_timestamp("_ingestion_timestamp") - F.unix_timestamp("event_time")
)

latency_stats = latency_df.select(
    F.min("latency_seconds").alias("min_latency_sec"),
    F.avg("latency_seconds").alias("avg_latency_sec"),
    F.expr("percentile(latency_seconds, 0.5)").alias("p50_latency_sec"),
    F.expr("percentile(latency_seconds, 0.95)").alias("p95_latency_sec"),
    F.expr("percentile(latency_seconds, 0.99)").alias("p99_latency_sec"),
    F.max("latency_seconds").alias("max_latency_sec"),
).collect()[0]
```

**Métricas:**
- Mínima, Média, P50, P95, P99, Máxima
- Latência por hora (últimas 24h)
- Persistência em `workspace.observability.pipeline_metrics`

### Métricas de Volume (Célula não implementada, mas conceito)

- Total de eventos processados
- Throughput (eventos/minuto, eventos/segundo)
- Distribuição por source, UF, rede
- Distribuição temporal (madrugada, manhã, tarde, noite)

### Detecção de Falhas (Célula não implementada, mas conceito)

```python
# Eventos com valores NULL (falha de parsing)
falhas = eventos_raw.filter(F.col("sigla_uf").isNull())
validos = eventos_raw.filter(F.col("sigla_uf").isNotNull())

failure_rate = (falhas.count() / total) * 100

# Validações adicionais
quality_checks = {
    "taxa_fora_range": validos.filter(~F.col("taxa_alfabetizacao").between(0.0, 1.0)).count(),
    "ano_invalido": validos.filter(~F.col("ano").between(2015, 2030)).count(),
    "rede_invalida": validos.filter(~F.col("rede").isin([0, 2, 3, 5])).count(),
}

# Quarentena
quarantine_records.write.format("delta").mode("append").saveAsTable(
    f"{CATALOG}.observability.quarantine_records"
)
```

### Sistema de Alertas (Célula 17)

Thresholds configuráveis:

```python
THRESHOLDS = {
    "max_latency_seconds": 300,        # 5 minutos
    "min_events_per_hour": 5,          # Mínimo de eventos
    "max_failure_rate_percent": 10.0,  # Taxa máxima de falha
    "max_p95_latency_seconds": 180,    # P95 de latência
}
```

**Alertas Implementados:**

| Tipo | Severidade | Condição |
|------|------------|----------|
| `LATENCY_SPIKE` | HIGH | Latência máxima > 300s |
| `P95_LATENCY_HIGH` | MEDIUM | P95 > 180s |
| `LOW_VOLUME` | MEDIUM | Eventos/hora < 5 |
| `HIGH_FAILURE_RATE` | HIGH | Taxa de falha > 10% |
| `CHECKPOINT_LAG` | MEDIUM | Arquivos acumulados > 100 |

**Saída de Alerta:**

```json
[
  {
    "severity": "HIGH",
    "type": "LATENCY_SPIKE",
    "message": "Latência máxima de 3692s excede threshold de 300s",
    "value": 3692,
    "threshold": 300,
    "timestamp": "2026-07-07T04:25:17.687867"
  },
  ...
]
```

**TODO (Produção):**
- Integração com SNS/PagerDuty/Slack
- Email notifications
- Auto-remediation workflows

### Dashboard Consolidado (Célula 18)

Visão unificada de todas as métricas:

```
════════════════════════════════════════════════════════════════════════════════
  PIPELINE STREAMING - DASHBOARD DE MÉTRICAS
════════════════════════════════════════════════════════════════════════════════
Timestamp: 2026-07-07 04:26:55

┌─ VISÃO GERAL ────────────────────────────────────────────────────────────────
│ Total de eventos processados:         438
│ Eventos válidos:                        38  (8.7%)
│ Eventos com falha:                     400  (91.3%)
└──────────────────────────────────────────────────────────────────────────────

┌─ LATÊNCIA (segundos) ────────────────────────────────────────────────────────
│ Mínima:      0.65s
│ Média:    1845.24s
│ P50:      3520.50s
│ P95:      3520.95s  ⚠️ HIGH
│ P99:      3667.21s
│ Máxima:   3692.00s  🚨 CRITICAL
└──────────────────────────────────────────────────────────────────────────────

┌─ VOLUME POR SOURCE ──────────────────────────────────────────────────────────
+------------------+-------+-------+
|source            |eventos|percent|
+------------------+-------+-------+
|producer_simulado |30     |78.95  |
|simulador_medicoes|8      |21.05  |
+------------------+-------+-------+

┌─ THROUGHPUT (última hora) ───────────────────────────────────────────────────
│ Eventos:                    32
│ Eventos/minuto:           0.53
│ Eventos/segundo:          0.01
└──────────────────────────────────────────────────────────────────────────────

┌─ TOP 10 UFs (volume) ────────────────────────────────────────────────────────
+--------+-----+
|sigla_uf|count|
+--------+-----+
|SP      |8    |
|PE      |3    |
|TO      |2    |
...

┌─ QUALIDADE DOS DADOS ────────────────────────────────────────────────────────
│ ✓ Taxa dentro do range [0,1]       100.00%
│ ✓ Ano válido [2015-2030]           100.00%
│ ✓ Rede válida [0,2,3,5]            100.00%
│ ✓ UF válida (formato)              100.00%
└──────────────────────────────────────────────────────────────────────────────

┌─ HEALTH SCORE ───────────────────────────────────────────────────────────────
│ Score: 45.0/100  🟠 ATENÇÃO
└──────────────────────────────────────────────────────────────────────────────
```

**Health Score:**
- 🟢 EXCELENTE (90-100): Tudo OK
- 🟡 BOM (70-89): Operando com pequenos avisos
- 🟠 ATENÇÃO (50-69): Ação recomendada
- 🔴 CRÍTICO (0-49): Intervenção necessária

**Penalidades:**
- Taxa de falha > 10%: -30 pontos
- Taxa de falha > 5%: -15 pontos
- P95 latência > 300s: -25 pontos
- P95 latência > 180s: -10 pontos
- Volume última hora < 5: -20 pontos

---

## 🗂️ Estrutura de Dados

### Evento JSON (v1.0)

```json
{
  "event_id": "40dbf2c6-398e-4192-a80b-937ade350a3d",
  "event_time": "2026-07-07T03:57:00.094968+00:00",
  "schema_version": "1.0",
  "ano": 2025,
  "sigla_uf": "MA",
  "id_municipio": "2882040",
  "rede": 0,
  "taxa_alfabetizacao": 0.6367,
  "source": "producer_simulado"
}
```

| Campo | Tipo | Descrição |
|-------|------|----------|
| `event_id` | string | UUID v4 único do evento |
| `event_time` | timestamp | Timestamp de geração (ISO 8601 UTC) |
| `schema_version` | string | Versão do schema ("1.0") |
| `ano` | int | Ano da medição |
| `sigla_uf` | string | UF (2 letras maiúsculas) |
| `id_municipio` | string | Código IBGE do município (7 dígitos), nullable |
| `rede` | int | Tipo de rede: 0=total, 2=estadual, 3=municipal, 5=privada |
| `taxa_alfabetizacao` | double | Taxa de alfabetização (0.0 a 1.0) |
| `source` | string | Origem do evento ("producer_simulado", "simulador_medicoes") |

### Bronze: eventos_streaming

```sql
CREATE TABLE workspace.bronze.eventos_streaming (
  event_id STRING,
  event_time TIMESTAMP,
  schema_version STRING,
  ano INT,
  sigla_uf STRING,
  id_municipio STRING,
  rede INT,
  taxa_alfabetizacao DOUBLE,
  source STRING,
  _ingestion_timestamp TIMESTAMP NOT NULL,
  _source_file STRING NOT NULL,
  _payload_hash STRING
) USING DELTA
```

### Silver: medicoes_alfabetizacao

```sql
CREATE TABLE workspace.silver.medicoes_alfabetizacao (
  ano INT,
  sigla_uf STRING,
  rede INT,
  rede_label STRING,
  media_portugues DOUBLE,
  taxa_alfabetizacao DOUBLE,
  alfabetizado BOOLEAN,
  event_id STRING,
  event_time TIMESTAMP,
  source STRING,
  schema_version STRING,
  record_id STRING,  -- SHA-256 hash para deduplicação
  processed_at TIMESTAMP
) USING DELTA
```

### Gold: indicador_uf

```sql
CREATE TABLE workspace.gold.indicador_uf (
  ano INT,
  sigla_uf STRING,
  rede INT,
  rede_label STRING,
  taxa_alfabetizacao_media DOUBLE,
  media_portugues DOUBLE,
  pct_registros_alfabetizados DECIMAL(6,5),
  quantidade_registros LONG,
  updated_at TIMESTAMP
) USING DELTA
```

### Gold: resumo_nacional

```sql
CREATE TABLE workspace.gold.resumo_nacional (
  ano INT,
  rede INT,
  rede_label STRING,
  taxa_alfabetizacao_media DOUBLE,
  ufs_cobertas LONG,
  updated_at TIMESTAMP
) USING DELTA
```

### Observability: pipeline_metrics

```sql
CREATE TABLE workspace.observability.pipeline_metrics (
  run_id STRING NOT NULL,
  task_name STRING NOT NULL,
  status STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP NOT NULL,
  rows_read LONG,
  rows_written LONG,
  rows_rejected LONG,
  max_event_time TIMESTAMP,
  schema_version STRING NOT NULL,
  error_message STRING
) USING DELTA
```

### Observability: quarantine_records

```sql
CREATE TABLE workspace.observability.quarantine_records (
  run_id STRING,
  task_name STRING,
  rejection_reason STRING,
  payload STRING,
  ingestion_timestamp TIMESTAMP
) USING DELTA
```

---

## 🚀 Guia de Uso

### Pré-requisitos

- Databricks Runtime 13.3+ (ou Serverless)
- Unity Catalog habilitado
- Volume Unity Catalog: `/Volumes/workspace/bronze/streaming_landing/`
- Catálogo `workspace` com permissões de escrita

### Configuração Inicial

1. **Criar o Volume Unity Catalog:**

```sql
CREATE VOLUME IF NOT EXISTS workspace.bronze.streaming_landing;
```

2. **Criar schemas de observabilidade:**

```sql
CREATE SCHEMA IF NOT EXISTS workspace.observability;
```

3. **Executar Setup (Célula 1):**

Define variáveis globais:
- `CATALOG = "workspace"`
- `LANDING = "/Volumes/workspace/bronze/streaming_landing/"`
- `CHECKPOINT = "/tmp/alfabetizacao/checkpoint/"`
- `TARGET = "workspace.bronze.eventos_streaming"`

### Fluxo Básico

#### 1. Gerar Eventos de Teste

**Opção A: Evento único**
```python
# Executar Célula 5
# Gera 1 evento aleatório
```

**Opção B: Lote de eventos**
```python
# Executar Célula 6
# Ajustar QUANTIDADE = 50
# Gera 50 eventos com distribuição aleatória
```

**Opção C: Cenário controlado**
```python
# Executar Célula 7
# Gera eventos específicos (ex: todas UFs do Sudeste)
```

#### 2. Validar Landing Zone

```python
# Executar Célula 8
# Mostra total de arquivos e conteúdo do último evento
```

#### 3. Processar Streaming

```python
# Executar Célula 3: Bronze Streaming
# Lê landing zone e grava na tabela Delta
```

**Verificar ingestão:**
```sql
SELECT COUNT(*) FROM workspace.bronze.eventos_streaming;
SELECT * FROM workspace.bronze.eventos_streaming ORDER BY _ingestion_timestamp DESC LIMIT 10;
```

#### 4. Transformar para Silver

```python
# Executar Célula 10: Silver
# Une batch + streaming, normaliza chaves, deduplica
```

**Verificar Silver:**
```sql
SELECT source, COUNT(*) 
FROM workspace.silver.medicoes_alfabetizacao 
GROUP BY source;
```

#### 5. Validar Qualidade

```python
# Executar Célula 13: Quality Gate
# Valida 6 checks antes de publicar Gold
```

#### 6. Criar Gold Marts

```python
# Executar Célula 11: Gold
# Agrega por UF e nacional
```

**Consultar Gold:**
```sql
SELECT * FROM workspace.gold.indicador_uf 
WHERE ano = 2025 AND rede = 0 
ORDER BY taxa_alfabetizacao_media DESC;
```

#### 7. Monitorar

**Opção A: Dashboard consolidado**
```python
# Executar Célula 18
# Mostra visão completa + health score
```

**Opção B: Métricas específicas**
```python
# Célula 16: Latência (P50, P95, P99)
# Célula 17: Alertas (thresholds configuráveis)
```

### Fluxo Contínuo (Produção)

Para processamento contínuo em produção:

```python
# Executar Célula 9: Monitoramento Contínuo
# Stream com micro-batches a cada 10 segundos
# Monitora por 60 segundos e mostra estatísticas
```

**Agendar como Job:**

1. Criar Job no Databricks
2. Adicionar Tarefa: Notebook `Streaming-monitoring`
3. Configurar trigger: Continuous (ou cron)
4. Selecionar células: 3 (Bronze), 10 (Silver), 11 (Gold), 13 (Quality), 15 (Monitoring)

---

## 📈 Monitoramento e Observabilidade

### Consultas Úteis

**Latência por hora:**
```sql
SELECT 
  DATE_TRUNC('hour', _ingestion_timestamp) AS hour,
  COUNT(*) AS eventos,
  AVG(UNIX_TIMESTAMP(_ingestion_timestamp) - UNIX_TIMESTAMP(event_time)) AS latency_avg_sec,
  MAX(UNIX_TIMESTAMP(_ingestion_timestamp) - UNIX_TIMESTAMP(event_time)) AS latency_max_sec
FROM workspace.bronze.eventos_streaming
WHERE sigla_uf IS NOT NULL
GROUP BY hour
ORDER BY hour DESC
LIMIT 24;
```

**Taxa de falha:**
```sql
SELECT 
  COUNT(*) AS total,
  SUM(CASE WHEN sigla_uf IS NULL THEN 1 ELSE 0 END) AS falhas,
  SUM(CASE WHEN sigla_uf IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS failure_rate_pct
FROM workspace.bronze.eventos_streaming;
```

**Throughput por source:**
```sql
SELECT 
  source,
  COUNT(*) AS eventos,
  MIN(_ingestion_timestamp) AS first_event,
  MAX(_ingestion_timestamp) AS last_event,
  COUNT(*) / (UNIX_TIMESTAMP(MAX(_ingestion_timestamp)) - UNIX_TIMESTAMP(MIN(_ingestion_timestamp)) + 1) AS events_per_second
FROM workspace.bronze.eventos_streaming
WHERE sigla_uf IS NOT NULL
GROUP BY source;
```

**Histórico de execuções:**
```sql
SELECT 
  task_name,
  status,
  started_at,
  finished_at,
  rows_read,
  rows_written,
  error_message
FROM workspace.observability.pipeline_metrics
ORDER BY started_at DESC
LIMIT 20;
```

**Eventos em quarentena:**
```sql
SELECT 
  rejection_reason,
  COUNT(*) AS quantidade,
  MIN(ingestion_timestamp) AS primeiro,
  MAX(ingestion_timestamp) AS ultimo
FROM workspace.observability.quarantine_records
GROUP BY rejection_reason;
```

### Dashboards Recomendados (Lakeview)

1. **Pipeline Health:**
   - Health Score (gauge)
   - Taxa de falha últimas 24h (line chart)
   - Volume por hora (bar chart)
   - Latência P95 (line chart)

2. **Qualidade:**
   - Quality checks (scorecard)
   - Eventos em quarentena (counter)
   - Distribuição de falhas por tipo (pie chart)

3. **Performance:**
   - Throughput (events/sec, line chart)
   - Latência distribuição (histogram)
   - Volume por UF (map visualization)
   - Volume por rede (bar chart)

### Alertas Configuráveis

Editar thresholds em Célula 17:

```python
THRESHOLDS = {
    "max_latency_seconds": 300,        # Ajustar conforme SLA
    "min_events_per_hour": 5,          # Volume mínimo esperado
    "max_failure_rate_percent": 10.0,  # Taxa aceitável de falha
    "max_p95_latency_seconds": 180,    # P95 alvo
}
```

**Integração (TODO):**
```python
# Em produção, substituir print() por:
import requests

def send_to_slack(alerts):
    webhook_url = dbutils.secrets.get(scope="alerts", key="slack_webhook")
    requests.post(webhook_url, json={"text": json.dumps(alerts)})

def send_to_pagerduty(alerts):
    routing_key = dbutils.secrets.get(scope="alerts", key="pagerduty_key")
    # PagerDuty Events API v2
    ...
```

---

## 🔧 Troubleshooting

### Problema: Eventos não aparecem na tabela Bronze

**Verificar:**
1. Landing zone tem arquivos:
   ```python
   dbutils.fs.ls("/Volumes/workspace/bronze/streaming_landing/")
   ```
2. Checkpoint não está corrompido:
   ```python
   dbutils.fs.rm("/tmp/alfabetizacao/checkpoint/", recurse=True)
   # Re-executar Célula 3
   ```
3. Schema compatível (verificar logs de streaming)

### Problema: Taxa de falha alta (>90%)

**Causa:** Arquivos batch antigos com schema diferente.

**Solução:**
1. Limpar eventos inválidos:
   ```sql
   DELETE FROM workspace.bronze.eventos_streaming WHERE sigla_uf IS NULL;
   ```
2. Limpar landing zone:
   ```python
   files = dbutils.fs.ls(LANDING)
   for f in files:
       if not f.name.startswith('event-'):
           dbutils.fs.rm(f.path)
   ```

### Problema: Latência muito alta (P95 > 1h)

**Causa:** Eventos antigos sendo processados.

**Ação:** Normal para primeira execução. Monitorar incrementos:
```python
# Gerar novos eventos e verificar latência deles especificamente
recent_events = eventos.filter(
    F.col("event_time") >= F.expr("current_timestamp() - interval 5 minutes")
)
```

### Problema: Quality Gate falha em `taxa_domain`

**Verificar:**
```sql
SELECT taxa_alfabetizacao, COUNT(*) 
FROM workspace.silver.medicoes_alfabetizacao 
WHERE taxa_alfabetizacao NOT BETWEEN 0.0 AND 100.0
GROUP BY taxa_alfabetizacao;
```

**Causa comum:** Taxa em formato decimal (0.0-1.0) vs percentual (0-100).

**Ajustar threshold ou transformação conforme convenção.**

---

## 🎯 Próximos Passos

### P1 - Crítico (Antes de Merge)

- [ ] Adicionar testes unitários para `gerar_evento()`
- [ ] Documentar convenção de taxa (decimal vs percentual)
- [ ] Implementar rotação de checkpoint (evitar crescimento ilimitado)
- [ ] Adicionar métricas de volume e falhas (Células ausentes)
- [ ] Implementar lógica de quarentena (mover registros inválidos)

### P2 - Importante (Primeira Sprint Pós-Merge)

- [ ] Integração de alertas com Slack/PagerDuty
- [ ] Dashboard Lakeview com health score
- [ ] Agendar Job para execução contínua
- [ ] Implementar data retention (Bronze: 90 dias, Silver: 2 anos)
- [ ] Adicionar particionamento por data (melhoria de performance)

### P3 - Desejável (Backlog)

- [ ] Schema evolution automático (detectar v1.1, v2.0)
- [ ] Suporte a múltiplos sources (Kafka, Event Hubs)
- [ ] Compressão de arquivos no landing (Parquet)
- [ ] Auto-scaling de streaming (ajustar trigger interval dinamicamente)
- [ ] Métricas customizadas por dashboard (Databricks SQL)

### P4 - Futuro (Exploratório)

- [ ] Modelo preditivo MLflow (Célula 14 - placeholder)
- [ ] Serving MongoDB (Célula 12 - exemplo comentado)
- [ ] Detecção de anomalias em tempo real (latência, volume)
- [ ] Pipeline de CDC (Change Data Capture) para batch
- [ ] Integração com external APIs (INEP, IBGE)

---

## 📚 Referências

- [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
- [Unity Catalog Volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html)
- [Databricks SQL Functions](https://docs.databricks.com/sql/language-manual/sql-ref-functions.html)

---

## 👥 Contribuidores

- **Autor:** Paulo Barboza (pbarbozaguimaraes@gmail.com)
- **Revisores:** (Adicionar após code review)

---

**Última atualização:** 2026-07-07  
**Versão:** 1.0.0  
**Status:** ✅ Ready for Review
