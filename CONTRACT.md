# CONTRACT.md - Contrato comum do pipeline

Este documento define as regras compartilhadas entre ingestão, transformação, qualidade e consumo do pipeline de alfabetização. Mudanças de schema, granularidade, domínio ou regra de negócio devem ser revisadas pelo time antes de chegar às camadas consumidoras.

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

- tabelas e colunas analíticas em `snake_case`, sem acento;
- identificadores territoriais armazenados como `string` quando necessário para preservar zeros à esquerda;
- timestamps operacionais em UTC;
- a Bronze preserva a unidade recebida da fonte;
- a Silver normaliza `taxa_alfabetizacao` para fração entre `0` e `1`;
- os indicadores oficiais batch e o replay oficial do streaming chegam em percentual entre `0` e `100` e são divididos por `100` na Silver;
- nenhuma credencial, token ou URI contendo segredo pode ser versionada;
- tabelas publicadas devem possuir propósito e responsável identificáveis na documentação ou no PR.

## 3. Fontes oficiais e granularidade

| Entidade | Origem | Grão | Destino Bronze |
|---|---|---|---|
| Indicador de alfabetização por UF | INEP | ano + UF + série + rede | `bronze.avaliacao_alfabetizacao` |
| Indicador de alfabetização por município | INEP | ano + município + rede | `bronze.avaliacao_alfabetizacao_municipio` |
| Dimensão UF | IBGE | UF | `bronze.uf` |
| Dimensão município | IBGE | município | `bronze.municipio` |
| Meta Brasil | INEP | ano | `bronze.meta_brasil` |
| Meta UF | INEP | ano + UF | `bronze.meta_uf` |
| Meta município | INEP | ano + município | `bronze.meta_municipio` |
| Microdados de alunos | INEP - `TS_ALUNO.csv` | aluno | `bronze.alunos` |
| Replay de eventos oficiais | derivado do indicador municipal oficial | evento | `bronze.eventos_streaming` |

Não existe fallback sintético para substituir fonte oficial ausente.

## 4. Chaves e níveis territoriais

- `sigla_uf`: duas letras maiúsculas;
- `id_municipio`: código IBGE com sete dígitos, armazenado como `string`;
- `id_municipio` é nulo quando o registro está no grão UF;
- `grao`: identifica explicitamente o nível territorial (`uf` ou `municipio`);
- `serie`: série avaliada quando disponível;
- `rede`: código normalizado conforme domínio do projeto;
- `record_id`: hash determinístico da chave de negócio e da origem;
- `event_id`: obrigatório e único no caminho de streaming;
- chave de serving municipal: `ano + id_municipio + rede`.

Marts que combinam UF e município devem preservar o nível territorial para impedir interpretação incorreta entre grãos.

## 5. Domínios

### 5.1 Rede

| Código | Rótulo |
|---:|---|
| 0 | total |
| 2 | estadual |
| 3 | municipal |
| 5 | privada |

O de-para deve permanecer rastreável à fonte utilizada. Caso uma fonte oficial use código diferente, a normalização deve ocorrer de forma explícita e documentada antes do uso analítico.

### 5.2 Regra de alfabetização

O ponto de corte adotado no projeto é:

```text
alfabetizado = proficiencia_portugues >= 743
```

O corte de **743 pontos é uma regra no grão de aluno**.

Nos microdados oficiais, a proficiência de Língua Portuguesa é obtida de `VL_PROFICIENCIA_LP`. A fonte também contém `IN_ALFABETIZADO`, que pode ser utilizada para validação de consistência da classificação oficial.

Na fonte agregada, `taxa_alfabetizacao` já representa o Indicador Criança Alfabetizada. Portanto:

- o corte de 743 não deve ser aplicado sobre a taxa agregada;
- a taxa oficial agregada é preservada como medida de negócio;
- qualquer flag derivada de média agregada é apenas auxiliar e não representa classificação individual.

## 6. Contrato do streaming

O streaming é um **replay controlado de registros oficiais do INEP**, utilizado para demonstrar propriedades técnicas de ingestão, contrato, checkpoint, deduplicação e quarentena.

`source` válido para o replay oficial:

```text
INEP_OFICIAL_REPLAY
```

| Campo | Tipo | Obrigatório | Regra |
|---|---|:---:|---|
| `event_id` | string UUID | sim | único e determinístico para o registro oficial |
| `event_time` | timestamp UTC | sim | representa o momento do replay/ingestão |
| `schema_version` | string | sim | versão suportada pelo consumer |
| `ano` | int | sim | intervalo válido do contrato |
| `sigla_uf` | string | sim | domínio das UFs brasileiras |
| `id_municipio` | string | sim | sete dígitos |
| `rede` | int | sim | domínio aceito |
| `taxa_alfabetizacao` | double | sim | percentual entre `0` e `100` na Bronze |
| `source` | string | sim | `INEP_OFICIAL_REPLAY` para evento analítico aceito |

A demonstração pode gerar:

- uma duplicata proposital com o mesmo `event_id`, para comprovar idempotência;
- um payload propositalmente inválido com `source = TESTE_CONTRATO_INVALIDO`, para comprovar a quarentena.

O payload inválido de teste não é fato analítico e nunca deve ser incorporado à Silver.

Eventos inválidos são enviados para `workspace.observability.quarantine_records` com `rejection_reason`, payload e timestamp de ingestão.

## 7. Metadados técnicos

### 7.1 Bronze batch

As tabelas batch devem conter, quando aplicável:

- `ingestion_timestamp`;
- `source_file`;
- `source_system`;
- `pipeline_run_id`;
- `schema_version`.

A ingestão deve preservar o dado original de negócio; normalizações analíticas pertencem à Silver.

### 7.2 Bronze streaming

O destino `bronze.eventos_streaming` mantém:

- `event_id`;
- `event_time`;
- `schema_version`;
- `source`;
- `_source_file`;
- `_ingestion_timestamp`;
- `_pipeline_run_id`.

### 7.3 Silver

A Silver deve manter, quando aplicável:

- `record_id`;
- `source`;
- `fonte_dados`;
- `event_id`;
- `event_time`;
- `processed_at`;
- `schema_version`;
- `grao`.

## 8. Idempotência

- batch: carga controlada e reconciliação entre origem e destino;
- Silver: `record_id` determinístico;
- streaming: deduplicação por `event_id`;
- checkpoint do streaming deve permanecer fora da landing zone;
- processamento do streaming usa `MERGE` idempotente no destino;
- serving: `upsert` por chave de negócio;
- reexecução não deve criar duplicidade lógica.

## 9. Metas oficiais

As tabelas:

- `meta_brasil`;
- `meta_uf`;
- `meta_municipio`;

são produzidas exclusivamente a partir das planilhas oficiais do INEP utilizadas pelo projeto.

**Não existe interpolação de metas no pipeline atual.**

A Silver deve associar a meta correspondente ao mesmo grão territorial:

```text
grao = municipio → meta_municipio
grao = uf        → meta_uf
```

É proibido usar a meta da UF como fallback para município sem meta oficial.

A meta nacional pode ser mantida como referência adicional, mas não substitui a meta territorial específica.

## 10. Proveniência dos microdados

A tabela `workspace.bronze.alunos` é derivada do arquivo oficial:

```text
microdados_inep/DADOS/TS_ALUNO.csv
```

da Avaliação da Alfabetização 2024 do INEP.

Campos relevantes da fonte incluem:

- `NU_ANO_AVALIACAO`;
- `CO_UF`;
- `SG_UF`;
- `ID_ALUNO`;
- `TP_SERIE`;
- `ID_ESCOLA`;
- `TP_DEPENDENCIA`;
- `CO_MUNICIPIO`;
- `NO_MUNICIPIO`;
- `IN_PRESENCA_LP`;
- `IN_PREENCHIMENTO_LP`;
- `CO_CADERNO_LP`;
- `VL_PESO_ALUNO_LP`;
- `VL_PROFICIENCIA_LP`;
- `IN_ALFABETIZADO`.

A execução validada carregou **2.120.560 registros**.

Os antigos dados sintéticos permanecem somente em `data/legacy_fontes_derivadas/` para rastreabilidade histórica e não participam do pipeline oficial.

O arquivo `fontes_oficiais_manifest.json` registra proveniência e hash das fontes preparadas.

## 11. Silver canônica

A Silver integra:

1. indicador oficial por UF;
2. indicador oficial por município;
3. replay oficial do streaming;
4. dimensões territoriais de UF e município;
5. metas oficiais Brasil, UF e município;
6. agregações derivadas dos microdados oficiais de alunos.

Regras obrigatórias:

- taxas normalizadas para `0` a `1`;
- chaves territoriais normalizadas;
- `grao` preservado;
- `fonte_dados = oficial_inep` para os fatos oficiais;
- nenhuma substituição por dado sintético;
- meta associada ao mesmo grão territorial;
- `record_id` determinístico;
- consistência UF × município rastreável.

## 12. Quality Gate

O Quality Gate é **bloqueante** e ocorre antes da publicação da fonte usada pela Gold.

### 12.1 Validações por registro

Incluem:

- campos críticos não nulos;
- UF válida;
- `id_municipio` válido quando o grão é municipal;
- domínio de rede;
- `taxa_alfabetizacao` entre `0` e `1` na Silver;
- integridade referencial contra dimensões;
- consistência entre UF e município.

Registros reprovados vão para quarentena com `rejection_reason`.

### 12.2 Validações sistêmicas

Incluem:

- Silver não vazia;
- existência de registros aprovados;
- unicidade de `record_id`;
- cobertura mínima de **80%**.

### 12.3 Ordem obrigatória de publicação

A sequência é:

```text
Silver
  → validações por registro
  → quarentena
  → validações sistêmicas
  → APROVADO: publica silver.medicoes_aprovadas
  → Gold liberada
```

Se qualquer validação sistêmica falhar:

- a execução recebe status `FAILED`;
- a auditoria registra a falha;
- `silver.medicoes_aprovadas` não deve ser sobrescrita;
- a última versão aprovada permanece disponível;
- a task encerra com erro;
- a Gold não é liberada.

A execução validada após a correção apresentou:

```text
silver_nao_vazia: OK
aprovados_maior_que_zero: OK
record_id_unico_nos_aprovados: OK
cobertura_min_80%: OK
cobertura: 100%
registros publicados: 10.737
```

## 13. Auditoria e observabilidade

Tabela principal:

```text
workspace.observability.pipeline_metrics
```

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

O mesmo `run_id` deve permitir correlacionar as tasks de uma execução do Workflow.

A observabilidade também acompanha:

- latência do streaming;
- taxa de rejeição;
- volume;
- frescor da Gold;
- quarentena;
- origem dos eventos.

## 14. Gold

A Gold consome exclusivamente tabelas Silver aprovadas pelo Quality Gate.

Fontes aprovadas:

```text
workspace.silver.medicoes_aprovadas
workspace.silver.alunos_modelagem_aprovados
```

Os quatro marts territoriais consomem `silver.medicoes_aprovadas`. A base de modelagem no grão de aluno consome `silver.alunos_modelagem_aprovados`.

Marts atuais:

| Tabela | Grão | Uso |
|---|---|---|
| `gold.indicador_municipio` | ano + município + rede | indicador municipal |
| `gold.resumo_uf` | ano + UF + rede | resumo por UF |
| `gold.meta_vs_resultado` | ano + território + rede | resultado observado x meta oficial |
| `gold.evolucao_temporal` | ano + território + rede | evolução temporal |
| `gold.base_modelagem_aluno` | aluno | base oficial preparada para classificação supervisionada na Fase 3 |

Os marts que misturam níveis territoriais devem preservar o campo de grão ou nível territorial.

### Base Gold no grão de aluno

`workspace.gold.base_modelagem_aluno` preserva uma linha por aluno e foi publicada com **2.120.560 registros oficiais** após Quality Gate com **100% de cobertura**.

O target preparado para a Fase 3 é:

```text
alfabetizado_oficial
```

A variável de proficiência (`VL_PROFICIENCIA_LP` / `proficiencia_portugues`) não é publicada como feature nessa Gold de modelagem, evitando vazamento direto do critério de 743 pontos.

## 15. Serving e MLflow

### MongoDB

- Delta Lake/Gold permanece como fonte analítica de verdade;
- MongoDB é uma camada opcional de serving;
- publicação municipal usa `upsert`;
- chave: `ano + id_municipio + rede`;
- URI deve ser recuperada por secret do Databricks;
- ausência do secret não autoriza exposição de credenciais nem alteração da fonte analítica.

### MLflow

O experimento da Fase 2 é uma prova de conceito sobre a Gold municipal.

Ele não substitui a futura modelagem supervisionada da Fase 3 e suas limitações devem permanecer registradas no model card.

## 16. Tabelas por notebook

| Notebook | Lê de | Escreve em |
|---|---|---|
| `00_setup_ambiente` | configuração | schemas, volumes e observabilidade |
| `01_bronze_batch` | raw files oficiais | `bronze.*` |
| `02_bronze_streaming` | indicador municipal oficial / landing | `bronze.eventos_streaming` + quarentena |
| `03_silver` | Bronze batch + streaming + dimensões + metas + alunos | `silver.medicoes_alfabetizacao` + `silver.alunos_modelagem` |
| `06_quality_checks` | Silver | `silver.medicoes_aprovadas` + `silver.alunos_modelagem_aprovados` + métricas + quarentena |
| `04_gold` | Silver aprovada | 5 marts em `gold.*`, incluindo `gold.base_modelagem_aluno` |
| `05_serving_mongodb` | Gold municipal | MongoDB, quando configurado |
| `07_ml_mlflow` | Gold municipal | MLflow |
| `08_monitoring` | tabelas e métricas | `observability.pipeline_metrics` |
| `09_dashboard` | Gold + observabilidade | Command Center executivo |

## 17. Definition of Done

### Bronze

- dado oficial preservado;
- schema explícito;
- metadados técnicos presentes;
- origem e destino reconciliados;
- ausência de fallback sintético.

### Silver

- modelo canônico integrando batch e replay oficial;
- `silver.alunos_modelagem` preservando o grão individual dos 2.120.560 alunos oficiais;
- chaves e domínios normalizados;
- metas associadas ao mesmo grão;
- `record_id` determinístico;
- origem identificada;
- regra dos 743 pontos aplicada somente no contexto correto.

### Quality Gate

- regras por registro executadas;
- quarentena operacional;
- checks sistêmicos aprovados antes da publicação;
- gate territorial validado com 10.737 registros aprovados e 100% de cobertura;
- gate de alunos validado com 2.120.560 registros aprovados, 0 rejeitados e 100% de cobertura;
- auditoria persistida;
- falha não sobrescreve a última Silver aprovada.

### Gold

- consome somente Silver aprovada;
- 5 marts publicados;
- `gold.base_modelagem_aluno` preserva o grão de aluno;
- grão documentado;
- origem oficial rastreável;
- métricas reproduzíveis;
- Quality Gate aprovado.

### Serving, ML e consumo

- segredo fora do código;
- escrita idempotente;
- experimento reproduzível;
- limitações documentadas;
- dashboard derivado da Gold.
