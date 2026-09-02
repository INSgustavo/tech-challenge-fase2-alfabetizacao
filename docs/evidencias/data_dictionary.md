# Dicionário de Dados

> Documento de referência das entidades, campos, granularidades e regras do pipeline de alfabetização.  
> A versão atual reflete somente as fontes oficiais e as tabelas efetivamente utilizadas pela pipeline corrigida da Fase 2.

---

## 1. Convenções gerais

- Catálogo principal: `workspace`
- Camadas: `bronze`, `silver`, `gold`, `observability`
- Identificadores territoriais são preservados como `string` quando necessário para manter zeros à esquerda.
- Na Bronze, o dado de negócio é preservado na unidade da fonte.
- Na Silver, `taxa_alfabetizacao` e metas são normalizadas para fração entre `0` e `1`.
- `grao` identifica explicitamente o nível territorial: `uf` ou `municipio`.
- `record_id` é determinístico na Silver.
- `event_id` é determinístico no replay oficial do streaming.
- Metas municipais não herdam metas de UF quando a meta municipal oficial não existe.
- Dados sintéticos não participam da pipeline oficial.

---

# 2. Camada Bronze

## 2.1 `workspace.bronze.avaliacao_alfabetizacao`

**Descrição:** indicador agregado de alfabetização por UF.

**Origem:** INEP  
**Grão:** `ano + sigla_uf + serie + rede`  
**Volume validado:** 145 registros

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `ano` | int | Sim | Ano de referência da avaliação. |
| `sigla_uf` | string | Sim | Sigla da UF. |
| `serie` | int | Sim | Série avaliada. |
| `rede` | int | Sim | Código da rede de ensino. |
| `taxa_alfabetizacao` | double | Não | Indicador oficial em percentual, escala `0–100` na Bronze. |
| `media_portugues` | double | Não | Média agregada de proficiência em Língua Portuguesa. |
| `proporcao_aluno_nivel_0` | double | Não | Proporção de alunos no nível 0. |
| `proporcao_aluno_nivel_1` | double | Não | Proporção de alunos no nível 1. |
| `proporcao_aluno_nivel_2` | double | Não | Proporção de alunos no nível 2. |
| `proporcao_aluno_nivel_3` | double | Não | Proporção de alunos no nível 3. |
| `proporcao_aluno_nivel_4` | double | Não | Proporção de alunos no nível 4. |
| `proporcao_aluno_nivel_5` | double | Não | Proporção de alunos no nível 5. |
| `proporcao_aluno_nivel_6` | double | Não | Proporção de alunos no nível 6. |
| `proporcao_aluno_nivel_7` | double | Não | Proporção de alunos no nível 7. |
| `proporcao_aluno_nivel_8` | double | Não | Proporção de alunos no nível 8. |
| `ingestion_timestamp` | timestamp | Sim | Momento da ingestão. |
| `source_file` | string | Sim | Caminho do arquivo de origem. |
| `source_system` | string | Sim | Sistema/fonte identificada na ingestão. |
| `pipeline_run_id` | string | Sim | Identificador da execução. |
| `schema_version` | string | Sim | Versão do schema aplicado. |

---

## 2.2 `workspace.bronze.avaliacao_alfabetizacao_municipio`

**Descrição:** indicador oficial de alfabetização no grão municipal.

**Origem:** INEP  
**Grão:** `ano + id_municipio + rede`  
**Volume validado:** 10.584 registros

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `ano` | int | Sim | Ano de referência. |
| `sigla_uf` | string | Sim | UF do município. |
| `id_municipio` | string | Sim | Código IBGE do município com 7 dígitos. |
| `serie` | int | Não | Série avaliada quando disponível. |
| `rede` | int | Não | Código da rede. |
| `taxa_alfabetizacao` | double | Não | Indicador oficial em percentual `0–100` na Bronze. |
| `media_portugues` | double | Não | Média agregada de proficiência, quando disponível. |
| `ingestion_timestamp` | timestamp | Sim | Momento da ingestão. |
| `source_file` | string | Sim | Arquivo de origem. |
| `source_system` | string | Sim | Fonte identificada. |
| `pipeline_run_id` | string | Sim | Identificador da execução. |
| `schema_version` | string | Sim | Versão do schema. |

---

## 2.3 `workspace.bronze.uf`

**Descrição:** dimensão territorial de UFs.

**Origem:** IBGE  
**Grão:** uma linha por UF  
**Volume validado:** 27 registros

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `codigo_uf` | int | Não | Código numérico da UF. |
| `sigla_uf` | string | Sim | Sigla da UF. |
| `nome` | string | Não | Nome da Unidade da Federação. |
| `regiao` | string | Não | Região brasileira. |

Também recebe os metadados técnicos de ingestão da Bronze.

---

## 2.4 `workspace.bronze.municipio`

**Descrição:** dimensão territorial de municípios.

**Origem:** IBGE  
**Grão:** uma linha por município  
**Volume validado:** 5.571 registros

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `id_municipio` | string | Sim | Código IBGE com 7 dígitos. |
| `nome` | string | Não | Nome do município. |
| `sigla_uf` | string | Sim | UF do município. |
| `capital` | int | Não | Indicador de capital conforme arquivo territorial. |
| `latitude` | double | Não | Latitude do município. |
| `longitude` | double | Não | Longitude do município. |

Também recebe os metadados técnicos de ingestão da Bronze.

---

## 2.5 `workspace.bronze.meta_brasil`

**Descrição:** metas nacionais oficiais.

**Origem:** INEP  
**Grão:** `ano`  
**Volume validado:** 7 registros

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `ano` | int | Sim | Ano da meta. |
| `meta` | double | Sim | Meta oficial na unidade da fonte. |
| `metodologia` | string | Não | Identificação/metodologia registrada na fonte preparada. |

---

## 2.6 `workspace.bronze.meta_uf`

**Descrição:** metas oficiais por UF.

**Origem:** INEP  
**Grão:** `ano + sigla_uf`  
**Volume validado:** 180 registros

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `sigla_uf` | string | Sim | UF da meta. |
| `ano` | int | Sim | Ano da meta. |
| `meta` | double | Sim | Meta oficial. |
| `metodologia` | string | Não | Informação metodológica/proveniência. |

---

## 2.7 `workspace.bronze.meta_municipio`

**Descrição:** metas oficiais por município.

**Origem:** INEP  
**Grão:** `ano + id_municipio`  
**Volume validado:** 37.344 registros

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `id_municipio` | string | Sim | Código IBGE do município. |
| `sigla_uf` | string | Sim | UF do município. |
| `ano` | int | Sim | Ano da meta. |
| `meta` | double | Sim | Meta municipal oficial. |
| `metodologia` | string | Não | Informação metodológica/proveniência. |

> A pipeline atual não cria metas por interpolação.

---

## 2.8 `workspace.bronze.alunos`

**Descrição:** microdados oficiais da Avaliação da Alfabetização 2024.

**Origem:** INEP, arquivo `microdados_inep/DADOS/TS_ALUNO.csv`  
**Grão:** um registro por aluno na fonte  
**Volume validado:** **2.120.560 registros**

A Bronze preserva os nomes originais do arquivo oficial.

| Campo | Tipo Bronze | Descrição |
|---|---|---|
| `NU_ANO_AVALIACAO` | int | Ano da avaliação. |
| `CO_UF` | int | Código numérico da UF. |
| `SG_UF` | string | Sigla da UF. |
| `ID_ALUNO` | string | Identificador do aluno na fonte. |
| `TP_SERIE` | string | Série/tipo de série informado na fonte. |
| `ID_ESCOLA` | string | Identificador da escola. |
| `TP_DEPENDENCIA` | int | Dependência administrativa da escola. |
| `CO_MUNICIPIO` | string | Código do município. |
| `NO_MUNICIPIO` | string | Nome do município. |
| `IN_PRESENCA_LP` | int | Indicador de presença em Língua Portuguesa. |
| `IN_PREENCHIMENTO_LP` | int | Indicador de preenchimento da avaliação de LP. |
| `CO_CADERNO_LP` | string | Código do caderno de Língua Portuguesa. |
| `VL_PESO_ALUNO_LP` | string | Peso do aluno na fonte; preservado como texto na Bronze. |
| `VL_PROFICIENCIA_LP` | string | Proficiência de Língua Portuguesa; preservada como texto na Bronze. |
| `IN_ALFABETIZADO` | int | Indicador oficial de alfabetização disponível na fonte. |
| `ingestion_timestamp` | timestamp | Momento da ingestão. |
| `source_file` | string | Arquivo de origem. |
| `source_system` | string | Identificação da fonte INEP. |
| `pipeline_run_id` | string | Identificador da execução. |
| `schema_version` | string | Versão do schema. |

### Regra dos 743 pontos

Na transformação, `VL_PROFICIENCIA_LP` é convertido para `double`.

A regra utilizada pelo projeto é:

```text
alfabetizado = proficiencia_portugues >= 743
```

O corte é aplicado no **grão de aluno**.

`IN_ALFABETIZADO` é preservado na Bronze e pode ser utilizado para validar a consistência entre a classificação oficial e a regra derivada.

---

## 2.9 `workspace.bronze.eventos_streaming`

**Descrição:** replay controlado de medições municipais oficiais para demonstrar o caminho de streaming.

**Origem:** `workspace.bronze.avaliacao_alfabetizacao_municipio`  
**Grão:** um evento por medição oficial selecionada  
**Fonte válida:** `INEP_OFICIAL_REPLAY`

| Campo | Tipo | Obrigatório | Descrição |
|---|---|:---:|---|
| `event_id` | string | Sim | UUID determinístico usado para idempotência. |
| `event_time` | timestamp | Sim | Momento do replay/ingestão. |
| `schema_version` | string | Sim | Versão do contrato do evento. |
| `ano` | int | Sim | Ano do indicador. |
| `sigla_uf` | string | Sim | UF. |
| `id_municipio` | string | Sim | Município com 7 dígitos. |
| `rede` | int | Sim | Rede de ensino. |
| `taxa_alfabetizacao` | double | Sim | Percentual oficial na escala `0–100` na Bronze streaming. |
| `source` | string | Sim | `INEP_OFICIAL_REPLAY` para evento válido. |
| `_source_file` | string | Sim | Arquivo JSON lido pelo stream. |
| `_ingestion_timestamp` | timestamp | Sim | Momento de processamento do evento. |
| `_pipeline_run_id` | string | Sim | Identificador da execução. |

A demonstração publica:

- 8 eventos válidos baseados em registros oficiais;
- 1 duplicata proposital com o mesmo `event_id`;
- 1 payload inválido controlado com `source = TESTE_CONTRATO_INVALIDO`.

O payload inválido nunca integra o fato analítico.

---

# 3. Camada Silver

## 3.1 `workspace.silver.medicoes_alfabetizacao`

**Descrição:** modelo canônico integrado utilizado pelo Quality Gate.

**Grão:** uma medição por chave territorial, rede e origem, identificada por `record_id`.

### Campos canônicos de fato

| Campo | Tipo | Descrição |
|---|---|---|
| `ano` | int | Ano de referência. |
| `sigla_uf` | string | UF normalizada. |
| `id_municipio` | string | Município quando `grao = municipio`; nulo no grão UF. |
| `grao` | string | `uf` ou `municipio`. |
| `serie` | int | Série quando disponível. |
| `rede` | int | Código normalizado da rede. |
| `rede_label` | string | Rótulo da rede. |
| `media_portugues` | double | Média agregada de proficiência quando disponível. |
| `taxa_alfabetizacao` | double | Indicador normalizado para fração `0–1`. |
| `alfabetizado` | boolean | Flag auxiliar quando a média agregada permite cálculo; não representa classificação individual. |
| `event_id` | string | ID do evento para fatos provenientes do streaming; nulo para batch. |
| `event_time` | timestamp | Timestamp do replay para fatos streaming. |
| `source` | string | Origem lógica do fato. |
| `fonte_dados` | string | Identificação de proveniência analítica; fatos atuais usam `oficial_inep`. |
| `schema_version` | string | Versão do schema. |

### Enriquecimento territorial

| Campo | Tipo | Descrição |
|---|---|---|
| `nome_municipio` | string | Nome do município. |
| `capital` | int | Indicador de capital. |
| `uf_consistente` | boolean | Valida se UF do fato coincide com a UF da dimensão municipal. |
| `nome_uf` | string | Nome da UF. |
| `regiao` | string | Região brasileira. |

### Metas

| Campo | Tipo | Descrição |
|---|---|---|
| `meta_municipio` | double | Meta municipal oficial normalizada para `0–1`. |
| `meta_uf` | double | Meta estadual oficial normalizada para `0–1`. |
| `meta_brasil` | double | Meta nacional oficial normalizada para `0–1`. |
| `meta_taxa` | double | Meta correspondente ao mesmo grão da medição. |

Regra de `meta_taxa`:

```text
grao = municipio → meta_municipio
grao = uf        → meta_uf
```

Se a meta específica não existir, `meta_taxa` permanece `NULL`.

### Enriquecimento derivado dos microdados

Os 2.120.560 registros de alunos são agregados antes do join com a Silver.

**Grão da agregação:** `ano + sigla_uf + rede`

| Campo | Tipo | Descrição |
|---|---|---|
| `alunos_proficiencia_media` | double | Média de `VL_PROFICIENCIA_LP` convertida para número. |
| `alunos_pct_alfabetizados` | double | Fração de alunos com proficiência `>= 743`. |
| `alunos_amostra` | long | Quantidade de alunos válidos usada na agregação. |

### Campos de controle

| Campo | Tipo | Descrição |
|---|---|---|
| `record_id` | string | SHA-256 determinístico da chave do fato e origem. |
| `processed_at` | timestamp | Timestamp da transformação Silver. |

**Volume validado:** 10.737 registros.

---

## 3.2 `workspace.silver.medicoes_aprovadas`

**Descrição:** versão da Silver liberada pelo Quality Gate e única fonte permitida para a Gold.

Possui o mesmo contrato de `silver.medicoes_alfabetizacao`, limitado aos registros aprovados.

**Volume validado após o Quality Gate corrigido:** 10.737 registros  
**Cobertura validada:** 100%

A publicação ocorre somente depois de:

- Silver não vazia;
- existência de aprovados;
- unicidade de `record_id`;
- cobertura mínima de 80%;
- validações de domínio e integridade referencial.

---

# 4. Camada Gold

## 4.1 `workspace.gold.indicador_municipio`

**Grão:** `ano + id_municipio + rede + fonte_dados`

**Volume validado:** 10.584 registros

| Campo | Tipo lógico | Descrição |
|---|---|---|
| `ano` | int | Ano de referência. |
| `sigla_uf` | string | UF. |
| `nome_uf` | string | Nome da UF. |
| `regiao` | string | Região. |
| `id_municipio` | string | Código IBGE do município. |
| `nome_municipio` | string | Nome do município. |
| `rede` | int | Código da rede. |
| `rede_label` | string | Rótulo da rede. |
| `fonte_dados` | string | Proveniência do fato. |
| `taxa_alfabetizacao_media` | double | Média da taxa oficial no grupo. |
| `media_portugues` | double | Média de proficiência agregada disponível. |
| `pct_registros_alfabetizados` | double | Média da flag auxiliar `alfabetizado` quando disponível. |
| `quantidade_registros` | long | Quantidade de fatos agregados. |
| `updated_at` | timestamp | Timestamp mais recente da Silver utilizada. |

---

## 4.2 `workspace.gold.resumo_uf`

**Grão:** `ano + sigla_uf + rede + fonte_dados`

**Volume validado:** 145 registros

| Campo | Tipo lógico | Descrição |
|---|---|---|
| `ano` | int | Ano. |
| `sigla_uf` | string | UF. |
| `nome_uf` | string | Nome da UF. |
| `regiao` | string | Região. |
| `rede` | int | Rede. |
| `rede_label` | string | Rótulo da rede. |
| `fonte_dados` | string | Proveniência. |
| `taxa_alfabetizacao_media` | double | Indicador médio da UF. |
| `media_portugues` | double | Média agregada de proficiência. |
| `alunos_proficiencia_media` | double | Proficiência média derivada dos microdados oficiais agregados. |
| `alunos_pct_alfabetizados` | double | Fração de alunos `>= 743` no agregado ano + UF + rede. |
| `municipios_cobertos` | long | Quantidade de municípios distintos no grupo; no mart UF atual tende a zero porque a fonte é de grão UF. |
| `updated_at` | timestamp | Atualização mais recente. |

---

## 4.3 `workspace.gold.meta_vs_resultado`

**Grão:** `ano + território + rede + fonte_dados`

**Volume validado:** 10.729 registros

> Este mart contém UF e município. Toda análise agregada deve filtrar `nivel_territorial`.

| Campo | Tipo lógico | Descrição |
|---|---|---|
| `ano` | int | Ano. |
| `grao` | string | Grão original: `uf` ou `municipio`. |
| `nivel_territorial` | string | Campo explícito para controle de agregação territorial. |
| `sigla_uf` | string | UF. |
| `nome_uf` | string | Nome da UF. |
| `regiao` | string | Região. |
| `id_municipio` | string | Município quando aplicável. |
| `nome_municipio` | string | Nome do município. |
| `rede` | int | Rede. |
| `rede_label` | string | Rótulo da rede. |
| `fonte_dados` | string | Proveniência. |
| `taxa_alfabetizacao_media` | double | Resultado observado. |
| `meta_taxa` | double | Meta oficial do mesmo grão territorial. |
| `meta_brasil` | double | Meta nacional de referência. |
| `gap_meta` | double | `taxa_alfabetizacao_media - meta_taxa`. |
| `atingiu_meta` | boolean | Resultado >= meta do território. |
| `atingiu_meta_brasil` | boolean | Resultado >= meta nacional. |
| `updated_at` | timestamp | Atualização mais recente. |

---

## 4.4 `workspace.gold.evolucao_temporal`

**Grão:** `ano + território + rede + fonte_dados`

**Volume validado:** 10.729 registros

> Também combina UF e município e exige filtro por `nivel_territorial` em agregações.

| Campo | Tipo lógico | Descrição |
|---|---|---|
| `ano` | int | Ano corrente. |
| `grao` | string | `uf` ou `municipio`. |
| `nivel_territorial` | string | Controle explícito do grão. |
| `sigla_uf` | string | UF. |
| `nome_uf` | string | Nome da UF. |
| `regiao` | string | Região. |
| `id_municipio` | string | Município quando aplicável. |
| `nome_municipio` | string | Nome do município. |
| `rede` | int | Rede. |
| `rede_label` | string | Rótulo da rede. |
| `fonte_dados` | string | Proveniência. |
| `taxa_alfabetizacao_media` | double | Resultado do ano corrente. |
| `taxa_ano_anterior` | double | Resultado imediatamente anterior no mesmo território/rede. |
| `ano_anterior` | int | Ano da observação anterior. |
| `variacao_absoluta` | double | Diferença entre taxa atual e anterior. |
| `variacao_relativa` | double | Variação relativa quando o valor anterior é maior que zero. |
| `tendencia` | string | `alta`, `queda` ou `estavel`. |
| `updated_at` | timestamp | Atualização mais recente. |

---

# 5. Domínio de rede utilizado no modelo canônico

| Código | Rótulo |
|---:|---|
| 0 | total |
| 2 | estadual |
| 3 | municipal |
| 5 | privada |

Nos microdados oficiais, `TP_DEPENDENCIA = 4` é mapeado para `rede = 5` para compatibilidade com o domínio já utilizado pelo projeto.

Esse de-para deve permanecer rastreável à documentação oficial utilizada pelo grupo.

---

# 6. Regras principais de qualidade

## Chaves territoriais

- `sigla_uf` deve pertencer ao domínio das UFs brasileiras.
- `id_municipio`, quando presente, deve possuir 7 dígitos.
- município deve existir em `bronze.municipio`.
- UF deve existir em `bronze.uf`.
- `uf_consistente` não pode ser falso para registros municipais aprovados.

## Taxa de alfabetização

- Bronze oficial: percentual `0–100`.
- Silver e Gold: fração `0–1`.

## Metas

- metas são oficiais;
- não existe interpolação;
- município usa somente `meta_municipio`;
- UF usa somente `meta_uf`;
- ausência da meta específica resulta em `NULL`.

## Idempotência

- `event_id` único no streaming;
- `record_id` único na Silver aprovada;
- replay duplicado não deve gerar duplicidade lógica.

## Quality Gate

Checks sistêmicos atuais:

```text
silver_nao_vazia
aprovados_maior_que_zero
record_id_unico_nos_aprovados
cobertura_min_80%
```

Execução validada:

```text
Cobertura: 100%
Silver aprovada: 10.737 registros
```

---

# 7. Proveniência

O arquivo `fontes_oficiais_manifest.json` registra proveniência e hash das fontes preparadas.

Fontes sintéticas antigas permanecem apenas em:

```text
data/legacy_fontes_derivadas/
```

e não alimentam a pipeline oficial.

---

# 8. Preparação para a Fase 3

O enunciado da Fase 3 exige modelagem supervisionada no **grão de aluno**, utilizando dados derivados da fundação criada na Fase 2.

A tabela oficial que preserva esse grão atualmente é:

```text
workspace.bronze.alunos
```

A Gold atual da Fase 2 é composta por marts agregados territoriais. Portanto, uma futura base analítica de aluno para a Fase 3 deverá ser criada explicitamente a partir dos microdados oficiais e de enriquecimentos territoriais/socioeconômicos.

Essa futura tabela **ainda não faz parte deste dicionário como tabela implementada**.

Ponto crítico para a modelagem futura:

- `IN_ALFABETIZADO` pode representar o target oficial após validação;
- `VL_PROFICIENCIA_LP` não deve ser usado como feature se o target for definido diretamente pela classificação de alfabetização, pois isso pode gerar data leakage;
- identificadores como `ID_ALUNO` e `ID_ESCOLA` exigem avaliação antes de serem utilizados como features;
- enriquecimentos socioeconômicos pertencem à Fase 3.

---

# 9. Pendências controladas

- validar e manter evidência documental do de-para oficial de `TP_DEPENDENCIA` / `rede`;
- manter rastreabilidade da regra dos 743 pontos;
- criar, na Fase 3, o contrato específico da base de modelagem no grão de aluno antes do treinamento;
- versionar qualquer nova variável socioeconômica ou educacional incluída no modelo.
