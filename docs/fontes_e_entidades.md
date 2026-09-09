# Fontes e Entidades

## Objetivo

Este documento registra as fontes oficiais utilizadas na versão corrigida da Fase 2, o grão de cada entidade e seu papel no pipeline.

A pipeline oficial não depende de dados sintéticos para substituir fonte ausente.

---

## 1. INEP - Indicador de Alfabetização por UF

**Arquivo:** `br_inep_avaliacao_alfabetizacao_uf.csv.gz`  
**Tipo:** Batch  
**Destino:** `workspace.bronze.avaliacao_alfabetizacao`  
**Grão:** `ano + sigla_uf + serie + rede`  
**Volume validado:** 145 registros

Principais campos:

- `ano`
- `sigla_uf`
- `serie`
- `rede`
- `taxa_alfabetizacao`
- `media_portugues`
- `proporcao_aluno_nivel_0` a `proporcao_aluno_nivel_8`

A taxa chega em percentual `0-100` na Bronze e é normalizada para `0-1` na Silver.

---

## 2. INEP - Indicador de Alfabetização por Município

**Arquivo:** `br_inep_avaliacao_alfabetizacao_municipio.csv.gz`  
**Tipo:** Batch  
**Destino:** `workspace.bronze.avaliacao_alfabetizacao_municipio`  
**Grão:** `ano + id_municipio + rede`  
**Volume validado:** 10.584 registros

Principais campos:

- `ano`
- `sigla_uf`
- `id_municipio`
- `serie`
- `rede`
- `taxa_alfabetizacao`
- `media_portugues`

Essa entidade alimenta os marts municipais com dado oficial.

---

## 3. INEP - Metas oficiais

### 3.1 Meta Brasil

**Destino:** `workspace.bronze.meta_brasil`  
**Grão:** `ano`  
**Volume validado:** 7 registros

### 3.2 Meta UF

**Destino:** `workspace.bronze.meta_uf`  
**Grão:** `ano + sigla_uf`  
**Volume validado:** 180 registros

### 3.3 Meta Município

**Destino:** `workspace.bronze.meta_municipio`  
**Grão:** `ano + id_municipio`  
**Volume validado:** 37.344 registros

As metas são obtidas das planilhas oficiais do INEP utilizadas pelo projeto.

**Não existe interpolação de metas no pipeline atual.**

Na Silver:

```text
grao = municipio → meta_municipio
grao = uf        → meta_uf
```

Meta municipal não herda meta de UF como fallback.

---

## 4. INEP - Microdados de alunos

**Arquivo oficial:** `microdados_inep/DADOS/TS_ALUNO.csv` (Saeb 2023, 2º ano EF — edição mais recente publicada; o resto do pipeline usa 2024)
**Destino:** `workspace.bronze.alunos`
**Grão:** aluno
**Volume:** varia por execução — ver output do `08_monitoring.py`, não é constante

Campos relevantes (schema real, conferido no cabeçalho do arquivo — o
schema abaixo já substituiu uma versão anterior baseada em suposição, não
no arquivo real):

- `ID_SAEB` (ano de aplicação)
- `ID_UF` (código numérico IBGE — traduzido pra sigla via `bronze.uf`)
- `ID_ALUNO`
- `ID_SERIE`
- `ID_ESCOLA`
- `IN_PUBLICA` (só público/privado, sem dependência administrativa granular)
- `ID_MUNICIPIO` (mascarado/anonimizado — não é o código real do IBGE, não usado pra join territorial)
- `IN_PRESENCA_LP`
- `IN_PREENCHIMENTO_LP`
- `ID_CADERNO_LP`
- `PESO_ALUNO_LP`
- `PROFICIENCIA_LP_SAEB` (escala 0-1000, é a usada no corte de 743 — não confundir com `PROFICIENCIA_LP`, z-score padronizado)
- `IN_ALFABETIZADO`

A regra de referência do projeto é:

```text
alfabetizado = proficiencia_portugues >= 743
```

O corte de 743 pontos é aplicado no grão de aluno.

`IN_ALFABETIZADO` é preservado como classificação oficial disponível na fonte.

---

## 5. IBGE - Dimensão de UFs

**Arquivo:** `data/external/estados.csv`  
**Destino:** `workspace.bronze.uf`  
**Grão:** UF  
**Volume validado:** 27 registros

Usada para:

- validação de `sigla_uf`;
- enriquecimento territorial;
- associação de nome da UF e região.

---

## 6. IBGE - Dimensão de Municípios

**Arquivo:** `data/external/municipios.csv`  
**Destino:** `workspace.bronze.municipio`  
**Grão:** município  
**Volume validado:** 5.571 registros

Usada para:

- validação de `id_municipio`;
- enriquecimento territorial;
- associação UF × município;
- nome do município;
- capital;
- coordenadas quando disponíveis.

---

## 7. Replay oficial em streaming

**Origem:** registros da tabela oficial municipal  
**Destino:** `workspace.bronze.eventos_streaming`  
**Fonte válida:** `INEP_OFICIAL_REPLAY`

O streaming não cria um indicador sintético de negócio.

Ele reproduz registros oficiais para demonstrar:

- Structured Streaming;
- `AvailableNow`;
- schema explícito;
- checkpoint;
- deduplicação por `event_id`;
- `MERGE` idempotente;
- quarentena;
- observabilidade.

A execução validada utiliza 8 eventos oficiais.

Um payload inválido controlado com:

```text
source = TESTE_CONTRATO_INVALIDO
```

é usado apenas para comprovar o comportamento do contrato e da quarentena.

Esse registro não entra na Silver analítica.

---

## 8. Manifesto de proveniência

O arquivo:

```text
fontes_oficiais_manifest.json
```

registra informações de proveniência e hash das fontes preparadas.

As antigas fontes sintéticas permanecem somente em:

```text
data/legacy_fontes_derivadas/
```

e não alimentam o pipeline oficial.

---

## 9. Relação entre entidades

```text
INEP indicador UF ───────────────┐
INEP indicador município ────────┤
Replay oficial streaming ────────┤
IBGE UF / município ─────────────┤
Metas Brasil / UF / município ───┘
                                 ↓
                  silver.medicoes_alfabetizacao
                                 ↓
                         QUALITY GATE
                                 ↓
                  silver.medicoes_aprovadas
                                 ↓
                    4 marts territoriais GOLD


Microdados oficiais INEP (TS_ALUNO)
                                 ↓
                       bronze.alunos
                                 ↓
                  silver.alunos_modelagem
                                 ↓
                    QUALITY GATE ALUNOS
                                 ↓
           silver.alunos_modelagem_aprovados
                                 ↓
                gold.base_modelagem_aluno
```

---

## 10. Papel das entidades na Gold

| Entidade Gold | Fonte principal | Grão |
|---|---|---|
| `gold.indicador_municipio` | indicador municipal oficial | ano + município + rede |
| `gold.resumo_uf` | indicador UF oficial | ano + UF + rede |
| `gold.meta_vs_resultado` | indicadores + metas oficiais | território + ano + rede |
| `gold.evolucao_temporal` | indicadores oficiais | território + ano + rede |
| `gold.base_modelagem_aluno` | microdados oficiais INEP aprovados pelo Quality Gate | aluno |

Os marts que combinam níveis territoriais preservam o nível/grão para evitar mistura indevida entre UF e município.

`gold.base_modelagem_aluno` é publicada com os registros identificados como `oficial_inep` que passarem no Quality Gate, preservando o grão individual — a contagem exata varia por execução (ver `08_monitoring.py`).

---

## 11. Preparação para a Fase 3

A Fase 3 exige modelagem supervisionada no grão de aluno. A fundação necessária já está implementada na Fase 2.

Fluxo oficial:

```text
workspace.bronze.alunos
        ↓
workspace.silver.alunos_modelagem
        ↓
workspace.silver.alunos_modelagem_aprovados
        ↓
workspace.gold.base_modelagem_aluno
```

Os volumes de cada etapa variam por execução — confira no output do
`08_monitoring.py` ou rodando a query de contagem do README raiz. Não trate
nenhum número fixo de linhas como constante deste projeto: já houve uma
versão anterior deste documento com "2.120.560" registros que nunca foi de
fato validada contra o microdado real (o manifest de fontes da época
registrava `alunos.status: pendente_microdados_oficiais`).

O target preparado para a classificação é `alfabetizado_oficial`.

A proficiência de Língua Portuguesa permanece disponível na Silver para QA, mas não é publicada como feature em `gold.base_modelagem_aluno`, evitando data leakage associado ao critério de 743 pontos.

Variáveis socioeconômicas e outras fontes complementares poderão ser adicionadas na Fase 3 desde que mantenham proveniência, contrato e grão compatíveis. Não deve ser criada variável sintética para substituir informação oficial inexistente.
