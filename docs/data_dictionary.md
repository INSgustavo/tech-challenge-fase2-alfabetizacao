# Dicionário de Dados

> Descreve as fontes e as tabelas produzidas em cada camada do Medalhão. As
> convenções (tipos, chaves, domínios, metadados obrigatórios) são definidas no
> [CONTRACT.md](../CONTRACT.md) — aqui está a materialização delas.

---

## 1. Fontes (entrada)

| Entidade | Arquivo | Grão | Chave lógica | Origem |
|---|---|---|---|---|
| Avaliação de alfabetização por UF | `br_inep_avaliacao_alfabetizacao_uf.csv.gz` | ano + UF + série + rede | `ano`, `sigla_uf`, `serie`, `rede` | INEP / Base dos Dados |
| Indicador por município (opcional) | `br_inep_avaliacao_alfabetizacao_municipio.csv.gz` | ano + município + rede | `ano`, `id_municipio`, `rede` | INEP / Base dos Dados |
| Eventos de alfabetização | landing JSON | evento | `event_id` | Simulador (notebook 02) |
| Dimensão UF | `uf.csv` | UF | `sigla_uf` | IBGE |
| Dimensão município | `municipio.csv` | município | `id_municipio` | IBGE |
| Meta nacional | `meta_brasil.csv` | ano | `ano` | Derivada |
| Meta por UF | `meta_uf.csv` | ano + UF | `ano`, `sigla_uf` | Derivada |
| Meta por município | `meta_municipio.csv` | ano + município | `ano`, `id_municipio` | Derivada |
| Microdados de alunos | `alunos_simulados.csv.gz` | aluno | `aluno_id` | **SIMULADO** |

> **A fonte municipal não traz `sigla_uf`.** A UF é derivada na Silver pelo join
> com `bronze.municipio` (notebook 03, seção 1b). Nunca leia `sigla_uf` direto de
> `bronze.avaliacao_alfabetizacao_municipio`.

---

## 2. Bronze

Cópia fiel da fonte, com os metadados técnicos exigidos pelo contrato (§6):
`ingestion_timestamp`, `source_file`, `source_system`, `pipeline_run_id`,
`schema_version`.

| Tabela | Grão |
|---|---|
| `bronze.avaliacao_alfabetizacao` | ano + UF + série + rede |
| `bronze.avaliacao_alfabetizacao_municipio` | ano + município + rede |
| `bronze.eventos_streaming` | evento (`event_id` único, deduplicado por MERGE) |
| `bronze.uf`, `bronze.municipio` | dimensões territoriais |
| `bronze.meta_brasil`, `bronze.meta_uf`, `bronze.meta_municipio` | metas |
| `bronze.alunos` | aluno (simulado) |

---

## 3. Silver

### `silver.medicoes_alfabetizacao` — modelo canônico

Grão: uma medição por `record_id`. Integra batch, streaming, dimensões, metas e o
agregado de alunos.

| Coluna | Tipo | Descrição | Regra / Origem |
|---|---|---|---|
| `record_id` | string | Chave determinística da medição. | SHA-256 de ano + UF + município + série + rede + source + event_id |
| `ano` | int | Ano de referência. | Fonte |
| `sigla_uf` | string | UF (2 letras maiúsculas). | Fonte; no grão municipal, **derivada da dimensão** |
| `id_municipio` | string | Código IBGE, 7 dígitos. **Nulo no grão UF.** | Fonte / dimensão |
| `grao` | string | `uf` ou `municipio`. Define o nível territorial. | Derivada |
| `serie` | int | Série avaliada. | Fonte |
| `rede` | int | Código da rede: `0` total, `2` estadual, `3` municipal, `5` privada. | Contrato §4 |
| `rede_label` | string | Rótulo do código acima. | `REDE_MAP` (src/utils.py) |
| `taxa_alfabetizacao` | double | Indicador Criança Alfabetizada, **fração 0–1**. | Batch chega em 0–100 e é dividido por 100 |
| `media_portugues` | double | Média de proficiência agregada. | Fonte |
| `alfabetizado` | boolean | `media_portugues >= 743`. **Sinal auxiliar sobre uma média** — não significa "a UF é alfabetizada" (contrato §4). | Regra |
| `alfabetizacao_rule_version` | string | Versão da regra do corte. | Contrato §4 |
| `nome_municipio`, `nome_uf`, `regiao`, `capital` | string/int | Atributos territoriais. | Join com dimensões IBGE |
| `uf_consistente` | boolean | A UF do registro bate com a UF do município na dimensão. | Validado no Gate |
| `meta_municipio`, `meta_uf`, `meta_brasil` | double | Metas do respectivo grão, fração 0–1. | Join com metas |
| `meta_taxa` | double | **Meta do grão da medição**: municipal quando há; senão a estadual. | Derivada |
| `event_id`, `event_time` | string/timestamp | Identidade do evento. Nulos no batch. | Streaming |
| `source` | string | Produtor: `batch_inep`, `batch_inep_municipio`, `stream_*`. | Derivada |
| `fonte_dados` | string | `oficial_inep` ou `simulado`. Acompanha o dado até o painel. | Derivada |
| `processed_at` | timestamp | Momento do processamento (UTC). | Contrato §6 |

### `silver.alunos_proficiencia` — grão de aluno

Microdados **SIMULADOS**. Existe para que a distribuição em torno do corte de 743
seja servida pela Gold, sem que o consumidor leia a Bronze.

| Coluna | Tipo | Descrição |
|---|---|---|
| `record_id` | string | SHA-256 de aluno_id + ano + UF + rede + source. |
| `aluno_id` | string | Identificador do aluno. |
| `ano`, `sigla_uf`, `serie`, `rede`, `rede_label` | — | Chaves normalizadas. |
| `proficiencia_portugues` | double | Proficiência do aluno (escala Saeb, 0–1000). |
| `alfabetizado` | boolean | `proficiencia_portugues >= 743`. Aqui o corte é aplicado **no grão em que o contrato o define**: por aluno. |
| `alfabetizacao_rule_version` | string | Versão da regra. |
| `fonte_dados` | string | Sempre `simulado`. |
| `source`, `event_time`, `processed_at` | — | Metadados obrigatórios da Silver. |

### Tabelas aprovadas pelo Quality Gate (notebook 06)

| Tabela | Descrição |
|---|---|
| `silver.medicoes_aprovadas` | Medições que passaram no Gate. **Fonte dos marts principais.** |
| `silver.alunos_aprovados` | Alunos que passaram no Gate. **Fonte do mart de distribuição.** |

Reprovados vão para `observability.quarantine_records` com `rejection_reason`.

---

## 4. Gold

> **Coluna transversal `fonte_preferencial`** (marts 1 e 3): um mesmo território
> pode ter medição oficial *e* simulada no mesmo ano. As duas linhas são
> preservadas para rastreabilidade, e esta flag elege a que o consumidor deve usar
> — o oficial ganha do simulado. **Serving, ML e dashboard filtram por ela**; sem o
> filtro, médias misturam dado real com simulado e o upsert do MongoDB perde
> documentos por colisão de chave.

| Mart | Grão | Observação |
|---|---|---|
| `gold.indicador_municipio` | ano + município + rede + fonte | Fonte do serving (MongoDB) e do ML. |
| `gold.resumo_uf` | ano + UF + rede | **Somente dado oficial do INEP** (`grao = 'uf'`). `municipios_cobertos` é métrica de cobertura e vem de fora do agregado. |
| `gold.meta_vs_resultado` | ano + território + rede + fonte | Cobre os dois grãos. Traz `gap_meta`, `atingiu_meta`, `atingiu_meta_brasil`. |
| `gold.evolucao_temporal` | ano + território + rede | Variação anual e `tendencia` (alta / queda / estável). |
| `gold.distribuicao_proficiencia` | ano + UF + rede + faixa | **SIMULADO.** Traz `faixa_pontos` (blocos de 25, histograma) e `faixa_label` (bandas de leitura). A segunda **não** é derivável da primeira: o corte de 743 cai dentro do bloco 725–749. |

---

## 5. Cobertura conhecida dos dados

Fatos observados na base atual — importantes para ler os indicadores sem se
enganar:

- A fonte do INEP tem **145 registros**, cobrindo **2023 e 2024**, sem duplicidade.
- **RR e DF não existem na fonte.** Como as metas são derivadas do baseline
  observado, essas duas UFs **não têm meta alguma**, e 16 municípios (15 de RR e
  Brasília) ficam sem meta municipal. O denominador dos KPIs de meta é **25 UFs**,
  não 27.
- As metas cobrem **2024 a 2030**; os fatos cobrem **2023 e 2024**. Logo:
  - **2023 não tem meta** — é o ano-base do qual a trajetória foi projetada. No
    painel, todas as UFs aparecem como "Meta indisponível" nesse ano.
  - As metas de **2026 a 2030 não têm fato correspondente** e não aparecem no
    join — são alvos futuros, órfãos por natureza. O notebook 03 (seção 5b)
    reconcilia e imprime isso a cada execução.
- `taxa_alfabetizacao` chega em 0–100 na fonte batch e em 0–1 no streaming; a
  Silver normaliza tudo para **fração 0–1**.
- Os códigos de `rede` estão documentados no contrato (§4) e validados no Gate.

---

## 6. Observability

| Tabela | Conteúdo |
|---|---|
| `observability.pipeline_metrics` | Auditoria por task: `run_id`, `status`, linhas lidas/escritas/rejeitadas, duração, erro. |
| `observability.quarantine_records` | Registros reprovados, com `rejection_reason` e payload original em JSON. |
