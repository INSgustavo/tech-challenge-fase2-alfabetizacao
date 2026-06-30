# CONTRACT.md — contrato comum

> Regras que os 4 seguem. Mudança aqui exige aviso no sync diário.

## Lakehouse / Delta (Unity Catalog)

- **Catálogo:** `workspace`
- **Schemas:** `workspace.bronze` · `workspace.silver` · `workspace.gold`
- **Volumes (arquivos brutos):**
  - `workspace.bronze.raw_files` → `/Volumes/workspace/bronze/raw_files/`
  - `workspace.bronze.streaming_landing` → `/Volumes/workspace/bronze/streaming_landing/`
- **Particionamento Delta:** `ano` / `sigla_uf`

> ⚠️ DBFS (`/FileStore/...`) está desabilitado neste workspace. Use sempre Unity Catalog.

## Nomenclatura

- `snake_case`, sem acento, em colunas e tabelas.

## Chaves

- `sigla_uf` (2 letras) · `id_municipio` (IBGE 7 dígitos, string).

## Domínio

- `alfabetizado = (media_portugues >= 743)`.
- De-para `rede`: 0=total · 2=estadual · 3=municipal · 5=privada  <!-- TODO P2: confirmar no dicionário INEP -->

## Referência de tabelas por notebook

| Notebook | Lê de | Escreve em |
|----------|--------|------------|
| 01_bronze_batch | `/Volumes/workspace/bronze/raw_files/*.csv` | `workspace.bronze.*` |
| 02_bronze_streaming | `/Volumes/workspace/bronze/streaming_landing/` | `workspace.bronze.eventos_streaming` |
| 03_silver | `workspace.bronze.avaliacao_alfabetizacao` | `workspace.silver.alfabetizacao` |
| 04_gold | `workspace.silver.alfabetizacao` | `workspace.gold.indicador_municipio` |
| 05_serving | `workspace.gold.indicador_municipio` | MongoDB |
| 06_quality | `workspace.silver.alfabetizacao` | — |
| 07_ml | `workspace.gold.indicador_municipio` | MLflow |

## Definition of Done por camada

- **Bronze:** bruto, sem transformação, histórico preservado.
- **Silver:** limpo, tipado, integrado, chaves normalizadas.
- **Gold:** marts prontos para consumo + serving MongoDB.
