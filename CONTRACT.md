# CONTRACT.md — contrato comum

> Regras que os 4 seguem. Mudança aqui exige aviso no sync diário.

## Lakehouse / Delta
- Catálogo/schema: `bronze`, `silver`, `gold` (Delta tables gerenciadas no DBFS).
- Particionamento Delta: `ano` / `sigla_uf`.
- Caminho base DBFS: `/FileStore/alfabetizacao/`.

## Nomenclatura
- `snake_case`, sem acento, em colunas e tabelas.

## Chaves
- `sigla_uf` (2 letras) · `id_municipio` (IBGE 7 dígitos, string).

## Domínio
- `alfabetizado = (media_portugues >= 743)`.
- De-para `rede`: 0=total · 2=estadual · 3=municipal · 5=privada  <!-- TODO P2: confirmar no dicionário INEP -->

## Definition of Done por camada
- Bronze: bruto, sem transformação, histórico preservado.
- Silver: limpo, tipado, integrado, chaves normalizadas.
- Gold: marts prontos para consumo + serving MongoDB.
