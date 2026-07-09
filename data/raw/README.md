# Fontes de entrada do pipeline

Suba todos os arquivos desta pasta para `/Volumes/workspace/bronze/raw_files/`
antes de rodar o notebook 01.

| Arquivo | Origem | Observação |
|---|---|---|
| br_inep_avaliacao_alfabetizacao_uf.csv.gz | INEP via Base dos Dados | grão UF; taxa em percentual 0–100 |
| uf.csv | IBGE | 27 UFs com região |
| municipio.csv | IBGE (5.570 municípios) | código de 7 dígitos |
| meta_brasil.csv / meta_uf.csv / meta_municipio.csv | **derivadas** | metodologia no CONTRACT.md |
| alunos_simulados.csv.gz | **simulação documentada** | NÃO usar para análise real |

Para regenerar as fontes derivadas: `python scripts/gerar_fontes.py`
(metodologia das metas e da simulação de alunos documentada no CONTRACT.md).
