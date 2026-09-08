# Fontes de entrada do pipeline

Estes arquivos são gerados e copiados automaticamente para
`/Volumes/workspace/bronze/raw_files/` pelo próprio `00_setup_ambiente.py`
(a lógica que antes era o script separado `scripts/gerar_fontes.py` está
embutida nele agora). Não precisa rodar nada manualmente para esta pasta.

| Arquivo | Origem | Observação |
|---|---|---|
| br_inep_avaliacao_alfabetizacao_uf.csv.gz | INEP via Base dos Dados | grão UF, taxa em percentual 0 a 100 |
| br_inep_avaliacao_alfabetizacao_municipio.csv.gz | INEP oficial | grão município, obrigatório para os marts municipais |
| uf.csv | IBGE | 27 UFs com região |
| municipio.csv | IBGE (5.571 municípios) | código de 7 dígitos |
| meta_brasil.csv / meta_uf.csv / meta_municipio.csv | INEP oficial | extraídas diretamente das planilhas oficiais, sem interpolação nem herança de meta |
| fontes_oficiais_manifest.json | gerado pelo pipeline | rastreabilidade e hash das fontes oficiais |

O único arquivo que não é gerado automaticamente é o microdado de aluno
(`TS_ALUNO.csv`), que precisa ser baixado do INEP e enviado manualmente
para `/Volumes/workspace/bronze/raw_files/microdados_inep/DADOS/` antes de
rodar `01_bronze_batch.py`.

O `alunos_simulados.csv.gz` (dado sintético usado antes da correção da
Fase 2) foi movido para `data/legacy_fontes_derivadas/` e não faz parte do
fluxo oficial. Não usar para análise real.
