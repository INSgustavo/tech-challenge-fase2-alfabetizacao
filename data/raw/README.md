# Fontes de entrada do pipeline

Estes arquivos são copiados automaticamente para
`/Volumes/workspace/bronze/raw_files/` pelo `00_setup_ambiente.py`, que chama
o script `scripts/gerar_fontes.py` (arquivo próprio, não embutido no
notebook). Não precisa rodar nada manualmente para esta pasta.

| Arquivo | Origem | Observação |
|---|---|---|
| br_inep_avaliacao_alfabetizacao_uf.csv.gz | INEP via Base dos Dados | grão UF, taxa em percentual 0 a 100 |
| br_inep_avaliacao_alfabetizacao_municipio.csv.gz | INEP oficial | grão município, obrigatório para os marts municipais |
| uf.csv | IBGE | 27 UFs com região |
| municipio.csv | IBGE (5.571 municípios) | código de 7 dígitos |
| meta_brasil.csv / meta_uf.csv / meta_municipio.csv | INEP oficial | extraídas diretamente das planilhas oficiais, sem interpolação nem herança de meta |
| fontes_oficiais_manifest.json | gerado pelo pipeline | rastreabilidade e hash das fontes oficiais |
| microdados_inep/DADOS/TS_ALUNO.csv | INEP oficial (Saeb 2023, 2º ano EF) | grão aluno; ano diferente do resto (2023, não 2024) e `id_municipio` anonimizado — ver aviso no README raiz |

O `TS_ALUNO.csv` já está versionado nessa pasta e é publicado
automaticamente no Volume junto com o resto — não precisa mais de upload
manual por pessoa. Se for atualizado (ex.: quando o Saeb 2024 for
publicado), basta substituir o arquivo aqui e rodar o setup de novo.

O `alunos_simulados.csv.gz` (dado sintético usado antes da correção da
Fase 2) foi movido para `data/legacy_fontes_derivadas/` e não faz parte do
fluxo oficial. Não usar para análise real.
