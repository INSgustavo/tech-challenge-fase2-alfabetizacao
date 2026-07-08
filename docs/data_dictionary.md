# Dicionário de Dados

> Este documento descreve a estrutura da base de dados utilizada no projeto, seus campos, tipos e regras conhecidas até o momento.

## Modelo Canônico

| Coluna | Tipo | Obrigatório | Descrição | Origem / Regra |
|---|---|:---:|---|---|
| `ano` | int | Sim | Ano de referência da avaliação. | Fonte |
| `sigla_uf` | string | Sim | Sigla da Unidade da Federação. | Fonte |
| `serie` | int | Sim | Série avaliada. | Fonte |
| `rede` | int | Sim | Código da rede de ensino. O significado dos códigos depende de documentação oficial. | Fonte |
| `taxa_alfabetizacao` | double | Sim | Percentual de estudantes alfabetizados. | Fonte |
| `media_portugues` | double | Sim | Média de proficiência em Língua Portuguesa. | Fonte |
| `proporcao_aluno_nivel_0` | double | Não | Proporção de alunos classificados no nível 0 de proficiência. | Fonte |
| `proporcao_aluno_nivel_1` | double | Não | Proporção de alunos classificados no nível 1 de proficiência. | Fonte |
| `proporcao_aluno_nivel_2` | double | Não | Proporção de alunos classificados no nível 2 de proficiência. | Fonte |
| `proporcao_aluno_nivel_3` | double | Não | Proporção de alunos classificados no nível 3 de proficiência. | Fonte |
| `proporcao_aluno_nivel_4` | double | Não | Proporção de alunos classificados no nível 4 de proficiência. | Fonte |
| `proporcao_aluno_nivel_5` | double | Não | Proporção de alunos classificados no nível 5 de proficiência. | Fonte |
| `proporcao_aluno_nivel_6` | double | Não | Proporção de alunos classificados no nível 6 de proficiência. | Fonte |
| `proporcao_aluno_nivel_7` | double | Não | Proporção de alunos classificados no nível 7 de proficiência. | Fonte |
| `proporcao_aluno_nivel_8` | double | Não | Proporção de alunos classificados no nível 8 de proficiência. | Fonte |

---

## Fonte de Dados

| Entidade | Arquivo | Grão | Chave lógica | Atualização | Responsável | Status |
|---|---|---|---|---|---|---|
| Avaliação de Alfabetização por UF | `br_inep_avaliacao_alfabetizacao_uf.csv` | Ano + UF + Série + Rede | `ano`, `sigla_uf`, `serie`, `rede` | Batch | INEP | Confirmado |

---

## Regras de Qualidade Identificadas

- A base contém **145 registros**.
- O período coberto compreende os anos **2023** e **2024**.
- Não foram encontrados registros duplicados.
- Não foram identificados valores nulos nas colunas `taxa_alfabetizacao` e `media_portugues`.
- Os valores de `taxa_alfabetizacao` encontram-se dentro da faixa esperada para percentuais.
- Os valores de `media_portugues` encontram-se dentro da escala do SAEB.
- Não existem registros para as UFs **RR (Roraima)** e **DF (Distrito Federal)**.
- Há inconsistências na quantidade de registros por UF entre os anos analisados, indicando possível diferença de granularidade.
- O significado dos códigos presentes na coluna `rede` não está documentado na base analisada.

---

## Pendências Documentais

- Confirmar o significado oficial dos códigos da coluna `rede`.
- Confirmar se a ausência de RR e DF é esperada na fonte oficial.
- Confirmar o motivo das diferenças na quantidade de registros entre algumas UFs.
- Validar a documentação oficial das colunas de proporção de alunos por nível de proficiência (`proporcao_aluno_nivel_0` a `proporcao_aluno_nivel_8`).