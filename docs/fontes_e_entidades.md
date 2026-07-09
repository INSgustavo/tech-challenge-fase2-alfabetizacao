# Fontes e Entidades

## Objetivo

Este documento descreve as entidades utilizadas no projeto, suas respectivas fontes oficiais e o nível de granularidade dos dados. O objetivo é documentar a origem dos dados que alimentam a camada Bronze e servir como referência para as próximas etapas do pipeline.

---

## Fonte Oficial

- **Instituição:** Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira (INEP)
- **Arquivo utilizado:** `br_inep_avaliacao_alfabetizacao_uf.csv`
- **Formato:** CSV
- **Tipo de ingestão:** Batch
- **Camada de destino:** Bronze (Delta Lake)

---

## Entidades

| Entidade | Descrição | Grão | Chave lógica |
|----------|-----------|-------|--------------|
| Avaliação de Alfabetização por UF | Indicadores de alfabetização e desempenho em Língua Portuguesa por Unidade da Federação, ano, série e rede de ensino. | Ano + UF + Série + Rede | `ano`, `sigla_uf`, `serie`, `rede` |

---

## Campos da Entidade

- ano
- sigla_uf
- serie
- rede
- taxa_alfabetizacao
- media_portugues
- proporcao_aluno_nivel_0
- proporcao_aluno_nivel_1
- proporcao_aluno_nivel_2
- proporcao_aluno_nivel_3
- proporcao_aluno_nivel_4
- proporcao_aluno_nivel_5
- proporcao_aluno_nivel_6
- proporcao_aluno_nivel_7
- proporcao_aluno_nivel_8

---

## Observações

Durante a etapa de profiling da base foram identificadas as seguintes características:

- A base contém **145 registros**.
- O período disponível compreende os anos **2023** e **2024**.
- Não há registros para as UFs **RR (Roraima)** e **DF (Distrito Federal)**.
- Foram identificadas diferenças na quantidade de registros por UF entre os anos, indicando possível inconsistência de granularidade.
- Não foram encontrados registros duplicados.
- Não foram identificados valores nulos nas colunas `taxa_alfabetizacao` e `media_portugues`.
- Os valores de `taxa_alfabetizacao` e `media_portugues` encontram-se dentro das faixas esperadas.
- O significado dos códigos presentes na coluna `rede` não está documentado na base analisada, sendo necessária validação junto à documentação oficial do INEP.

---

## Considerações

A base apresenta qualidade satisfatória para ingestão na camada Bronze. As principais pendências identificadas estão relacionadas à documentação dos códigos da coluna `rede`, à ausência de registros para RR e DF e às diferenças de granularidade observadas entre algumas Unidades da Federação.