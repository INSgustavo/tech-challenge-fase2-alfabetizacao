# Dicionário de dados

> O responsável por cada fonte deve completar tipo de origem, nulabilidade, domínio e regra de transformação. Não preencher por suposição.

## Modelo canônico Silver

| coluna | tipo | obrigatório | descrição | origem / regra |
|---|---|---:|---|---|
| `record_id` | string | sim | hash determinístico do registro | gerado na Silver |
| `ano` | int | sim | ano de referência | fonte |
| `sigla_uf` | string | sim | UF em duas letras | normalizada |
| `id_municipio` | string | sim | código IBGE com sete dígitos | normalizado |
| `rede` | int | sim | código da rede de ensino | fonte |
| `rede_label` | string | sim | rótulo padronizado | de-para do contrato |
| `media_portugues` | double | não | média na escala da fonte | batch |
| `taxa_alfabetizacao` | double | não | proporção normalizada entre 0 e 1 | batch ou streaming |
| `alfabetizado` | boolean | não | aplicação da regra de corte quando possível | regra versionada |
| `source` | string | sim | sistema ou produtor | metadado |
| `event_time` | timestamp | não | instante do evento de streaming | streaming |
| `processed_at` | timestamp | sim | instante de processamento | Silver |
| `schema_version` | string | sim | versão do contrato | fonte / pipeline |

## Fontes

| entidade | arquivo ou tabela | grão | chave | atualização | responsável | status |
|---|---|---|---|---|---|---|
| avaliação de alfabetização | a confirmar | a confirmar | a confirmar | a confirmar | P2 | pendente |
| UF | a confirmar | UF | `sigla_uf` | a confirmar | P2 | pendente |
| município | a confirmar | município | `id_municipio` | a confirmar | P2 | pendente |
| meta Brasil | a confirmar | ano | `ano` | a confirmar | P2 | pendente |
| meta UF | a confirmar | ano + UF | `ano, sigla_uf` | a confirmar | P2 | pendente |
| meta município | a confirmar | ano + município | `ano, id_municipio` | a confirmar | P2 | pendente |

## Regras que precisam de validação documental

- de-para do campo `rede`;
- unidade original de `taxa_alfabetizacao`;
- regra e referência do corte `743`;
- grão real da tabela de avaliação;
- cobertura temporal e territorial;
- tratamento de registros agregados sem município.
