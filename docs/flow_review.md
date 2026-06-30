# Revisão do fluxo e melhorias propostas

## Pontos identificados na versão inicial

1. O streaming escrevia na Bronze, mas não participava da construção da Silver.
2. A tabela `indicador_municipio` agrupava apenas por UF e rede, sem `id_municipio`.
3. O Workflow não executava MLflow nem monitoramento.
4. A qualidade era executada depois da Gold ou em paralelo, sem bloquear a publicação.
5. O checkpoint ficava dentro da própria landing zone do streaming.
6. O producer exibia um evento, mas não o gravava.
7. O serving usava `toPandas()` e sugeria apagar toda a coleção.
8. O README apontava para `architecture.png`, arquivo inexistente.
9. O diagrama apresentava métricas fixas que ainda não eram reproduzidas pelo código.
10. O arquivo de exclusões estava nomeado `gitignore`, e não `.gitignore`.

## Fluxo recomendado

```text
Fontes
  ├─ Batch ───────────────┐
  └─ Streaming ───────────┤
                          v
                 Validação de contrato
                    ├─ inválido → Quarentena
                    └─ válido   → Bronze
                                      v
                            Silver canônica
                                      v
                              Quality Gate
                    ├─ falha sistêmica → interrompe
                    └─ aprovado        → Gold
                                      v
                    MongoDB · MLflow · Dashboard · API
```

## Por que esse desenho é melhor

- **Consistência:** batch e streaming alimentam o mesmo modelo de negócio.
- **Resiliência:** registros ruins ficam disponíveis para investigação e replay.
- **Auditabilidade:** cada execução produz métricas e um identificador comum.
- **Confiabilidade:** a Gold só é publicada depois dos testes.
- **Escalabilidade:** serving usa operações incrementais, evitando cópia integral local.
- **Demonstração:** o time consegue mostrar erros controlados, recuperação e rastreabilidade.

## Prioridade de implementação

### Prioridade 1 — corrigir incoerências

- corrigir grão da Gold;
- integrar o streaming na Silver;
- reordenar o Workflow;
- remover métricas não comprovadas do diagrama e README;
- criar `.gitignore` válido.

### Prioridade 2 — confiabilidade

- schema explícito;
- `record_id` e `event_id`;
- quarentena;
- Quality Gate;
- tabela de auditoria.

### Prioridade 3 — diferenciais

- replay da quarentena;
- CI;
- dashboard;
- model card;
- análise de custo por execução;
- API de consulta.
