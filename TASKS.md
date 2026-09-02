# TASKS.md — Backlog do time

Status atualizado em 2026-09-01.

> Status do projeto: a entrega principal do pipeline foi concluída e validada. Os itens restantes abaixo representam evolução de roadmap, release final e continuidade operacional, e não invalidam a entrega atual.

Use os marcadores abaixo como checklist. Cada item concluído deve ter evidência no PR, no notebook ou na documentação (prints em `docs/evidencias/`).

## Entregas compartilhadas

- [x] Validar a arquitetura e o contrato comum. *(CONTRACT.md, docs/flow_review.md)*
- [x] Definir o grão correto de cada fonte e tabela Gold. *(CONTRACT.md, notebooks 03/04)*
- [x] Executar revisão cruzada entre integrantes.
- [x] Rodar o pipeline ponta a ponta com um único `run_id`. *(workflows/job_pipeline.json injeta `{{job.run_id}}` em todas as tasks)*
- [x] Registrar evidências: prints, queries, métricas e amostras. *(32 prints em docs/evidencias/)*
- [x] Ensaiar o roteiro de demonstração. *(docs/runbook.md + docs/video_roteiro.md)*
- [ ] Criar tag da versão final. *(pendente de release final / geração de tag no GitHub)*

## P1 — Plataforma, DevOps e governança

### Essencial

- [x] Configurar Databricks Repos e estratégia `feature/* → develop → main`. *(CONTRIBUTING.md)*
- [x] Criar schemas e volumes do Unity Catalog. *(notebook 00, evidências 01–03)*
- [x] Montar o Workflow completo com dependências corretas. *(evidência 32)*
- [x] Configurar auto-termination e política de cluster. *(serverless — gerenciado pela plataforma)*
- [x] Consolidar README, arquitetura e contrato.

### Diferenciais

- [x] Criar tabela `observability.pipeline_metrics`. *(notebook 00; usada por 06 e 08)*
- [ ] Implementar função de auditoria reutilizável. *(roadmap de governança e reutilização)*
- [ ] Adicionar GitHub Action para validar Python, JSON e Markdown. *(não entregue nesta fase; backlog de CI/CD)*
- [x] Criar runbook de falha, reprocessamento e demo. *(docs/runbook.md)*
- [x] Configurar secrets para MongoDB. *(dbutils.secrets no notebook 05)*
- [x] Documentar estratégia de custo baseada em métricas reais. *(README, seção FinOps)*
- [ ] Criar release tag e changelog da entrega. *(pendente de fechamento oficial da versão)*

## P2 — Fontes, profiling e Bronze batch

### Essencial

- [x] Mapear as entidades e confirmar a fonte oficial. *(docs/fontes_e_entidades.md)*
- [x] Completar `docs/data_dictionary.md`.
- [x] Implementar schemas explícitos. *(notebook 01 — todas as fontes)*
- [x] Gravar as fontes em Delta com metadados técnicos.
- [x] Reconciliar contagem de origem e destino. *(notebook 01)*

### Diferenciais

- [ ] Criar relatório de profiling por fonte.
- [ ] Registrar hash ou versão do arquivo de origem.
- [ ] Implementar ingestão incremental ou por partição.
- [ ] Validar mudança inesperada de schema.
- [ ] Enriquecer com IBGE ou Censo Escolar. *(dimensões IBGE em data/external/; socioeconômico no roadmap)*
- [ ] Criar uma pequena amostra versionável para testes.

## P3 — Streaming, resiliência e observabilidade

### Essencial

- [x] Implementar producer de eventos JSON. *(notebook 02)*
- [x] Implementar consumer Structured Streaming. *(AvailableNow + foreachBatch)*
- [x] Usar schema explícito e checkpoint isolado.
- [x] Registrar volume, latência e falhas. *(notebook 08 — percentis de latência)*

### Diferenciais

- [x] Incluir `event_id`, `event_time`, `source` e `schema_version`.
- [x] Deduplicar por `event_id`. *(MERGE Delta)*
- [x] Criar quarentena para payload inválido. *(observability.quarantine_records)*
- [x] Calcular atraso entre evento e ingestão. *(notebook 08)*
- [ ] Criar rotina de replay da quarentena. *(roadmap)*
- [x] Simular evento duplicado, atrasado e com schema inválido. *(cenário de demo do notebook 02)*
- [x] Definir SLIs, SLOs e alertas do pipeline. *(THRESHOLDS no notebook 08)*

## P4 — Silver, Gold, serving e IA

### Essencial

- [x] Criar modelo canônico integrando batch e streaming. *(notebook 03)*
- [x] Normalizar chaves e domínio de rede.
- [x] Implementar marts Gold com grão documentado. *(notebook 04)*
- [x] Implementar controles de qualidade. *(notebook 06 — inclui integridade referencial)*
- [x] Publicar dados no MongoDB. *(notebook 05)*
- [x] Registrar experimento no MLflow. *(notebook 07)*

### Diferenciais

- [x] Criar `record_id` determinístico.
- [x] Implementar `upsert` no MongoDB em vez de apagar a coleção.
- [x] Criar mart de meta versus resultado e evolução temporal.
- [x] Comparar baseline com outro modelo. *(baseline vs Random Forest)*
- [x] Criar model card com limitações e riscos. *(notebook 07)*
- [x] Implementar dashboard ou notebook executivo. *(notebook 09)*
- [ ] Adicionar teste de não regressão para indicadores principais.

## Critérios para considerar o projeto pronto

- [x] README não contém números ou métricas não reproduzidos pelo código.
- [x] `gold.indicador_municipio` realmente possui `id_municipio` no grão.
- [x] Silver depende das duas entradas: batch e streaming — **e integra as dimensões, metas e alunos via join**.
- [x] Quality Gate executa antes da Gold.
- [x] Workflow inclui setup, ingestão, Silver, qualidade, Gold, serving, ML e monitoramento.
- [x] Quarentena e auditoria estão demonstráveis. *(evidências + tabelas observability)*
- [x] Não há segredo versionado.
- [x] O fluxo completo pode ser explicado em até dez minutos. *(docs/video_roteiro.md)*
- [ ] Vídeo executivo gravado e link adicionado ao README. *(entrega de comunicação / apresentação final)*
- [ ] Link do repositório GitHub (com histórico de commits, branches e PRs) incluído na entrega. *(revisar no README antes da publicação final)*
