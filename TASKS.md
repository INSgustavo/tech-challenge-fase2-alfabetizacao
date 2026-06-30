# TASKS.md — Backlog do time

Use os marcadores abaixo como checklist. Cada item concluído deve ter evidência no PR, no notebook ou na documentação.

## Entregas compartilhadas

- [ ] Validar a arquitetura e o contrato comum.
- [ ] Definir o grão correto de cada fonte e tabela Gold.
- [ ] Executar revisão cruzada entre integrantes.
- [ ] Rodar o pipeline ponta a ponta com um único `run_id`.
- [ ] Registrar evidências: prints, queries, métricas e amostras.
- [ ] Ensaiar o roteiro de demonstração.
- [ ] Criar tag da versão final.

## P1 — Plataforma, DevOps e governança

### Essencial

- [ ] Configurar Databricks Repos e estratégia `feature/* → develop → main`.
- [ ] Criar schemas e volumes do Unity Catalog.
- [ ] Montar o Workflow completo com dependências corretas.
- [ ] Configurar auto-termination e política de cluster.
- [ ] Consolidar README, arquitetura e contrato.

### Diferenciais

- [ ] Criar tabela `observability.pipeline_metrics`.
- [ ] Implementar função de auditoria reutilizável.
- [ ] Adicionar GitHub Action para validar Python, JSON e Markdown.
- [ ] Criar runbook de falha, reprocessamento e demo.
- [ ] Configurar secrets para MongoDB.
- [ ] Documentar estratégia de custo baseada em métricas reais.
- [ ] Criar release tag e changelog da entrega.

## P2 — Fontes, profiling e Bronze batch

### Essencial

- [ ] Mapear as entidades e confirmar a fonte oficial.
- [ ] Completar `docs/data_dictionary.md`.
- [ ] Implementar schemas explícitos.
- [ ] Gravar as fontes em Delta com metadados técnicos.
- [ ] Reconciliar contagem de origem e destino.

### Diferenciais

- [ ] Criar relatório de profiling por fonte.
- [ ] Registrar hash ou versão do arquivo de origem.
- [ ] Implementar ingestão incremental ou por partição.
- [ ] Validar mudança inesperada de schema.
- [ ] Enriquecer com IBGE ou Censo Escolar.
- [ ] Criar uma pequena amostra versionável para testes.

## P3 — Streaming, resiliência e observabilidade

### Essencial

- [ ] Implementar producer de eventos JSON.
- [ ] Implementar consumer Structured Streaming.
- [ ] Usar schema explícito e checkpoint isolado.
- [ ] Registrar volume, latência e falhas.

### Diferenciais

- [ ] Incluir `event_id`, `event_time`, `source` e `schema_version`.
- [ ] Deduplicar por `event_id`.
- [ ] Criar quarentena para payload inválido.
- [ ] Calcular atraso entre evento e ingestão.
- [ ] Criar rotina de replay da quarentena.
- [ ] Simular evento duplicado, atrasado e com schema inválido.
- [ ] Definir SLIs, SLOs e alertas do pipeline.

## P4 — Silver, Gold, serving e IA

### Essencial

- [ ] Criar modelo canônico integrando batch e streaming.
- [ ] Normalizar chaves e domínio de rede.
- [ ] Implementar marts Gold com grão documentado.
- [ ] Implementar controles de qualidade.
- [ ] Publicar dados no MongoDB.
- [ ] Registrar experimento no MLflow.

### Diferenciais

- [ ] Criar `record_id` determinístico.
- [ ] Implementar `upsert` no MongoDB em vez de apagar a coleção.
- [ ] Criar mart de meta versus resultado e evolução temporal.
- [ ] Comparar baseline com outro modelo.
- [ ] Criar model card com limitações e riscos.
- [ ] Implementar dashboard ou notebook executivo.
- [ ] Adicionar teste de não regressão para indicadores principais.

## Critérios para considerar o projeto pronto

- [ ] README não contém números ou métricas não reproduzidos pelo código.
- [ ] `gold.indicador_municipio` realmente possui `id_municipio` no grão.
- [ ] Silver depende das duas entradas: batch e streaming.
- [ ] Quality Gate executa antes da Gold.
- [ ] Workflow inclui setup, ingestão, Silver, qualidade, Gold, serving, ML e monitoramento.
- [ ] Quarentena e auditoria estão demonstráveis.
- [ ] Não há segredo versionado.
- [ ] O fluxo completo pode ser explicado em até dez minutos.
