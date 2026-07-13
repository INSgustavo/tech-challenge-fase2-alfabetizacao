# Roteiro — Vídeo Executivo (até 5 minutos)

Apresentação em linguagem executiva, simulando uma reunião com liderança /
stakeholders de uma organização pública de análise educacional. Um integrante
apresenta; a tela alterna entre slides simples e o Databricks.

## 0:00 – 0:45 · O problema de negócio

> "O Brasil assumiu um compromisso: toda criança alfabetizada até o final do
> 2º ano até 2030. O INEP definiu a régua — 743 pontos na escala Saeb — e
> criou o Indicador Criança Alfabetizada. O problema: os dados que explicam
> esse indicador estão espalhados em seis fontes diferentes — metas nacionais,
> estaduais e municipais, dados territoriais e microdados de alunos. Hoje,
> comparar resultado com meta por município é um trabalho manual e frágil."

*Tela: slide com a meta 2030 e as seis fontes desconectadas.*

## 0:45 – 2:00 · A solução (arquitetura)

> "Construímos um pipeline híbrido na nuvem, no Databricks. Dados históricos
> entram em batch; atualizações de medições chegam como eventos em streaming,
> com deduplicação e quarentena automática do que viola o contrato. Tudo flui
> pela arquitetura Medalhão: Bronze guarda o bruto, a Silver integra as seis
> fontes num modelo único — cada medição já sai com sua meta, seu município e
> sua região — e a Gold publica indicadores prontos para consumo."

*Tela: diagrama `docs/architecture.png`, depois o grafo do Workflow verde
(evidência 32).*

> "Antes de qualquer indicador ser publicado, um Quality Gate bloqueante
> valida duplicidade, domínios e integridade referencial. Indicador errado
> simplesmente não chega ao gestor."

## 2:00 – 3:15 · Valor para análises educacionais

> "O que a liderança ganha com isso: três respostas imediatas. Onde estamos
> versus a meta — a tabela meta_vs_resultado mostra o gap de cada território.
> Para onde vamos — a evolução temporal mostra tendência de alta ou queda por
> rede de ensino. E onde está a desigualdade — dá para comparar regiões,
> redes pública e privada, capitais e interior."

*Tela: dashboard (notebook 09) ou consulta SQL na Gold; documento no MongoDB
mostrando o consumo por aplicação.*

## 3:15 – 4:15 · Potencial de IA

> "A mesma base alimenta inteligência artificial. Já treinamos um modelo de
> referência com MLflow que estima a taxa de alfabetização — o caminho para
> prever, hoje, quais municípios não atingirão a meta de 2030 e agir antes da
> próxima avaliação. Com enriquecimento socioeconômico, o passo seguinte são
> clusters de vulnerabilidade educacional para priorizar investimento."

*Tela: experimento no MLflow (baseline vs modelo).*

## 4:15 – 5:00 · Custo e fechamento

> "Tudo isso rodou com custo zero na Free Edition. Em produção, com os
> microdados completos, a arquitetura serverless que escolhemos custa cerca
> de US$ 140 por mês — 75% menos que um cluster dedicado. É uma fundação de
> dados barata, auditável e pronta para escalar. Obrigado."

*Tela: tabela de estimativa FinOps do README.*

---

**Checklist de gravação:** ≤ 5 min · pelo menos um integrante em câmera ou
narrando · linguagem executiva (sem jargão de código) · cobre problema,
arquitetura, valor analítico e IA · link do vídeo adicionado ao README.
