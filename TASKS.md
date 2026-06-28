# TASKS.md — divisão por pessoa (marque conforme avança)

## 🔵 P1 — Plataforma (Databricks · Workflows · Git · FinOps)
- [ ] Criar workspace no Databricks Free Edition; configurar cluster Starter com **auto-termination**.
- [ ] Conectar o repositório via **Databricks Repos** (GitHub) e definir branches main/develop.
- [ ] Criar a estrutura de pastas DBFS (`/FileStore/alfabetizacao/bronze|silver|gold`).
- [ ] Definir e manter o `CONTRACT.md` com o grupo.
- [ ] Montar o **Workflow (Job)** encadeando os notebooks 01→08.
- [ ] Configurar Git: PR por feature, merge só após revisão.
- [ ] Escrever a seção **FinOps**: cluster efêmero, auto-terminate, Spot, Z-ORDER, particionamento + custo estimado.
- [ ] Consolidar o **README** e inserir o diagrama.

## 🩷 P2 — Ingestão Batch (PySpark → Bronze Delta)
- [ ] Mapear as 6 entidades na Base dos Dados e achar os IDs das tabelas.
- [ ] Subir o CSV de avaliação para o DBFS.
- [ ] Ler cada fonte com PySpark e gravar **Bronze como Delta**, particionado `ano`/`sigla_uf`.
- [ ] Garantir Bronze = bruto (sem transformação; só ingestão + histórico).
- [ ] **Entregar 2 tabelas em Bronze até o Dia 2** (destrava o P4).
- [ ] Escrever `docs/data_dictionary.md` e confirmar o de-para de `rede`.
- [ ] (Adicional) Enriquecimento: Censo Escolar / IBGE por município → Bronze.

## 🌸 P3 — Streaming + Observabilidade
- [ ] `producer`: notebook que gera eventos de "novas medições" gravando arquivos numa landing.
- [ ] **Spark Structured Streaming** (`readStream`) lendo a landing → append na zona streaming do Bronze Delta.
- [ ] Garantir que o evento segue o mesmo contrato do batch (integra na Silver).
- [ ] Métricas: falhas, latência (evento→Bronze), volume, alertas (via streaming metrics + job logs).
- [ ] Escrever as seções "Streaming" e "Monitoramento" do README (frisar que é simulação).

## 💗 P4 — Analytics (Silver · Gold · Qualidade · Serving · IA)
- [ ] **Silver** (PySpark): limpeza, tratar nulos de `proporcao_aluno_nivel_X`, padronizar `rede`, normalizar chaves, **integrar as 6 bases**, criar flag `alfabetizado`.
- [ ] **Gold** (Spark SQL): indicador por município · meta vs. resultado · evolução 2023→2025 (marts Delta).
- [ ] **Qualidade**: validações PySpark + Delta constraints (duplicidade, nulos, chaves, accepted values).
- [ ] **Serving**: exportar Gold → **MongoDB** (um documento por município).
- [ ] (Adicional) **MLflow**: regressão da taxa por município / clustering de vulnerabilidade.
- [ ] (Adicional) Dashboard sobre a Gold.
- [ ] Escrever a seção "Aplicação em IA" e **apresentar o vídeo**.
