# Databricks notebook source
# MAGIC %md
# MAGIC # Runner do pipeline completo
# MAGIC Chama cada notebook via `dbutils.notebook.run`, na ORDEM REAL de
# MAGIC dependência (não a ordem numérica dos arquivos).
# MAGIC
# MAGIC Ordem: `00 -> {01, 02 em paralelo} -> 03 -> 06 -> 04 -> {05, 07 em paralelo} -> 08 -> 09`
# MAGIC
# MAGIC O `00` (setup) já dispara a preparação das fontes oficiais
# MAGIC internamente (lógica de `gerar_fontes.py` embutida no próprio
# MAGIC `00_setup_ambiente.py`, ao final, depois de criar os schemas e o
# MAGIC Volume `bronze.raw_files`). Por isso não existe uma etapa
# MAGIC `gerar_fontes` separada aqui: rodar `setup` já cobre schemas,
# MAGIC Volumes, tabelas de observabilidade e a cópia das fontes pro Volume,
# MAGIC tudo em uma chamada só.
# MAGIC
# MAGIC Por que o `06` vem antes do `04`: o Gold (`04`) lê de
# MAGIC `silver.medicoes_aprovadas` e `silver.alunos_modelagem_aprovados`,
# MAGIC que só existem depois do Quality Gate (`06`) aprovar a Silver. Rodar
# MAGIC na ordem numérica (`04` antes do `06`) quebra com tabela inexistente.
# MAGIC
# MAGIC O `09` (dashboard) depende do `08` (monitoring), não direto do Gold,
# MAGIC porque ele também lê `observability.pipeline_metrics`/
# MAGIC `quarantine_records`. Sem o `08` já ter rodado, a aba de saúde do
# MAGIC pipeline sobe vazia.

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Detecção do ambiente e lista de etapas
# MAGIC Acha sozinho a pasta `notebooks/` do projeto (sem ninguém precisar
# MAGIC preencher nada), usando o próprio caminho do runner em execução.
# MAGIC Define também a ordem e as dependências de cada etapa.

# COMMAND ----------
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Detecta sozinho a pasta notebooks/ do projeto, sem ninguém precisar
# preencher nada. Pega o caminho do próprio runner em execução (que fica em
# workflows/00_pipeline_runner), sobe um nível pra raiz do projeto, e desce
# em notebooks/. O prefixo /Workspace precisa ser somado na mão porque a
# API do Databricks devolve o caminho sem ele (comportamento documentado).
try:
    _runner_path = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )
    _project_root = Path("/Workspace" + _runner_path).parent.parent
    NOTEBOOKS_DIR = str(_project_root / "notebooks")
    print(f"NOTEBOOKS_DIR detectado automaticamente: {NOTEBOOKS_DIR}")
except Exception as exc:
    raise RuntimeError(
        "Não foi possível detectar a pasta notebooks/ automaticamente "
        f"({type(exc).__name__}: {exc}). Isso só deveria falhar rodando "
        "fora de um notebook Databricks de verdade."
    ) from exc

DEFAULT_TIMEOUT_S = 3600

# Cada etapa: (nome_amigavel, arquivo, depende_de)
PIPELINE = [
    ("setup", "00_setup_ambiente", []),
    ("bronze_batch", "01_bronze_batch", ["setup"]),
    ("bronze_streaming", "02_bronze_streaming", ["setup"]),
    ("silver", "03_silver", ["bronze_batch", "bronze_streaming"]),
    ("quality_gate", "06_quality_checks", ["silver"]),
    ("gold", "04_gold", ["quality_gate"]),
    ("serving_mongodb", "05_serving_mongodb", ["gold"]),
    ("ml_mlflow", "07_ml_mlflow", ["gold"]),
    ("monitoring", "08_monitoring", ["serving_mongodb", "ml_mlflow"]),
    ("dashboard", "09_dashboard", ["monitoring"]),
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Checagem de pré-requisitos
# MAGIC Confere, antes de disparar qualquer notebook, os dois pontos que
# MAGIC mais causaram falha até agora: `NOTEBOOKS_DIR` incompleto e o
# MAGIC `TS_ALUNO.csv` (upload manual) faltando no Volume. Se faltar algo,
# MAGIC avisa exatamente o quê, em vez de deixar o pipeline quebrar no meio.

# COMMAND ----------
def checar_pre_requisitos():
    """
    Confere, antes de disparar qualquer notebook, os pontos que mais
    causaram falha até agora: NOTEBOOKS_DIR incompleto ou o TS_ALUNO.csv
    (upload manual) faltando. Se algo estiver faltando, avisa exatamente o
    que, em vez de deixar o pipeline quebrar lá na frente, no meio de uma
    etapa, com erro genérico.
    """
    problemas = []

    try:
        arquivos_no_dir = {
            item.name.rstrip("/").replace(".py", "")
            for item in dbutils.fs.ls(NOTEBOOKS_DIR)
        }
        esperados = {arquivo for _nome, arquivo, _deps in PIPELINE}
        faltando = esperados - arquivos_no_dir
        if faltando:
            problemas.append(
                f"NOTEBOOKS_DIR ({NOTEBOOKS_DIR}) existe, mas faltam os "
                f"notebooks: {sorted(faltando)}. Confira se o clone do Git "
                "trouxe todos os arquivos."
            )
    except Exception as e:
        problemas.append(
            f"NOTEBOOKS_DIR ({NOTEBOOKS_DIR}) não existe ou não é "
            f"acessível ({type(e).__name__}). Confirme se este runner está "
            "salvo dentro de workflows/, na raiz do mesmo projeto que tem "
            "a pasta notebooks/ ao lado."
        )

    caminho_ts_aluno = (
        "/Volumes/workspace/bronze/raw_files/microdados_inep/DADOS/TS_ALUNO.csv"
    )
    try:
        dbutils.fs.ls(caminho_ts_aluno)
    except Exception:
        problemas.append(
            f"TS_ALUNO.csv não encontrado em {caminho_ts_aluno}. Baixe o "
            "microdado oficial do INEP e suba manualmente pela interface "
            "(Catalog, workspace, bronze, Volumes, raw_files, "
            "microdados_inep, DADOS, Upload to this volume) antes de rodar "
            "o pipeline. Sem ele, 01_bronze_batch falha."
        )

    if problemas:
        print("Pré-requisitos não atendidos:\n")
        for i, p in enumerate(problemas, 1):
            print(f"{i}. {p}\n")
        raise RuntimeError(
            f"{len(problemas)} pré-requisito(s) faltando. Corrija antes "
            "de rodar o pipeline. Veja as mensagens acima."
        )

    print("Pré-requisitos ok: NOTEBOOKS_DIR e TS_ALUNO.csv confirmados.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Execução de uma etapa
# MAGIC Roda um notebook via `dbutils.notebook.run` e devolve se deu certo,
# MAGIC sem deixar a exceção subir e derrubar o runner inteiro de uma vez.

# COMMAND ----------
def run_step(nome, arquivo):
    """Executa um notebook via dbutils.notebook.run e devolve (nome, ok, detalhe)."""
    path = f"{NOTEBOOKS_DIR}/{arquivo}"
    print(f"Iniciando {nome} ({arquivo})...")
    try:
        resultado = dbutils.notebook.run(path, DEFAULT_TIMEOUT_S)
        print(f"OK {nome} concluído. Retorno: {resultado}")
        return nome, True, resultado
    except Exception as e:
        print(f"FALHOU {nome}: {e}")
        return nome, False, str(e)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Orquestração das etapas
# MAGIC Respeita as dependências: cada etapa só roda depois que todas as
# MAGIC anteriores dela terminarem com sucesso. Etapas que compartilham a
# MAGIC mesma dependência (`05` e `07`, ambas dependendo do Gold) rodam em
# MAGIC paralelo.

# COMMAND ----------
def run_pipeline(steps):
    """
    Executa a pipeline respeitando dependências: cada etapa só roda depois
    que TODAS as etapas de que depende terminarem com sucesso. Etapas que
    compartilham a mesma dependência (05 e 07, ambas dependendo do Gold)
    rodam em paralelo.
    """
    concluidos = {}
    pendentes = {nome: (arquivo, deps) for nome, arquivo, deps in steps}
    falhou = False

    while pendentes and not falhou:
        prontos = [
            nome for nome, (arquivo, deps) in pendentes.items()
            if all(d in concluidos and concluidos[d] for d in deps)
        ]

        if not prontos:
            print("Dependência não satisfeita ou ciclo detectado. Parando.")
            break

        with ThreadPoolExecutor(max_workers=max(len(prontos), 1)) as executor:
            futures = {
                executor.submit(run_step, nome, pendentes[nome][0]): nome
                for nome in prontos
            }

            for future in as_completed(futures):
                nome, ok, _detalhe = future.result()
                concluidos[nome] = ok
                del pendentes[nome]

                if not ok:
                    falhou = True

        if falhou:
            print("Uma etapa falhou nesta rodada. Pipeline interrompida "
                  "para não deixar etapas seguintes lerem dado incompleto.")
            break

    return concluidos, pendentes

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Execução e resumo final
# MAGIC Roda a checagem, dispara o pipeline inteiro, e imprime o status de
# MAGIC cada etapa no final (OK, falhou, ou não executada por dependência).

# COMMAND ----------
checar_pre_requisitos()
concluidos, pendentes = run_pipeline(PIPELINE)

print("\n=== RESUMO DA EXECUÇÃO ===")
for nome, _arquivo, _deps in PIPELINE:
    if nome in concluidos:
        status = "OK" if concluidos[nome] else "FALHOU"
    else:
        status = "não executado (dependência não satisfeita)"
    print(f"{nome:20s} {status}")

falhas = [nome for nome, ok in concluidos.items() if not ok]
if falhas:
    raise RuntimeError(
        f"Pipeline falhou. Etapas com erro: {falhas}. "
        f"Etapas não executadas devido a falhas: {list(pendentes.keys())}"
    )

if pendentes:
    print(f"\nPipeline parcialmente completa. Etapas não executadas: {list(pendentes.keys())}")
else:
    print("\nPipeline completa executada com sucesso, na ordem correta.")
