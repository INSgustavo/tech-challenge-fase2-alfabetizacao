# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 · Setup do ambiente
# MAGIC
# MAGIC **Pra que serve:** é o primeiro notebook que todo mundo do grupo precisa
# MAGIC rodar, sempre, antes de qualquer outro. Ele monta toda a "casa" do
# MAGIC projeto no seu workspace Databricks — sem ele, nenhum dos outros
# MAGIC notebooks funciona.
# MAGIC
# MAGIC **O que ele cria, na ordem:**
# MAGIC 1. Os 4 schemas do catálogo: `bronze`, `silver`, `gold`, `observability`.
# MAGIC 2. Os 4 Volumes usados pelo pipeline (onde ficam os arquivos de dados).
# MAGIC 3. As 2 tabelas de observabilidade (log de execuções e quarentena).
# MAGIC 4. Copia as fontes oficiais (planilhas do INEP, dados do IBGE e o
# MAGIC    microdado de alunos) — que já vêm junto no repositório — pro Volume,
# MAGIC    prontas pra serem lidas pelos próximos notebooks.
# MAGIC
# MAGIC **Pré-requisito:** só ter clonado este repositório no seu próprio
# MAGIC workspace Databricks (conta gratuita "Community Edition" serve). Não
# MAGIC precisa editar nada, nem preencher caminho de pasta — ele acha tudo
# MAGIC sozinho.
# MAGIC
# MAGIC **Se der erro:** normalmente é porque o arquivo de entrada esperado não
# MAGIC está no repositório (confira `data/source/`, `data/external/` e
# MAGIC `data/raw/microdados_inep/DADOS/`), não porque o notebook está quebrado.
# MAGIC
# MAGIC ## Execução manual
# MAGIC Execute este notebook primeiro. Depois que ele terminar com sucesso,
# MAGIC execute manualmente os notebooks na ordem abaixo:
# MAGIC
# MAGIC `01_bronze_batch` e `02_bronze_streaming` → `03_silver` →
# MAGIC `06_quality_checks` → `04_gold` → `05_serving_mongodb` e
# MAGIC `07_ml_mlflow` → `08_monitoring` → `09_dashboard`.
# MAGIC
# MAGIC O notebook `09_dashboard` deve ser aberto e executado diretamente para
# MAGIC visualizar o HTML completo do dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Schemas
# MAGIC Cria os 4 schemas do projeto no catálogo `workspace`, se ainda não existirem.

# COMMAND ----------

CATALOG = "workspace"

EM_DATABRICKS = "spark" in globals() and "dbutils" in globals()

if EM_DATABRICKS:
    for schema in ["bronze", "silver", "gold", "observability"]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
        print(f"Schema disponível: {CATALOG}.{schema}")
else:
    print("Modo local: schemas e tabelas Delta serão ignorados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Volumes
# MAGIC Cria os 4 Volumes usados pelo pipeline: `raw_files` e `streaming_landing`
# MAGIC na Bronze, `checkpoints` e `quarantine` na observabilidade.

# COMMAND ----------

if EM_DATABRICKS:
    for schema, volume in [
        ("bronze", "raw_files"),
        ("bronze", "streaming_landing"),
        ("observability", "checkpoints"),
        ("observability", "quarantine"),
    ]:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{schema}.{volume}")
        print(f"Volume disponível: /Volumes/{CATALOG}/{schema}/{volume}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tabelas de observabilidade
# MAGIC `pipeline_metrics` guarda o resultado de cada execução (linhas lidas,
# MAGIC gravadas, rejeitadas, status); `quarantine_records` guarda os registros
# MAGIC rejeitados por qualquer etapa, com o motivo da rejeição.

# COMMAND ----------

if EM_DATABRICKS:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.observability.pipeline_metrics (
        run_id STRING,
        task_name STRING,
        status STRING,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        rows_read BIGINT,
        rows_written BIGINT,
        rows_rejected BIGINT,
        max_event_time TIMESTAMP,
        schema_version STRING,
        error_message STRING
    ) USING DELTA
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.observability.quarantine_records (
        run_id STRING,
        task_name STRING,
        rejection_reason STRING,
        payload STRING,
        ingestion_timestamp TIMESTAMP
    ) USING DELTA
    """)

    print(f"Ambiente validado com Spark {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preparação das fontes oficiais
# MAGIC Escrever dentro de `/Workspace/Repos` costuma ser bloqueado ou não
# MAGIC persiste os arquivos (é assim que a pasta `data/raw` ficava sempre vazia).
# MAGIC Por isso, aqui embaixo o setup:
# MAGIC
# MAGIC - localiza o `gerar_fontes.py` e as entradas oficiais (`data/source/*.xlsx`,
# MAGIC   `data/external/*.csv`) dentro do repositório (só leitura);
# MAGIC - copia tudo pra uma pasta de trabalho no disco local do cluster
# MAGIC   (`/tmp/tech_challenge_alfabetizacao`, fora do Workspace);
# MAGIC - roda o `gerar_fontes.py` a partir dessa pasta local, sem nenhum hack de
# MAGIC   Databricks — ele usa a própria lógica original de `resolver_base()`;
# MAGIC - copia só os CSVs finais gerados em `data/raw/` pro Volume
# MAGIC   `bronze.raw_files`.
# MAGIC
# MAGIC `gerar_fontes.py` continua sendo um arquivo à parte no repositório —
# MAGIC o setup só o "puxa" e executa, não embute a lógica dele aqui.

# COMMAND ----------

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

VOLUME_BRONZE = Path("/Volumes/workspace/bronze/raw_files")
MICRODADOS_ESPERADOS = (
    "TS_ALUNO.csv",
    "TS_ESTADO.csv",
    "TS_ITEM.csv",
    "TS_MUNICIPIO.csv",
)


def localizar_repo_base() -> Path:
    """
    Acha a raiz do repositório so para LEITURA das entradas oficiais e do
    gerar_fontes.py. Nunca e usada como destino de escrita.
    """
    if EM_DATABRICKS:
        _notebook_path = (
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )
        _notebook_dir = Path("/Workspace" + _notebook_path).parent
        for p in (_notebook_dir, *_notebook_dir.parents):
            if (p / "data").exists():
                return p
        raise FileNotFoundError(
            f"Nao encontrei uma pasta 'data/' subindo a partir de {_notebook_dir}."
        )

    cwd = Path.cwd().resolve()
    for p in (cwd, *cwd.parents):
        if (p / "data").exists():
            return p
    raise FileNotFoundError(
        "Nao encontrei uma pasta 'data/' subindo a partir do diretorio atual."
    )


def preparar_pasta_local(repo_base: Path) -> Path:
    """
    Copia as entradas oficiais e o gerar_fontes.py pro disco local do
    cluster, fora do Workspace, montando a mesma estrutura que
    gerar_fontes.py espera encontrar sozinho.
    """
    local_base = Path(tempfile.gettempdir()) / "tech_challenge_alfabetizacao"
    local_data = local_base / "data"
    local_scripts = local_base / "scripts"

    for sub in ("source", "external", "raw"):
        (local_data / sub).mkdir(parents=True, exist_ok=True)
    local_scripts.mkdir(parents=True, exist_ok=True)

    # Caminho esperado: <repo>/scripts/gerar_fontes.py (ou script/, no
    # singular, se algum dia mudar). So cai pro rglob (com aviso) se nao
    # achar em nenhum dos dois, pra nao arriscar pegar sem querer uma copia
    # antiga guardada em docs/fase2-alfabetizacao/ (referencia da Fase 2).
    candidatos_esperados = [
        repo_base / "scripts" / "gerar_fontes.py",
        repo_base / "script" / "gerar_fontes.py",
    ]
    script_esperado = next((p for p in candidatos_esperados if p.exists()), None)
    if script_esperado is not None:
        script_origem = script_esperado
    else:
        script_esperado = candidatos_esperados[0]  # so para a mensagem de aviso
        candidatos_script = [
            p for p in repo_base.rglob("gerar_fontes.py")
            if "docs" not in p.relative_to(repo_base).parts
        ]
        if not candidatos_script:
            raise FileNotFoundError(
                f"Nao encontrei gerar_fontes.py em {script_esperado} "
                f"nem em nenhuma outra pasta (fora de docs/) dentro de {repo_base}."
            )
        script_origem = candidatos_script[0]
        print(
            f"[AVISO] gerar_fontes.py nao estava em {script_esperado}, "
            f"usando {script_origem} em vez disso."
        )
    script_destino = local_scripts / "gerar_fontes.py"
    shutil.copy2(script_origem, script_destino)
    print(f"[OK] gerar_fontes.py copiado de {script_origem}")

    for sub in ("source", "external"):
        origem_dir = repo_base / "data" / sub
        if not origem_dir.exists():
            raise FileNotFoundError(f"Pasta ausente no repositorio: {origem_dir}")
        for arquivo in origem_dir.iterdir():
            if arquivo.is_file():
                shutil.copy2(arquivo, local_data / sub / arquivo.name)
                print(f"[OK] {sub}/{arquivo.name} copiado pro disco local")

    # data/raw pode ja trazer arquivos oficiais prontos versionados no repo
    # (ex.: br_inep_avaliacao_alfabetizacao_uf.csv.gz, que nao e gerado pelo
    # gerar_fontes.py, so reaproveitado por ele). Sem copiar isso tambem,
    # copiar_raw_para_bronze() falha procurando um arquivo que nunca chega
    # no disco local.
    origem_raw = repo_base / "data" / "raw"
    if origem_raw.exists():
        for arquivo in origem_raw.iterdir():
            if arquivo.is_file():
                shutil.copy2(arquivo, local_data / "raw" / arquivo.name)
                print(f"[OK] raw/{arquivo.name} copiado pro disco local (arquivo oficial pre-existente)")

    # microdados_inep/DADOS (ex.: TS_ALUNO.csv) tambem pode vir versionado no
    # repo, dentro de data/raw/. Quando existir, copia a subpasta inteira pro
    # disco local tambem, pra publicar_no_volume() encontrar sem upload manual.
    origem_microdados = repo_base / "data" / "raw" / "microdados_inep" / "DADOS"
    if origem_microdados.exists():
        destino_microdados_local = local_data / "raw" / "microdados_inep" / "DADOS"
        destino_microdados_local.mkdir(parents=True, exist_ok=True)
        for arquivo in origem_microdados.iterdir():
            if arquivo.is_file():
                shutil.copy2(arquivo, destino_microdados_local / arquivo.name)
                print(f"[OK] microdados_inep/DADOS/{arquivo.name} copiado pro disco local")

    return local_base


def rodar_gerar_fontes(local_base: Path):
    script = local_base / "scripts" / "gerar_fontes.py"

    print("\n=== RODANDO gerar_fontes.py NO DISCO LOCAL ===")
    resultado = subprocess.run(
        [sys.executable, str(script)],
        cwd=str(local_base),
        capture_output=True,
        text=True,
    )
    print(resultado.stdout)
    if resultado.returncode != 0:
        print(resultado.stderr)
        raise RuntimeError(
            "gerar_fontes.py terminou com erro. Veja o stderr acima."
        )


def publicar_no_volume(local_base: Path):
    local_raw = local_base / "data" / "raw"
    destino = VOLUME_BRONZE if EM_DATABRICKS else local_raw

    if EM_DATABRICKS:
        VOLUME_BRONZE.mkdir(parents=True, exist_ok=True)
        print("\n=== PUBLICANDO CSVs GERADOS NO VOLUME bronze.raw_files ===")
        for arquivo in local_raw.iterdir():
            if arquivo.is_file():
                shutil.copy2(arquivo, destino / arquivo.name)
                print(f"[OK] {arquivo.name}")

        # microdados_inep/DADOS pode ter vindo do repo (via preparar_pasta_local);
        # publica tambem, pra ninguem mais precisar de upload manual no Volume.
        local_microdados = local_raw / "microdados_inep" / "DADOS"
        if local_microdados.exists():
            destino_microdados = destino / "microdados_inep" / "DADOS"
            destino_microdados.mkdir(parents=True, exist_ok=True)
            print("\n=== PUBLICANDO MICRODADOS (vindos do repositorio) NO VOLUME ===")
            for arquivo in local_microdados.iterdir():
                if arquivo.is_file():
                    shutil.copy2(arquivo, destino_microdados / arquivo.name)
                    print(f"[OK] microdados_inep/DADOS/{arquivo.name}")
    else:
        print(f"\nModo local: arquivos ja estao em {local_raw}, nada pra publicar.")

    ausentes = [
        nome for nome in MICRODADOS_ESPERADOS
        if not (destino / "microdados_inep" / "DADOS" / nome).exists()
    ]
    if ausentes:
        print(
            "\n\u26a0 Microdados ausentes: " + ", ".join(ausentes)
            + f"\n  Suba os arquivos manualmente em {destino}/microdados_inep/DADOS/, "
            "ou versione-os em data/raw/microdados_inep/DADOS/ no repositorio."
        )


def preparar_e_publicar_fontes():
    repo_base = localizar_repo_base()
    print(f"Repositorio (leitura): {repo_base}")

    local_base = preparar_pasta_local(repo_base)
    print(f"Pasta de trabalho local: {local_base}")

    rodar_gerar_fontes(local_base)
    publicar_no_volume(local_base)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execução
# MAGIC Roda as funções definidas na célula anterior: `main()` gera os CSVs
# MAGIC oficiais em `data/raw/`, e `copiar_raw_para_bronze()` publica tudo no
# MAGIC Volume.

# COMMAND ----------

preparar_e_publicar_fontes()

# COMMAND ----------

if EM_DATABRICKS:
    dbutils.notebook.exit("4 schemas, 4 volumes e 2 tabelas de observabilidade prontos")