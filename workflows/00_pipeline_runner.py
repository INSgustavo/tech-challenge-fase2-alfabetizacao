Runner do pipeline completo chama cada notebook via dbutils.notebook.run, na ORDEM REAL de dependência (não a ordem numérica dos arquivos).
Ordem: 00 -> {01, 02 em paralelo} -> 03 -> 06 -> 04 -> {05, 07 em paralelo} -> 08 -> 09

Por que o 06 vem antes do 04: o Gold (04) lê de silver.medicoes_aprovadas e silver.alunos_modelagem_aprovados, que só existem depois do Quality Gate (06) aprovar a Silver. Rodar na ordem numérica (04 antes do 06) quebra com tabela inexistente.

O 09 (dashboard) depende do 08 (monitoring), não direto do Gold, porque ele também lê observability.pipeline_metrics/quarantine_records sem o 08 já ter rodado, a aba de saúde do pipeline sobe vazia.

    
from concurrent.futures import ThreadPoolExecutor, as_completed

NOTEBOOKS_DIR = " colocar o caminho do seu NOTEBOOKS_DIR = "/Workspace "
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

def run_step(nome, arquivo):
    """Executa um notebook via dbutils.notebook.run e devolve (nome, ok, detalhe)."""
    path = f"{NOTEBOOKS_DIR}/{arquivo}"
    print(f"▶ Iniciando {nome} ({arquivo})...")
    try:
        resultado = dbutils.notebook.run(path, DEFAULT_TIMEOUT_S)
        print(f"✓ {nome} concluído. Retorno: {resultado}")
        return nome, True, resultado
    except Exception as e:
        print(f"✗ {nome} falhou: {e}")
        return nome, False, str(e)

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
            print("✗ Dependência não satisfeita ou ciclo detectado. Parando.")
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
            print("✗ Uma etapa falhou nesta rodada — pipeline interrompida "
                  "para não deixar etapas seguintes lerem dado incompleto.")
            break

    return concluidos, pendentes

# COMMAND ----------

# DBTITLE 1,Cell 5
concluidos, pendentes = run_pipeline(PIPELINE)

print("\n=== RESUMO DA EXECUÇÃO ===")
for nome, _arquivo, _deps in PIPELINE:
    if nome in concluidos:
        status = "✓ OK" if concluidos[nome] else "✗ FALHOU"
    else:
        status = "— não executado (dependência não satisfeita)"
    print(f"{nome:20s} {status}")

# Only raise error if there are failed steps, not if some steps couldn't run due to upstream failures
falhas = [nome for nome, ok in concluidos.items() if not ok]
if falhas:
    raise RuntimeError(
        f"Pipeline falhou. Etapas com erro: {falhas}. "
        f"Etapas não executadas devido a falhas: {list(pendentes.keys())}"
    )

if pendentes:
    print(f"\n⚠ Pipeline parcialmente completa. Etapas não executadas: {list(pendentes.keys())}")
else:
    print("\n✓ Pipeline completa executada com sucesso, na ordem correta.")
