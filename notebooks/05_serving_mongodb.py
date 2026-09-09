# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "pymongo",
# ]
# ///
# MAGIC %md
# MAGIC # 05 · Serving MongoDB
# MAGIC
# MAGIC **Pra que serve:** publica os dados da Gold no MongoDB, pra simular um
# MAGIC cenário de app/API consumindo os indicadores (uma camada de "serving",
# MAGIC separada da analítica).
# MAGIC
# MAGIC **Pré-requisito:** `04_gold.py` já ter rodado, e ter o secret do MongoDB
# MAGIC configurado (ver abaixo). **Sem o secret, esse notebook não roda** —
# MAGIC isso é esperado, não precisa rodar se você não for demonstrar essa parte.
# MAGIC
# MAGIC Publica a Gold no MongoDB Atlas com **upsert** por município (um documento
# MAGIC por `ano + id_municipio + rede`), sem apagar a coleção inteira.
# MAGIC
# MAGIC Pré-requisito: o P1 cria o cluster Atlas M0 e cadastra o secret:
# MAGIC ```
# MAGIC databricks secrets create-scope alfabetizacao
# MAGIC databricks secrets put-secret alfabetizacao mongo_uri
# MAGIC ```
# MAGIC A connection string **nunca** vai versionada (contrato, seção 2).
# MAGIC
# MAGIC **Justificativa do serving:**
# MAGIC
# MAGIC Delta Lake permanece como a fonte analítica de verdade do pipeline.
# MAGIC
# MAGIC  O MongoDB não substitui a camada Gold e não é utilizado para processamento analítico.
# MAGIC
# MAGIC  Sua função é atuar como camada de serving para consumo por aplicações e APIs, disponibilizando o indicador municipal em um modelo orientado a documentos.
# MAGIC
# MAGIC O documento mantém o mesmo grão do mart gold.indicador_municipio:
# MAGIC
# MAGIC - ano + id_municipio + rede
# MAGIC
# MAGIC -  A publicação utiliza upsert nessa chave, permitindo reexecuções idempotentes
# MAGIC -  sem apagar a coleção inteira.
# MAGIC
# MAGIC **Essa separação mantém:**
# MAGIC
# MAGIC - Delta Lake para processamento, histórico e análises;
# MAGIC
# MAGIC - MongoDB para serving operacional orientado a consultas por município.
# MAGIC  
# MAGIC -  Neste projeto o MongoDB representa uma camada de serving demonstrativa.
# MAGIC
# MAGIC -  Não é afirmado ganho de desempenho sem benchmark específico.
# MAGIC baixa latência por aplicação/API, com documento no mesmo grão do mart municipal e upsert idempotente por `ano + id_municipio + rede`.

# COMMAND ----------

# MAGIC %pip install pymongo

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "workspace"
SOURCE = f"{CATALOG}.gold.indicador_municipio"
DATABASE = "alfabetizacao"
COLLECTION = "indicador_municipio"

# COMMAND ----------

# Lê o segredo. Se não estiver configurado, o notebook não falha o Workflow:
# apenas avisa e encerra (permite rodar o pipeline sem Atlas no Free Edition).
MONGO_URI = None
try:
    MONGO_URI = dbutils.secrets.get(scope="alfabetizacao", key="mongo_uri")
except Exception as exc:
    print(f"⚠ Secret alfabetizacao/mongo_uri não encontrado: {exc}")
    print("Configure o secret (P1) para habilitar a publicação no MongoDB.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita distribuída com bulk upsert
# MAGIC Cada partição do Spark abre sua própria conexão e envia as operações em lote.

# COMMAND ----------

def make_writer(mongo_uri, database, collection):
    """Fábrica de função de partição - captura a URI por closure (serializável)."""
    def write_partition(rows):
        from pymongo import MongoClient, UpdateOne

        operations = []
        for row in rows:
            doc = row.asDict(recursive=True)
            key = {
                "ano": doc["ano"],
                "id_municipio": doc["id_municipio"],
                "rede": doc["rede"],
            }
            operations.append(UpdateOne(key, {"$set": doc}, upsert=True))

        if operations:
            client = MongoClient(mongo_uri)
            try:
                client[database][collection].bulk_write(operations, ordered=False)
            finally:
                client.close()
    return write_partition

# COMMAND ----------

if MONGO_URI:
    df = spark.table(SOURCE)
    total = df.count()

    df.foreachPartition(make_writer(MONGO_URI, DATABASE, COLLECTION))
    print(f"✓ {total:,} documentos publicados/atualizados em {DATABASE}.{COLLECTION}")

    # Verificação idempotente: nº de documentos na coleção (executa no driver)
    from pymongo import MongoClient
    client = MongoClient(MONGO_URI)
    try:
        n_docs = client[DATABASE][COLLECTION].count_documents({})
        exemplo = client[DATABASE][COLLECTION].find_one()
    finally:
        client.close()
    print(f"Coleção contém {n_docs:,} documentos. Exemplo:")
    print(exemplo)
else:
    print(f"Serving não executado. Fonte pronta: {SOURCE}")

# COMMAND ----------

if MONGO_URI:
    dbutils.notebook.exit(f"MongoDB: {n_docs:,} documentos publicados")
else:
    dbutils.notebook.exit("Serving pulado: secret alfabetizacao/mongo_uri ausente")