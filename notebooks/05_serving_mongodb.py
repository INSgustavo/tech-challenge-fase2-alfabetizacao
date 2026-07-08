# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Serving MongoDB
# MAGIC Publica a Gold no MongoDB Atlas com **upsert** por município (um documento
# MAGIC por `ano + id_municipio + rede`), sem apagar a coleção inteira.
# MAGIC
# MAGIC Pré-requisito: o P1 cria o cluster Atlas M0 e cadastra o secret:
# MAGIC ```
# MAGIC databricks secrets create-scope alfabetizacao
# MAGIC databricks secrets put-secret alfabetizacao mongo_uri
# MAGIC ```
# MAGIC A connection string **nunca** vai versionada (contrato, seção 2).

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
    """Fábrica de função de partição — captura a URI por closure (serializável)."""
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
