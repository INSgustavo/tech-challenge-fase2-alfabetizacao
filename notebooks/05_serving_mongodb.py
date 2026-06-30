# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Serving MongoDB
# MAGIC Publica a Gold com `upsert`, sem apagar a coleção inteira.

# COMMAND ----------
CATALOG = "workspace"
SOURCE = f"{CATALOG}.gold.indicador_municipio"

# Em ambiente configurado:
# MONGO_URI = dbutils.secrets.get(scope="alfabetizacao", key="mongo_uri")
# DATABASE = "alfabetizacao"
# COLLECTION = "indicador_municipio"

# COMMAND ----------
def write_partition(rows):
    """Exemplo de escrita distribuída com bulk upsert."""
    from pymongo import MongoClient, UpdateOne

    # client = MongoClient(MONGO_URI)
    # collection = client[DATABASE][COLLECTION]
    operations = []
    for row in rows:
        doc = row.asDict(recursive=True)
        key = {
            "ano": doc["ano"],
            "id_municipio": doc["id_municipio"],
            "rede": doc["rede"],
        }
        operations.append(UpdateOne(key, {"$set": doc}, upsert=True))

    # if operations:
    #     collection.bulk_write(operations, ordered=False)
    # client.close()
    return len(operations)

# Descomentar após configurar o secret:
# spark.table(SOURCE).foreachPartition(write_partition)

print(f"Fonte pronta para serving: {SOURCE}")
print("Configure o secret e habilite foreachPartition para publicar no MongoDB.")
