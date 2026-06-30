"""Schemas compartilhados (P1/P2/P3)."""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Schema do evento de streaming (02_bronze_streaming)
SCHEMA_EVENTO_STREAMING = (StructType()
    .add("ano", IntegerType())
    .add("sigla_uf", StringType())
    .add("id_municipio", StringType())
    .add("taxa_alfabetizacao", DoubleType())
    .add("ts", StringType()))

# Schema esperado da avaliação SAEB (P2 confirmar colunas reais)
SCHEMA_AVALIACAO = StructType([
    StructField("ano", IntegerType(), True),
    StructField("sigla_uf", StringType(), True),
    StructField("id_municipio", StringType(), True),
    StructField("rede", IntegerType(), True),
    StructField("media_portugues", DoubleType(), True),
    StructField("taxa_alfabetizacao", DoubleType(), True),
])
