"""Schemas compartilhados do pipeline."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA_EVENTO_STREAMING = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("schema_version", StringType(), False),
    StructField("ano", IntegerType(), False),
    StructField("sigla_uf", StringType(), False),
    StructField("id_municipio", StringType(), False),
    StructField("rede", IntegerType(), False),
    StructField("taxa_alfabetizacao", DoubleType(), False),
    StructField("source", StringType(), False),
])

# Ajustar após validação do schema real da fonte.
SCHEMA_AVALIACAO = StructType([
    StructField("ano", IntegerType(), True),
    StructField("sigla_uf", StringType(), True),
    StructField("id_municipio", StringType(), True),
    StructField("rede", IntegerType(), True),
    StructField("media_portugues", DoubleType(), True),
    StructField("taxa_alfabetizacao", DoubleType(), True),
])
