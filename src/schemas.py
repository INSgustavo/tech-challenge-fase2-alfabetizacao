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

# Schema real da fonte br_inep_avaliacao_alfabetizacao_uf (grão UF — sem id_municipio).
SCHEMA_AVALIACAO = StructType([
    StructField("ano", IntegerType(), True),
    StructField("sigla_uf", StringType(), True),
    StructField("serie", IntegerType(), True),
    StructField("rede", IntegerType(), True),
    StructField("taxa_alfabetizacao", DoubleType(), True),  # percentual 0-100 na origem
    StructField("media_portugues", DoubleType(), True),
    *[StructField(f"proporcao_aluno_nivel_{i}", DoubleType(), True) for i in range(9)],
])
