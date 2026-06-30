"""Funções utilitárias compartilhadas."""

CATALOG = "workspace"
REDE_MAP = {0: "total", 2: "estadual", 3: "municipal", 5: "privada"}


def full_table(schema: str, table: str) -> str:
    """Retorna o nome qualificado de uma tabela."""
    if not schema or not table:
        raise ValueError("schema e table são obrigatórios")
    return f"{CATALOG}.{schema}.{table}"


def volume_path(schema: str, volume: str) -> str:
    """Retorna o caminho de um Volume do Unity Catalog."""
    if not schema or not volume:
        raise ValueError("schema e volume são obrigatórios")
    return f"/Volumes/{CATALOG}/{schema}/{volume}/"


def padronizar_rede(df, col_rede: str = "rede"):
    """Adiciona `rede_label` com base no domínio compartilhado."""
    from pyspark.sql import functions as F

    mapping = F.create_map([F.lit(x) for pair in REDE_MAP.items() for x in pair])
    return df.withColumn("rede_label", mapping[F.col(col_rede)])


def table_exists(spark, full_name: str) -> bool:
    """Verifica se uma tabela existe sem falhar o pipeline."""
    return spark.catalog.tableExists(full_name)
