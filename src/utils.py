"""Funções utilitárias compartilhadas."""

CATALOG = "workspace"

# Mapa de redes (conforme CONTRACT.md — P2 confirmar)
REDE_MAP = {0: "total", 2: "estadual", 3: "municipal", 5: "privada"}

def full_table(schema: str, table: str) -> str:
    """Retorna o nome completo da tabela: workspace.schema.table"""
    return f"{CATALOG}.{schema}.{table}"

def volume_path(schema: str, volume: str) -> str:
    """Retorna o caminho do volume: /Volumes/workspace/schema/volume/"""
    return f"/Volumes/{CATALOG}/{schema}/{volume}"

def padronizar_rede(df, col_rede: str = "rede"):
    """Adiciona coluna rede_label com o rótulo da rede."""
    from pyspark.sql import functions as F
    mapping = F.create_map([F.lit(x) for kv in REDE_MAP.items() for x in kv])
    return df.withColumn("rede_label", mapping[F.col(col_rede)])
