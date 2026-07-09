from src.utils import (
    ALFABETIZACAO_CORTE,
    full_table,
    is_alfabetizado,
    volume_path,
)


def test_full_table():
    assert full_table("silver", "medicoes") == "workspace.silver.medicoes"


def test_volume_path():
    assert volume_path("bronze", "raw_files") == "/Volumes/workspace/bronze/raw_files/"


# --- Não-regressão do indicador principal (regra dos 743) ---

def test_corte_alfabetizacao_nao_muda_sem_revisao():
    # Guarda contra alteração silenciosa da regra de negócio (CONTRACT.md).
    assert ALFABETIZACAO_CORTE == 743


def test_is_alfabetizado_no_corte():
    assert is_alfabetizado(743) is True
    assert is_alfabetizado(742.9) is False


def test_is_alfabetizado_sem_media_retorna_none():
    assert is_alfabetizado(None) is None
