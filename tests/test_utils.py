from src.utils import full_table, volume_path


def test_full_table():
    assert full_table("silver", "medicoes") == "workspace.silver.medicoes"


def test_volume_path():
    assert volume_path("bronze", "raw_files") == "/Volumes/workspace/bronze/raw_files/"
