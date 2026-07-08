"""Gera as fontes derivadas do pipeline a partir de dados públicos.

Entradas:
- data/raw/br_inep_avaliacao_alfabetizacao_uf.csv.gz (INEP via Base dos Dados)
- data/external/municipios.csv e estados.csv (IBGE, via kelvins/municipios-brasileiros, MIT)

Saídas (metodologia no CONTRACT.md e em data/raw/README.md):
- uf.csv · municipio.csv · meta_brasil.csv · meta_uf.csv · meta_municipio.csv
- alunos_simulados.csv.gz (SIMULADO — semente fixa, reprodutível)
"""
import csv
import gzip
import random
import statistics
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
RAW = BASE / "data" / "raw"
EXT = BASE / "data" / "external"

ANO_BASE, ANO_META = 2023, 2030
REDES_PUBLICAS = {"2", "3"}


def carregar_avaliacao():
    with gzip.open(RAW / "br_inep_avaliacao_alfabetizacao_uf.csv.gz", "rt") as f:
        return list(csv.DictReader(f))


def baseline_por_uf(avaliacao):
    """Baseline = média da taxa 2023 nas redes públicas; fallback 2024."""
    out = {}
    for ano in ("2023", "2024"):
        por_uf = {}
        for row in avaliacao:
            if row["ano"] == ano and row["rede"] in REDES_PUBLICAS and row["taxa_alfabetizacao"]:
                por_uf.setdefault(row["sigla_uf"], []).append(float(row["taxa_alfabetizacao"]))
        for uf, taxas in por_uf.items():
            out.setdefault(uf, statistics.mean(taxas))
    return out


def trajetoria(baseline_pct):
    """Interpolação linear do baseline (2023) até 100% em 2030, em fração 0-1."""
    return {
        ano: round((baseline_pct + (100.0 - baseline_pct)
                    * (ano - ANO_BASE) / (ANO_META - ANO_BASE)) / 100.0, 4)
        for ano in range(ANO_BASE + 1, ANO_META + 1)
    }


def main():
    avaliacao = carregar_avaliacao()
    baselines = baseline_por_uf(avaliacao)
    estados = list(csv.DictReader(open(EXT / "estados.csv", encoding="utf-8-sig")))
    municipios = list(csv.DictReader(open(EXT / "municipios.csv", encoding="utf-8-sig")))
    uf_por_codigo = {e["codigo_uf"]: e["uf"] for e in estados}

    with open(RAW / "uf.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["codigo_uf", "sigla_uf", "nome", "regiao"])
        for e in sorted(estados, key=lambda x: x["codigo_uf"]):
            w.writerow([e["codigo_uf"], e["uf"], e["nome"], e["regiao"]])

    with open(RAW / "municipio.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id_municipio", "nome", "sigla_uf", "capital", "latitude", "longitude"])
        for m in sorted(municipios, key=lambda x: x["codigo_ibge"]):
            w.writerow([m["codigo_ibge"], m["nome"], uf_por_codigo[m["codigo_uf"]],
                        m["capital"], m["latitude"], m["longitude"]])

    with open(RAW / "meta_uf.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["sigla_uf", "ano", "meta", "metodologia"])
        for uf in sorted(baselines):
            for ano, meta in trajetoria(baselines[uf]).items():
                w.writerow([uf, ano, meta, "interp_linear_2023_2030_v1"])

    with open(RAW / "meta_brasil.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ano", "meta", "metodologia"])
        for ano, meta in trajetoria(statistics.mean(baselines.values())).items():
            w.writerow([ano, meta, "interp_linear_2023_2030_v1"])

    with open(RAW / "meta_municipio.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["id_municipio", "sigla_uf", "ano", "meta", "metodologia"])
        for m in sorted(municipios, key=lambda x: x["codigo_ibge"]):
            uf = uf_por_codigo[m["codigo_uf"]]
            if uf not in baselines:
                continue
            for ano, meta in trajetoria(baselines[uf]).items():
                w.writerow([m["codigo_ibge"], uf, ano, meta, "herda_trajetoria_uf_v1"])

    rng = random.Random(42)
    with gzip.open(RAW / "alunos_simulados.csv.gz", "wt", newline="") as f:
        w = csv.writer(f)
        w.writerow(["aluno_id", "ano", "sigla_uf", "serie", "rede",
                    "proficiencia_portugues", "fonte"])
        for row in avaliacao:
            if not row["media_portugues"]:
                continue
            media = float(row["media_portugues"])
            for i in range(80):
                prof = round(min(900.0, max(500.0, rng.gauss(media, 50.0))), 1)
                w.writerow([f"{row['ano']}-{row['sigla_uf']}-{row['rede']}-{i:03d}",
                            row["ano"], row["sigla_uf"], row["serie"], row["rede"],
                            prof, "SIMULADO"])

    for p in sorted(RAW.iterdir()):
        print(f"  {p.name:45s} {p.stat().st_size/1024:8.1f} KB")


if __name__ == "__main__":
    main()
