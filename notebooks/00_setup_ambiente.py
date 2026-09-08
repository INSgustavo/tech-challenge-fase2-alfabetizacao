# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 00 — Setup do ambiente
# MAGIC Cria schemas, Volumes e a estrutura de observabilidade, e em seguida
# MAGIC já prepara e publica as fontes oficiais (lógica de `gerar_fontes.py`
# MAGIC embutida ao final deste notebook, sem depender de arquivo externo).

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Schemas
# MAGIC Cria os 4 schemas do projeto no catálogo `workspace`, se ainda não existirem.

# COMMAND ----------
CATALOG = "workspace"

for schema in ["bronze", "silver", "gold", "observability"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"Schema disponível: {CATALOG}.{schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Volumes
# MAGIC Cria os 4 Volumes usados pelo pipeline: `raw_files` e `streaming_landing`
# MAGIC na Bronze, `checkpoints` e `quarantine` na observabilidade.

# COMMAND ----------
for schema, volume in [
    ("bronze", "raw_files"),
    ("bronze", "streaming_landing"),
    ("observability", "checkpoints"),
    ("observability", "quarantine"),
]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{schema}.{volume}")
    print(f"Volume disponível: /Volumes/{CATALOG}/{schema}/{volume}/")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Tabelas de observabilidade
# MAGIC `pipeline_metrics` guarda o resultado de cada execução (linhas lidas,
# MAGIC gravadas, rejeitadas, status); `quarantine_records` guarda os registros
# MAGIC rejeitados por qualquer etapa, com o motivo da rejeição.

# COMMAND ----------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.observability.pipeline_metrics (
    run_id STRING,
    task_name STRING,
    status STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    rows_read BIGINT,
    rows_written BIGINT,
    rows_rejected BIGINT,
    max_event_time TIMESTAMP,
    schema_version STRING,
    error_message STRING
) USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.observability.quarantine_records (
    run_id STRING,
    task_name STRING,
    rejection_reason STRING,
    payload STRING,
    ingestion_timestamp TIMESTAMP
) USING DELTA
""")

print(f"Ambiente validado com Spark {spark.version}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Preparação das fontes oficiais
# MAGIC A partir daqui, a lógica de `gerar_fontes.py` está embutida direto neste
# MAGIC notebook (sem chamar arquivo externo), já que os schemas e o Volume
# MAGIC `bronze.raw_files` das células acima precisam existir antes dela rodar:
# MAGIC
# MAGIC - lê `data/source/*.xlsx` (planilhas oficiais do INEP) e `data/external/`
# MAGIC   (dimensões IBGE);
# MAGIC - gera os CSVs em `data/raw/` sem interpolar meta nem simular aluno;
# MAGIC - copia tudo para `/Volumes/workspace/bronze/raw_files/`;
# MAGIC - confirma se o `TS_ALUNO.csv` (microdado oficial, upload manual) já
# MAGIC   está no lugar certo.
# MAGIC
# MAGIC **Ajuste antes de rodar**: a variável `BASE` logo abaixo precisa apontar
# MAGIC pra pasta do projeto no **seu** Workspace — não tem como detectar isso
# MAGIC sozinho rodando como célula de notebook (veja o comentário junto da
# MAGIC variável).

# COMMAND ----------
"""
Gera as fontes do pipeline a partir de dados OFICIAIS.

Estrutura esperada do projeto
=============================

data/
├── external/
│   ├── estados.csv
│   └── municipios.csv
├── source/
│   ├── resultados_e_metas_ufs_2024_2.xlsx
│   └── resultados_e_metas_municipios_2024.xlsx
└── raw/
    └── br_inep_avaliacao_alfabetizacao_uf.csv.gz   # já existente/oficial

Saídas
======
data/raw/
├── uf.csv
├── municipio.csv
├── meta_brasil.csv
├── meta_uf.csv
├── meta_municipio.csv
├── br_inep_avaliacao_alfabetizacao_municipio.csv.gz
└── fontes_oficiais_manifest.json

IMPORTANTE
==========
- Não interpola metas.
- Não faz município herdar meta da UF.
- Não gera alunos simulados.
- As metas são extraídas diretamente das planilhas oficiais do INEP.
- O indicador municipal é extraído diretamente da planilha oficial do INEP.
- O arquivo de alunos será tratado separadamente quando os microdados oficiais
  forem disponibilizados no Databricks/Volume.

Compatível com execução como .py, notebook Jupyter e Databricks.
"""
##%pip install openpyxl

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import re
import shutil
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

try:
    import pandas as pd
except ImportError as exc:
    raise RuntimeError(
        "Este script requer pandas. No Databricks ele já costuma estar disponível."
    ) from exc

# openpyxl é exigido pelo pandas para ler os .xlsx oficiais (ARQUIVO_UF /
# ARQUIVO_MUNICIPIO). Não vem por padrão no runtime serverless do Databricks.
# A instalação automática tem timeout pra nunca travar indefinidamente — se o
# ambiente bloquear a saída de rede do subprocess (comum em serverless com
# rede restrita), o script falha rápido com instrução clara em vez de girar
# pra sempre.
try:
    import openpyxl  # noqa: F401
except ImportError:
    import subprocess
    import sys

    print("openpyxl não encontrado — tentando instalar automaticamente...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "openpyxl"],
            timeout=60,
        )
        import openpyxl  # noqa: F401

        print("✓ openpyxl instalado.")
    except Exception as exc:
        raise RuntimeError(
            "Não foi possível instalar openpyxl automaticamente "
            f"({exc}). Rode manualmente numa célula separada, antes deste "
            "script:\n\n"
            "    %pip install openpyxl\n"
            "    dbutils.library.restartPython()\n\n"
            "E execute este script de novo depois disso."
        ) from exc


ARQUIVO_UF = "resultados_e_metas_ufs_2024_2.xlsx"
ARQUIVO_MUNICIPIO = "resultados_e_metas_municipios_2024.xlsx"

ANOS_META = list(range(2024, 2031))
METODOLOGIA = "oficial_inep_compromisso_nacional_crianca_alfabetizada"

REDE_MAP = {
    "TOTAL": 0,
    "PUBLICA": 0,
    "PUBLICO": 0,
    "REDE PUBLICA": 0,
    "ESTADUAL": 2,
    "MUNICIPAL": 3,
    "PRIVADA": 5,
}


def resolver_base() -> Path:
    """
    Localiza a raiz do repositório.

    Funciona:
    - via python scripts/gerar_fontes.py (arquivo próprio, com __file__)
    - em notebook Jupyter local (cwd = raiz do projeto)

    NÃO funciona quando este código roda como célula dentro de um notebook
    Databricks: não existe __file__, e o cwd do driver aponta pra um disco
    local do cluster, não pra pasta do projeto no Workspace. Nesse caso,
    defina BASE manualmente logo abaixo em vez de confiar nesta função.
    """
    if "__file__" in globals():
        candidato = Path(__file__).resolve().parent
        for p in (candidato, *candidato.parents):
            if (p / "data").exists():
                return p

    cwd = Path.cwd().resolve()
    for p in (cwd, *cwd.parents):
        if (p / "data").exists():
            return p

    raise FileNotFoundError(
        "Não foi possível localizar a raiz do projeto. "
        "Execute o script dentro do repositório que contém a pasta data/, "
        "ou defina BASE manualmente (veja o comentário acima desta função)."
    )


# PREENCHA com o caminho da SUA pasta do projeto no Workspace — cada pessoa
# tem um caminho diferente, porque cada uma está no próprio Free Edition.
# Pra achar o seu: abra qualquer notebook do projeto e copie o caminho que
# aparece no topo da tela (ou clique com o botão direito na pasta → Copy
# path). Rodando como célula dentro de um notebook Databricks (o caso de
# 00_setup_ambiente.py), resolver_base() sozinha NÃO funciona — por isso
# essa linha existe, em vez de chamar resolver_base() direto.
BASE = Path(
    "/Workspace/Users/hermistark@gmail.com/tech-challenge-fase2-alfabetizacao"
)
RAW = BASE / "data" / "raw"
EXT = BASE / "data" / "external"
SOURCE = BASE / "data" / "source"
LEGACY = BASE / "data" / "legacy_fontes_derivadas"


def normalizar_texto(valor) -> str:
    if valor is None:
        return ""
    texto = str(valor).strip()
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = texto.upper()
    texto = re.sub(r"\s+", " ", texto)
    return texto.strip()


def normalizar_coluna(valor) -> str:
    texto = normalizar_texto(valor).lower()
    texto = re.sub(r"[^a-z0-9]+", "_", texto)
    return texto.strip("_")


def numero(valor):
    """Converte números do XLSX, inclusive valores com vírgula decimal e %."""
    if pd.isna(valor):
        return None

    if isinstance(valor, (int, float)):
        return float(valor)

    texto = str(valor).strip()
    if not texto or texto in {"-", "--", "NA", "N/A", "NAN"}:
        return None

    texto = texto.replace("%", "").replace(">", "").replace("<", "").strip()

    # 1.234,56 -> 1234.56 | 56,78 -> 56.78
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")

    texto = re.sub(r"[^0-9.\-]", "", texto)

    if not texto:
        return None

    try:
        return float(texto)
    except ValueError:
        return None


def inteiro(valor):
    n = numero(valor)
    return int(n) if n is not None else None


def id_municipio(valor) -> str | None:
    n = inteiro(valor)
    if n is None:
        return None
    return str(n).zfill(7)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for bloco in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloco)
    return h.hexdigest()


def localizar_header(path: Path, obrigatorios: tuple[str, ...]) -> tuple[str, int]:
    """
    Localiza automaticamente a aba e a linha de cabeçalho do XLSX.

    As planilhas oficiais podem ter título/notas antes da tabela.
    """
    xls = pd.ExcelFile(path, engine="openpyxl")

    for aba in xls.sheet_names:
        preview = pd.read_excel(
            path,
            sheet_name=aba,
            header=None,
            nrows=40,
            engine="openpyxl",
        )

        for idx, row in preview.iterrows():
            linha = " | ".join(normalizar_texto(v) for v in row.tolist() if not pd.isna(v))
            if all(normalizar_texto(token) in linha for token in obrigatorios):
                return aba, int(idx)

    raise ValueError(
        f"Não consegui localizar o cabeçalho esperado em {path.name}. "
        f"Tokens procurados: {obrigatorios}"
    )


def carregar_planilha(path: Path, obrigatorios: tuple[str, ...]) -> pd.DataFrame:
    aba, header_row = localizar_header(path, obrigatorios)

    df = pd.read_excel(
        path,
        sheet_name=aba,
        header=header_row,
        engine="openpyxl",
    )

    # Remove colunas completamente vazias
    df = df.dropna(axis=1, how="all")

    # Normaliza nomes, mantendo unicidade
    nomes = []
    usados = {}
    for c in df.columns:
        base = normalizar_coluna(c) or "coluna"
        n = usados.get(base, 0)
        usados[base] = n + 1
        nomes.append(base if n == 0 else f"{base}_{n+1}")

    df.columns = nomes

    # Remove linhas completamente vazias
    return df.dropna(how="all").reset_index(drop=True)


def achar_coluna(
    df: pd.DataFrame,
    *,
    contem: tuple[str, ...],
    exclui: tuple[str, ...] = (),
    obrigatoria: bool = True,
) -> str | None:
    contem_n = tuple(normalizar_coluna(x) for x in contem)
    exclui_n = tuple(normalizar_coluna(x) for x in exclui)

    candidatos = []
    for col in df.columns:
        if all(token in col for token in contem_n) and not any(
            token in col for token in exclui_n
        ):
            candidatos.append(col)

    if candidatos:
        # prefere o nome mais curto/específico
        return sorted(candidatos, key=len)[0]

    if obrigatoria:
        raise KeyError(
            f"Coluna não encontrada. Deve conter={contem} excluir={exclui}. "
            f"Colunas disponíveis: {list(df.columns)}"
        )
    return None


def achar_meta(df: pd.DataFrame, ano: int) -> str:
    return achar_coluna(df, contem=("meta", str(ano)))


def achar_resultado(df: pd.DataFrame, ano: int) -> str | None:
    # Percentual de alunos alfabetizados - 2023 / 2024
    candidatos = []
    for col in df.columns:
        if (
            str(ano) in col
            and "alfabet" in col
            and "meta" not in col
            and "nivel" not in col
        ):
            candidatos.append(col)

    if not candidatos:
        return None

    return sorted(candidatos, key=len)[0]


def achar_sigla_uf(df: pd.DataFrame) -> str:
    return achar_coluna(df, contem=("sigla", "uf"))


def achar_rede(df: pd.DataFrame) -> str | None:
    return achar_coluna(df, contem=("rede",), obrigatoria=False)


def mapear_rede(valor) -> int:
    if pd.isna(valor):
        return 0

    texto = normalizar_texto(valor)

    # se já vier código numérico
    try:
        return int(float(str(valor)))
    except (ValueError, TypeError):
        pass

    for nome, codigo in REDE_MAP.items():
        if nome in texto:
            return codigo

    raise ValueError(f"Rede de ensino não reconhecida na fonte oficial: {valor!r}")


def gerar_dimensoes_ibge():
    """
    Mantém a estratégia original:
    dimensões territoriais são geradas dos arquivos IBGE em data/external.
    """
    estados_path = EXT / "estados.csv"
    municipios_path = EXT / "municipios.csv"

    if not estados_path.exists() or not municipios_path.exists():
        raise FileNotFoundError(
            "Arquivos territoriais ausentes em data/external/: "
            "estados.csv e municipios.csv são obrigatórios."
        )

    estados = list(
        csv.DictReader(estados_path.open(encoding="utf-8-sig"))
    )
    municipios = list(
        csv.DictReader(municipios_path.open(encoding="utf-8-sig"))
    )

    uf_por_codigo = {e["codigo_uf"]: e["uf"] for e in estados}

    with (RAW / "uf.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["codigo_uf", "sigla_uf", "nome", "regiao"])
        for e in sorted(estados, key=lambda x: x["codigo_uf"]):
            w.writerow(
                [e["codigo_uf"], e["uf"], e["nome"], e["regiao"]]
            )

    with (RAW / "municipio.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["id_municipio", "nome", "sigla_uf", "capital", "latitude", "longitude"]
        )

        for m in sorted(municipios, key=lambda x: x["codigo_ibge"]):
            w.writerow(
                [
                    m["codigo_ibge"],
                    m["nome"],
                    uf_por_codigo[m["codigo_uf"]],
                    m["capital"],
                    m["latitude"],
                    m["longitude"],
                ]
            )

    return len(estados), len(municipios)


def carregar_ufs_oficiais(path: Path) -> pd.DataFrame:
    return carregar_planilha(
        path,
        obrigatorios=("META 2024", "META 2030"),
    )


def carregar_municipios_oficiais(path: Path) -> pd.DataFrame:
    return carregar_planilha(
        path,
        obrigatorios=("MUNIC", "META 2024", "META 2030"),
    )


def linha_brasil(df: pd.DataFrame) -> pd.Series:
    """
    Localiza a linha Brasil sem depender do nome exato da coluna.
    """
    for _, row in df.iterrows():
        texto = " | ".join(
            normalizar_texto(v) for v in row.tolist() if not pd.isna(v)
        )
        if re.search(r"(^|\W)BRASIL($|\W)", texto):
            return row

    raise ValueError(
        "A linha BRASIL não foi localizada na planilha de resultados/metas UF. "
        "Fail-closed: meta nacional não será inventada."
    )


def gerar_meta_brasil(df_uf: pd.DataFrame) -> int:
    br = linha_brasil(df_uf)
    registros = []

    for ano in ANOS_META:
        col_meta = achar_meta(df_uf, ano)
        valor = numero(br[col_meta])

        if valor is None:
            raise ValueError(f"Meta Brasil {ano} está vazia na fonte oficial.")

        registros.append(
            {
                "ano": ano,
                "meta": valor,
                "metodologia": METODOLOGIA,
            }
        )

    pd.DataFrame(registros).to_csv(
        RAW / "meta_brasil.csv",
        index=False,
        encoding="utf-8",
    )
    return len(registros)


def gerar_meta_uf(df_uf: pd.DataFrame) -> int:
    col_sigla = achar_sigla_uf(df_uf)
    registros = []

    for _, row in df_uf.iterrows():
        sigla = normalizar_texto(row.get(col_sigla))

        # somente siglas oficiais de UF; exclui Brasil/linhas de região/notas
        if not re.fullmatch(r"[A-Z]{2}", sigla) or sigla == "BR":
            continue

        for ano in ANOS_META:
            valor = numero(row[achar_meta(df_uf, ano)])
            if valor is None:
                continue

            registros.append(
                {
                    "sigla_uf": sigla,
                    "ano": ano,
                    "meta": valor,
                    "metodologia": METODOLOGIA,
                }
            )

    out = pd.DataFrame(registros).drop_duplicates(
        subset=["sigla_uf", "ano"],
        keep="first",
    )

    if out.empty:
        raise ValueError("Nenhuma meta oficial de UF foi extraída.")

    out = out.sort_values(["sigla_uf", "ano"])
    out.to_csv(RAW / "meta_uf.csv", index=False, encoding="utf-8")

    return len(out)


def col_id_municipio(df: pd.DataFrame) -> str:
    # prefere explicitamente código município
    for tokens in (
        ("codigo", "municipio"),
        ("id", "municipio"),
        ("cod", "municipio"),
    ):
        try:
            return achar_coluna(df, contem=tokens)
        except KeyError:
            pass

    raise KeyError(
        f"Não encontrei código/id do município. Colunas: {list(df.columns)}"
    )


def gerar_meta_municipio(df_mun: pd.DataFrame) -> int:
    col_id = col_id_municipio(df_mun)
    col_sigla = achar_sigla_uf(df_mun)

    registros = []

    for _, row in df_mun.iterrows():
        municipio = id_municipio(row.get(col_id))
        sigla = normalizar_texto(row.get(col_sigla))

        if municipio is None or not re.fullmatch(r"[A-Z]{2}", sigla):
            continue

        for ano in ANOS_META:
            valor = numero(row[achar_meta(df_mun, ano)])
            if valor is None:
                continue

            registros.append(
                {
                    "id_municipio": municipio,
                    "sigla_uf": sigla,
                    "ano": ano,
                    "meta": valor,
                    "metodologia": METODOLOGIA,
                }
            )

    out = pd.DataFrame(registros).drop_duplicates(
        subset=["id_municipio", "ano"],
        keep="first",
    )

    if out.empty:
        raise ValueError("Nenhuma meta municipal oficial foi extraída.")

    out = out.sort_values(["id_municipio", "ano"])
    out.to_csv(
        RAW / "meta_municipio.csv",
        index=False,
        encoding="utf-8",
    )

    return len(out)


def gerar_indicador_municipio(df_mun: pd.DataFrame) -> int:
    """
    Materializa os resultados municipais oficiais no schema que a Bronze atual espera:

    ano, sigla_uf, id_municipio, serie, rede,
    taxa_alfabetizacao, media_portugues

    A planilha oficial traz percentual de alfabetizados, não média de proficiência.
    Portanto media_portugues fica nula — sem inventar valor.
    """
    col_id = col_id_municipio(df_mun)
    col_sigla = achar_sigla_uf(df_mun)
    col_rede = achar_rede(df_mun)

    col_2023 = achar_resultado(df_mun, 2023)
    col_2024 = achar_resultado(df_mun, 2024)

    if col_2023 is None and col_2024 is None:
        raise ValueError(
            "Não encontrei as colunas oficiais de percentual de alunos "
            "alfabetizados 2023/2024 na planilha municipal."
        )

    registros = []

    for _, row in df_mun.iterrows():
        municipio = id_municipio(row.get(col_id))
        sigla = normalizar_texto(row.get(col_sigla))

        if municipio is None or not re.fullmatch(r"[A-Z]{2}", sigla):
            continue

        rede = mapear_rede(row.get(col_rede)) if col_rede else 3

        for ano, coluna in ((2023, col_2023), (2024, col_2024)):
            if coluna is None:
                continue

            taxa = numero(row.get(coluna))
            if taxa is None:
                continue

            registros.append(
                {
                    "ano": ano,
                    "sigla_uf": sigla,
                    "id_municipio": municipio,
                    "serie": 2,
                    "rede": rede,
                    "taxa_alfabetizacao": taxa,
                    "media_portugues": None,
                }
            )

    out = pd.DataFrame(registros).drop_duplicates(
        subset=["ano", "id_municipio", "rede"],
        keep="first",
    )

    if out.empty:
        raise ValueError("Nenhum indicador municipal oficial foi extraído.")

    out = out.sort_values(["ano", "sigla_uf", "id_municipio", "rede"])

    with gzip.open(
        RAW / "br_inep_avaliacao_alfabetizacao_municipio.csv.gz",
        "wt",
        encoding="utf-8",
        newline="",
    ) as f:
        out.to_csv(f, index=False)

    return len(out)


def arquivar_simulacao_antiga():
    """
    Impede que alunos_simulados.csv.gz continue sendo tratado como fonte oficial.
    O arquivo é preservado apenas como legado.
    """
    antigo = RAW / "alunos_simulados.csv.gz"
    if not antigo.exists():
        return None

    LEGACY.mkdir(parents=True, exist_ok=True)

    destino = LEGACY / "alunos_simulados.csv.gz"
    if destino.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destino = LEGACY / f"alunos_simulados_{timestamp}.csv.gz"

    shutil.move(str(antigo), str(destino))
    return destino


def gerar_manifest(contagens: dict):
    fontes = [
        SOURCE / ARQUIVO_UF,
        SOURCE / ARQUIVO_MUNICIPIO,
        EXT / "estados.csv",
        EXT / "municipios.csv",
    ]

    manifest = {
        "gerado_em_utc": datetime.now(timezone.utc).isoformat(),
        "politica_dados": (
            "Fontes oficiais. Sem interpolação de metas, sem herança de meta da UF "
            "para municípios e sem geração sintética de alunos."
        ),
        "fontes": [
            {
                "arquivo": str(p.relative_to(BASE)),
                "sha256": sha256(p),
                "bytes": p.stat().st_size,
            }
            for p in fontes
            if p.exists()
        ],
        "saidas": contagens,
        "alunos": {
            "status": "pendente_microdados_oficiais",
            "simulacao_gerada": False,
        },
    }

    path = RAW / "fontes_oficiais_manifest.json"
    path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def validar_entradas():
    RAW.mkdir(parents=True, exist_ok=True)

    obrigatorios = [
        SOURCE / ARQUIVO_UF,
        SOURCE / ARQUIVO_MUNICIPIO,
        EXT / "estados.csv",
        EXT / "municipios.csv",
    ]

    faltantes = [p for p in obrigatorios if not p.exists()]

    if faltantes:
        raise FileNotFoundError(
            "Entradas obrigatórias ausentes:\n"
            + "\n".join(f"  - {p}" for p in faltantes)
            + "\n\nEstrutura esperada: data/source/ com os dois XLSX oficiais "
              "e data/external/ com estados.csv e municipios.csv."
        )


def main():
    print("=" * 72)
    print("PREPARAÇÃO DE FONTES OFICIAIS — ALFABETIZAÇÃO")
    print("=" * 72)
    print(f"BASE   : {BASE}")
    print(f"SOURCE : {SOURCE}")
    print(f"RAW    : {RAW}")
    print()

    validar_entradas()

    # A dimensão territorial continua vindo do IBGE.
    qtd_ufs, qtd_municipios = gerar_dimensoes_ibge()
    print(f"[OK] uf.csv                          {qtd_ufs:>8,d} linhas")
    print(f"[OK] municipio.csv                   {qtd_municipios:>8,d} linhas")

    # Fontes oficiais INEP.
    arquivo_uf = SOURCE / ARQUIVO_UF
    arquivo_mun = SOURCE / ARQUIVO_MUNICIPIO

    print("\nLendo planilhas oficiais do INEP...")
    df_uf = carregar_ufs_oficiais(arquivo_uf)
    df_mun = carregar_municipios_oficiais(arquivo_mun)

    qtd_meta_br = gerar_meta_brasil(df_uf)
    qtd_meta_uf = gerar_meta_uf(df_uf)
    qtd_meta_mun = gerar_meta_municipio(df_mun)
    qtd_ind_mun = gerar_indicador_municipio(df_mun)

    print(f"[OK] meta_brasil.csv                 {qtd_meta_br:>8,d} linhas")
    print(f"[OK] meta_uf.csv                     {qtd_meta_uf:>8,d} linhas")
    print(f"[OK] meta_municipio.csv              {qtd_meta_mun:>8,d} linhas")
    print(
        "[OK] br_inep_avaliacao_alfabetizacao_municipio.csv.gz "
        f"{qtd_ind_mun:>8,d} linhas"
    )

    legado = arquivar_simulacao_antiga()
    if legado:
        print(
            "\n[LEGADO] alunos_simulados.csv.gz removido de data/raw e arquivado em:"
        )
        print(f"         {legado}")

    contagens = {
        "uf.csv": qtd_ufs,
        "municipio.csv": qtd_municipios,
        "meta_brasil.csv": qtd_meta_br,
        "meta_uf.csv": qtd_meta_uf,
        "meta_municipio.csv": qtd_meta_mun,
        "br_inep_avaliacao_alfabetizacao_municipio.csv.gz": qtd_ind_mun,
    }
    gerar_manifest(contagens)

    print("\n" + "=" * 72)
    print("CONCLUÍDO")
    print("=" * 72)
    print("✓ metas Brasil: oficiais")
    print("✓ metas UF: oficiais")
    print("✓ metas município: oficiais")
    print("✓ indicador municipal: oficial")
    print("✓ dimensões territoriais: IBGE")
    print("✓ alunos simulados: NÃO gerados")
    print("⚠ microdados de alunos: próxima etapa, usando somente fonte oficial")


import shutil

VOLUME_BRONZE = Path("/Volumes/workspace/bronze/raw_files")


def copiar_raw_para_bronze():
    VOLUME_BRONZE.mkdir(parents=True, exist_ok=True)

    # O TS_ALUNO.csv não é gerado por este script (é o microdado oficial do
    # INEP, grande demais pra derivar aqui) — mas a pasta onde ele precisa
    # ser enviado manualmente já fica pronta, pra ninguém errar o caminho.
    PASTA_MICRODADOS = VOLUME_BRONZE / "microdados_inep" / "DADOS"
    PASTA_MICRODADOS.mkdir(parents=True, exist_ok=True)

    arquivos = [
        "br_inep_avaliacao_alfabetizacao_uf.csv.gz",
        "br_inep_avaliacao_alfabetizacao_municipio.csv.gz",
        "uf.csv",
        "municipio.csv",
        "meta_brasil.csv",
        "meta_uf.csv",
        "meta_municipio.csv",
        "fontes_oficiais_manifest.json",
    ]

    print("\n=== COPIANDO RAW PARA O VOLUME BRONZE ===")

    for nome in arquivos:
        origem = RAW / nome
        destino = VOLUME_BRONZE / nome

        if not origem.exists():
            raise FileNotFoundError(
                f"Arquivo obrigatório não encontrado: {origem}"
            )

        shutil.copy2(origem, destino)
        print(f"[OK] {nome}")

    print(
        f"\n✓ Arquivos oficiais disponíveis em: {VOLUME_BRONZE}"
    )

    caminho_ts_aluno = PASTA_MICRODADOS / "TS_ALUNO.csv"
    if caminho_ts_aluno.exists():
        print(f"✓ TS_ALUNO.csv já está presente em: {caminho_ts_aluno}")
    else:
        print(
            "\n⚠ AÇÃO MANUAL NECESSÁRIA: TS_ALUNO.csv não encontrado.\n"
            f"  A pasta já foi criada em: {PASTA_MICRODADOS}\n"
            "  Baixe o microdado oficial do INEP (Avaliação da "
            "Alfabetização) e suba o arquivo TS_ALUNO.csv nessa pasta "
            "pela interface do Databricks (Catalog → workspace → bronze "
            "→ Volumes → raw_files → microdados_inep → DADOS → "
            "Upload to this volume) antes de rodar 01_bronze_batch.py."
        )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Execução
# MAGIC Roda as funções definidas na célula anterior: `main()` gera os CSVs
# MAGIC oficiais em `data/raw/`, e `copiar_raw_para_bronze()` publica tudo no
# MAGIC Volume.

# COMMAND ----------
main()
copiar_raw_para_bronze()
