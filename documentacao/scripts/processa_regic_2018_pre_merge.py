#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Converte os arquivos REGIC 2018 para CSV e replica, no arquivo de cidades,
os municipios integrantes que aparecem apenas no arquivo de arranjos.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
BRONZE_DIR = BASE_DIR / "bronze"
OUTPUT_DIR = BASE_DIR / "prata" / "pre_merge" / "regic_2018"

ARRANJOS_XLSX = BRONZE_DIR / "REGIC2018_Arranjos_Populacionais_v2 (1).xlsx"
CIDADES_XLSX = BRONZE_DIR / "REGIC2018_Cidades_v2 (1).xlsx"
ARRANJOS_CSV = OUTPUT_DIR / "REGIC2018_Arranjos_Populacionais_v2 (1).csv"
CIDADES_CSV = OUTPUT_DIR / "REGIC2018_Cidades_v2 (1).csv"

UF_MAP = {
    11: "RO",
    12: "AC",
    13: "AM",
    14: "RR",
    15: "PA",
    16: "AP",
    17: "TO",
    21: "MA",
    22: "PI",
    23: "CE",
    24: "RN",
    25: "PB",
    26: "PE",
    27: "AL",
    28: "SE",
    29: "BA",
    31: "MG",
    32: "ES",
    33: "RJ",
    35: "SP",
    41: "PR",
    42: "SC",
    43: "RS",
    50: "MS",
    51: "MT",
    52: "GO",
    53: "DF",
}


def converter_xlsx_para_csv(origem: Path, destino: Path) -> str:
    xls = pd.ExcelFile(origem)
    if not xls.sheet_names:
        raise ValueError(f"Nenhuma aba encontrada em {origem}.")

    aba = xls.sheet_names[0]
    df = pd.read_excel(origem, sheet_name=aba)
    df.to_csv(destino, index=False)
    return aba


def expandir_cidades_com_arranjos(arranjos_csv: Path, cidades_csv: Path) -> tuple[int, int]:
    arranjos = pd.read_csv(arranjos_csv)
    cidades = pd.read_csv(cidades_csv)

    faltantes = arranjos.loc[~arranjos["Codmun"].isin(cidades["COD_CIDADE"])].copy()
    if faltantes.empty:
        return 0, len(cidades)

    base_arranjos = cidades.set_index("COD_CIDADE").loc[faltantes["Código do AP"]].reset_index()

    novas_linhas = base_arranjos.copy()
    novas_linhas["COD_CIDADE"] = faltantes["Codmun"].to_numpy()
    novas_linhas["NOME_CIDADE"] = faltantes["Nome do Município"].to_numpy()
    novas_linhas["COD_UF"] = (
        novas_linhas["COD_CIDADE"].astype(str).str.zfill(7).str[:2].astype(int)
    )
    novas_linhas["UF"] = novas_linhas["COD_UF"].map(UF_MAP)

    if novas_linhas["UF"].isna().any():
        codigos = sorted(novas_linhas.loc[novas_linhas["UF"].isna(), "COD_UF"].unique())
        raise ValueError(f"UF nao mapeada para os codigos: {codigos}")

    resultado = pd.concat([cidades, novas_linhas], ignore_index=True)
    resultado = resultado.drop_duplicates(subset=["COD_CIDADE"], keep="first")
    resultado = resultado.sort_values("COD_CIDADE").reset_index(drop=True)
    resultado.to_csv(cidades_csv, index=False)

    return len(novas_linhas), len(resultado)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    aba_arranjos = converter_xlsx_para_csv(ARRANJOS_XLSX, ARRANJOS_CSV)
    aba_cidades = converter_xlsx_para_csv(CIDADES_XLSX, CIDADES_CSV)
    adicionados, total_final = expandir_cidades_com_arranjos(ARRANJOS_CSV, CIDADES_CSV)

    print(f"Arquivo gerado: {ARRANJOS_CSV} | aba usada: {aba_arranjos}")
    print(f"Arquivo gerado: {CIDADES_CSV} | aba usada: {aba_cidades}")
    print(f"Municipios adicionados ao CSV de cidades: {adicionados}")
    print(f"Total final de linhas no CSV de cidades: {total_final}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
