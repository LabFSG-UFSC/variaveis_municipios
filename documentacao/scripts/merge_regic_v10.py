#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da v10 com a base REGIC 2018 e incorpora apenas as variaveis VAR56 a VAR66.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V10_FILE = BASE_DIR / "prata" / "processamento" / "merge_v10.csv"
REGIC_FILE = BASE_DIR / "prata" / "pre_merge" / "regic_2018" / "REGIC2018_Cidades_v2 (1).csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v11.csv"
COLUNAS_REGIC = [f"VAR{i}" for i in range(56, 67)]


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v10() -> pd.DataFrame:
    df = pd.read_csv(V10_FILE)
    if "Cód." not in df.columns:
        raise ValueError("Coluna 'Cód.' nao encontrada na base v10.")

    df["_codigo_merge"] = df["Cód."].map(normalizar_codigo)
    return df


def carregar_regic() -> pd.DataFrame:
    df = pd.read_csv(REGIC_FILE)
    colunas_necessarias = ["COD_CIDADE", *COLUNAS_REGIC]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes no arquivo da REGIC: {faltantes}")

    base = df[colunas_necessarias].copy()
    base["_codigo_merge"] = base["COD_CIDADE"].map(normalizar_codigo)
    base = base.drop(columns=["COD_CIDADE"])
    base = base.drop_duplicates(subset=["_codigo_merge"], keep="first")
    return base


def main() -> int:
    v10 = carregar_v10()
    regic = carregar_regic()

    resultado = v10.merge(regic, on="_codigo_merge", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia na REGIC: {correspondencias}")
    print(f"Municipios sem correspondencia na REGIC: {len(resultado) - correspondencias}")
    print(f"Colunas incorporadas: {', '.join(COLUNAS_REGIC)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
