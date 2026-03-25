#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Faz merge da tabela 5882 com a base v12 e incorpora o indicador de existencia de plano diretor.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V12_FILE = BASE_DIR / "prata" / "processamento" / "merge_v12.csv"
TABELA5882_FILE = BASE_DIR / "bronze" / "tabela5882.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v13.csv"
COLUNA_SAIDA = "Existência de Plano Diretor - Existe"


def normalizar_codigo(valor: object) -> pd.NA | str:
    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def carregar_v12() -> pd.DataFrame:
    df = pd.read_csv(V12_FILE)
    if "Cód." not in df.columns:
        raise ValueError("Coluna 'Cód.' nao encontrada na base v12.")

    df["_codigo_merge"] = df["Cód."].map(normalizar_codigo)
    return df


def carregar_tabela5882() -> pd.DataFrame:
    df = pd.read_csv(TABELA5882_FILE, skiprows=4)

    colunas_necessarias = ["Cód.", "Total"]
    faltantes = [col for col in colunas_necessarias if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na tabela 5882: {faltantes}")

    base = df[colunas_necessarias].copy()
    base["Cód."] = pd.to_numeric(base["Cód."], errors="coerce")
    base = base[base["Cód."].notna()].copy()
    base = base[base["Cód."].ne(0)].copy()

    base[COLUNA_SAIDA] = base["Total"].replace({"-": 0, "1": 1})
    base[COLUNA_SAIDA] = pd.to_numeric(base[COLUNA_SAIDA], errors="coerce")
    base["_codigo_merge"] = base["Cód."].map(normalizar_codigo)
    base = base[["_codigo_merge", COLUNA_SAIDA]]

    duplicados = base["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        raise ValueError(f"Foram encontrados {duplicados} codigos duplicados na tabela 5882.")

    return base


def main() -> int:
    v12 = carregar_v12()
    tabela5882 = carregar_tabela5882()

    resultado = v12.merge(tabela5882, on="_codigo_merge", how="left", indicator=True)
    correspondencias = resultado["_merge"].eq("both").sum()
    resultado = resultado.drop(columns=["_codigo_merge", "_merge"])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Municipios com correspondencia na tabela 5882: {correspondencias}")
    print(f"Municipios sem correspondencia na tabela 5882: {len(resultado) - correspondencias}")
    print(f"Coluna incorporada: {COLUNA_SAIDA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
