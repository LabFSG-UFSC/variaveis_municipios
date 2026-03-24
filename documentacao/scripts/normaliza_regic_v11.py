#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Normaliza as variaveis VAR56 a VAR66 da v11 para a faixa [0, 1] e gera a v12.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
V11_FILE = BASE_DIR / "prata" / "processamento" / "merge_v11.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v12.csv"
COLUNAS_REGIC = [f"VAR{i}" for i in range(56, 67)]


def normalizar_min_max(serie: pd.Series) -> pd.Series:
    serie = pd.to_numeric(serie, errors="coerce")
    minimo = serie.min()
    maximo = serie.max()

    if pd.isna(minimo) or pd.isna(maximo):
        return serie

    amplitude = maximo - minimo
    if amplitude == 0:
        return pd.Series(0.0, index=serie.index)

    return (serie - minimo) / amplitude


def main() -> int:
    df = pd.read_csv(V11_FILE)

    faltantes = [col for col in COLUNAS_REGIC if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na v11: {faltantes}")

    resultado = df.copy()
    for coluna in COLUNAS_REGIC:
        resultado[coluna] = normalizar_min_max(resultado[coluna])

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8", float_format="%.15f")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Colunas normalizadas: {', '.join(COLUNAS_REGIC)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
