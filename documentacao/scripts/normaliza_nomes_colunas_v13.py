#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Normaliza os nomes das colunas da v13 para um padrao mais curto e legivel e gera a v14.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[2]
INPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v13.csv"
OUTPUT_FILE = BASE_DIR / "prata" / "processamento" / "merge_v14.csv"

RENOMEAR_COLUNAS = {
    "Cód.": "cod_mun",
    "Município": "municipio",
    "impostos - sub": "impostos_sub",
    "pib_serviços": "pib_servicos",
    "pop": "pop_total",
    "dom": "dom_total",
    "total_empresas": "empresas_total",
    "fundeb_coun_vaaf": "fundeb_vaaf",
    "fundeb_coun_vaar": "fundeb_vaar",
    "fundeb_coun_vaat": "fundeb_vaat",
    "Atendimento da população total com rede coletora de esgoto": "esgoto_pop_total_rede",
    "Atendimento da população urbana com rede coletora de esgoto": "esgoto_pop_urb_rede",
    "Atendimento dos domicílios totais com rede coletora de esgoto": "esgoto_dom_total_rede",
    "Atendimento dos domicílios urbanos com rede coletora de esgoto": "esgoto_dom_urb_rede",
    "Atendimento dos domicílios totais com coleta e tratamento de esgoto": "esgoto_dom_total_trat",
    "Atendimento dos domicílios urbanos com coleta e tratamento de esgoto": "esgoto_dom_urb_trat",
    "Via pavimentada - Existe (%)": "via_pav_pct",
    "Via pavimentada - Existe (N)": "via_pav_n",
    "Existência de iluminação pública - Existe (%)": "ilum_pub_pct",
    "Existência de iluminação pública - Existe (N)": "ilum_pub_n",
    "Existência de calçada / passeio - Existe (%)": "calcada_pct",
    "Existência de calçada / passeio - Existe (N)": "calcada_n",
    "VAR56": "regic_var56",
    "VAR57": "regic_var57",
    "VAR58": "regic_var58",
    "VAR59": "regic_var59",
    "VAR60": "regic_var60",
    "VAR61": "regic_var61",
    "VAR62": "regic_var62",
    "VAR63": "regic_var63",
    "VAR64": "regic_var64",
    "VAR65": "regic_var65",
    "VAR66": "regic_var66",
    "Existência de Plano Diretor - Existe": "plano_diretor",
}


def main() -> int:
    df = pd.read_csv(INPUT_FILE)

    faltantes = [col for col in RENOMEAR_COLUNAS if col not in df.columns]
    if faltantes:
        raise ValueError(f"Colunas ausentes na v13: {faltantes}")

    resultado = df.rename(columns=RENOMEAR_COLUNAS)

    duplicadas = resultado.columns[resultado.columns.duplicated()].tolist()
    if duplicadas:
        raise ValueError(f"Renomeacao gerou colunas duplicadas: {duplicadas}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    resultado.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f"Arquivo gerado: {OUTPUT_FILE}")
    print(f"Linhas na base final: {len(resultado)}")
    print(f"Colunas renomeadas: {len(RENOMEAR_COLUNAS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
