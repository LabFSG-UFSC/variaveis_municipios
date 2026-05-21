#!/usr/bin/env python3
"""Gera um GeoPackage para cada categoria do novo fuzzy reduzido."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CLASSIFICACAO_CSV = ROOT / "outputs" / "classificacao_capitais_intermediarias_fuzzy.csv"
GEOMETRIAS_GPKG = ROOT / "inputs" / "capitais_intermediarias_geometrias.gpkg"
OUTPUT_DIR = ROOT / "outputs" / "gpkg_fuzzy_capitais_intermediarias"
LAYER_NAME = "capitais_intermediarias"


def slug_categoria(valor: str) -> str:
    return valor.strip().lower().replace(" ", "_")


def main() -> int:
    classificacao = pd.read_csv(CLASSIFICACAO_CSV)
    classificacao["cod_mun"] = classificacao["cod_mun"].astype(str).str.strip()
    geometrias = gpd.read_file(GEOMETRIAS_GPKG, layer=LAYER_NAME)
    geometrias["cod_mun"] = geometrias["CD_MUN"].astype(str).str.strip()
    base = geometrias.merge(classificacao, on="cod_mun", how="inner")
    if len(base) != len(classificacao):
        raise ValueError("Nem todos os municipios da classificacao fuzzy foram encontrados na malha do pacote.")
    colunas_saida = [
        "cod_mun", "NM_MUN", "SIGLA_UF", "CD_RGINT", "NM_RGINT", "ranking_final",
        "classificacao_fuzzy", "confianca_classificacao", "score_final",
        "dinamismo_economico", "infraestrutura_urbana", "conectividade_digital",
        "oferta_servicos", "seguranca_territorial", "empresas_1k", "regic_var60",
        "via_pav_pct", "indice_conectividade", "estab_saude_10k", "homicidios_100k",
        "geometry",
    ]
    base = base[colunas_saida].rename(columns={"NM_MUN": "municipio", "SIGLA_UF": "uf", "CD_RGINT": "cd_rgint", "NM_RGINT": "nm_rgint"})
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for categoria in sorted(base["classificacao_fuzzy"].dropna().unique()):
        subset = base[base["classificacao_fuzzy"] == categoria].copy()
        output_file = OUTPUT_DIR / f"capitais_intermediarias_fuzzy_{slug_categoria(categoria)}.gpkg"
        subset.to_file(output_file, layer=f"capitais_{slug_categoria(categoria)}", driver="GPKG")
        print(f"{categoria}: {len(subset)} municipios -> {output_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
