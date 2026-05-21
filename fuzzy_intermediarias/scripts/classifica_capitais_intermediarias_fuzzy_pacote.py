#!/usr/bin/env python3
"""
Versao reduzida do fuzzy das capitais de regioes intermediarias.

Mantem apenas os indicadores mais explicativos e corta variaveis redundantes
ou muito sobrepostas ao mesmo fenomeno.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
BASE_MUNICIPAL_PADRAO = ROOT / "inputs" / "merge_v28_fuzzy_subset.csv"
CAPITAIS_PADRAO = ROOT / "inputs" / "capitais_intermediarias_nao_uf.csv"
ARQUIVO_SAIDA_PADRAO = ROOT / "outputs" / "classificacao_capitais_intermediarias_fuzzy.csv"
ARQUIVO_RESUMO_PADRAO = ROOT / "outputs" / "classificacao_capitais_intermediarias_fuzzy_resumo.csv"

COLUNAS_BASE = [
    "cod_mun",
    "municipio",
    "pop_total",
    "empresas_total",
    "estab_total",
    "vitimas_homicidio_2022",
    "via_pav_pct",
    "regic_var60",
    "indice_conectividade",
]

PESOS_EIXOS = {
    "dinamismo_economico": {
        "empresas_1k": 0.55,
        "regic_var60": 0.45,
    },
    "infraestrutura_urbana": {
        "via_pav_pct": 1.00,
    },
    "conectividade_digital": {
        "indice_conectividade": 1.00,
    },
    "oferta_servicos": {
        "estab_saude_10k": 1.00,
    },
    "seguranca_territorial": {
        "homicidios_100k": 1.00,
    },
}

PESOS_CLASSIFICACAO = {
    "dinamismo_economico": 0.30,
    "infraestrutura_urbana": 0.15,
    "conectividade_digital": 0.20,
    "oferta_servicos": 0.20,
    "seguranca_territorial": 0.15,
}

COLUNAS_EIXOS = list(PESOS_EIXOS.keys())
COLUNAS_PERTINENCIA = [
    "pert_muito_alto",
    "pert_alto",
    "pert_medio",
    "pert_baixo",
    "pert_muito_baixo",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Classifica as capitais de regioes intermediarias com fuzzy reduzido.")
    parser.add_argument("--base-municipal", default=str(BASE_MUNICIPAL_PADRAO))
    parser.add_argument("--capitais", default=str(CAPITAIS_PADRAO))
    parser.add_argument("--output", default=str(ARQUIVO_SAIDA_PADRAO))
    parser.add_argument("--output-resumo", default=str(ARQUIVO_RESUMO_PADRAO))
    return parser.parse_args()


def clip_0_100(serie: pd.Series) -> pd.Series:
    return pd.to_numeric(serie, errors="coerce").clip(lower=0, upper=100)


def safe_div(numerador: pd.Series, denominador: pd.Series, multiplicador: float = 1.0) -> pd.Series:
    resultado = np.where(denominador > 0, multiplicador * numerador / denominador, np.nan)
    return pd.Series(resultado, index=numerador.index, dtype="float64")


def percent_rank(serie: pd.Series, ascending: bool = True) -> pd.Series:
    valores = pd.to_numeric(serie, errors="coerce")
    return valores.rank(method="average", pct=True, ascending=ascending).fillna(0.5)


def weighted_mean(df: pd.DataFrame, pesos: dict[str, float]) -> pd.Series:
    colunas = list(pesos.keys())
    pesos_array = np.array([pesos[coluna] for coluna in colunas], dtype=float)
    matriz = df[colunas].to_numpy(dtype=float)
    mascara = np.isfinite(matriz)
    soma_pesos = (mascara * pesos_array).sum(axis=1)
    matriz_ajustada = np.where(mascara, matriz, 0.0)
    soma_ponderada = (matriz_ajustada * pesos_array).sum(axis=1)
    resultado = np.where(soma_pesos > 0, soma_ponderada / soma_pesos, np.nan)
    return pd.Series(resultado, index=df.index, dtype="float64")


def pertinencia_quantis(score: pd.Series) -> dict[str, pd.Series]:
    score = pd.to_numeric(score, errors="coerce")
    q20, q35, q50, q65, q80 = score.quantile([0.20, 0.35, 0.50, 0.65, 0.80]).tolist()
    q35 = max(q35, q20 + 1e-9)
    q50 = max(q50, q35 + 1e-9)
    q65 = max(q65, q50 + 1e-9)
    q80 = max(q80, q65 + 1e-9)

    pert_mb = np.where(score <= q20, 1.0, np.where(score <= q35, (q35 - score) / (q35 - q20), 0.0))
    pert_b = np.where(score <= q20, 0.0, np.where(score <= q35, (score - q20) / (q35 - q20), np.where(score <= q50, (q50 - score) / (q50 - q35), 0.0)))
    pert_m = np.where(score <= q35, 0.0, np.where(score <= q50, (score - q35) / (q50 - q35), np.where(score <= q65, (q65 - score) / (q65 - q50), 0.0)))
    pert_a = np.where(score <= q50, 0.0, np.where(score <= q65, (score - q50) / (q65 - q50), np.where(score <= q80, (q80 - score) / (q80 - q65), 0.0)))
    pert_ma = np.where(score <= q65, 0.0, np.where(score <= q80, (score - q65) / (q80 - q65), 1.0))
    return {
        "muito_baixo": pd.Series(np.clip(pert_mb, 0, 1), index=score.index, dtype="float64"),
        "baixo": pd.Series(np.clip(pert_b, 0, 1), index=score.index, dtype="float64"),
        "medio": pd.Series(np.clip(pert_m, 0, 1), index=score.index, dtype="float64"),
        "alto": pd.Series(np.clip(pert_a, 0, 1), index=score.index, dtype="float64"),
        "muito_alto": pd.Series(np.clip(pert_ma, 0, 1), index=score.index, dtype="float64"),
    }


def carrega_capitais(caminho: Path) -> pd.DataFrame:
    df = pd.read_csv(caminho)
    df["cod_mun"] = pd.to_numeric(df["cod_mun"], errors="coerce").astype("Int64")
    return df[["cod_mun", "municipio", "uf"]].drop_duplicates()


def carrega_base(caminho: Path) -> pd.DataFrame:
    df = pd.read_csv(caminho, usecols=COLUNAS_BASE)
    df["cod_mun"] = pd.to_numeric(df["cod_mun"], errors="coerce").astype("Int64")
    for coluna in [c for c in COLUNAS_BASE if c not in {"cod_mun", "municipio"}]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
    return df


def prepara_base_capitais(base: pd.DataFrame, capitais: pd.DataFrame) -> pd.DataFrame:
    df = base.merge(capitais, on="cod_mun", how="inner", suffixes=("", "_capital"))
    if len(df) != len(capitais):
        raise ValueError("Nem todas as capitais de intermediaria foram encontradas na base municipal.")
    return df


def calcula_indicadores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["empresas_1k"] = safe_div(df["empresas_total"], df["pop_total"], multiplicador=1000.0)
    df["estab_saude_10k"] = safe_div(df["estab_total"], df["pop_total"], multiplicador=10000.0)
    df["homicidios_100k"] = safe_div(df["vitimas_homicidio_2022"], df["pop_total"], multiplicador=100000.0)
    df["via_pav_pct"] = clip_0_100(df["via_pav_pct"])
    df["indice_conectividade"] = clip_0_100(df["indice_conectividade"])
    return df


def calcula_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    positivas = ["empresas_1k", "regic_var60", "via_pav_pct", "indice_conectividade", "estab_saude_10k"]
    negativas = ["homicidios_100k"]
    for coluna in positivas:
        df[f"score_{coluna}"] = percent_rank(df[coluna], ascending=True)
    for coluna in negativas:
        df[f"score_{coluna}"] = percent_rank(df[coluna], ascending=False)
    return df


def calcula_eixos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for eixo, pesos in PESOS_EIXOS.items():
        base = pd.DataFrame({coluna: df[f"score_{coluna}"] for coluna in pesos}, index=df.index)
        df[eixo] = weighted_mean(base, pesos)
    return df


def classifica_fuzzy(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["score_final"] = weighted_mean(df[COLUNAS_EIXOS], PESOS_CLASSIFICACAO)
    pertinencias = pertinencia_quantis(df["score_final"])
    df["pert_muito_alto"] = pertinencias["muito_alto"]
    df["pert_alto"] = pertinencias["alto"]
    df["pert_medio"] = pertinencias["medio"]
    df["pert_baixo"] = pertinencias["baixo"]
    df["pert_muito_baixo"] = pertinencias["muito_baixo"]
    df["classificacao_fuzzy"] = df[COLUNAS_PERTINENCIA].idxmax(axis=1).str.removeprefix("pert_")
    df["confianca_classificacao"] = df[COLUNAS_PERTINENCIA].max(axis=1)
    df["ranking_final"] = df["score_final"].rank(method="min", ascending=False).astype(int)
    return df.sort_values(["ranking_final", "score_final", "municipio"], ascending=[True, False, True]).reset_index(drop=True)


def gera_resumo(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby("classificacao_fuzzy", dropna=False)
        .agg(
            qtd_municipios=("cod_mun", "count"),
            score_final_medio=("score_final", "mean"),
            confianca_media=("confianca_classificacao", "mean"),
        )
        .reset_index()
        .sort_values(["score_final_medio", "classificacao_fuzzy"], ascending=[False, True])
    )


def main() -> int:
    args = parse_args()
    capitais = carrega_capitais(Path(args.capitais))
    base = carrega_base(Path(args.base_municipal))
    df = prepara_base_capitais(base, capitais)
    df = calcula_indicadores(df)
    df = calcula_scores(df)
    df = calcula_eixos(df)
    df = classifica_fuzzy(df)
    resumo = gera_resumo(df)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    resumo_path = Path(args.output_resumo)
    colunas_saida = [
        "ranking_final",
        "cod_mun",
        "municipio",
        "uf",
        "classificacao_fuzzy",
        "confianca_classificacao",
        "score_final",
        *COLUNAS_EIXOS,
        "empresas_1k",
        "regic_var60",
        "via_pav_pct",
        "indice_conectividade",
        "estab_saude_10k",
        "homicidios_100k",
    ]
    df[colunas_saida].to_csv(output, index=False, encoding="utf-8")
    resumo.to_csv(resumo_path, index=False, encoding="utf-8")
    print(f"Arquivo gerado: {output}")
    print(f"Resumo gerado: {resumo_path}")
    print(f"Total de capitais classificadas: {len(df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
