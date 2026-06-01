#!/usr/bin/env python3
"""Calcula a matriz de correlacao das variaveis usadas no fuzzy."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
BASE_MUNICIPAL_PADRAO = ROOT / "inputs" / "merge_v28_fuzzy_subset.csv"
CAPITAIS_PADRAO = ROOT / "inputs" / "capitais_intermediarias_nao_uf.csv"
SAIDA_MATRIZ_PADRAO = ROOT / "outputs" / "correlacao_variaveis_fuzzy.csv"
SAIDA_PARES_PADRAO = ROOT / "outputs" / "correlacao_variaveis_fuzzy_pares.csv"
SAIDA_RESUMO_PADRAO = ROOT / "outputs" / "correlacao_variaveis_fuzzy_resumo.md"
SAIDA_PNG_PADRAO = ROOT / "outputs" / "correlacao_variaveis_fuzzy_heatmap.png"

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

COLUNAS_ANALISE = [
    "empresas_1k",
    "regic_var60",
    "via_pav_pct",
    "indice_conectividade",
    "estab_saude_10k",
    "homicidios_100k",
    "dinamismo_economico",
    "infraestrutura_urbana",
    "conectividade_digital",
    "oferta_servicos",
    "seguranca_territorial",
    "score_final",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calcula a correlacao das variaveis usadas no fuzzy.")
    parser.add_argument("--base-municipal", default=str(BASE_MUNICIPAL_PADRAO))
    parser.add_argument("--capitais", default=str(CAPITAIS_PADRAO))
    parser.add_argument("--output", default=str(SAIDA_MATRIZ_PADRAO))
    parser.add_argument("--output-pares", default=str(SAIDA_PARES_PADRAO))
    parser.add_argument("--output-resumo", default=str(SAIDA_RESUMO_PADRAO))
    parser.add_argument("--output-png", default=str(SAIDA_PNG_PADRAO))
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
        raise ValueError("Nem todas as capitais intermediárias foram encontradas na base municipal.")
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
    pesos_eixos = {
        "dinamismo_economico": {"empresas_1k": 0.55, "regic_var60": 0.45},
        "infraestrutura_urbana": {"via_pav_pct": 1.00},
        "conectividade_digital": {"indice_conectividade": 1.00},
        "oferta_servicos": {"estab_saude_10k": 1.00},
        "seguranca_territorial": {"homicidios_100k": 1.00},
    }
    df = df.copy()
    for eixo, pesos in pesos_eixos.items():
        base = pd.DataFrame({coluna: df[f"score_{coluna}"] for coluna in pesos}, index=df.index)
        df[eixo] = weighted_mean(base, pesos)
    return df


def classifica_fuzzy(df: pd.DataFrame) -> pd.DataFrame:
    pesos_classificacao = {
        "dinamismo_economico": 0.30,
        "infraestrutura_urbana": 0.15,
        "conectividade_digital": 0.20,
        "oferta_servicos": 0.20,
        "seguranca_territorial": 0.15,
    }
    df = df.copy()
    df["score_final"] = weighted_mean(df[list(pesos_classificacao.keys())], pesos_classificacao)
    return df


def calcula_matriz_correlacao(df: pd.DataFrame) -> pd.DataFrame:
    matriz = df[COLUNAS_ANALISE].corr(method="pearson")
    return matriz.round(4)


def calcula_pares_relevantes(matriz: pd.DataFrame) -> pd.DataFrame:
    pares = []
    colunas = list(matriz.columns)
    for i, col_a in enumerate(colunas):
        for col_b in colunas[i + 1 :]:
            valor = matriz.loc[col_a, col_b]
            pares.append(
                {
                    "variavel_a": col_a,
                    "variavel_b": col_b,
                    "correlacao_pearson": float(valor),
                    "correlacao_absoluta": float(abs(valor)),
                }
            )
    return (
        pd.DataFrame(pares)
        .sort_values("correlacao_absoluta", ascending=False)
        .reset_index(drop=True)
    )


def gera_resumo(df: pd.DataFrame, matriz: pd.DataFrame, pares: pd.DataFrame) -> str:
    pares_ordenados = pares.sort_values("correlacao_pearson", ascending=False).reset_index(drop=True)
    pares_negativos = pares.sort_values("correlacao_pearson", ascending=True).reset_index(drop=True)
    linhas = []
    linhas.append("# Correlacao Das Variaveis Do Fuzzy")
    linhas.append("")
    linhas.append(f"- municipios analisados: `{len(df)}`")
    linhas.append(f"- variaveis na matriz: `{len(matriz.columns)}`")
    linhas.append(f"- maior correlacao positiva: `{pares_ordenados.iloc[0]['variavel_a']}` x `{pares_ordenados.iloc[0]['variavel_b']}` = `{pares_ordenados.iloc[0]['correlacao_pearson']:.4f}`")
    linhas.append(f"- maior correlacao negativa: `{pares_negativos.iloc[0]['variavel_a']}` x `{pares_negativos.iloc[0]['variavel_b']}` = `{pares_negativos.iloc[0]['correlacao_pearson']:.4f}`")
    linhas.append("")
    linhas.append("## Matriz de correlacao")
    linhas.append("")
    linhas.append("```text")
    linhas.append(matriz.to_string())
    linhas.append("```")
    linhas.append("")
    linhas.append("## Maiores correlacoes absolutas")
    linhas.append("")
    linhas.append("```text")
    linhas.append(pares.head(15).to_string(index=False))
    linhas.append("```")
    linhas.append("")
    return "\n".join(linhas)


def tenta_fontes() -> dict[str, ImageFont.FreeTypeFont | ImageFont.ImageFont]:
    candidatos = [
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Helvetica.ttc",
        "/Library/Fonts/Arial.ttf",
        "/System/Library/Fonts/Supplemental/DejaVu Sans.ttf",
    ]

    def load(size: int):
        for caminho in candidatos:
            try:
                return ImageFont.truetype(caminho, size=size)
            except OSError:
                continue
        return ImageFont.load_default()

    return {
        "title": load(28),
        "subtitle": load(16),
        "label": load(13),
        "value": load(12),
        "small": load(11),
        "tiny": load(10),
    }


def interpolar_cor(valor: float) -> tuple[int, int, int]:
    valor = float(np.clip(valor, -1.0, 1.0))
    if valor >= 0:
        t = valor
        r = int(255)
        g = int(255 * (1 - t * 0.72))
        b = int(255 * (1 - t * 0.72))
    else:
        t = abs(valor)
        r = int(255 * (1 - t * 0.70))
        g = int(255 * (1 - t * 0.70))
        b = int(255)
    return (r, g, b)


def gerar_png_heatmap(matriz: pd.DataFrame, pares: pd.DataFrame, saida: Path) -> None:
    saida.parent.mkdir(parents=True, exist_ok=True)
    fontes = tenta_fontes()

    labels = list(matriz.columns)
    n = len(labels)
    cell = 44
    margin_left = 260
    margin_top = 170
    legend_h = 54
    width = margin_left + n * cell + 340
    height = margin_top + n * cell + legend_h + 90
    img = Image.new("RGB", (width, height), "white")
    draw = ImageDraw.Draw(img)

    draw.text((35, 26), "Correlacao das variaveis usadas no fuzzy", fill="#111111", font=fontes["title"])
    draw.text((35, 64), "Matriz de Pearson entre variaveis brutas, eixos sinteticos e score_final", fill="#555555", font=fontes["subtitle"])

    # legenda
    lx0 = 35
    ly0 = 100
    lx1 = 35 + 420
    draw.text((lx0, ly0), "-1", fill="#444444", font=fontes["small"])
    draw.text((lx1 - 18, ly0), "+1", fill="#444444", font=fontes["small"])
    for i in range(320):
        frac = i / 319
        valor = -1 + 2 * frac
        cor = interpolar_cor(valor)
        x0 = lx0 + 18 + i
        draw.line((x0, ly0 + 10, x0, ly0 + 24), fill=cor, width=1)
    draw.text((lx0 + 150, ly0 + 28), "Escala de cor: negativo -> neutro -> positivo", fill="#666666", font=fontes["small"])

    # labels do topo
    for j, label in enumerate(labels):
        x = margin_left + j * cell + cell / 2
        label_img = Image.new("RGBA", (180, 60), (255, 255, 255, 0))
        label_draw = ImageDraw.Draw(label_img)
        label_draw.text((90, 30), label, fill="#222222", font=fontes["tiny"], anchor="mm")
        rot = label_img.rotate(45, resample=Image.Resampling.BICUBIC, expand=True)
        img.paste(rot, (int(x - rot.width / 2), margin_top - 92), rot)

    # labels da esquerda
    for i, label in enumerate(labels):
        y = margin_top + i * cell + cell / 2
        draw.text((margin_left - 10, y - 6), label, fill="#222222", font=fontes["tiny"], anchor="ra")

    # células
    for i, row_label in enumerate(labels):
        for j, col_label in enumerate(labels):
            valor = float(matriz.loc[row_label, col_label])
            cor = interpolar_cor(valor)
            x0 = margin_left + j * cell
            y0 = margin_top + i * cell
            x1 = x0 + cell
            y1 = y0 + cell
            draw.rectangle((x0, y0, x1, y1), fill=cor, outline="#ffffff")
            txt = f"{valor:.2f}"
            bbox = draw.textbbox((0, 0), txt, font=fontes["value"])
            tw = bbox[2] - bbox[0]
            th = bbox[3] - bbox[1]
            tx = x0 + (cell - tw) / 2
            ty = y0 + (cell - th) / 2 - 1
            cor_texto = "#ffffff" if abs(valor) >= 0.55 else "#222222"
            draw.text((tx, ty), txt, fill=cor_texto, font=fontes["value"])

    # borda geral
    draw.rectangle((margin_left, margin_top, margin_left + n * cell, margin_top + n * cell), outline="#cccccc", width=2)

    # resumo lateral
    top_pairs = pares.head(5)
    box_x0 = margin_left + n * cell + 20
    box_y0 = margin_top
    box_x1 = width - 30
    box_y1 = box_y0 + 250
    draw.rounded_rectangle((box_x0, box_y0, box_x1, box_y1), radius=14, fill="#fafafa", outline="#dddddd", width=1)
    draw.text((box_x0 + 14, box_y0 + 12), "Maiores correlacoes", fill="#222222", font=fontes["label"])
    for idx, (_, row) in enumerate(top_pairs.iterrows()):
        yy = box_y0 + 42 + idx * 38
        linha = f"{row['variavel_a']} x {row['variavel_b']}: {row['correlacao_pearson']:.4f}"
        draw.text((box_x0 + 14, yy), linha, fill="#444444", font=fontes["small"])

    draw.text(
        (35, height - 40),
        "Arquivos correlatos: correlacao_variaveis_fuzzy.csv, correlacao_variaveis_fuzzy_pares.csv e correlacao_variaveis_fuzzy_resumo.md",
        fill="#666666",
        font=fontes["small"],
    )

    img.save(saida)


def main() -> int:
    args = parse_args()
    capitais = carrega_capitais(Path(args.capitais))
    base = carrega_base(Path(args.base_municipal))
    df = prepara_base_capitais(base, capitais)
    df = calcula_indicadores(df)
    df = calcula_scores(df)
    df = calcula_eixos(df)
    df = classifica_fuzzy(df)

    matriz = calcula_matriz_correlacao(df)
    pares = calcula_pares_relevantes(matriz)
    resumo = gera_resumo(df, matriz, pares)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    matriz.to_csv(output, index=True, encoding="utf-8")
    pares.to_csv(Path(args.output_pares), index=False, encoding="utf-8")
    Path(args.output_resumo).write_text(resumo, encoding="utf-8")
    gerar_png_heatmap(matriz, pares, Path(args.output_png))

    print(f"Matriz gerada: {output}")
    print(f"Pares relevantes gerados: {Path(args.output_pares)}")
    print(f"Resumo gerado: {Path(args.output_resumo)}")
    print(f"PNG gerado: {Path(args.output_png)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
