#!/usr/bin/env python3
"""Gera um plot PCA do resultado fuzzy das capitais intermediarias."""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
CLASSIFICACAO_PADRAO = ROOT / "outputs" / "classificacao_capitais_intermediarias_fuzzy.csv"
SAIDA_PNG_PADRAO = ROOT / "outputs" / "pca_resultado_fuzzy_capitais_intermediarias.png"
SAIDA_PNG_HULLS_PADRAO = ROOT / "outputs" / "pca_resultado_fuzzy_capitais_intermediarias_hulls.png"
SAIDA_CSV_PADRAO = ROOT / "outputs" / "pca_resultado_fuzzy_capitais_intermediarias.csv"

COLUNAS_PCA = [
    "empresas_1k",
    "regic_var60",
    "via_pav_pct",
    "indice_conectividade",
    "estab_saude_10k",
    "homicidios_100k",
]

ORDEM_CLASSES = [
    "muito_baixo",
    "baixo",
    "medio",
    "alto",
    "muito_alto",
]

TITULOS_CLASSES = {
    "muito_baixo": "Muito baixo",
    "baixo": "Baixo",
    "medio": "Medio",
    "alto": "Alto",
    "muito_alto": "Muito alto",
}

CORES_CLASSES = {
    "muito_baixo": "#7f0000",
    "baixo": "#d7301f",
    "medio": "#fdae61",
    "alto": "#1a9850",
    "muito_alto": "#006837",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera um PCA do resultado fuzzy das capitais intermediarias.")
    parser.add_argument("--classificacao", default=str(CLASSIFICACAO_PADRAO))
    parser.add_argument("--output", default=str(SAIDA_PNG_PADRAO))
    parser.add_argument("--output-csv", default=str(SAIDA_CSV_PADRAO))
    return parser.parse_args()


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
        "body": load(13),
        "small": load(11),
        "tiny": load(10),
    }


def zscore(matriz: np.ndarray) -> np.ndarray:
    media = np.nanmean(matriz, axis=0)
    desvio = np.nanstd(matriz, axis=0, ddof=0)
    desvio = np.where(desvio == 0, 1.0, desvio)
    return (matriz - media) / desvio


def pca_2d(matriz: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    x = zscore(matriz)
    x = np.nan_to_num(x, nan=0.0)
    x = x - x.mean(axis=0, keepdims=True)

    cov = np.cov(x, rowvar=False)
    autovalores, autovetores = np.linalg.eigh(cov)
    ordem = np.argsort(autovalores)[::-1]
    autovalores = autovalores[ordem]
    autovetores = autovetores[:, ordem]

    componentes = x @ autovetores[:, :2]
    variancia_explicada = autovalores / autovalores.sum()
    return componentes, variancia_explicada, autovetores[:, :2]


def carrega_dados(caminho: Path) -> pd.DataFrame:
    df = pd.read_csv(caminho)
    df["classificacao_fuzzy"] = df["classificacao_fuzzy"].astype(str)
    for coluna in COLUNAS_PCA + ["confianca_classificacao"]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
    return df


def project_points(xs: pd.Series, ys: pd.Series, bounds: tuple[float, float, float, float], area: tuple[int, int, int, int]) -> tuple[np.ndarray, np.ndarray]:
    min_x, max_x, min_y, max_y = bounds
    x0, y0, x1, y1 = area
    px = x0 + (xs - min_x) / (max_x - min_x) * (x1 - x0)
    py = y1 - (ys - min_y) / (max_y - min_y) * (y1 - y0)
    return px.to_numpy(), py.to_numpy()


def convex_hull(points: np.ndarray) -> np.ndarray:
    if len(points) <= 1:
        return points

    pts = sorted(set(map(tuple, points.tolist())))
    if len(pts) <= 1:
        return np.array(pts, dtype=float)

    def cross(o, a, b):
        return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])

    lower = []
    for p in pts:
        while len(lower) >= 2 and cross(lower[-1], lower[-2], p) <= 0:
            lower.pop()
        lower.append(p)

    upper = []
    for p in reversed(pts):
        while len(upper) >= 2 and cross(upper[-1], upper[-2], p) <= 0:
            upper.pop()
        upper.append(p)

    hull = lower[:-1] + upper[:-1]
    return np.array(hull, dtype=float)


def draw_arrow(draw: ImageDraw.ImageDraw, p0: tuple[float, float], p1: tuple[float, float], color: str, width: int = 3) -> None:
    draw.line((p0[0], p0[1], p1[0], p1[1]), fill=color, width=width)
    angle = np.arctan2(p1[1] - p0[1], p1[0] - p0[0])
    head_len = 14
    head_angle = np.pi / 8
    p2 = (p1[0] - head_len * np.cos(angle - head_angle), p1[1] - head_len * np.sin(angle - head_angle))
    p3 = (p1[0] - head_len * np.cos(angle + head_angle), p1[1] - head_len * np.sin(angle + head_angle))
    draw.polygon([p1, p2, p3], fill=color)


def draw_class_hulls(imagem: Image.Image, xs: np.ndarray, ys: np.ndarray, classes: pd.Series, cores: dict[str, str]) -> None:
    for classe in ORDEM_CLASSES:
        mask = classes == classe
        pontos = np.column_stack([xs[mask], ys[mask]])
        if len(pontos) < 3:
            continue
        hull = convex_hull(pontos)
        if len(hull) < 3:
            continue
        cor = cores[classe]
        rgb = tuple(int(cor.lstrip("#")[i : i + 2], 16) for i in (0, 2, 4))
        fill = (*rgb, 45)
        outline = (*rgb, 200)
        hull_img = Image.new("RGBA", imagem.size, (255, 255, 255, 0))
        hull_draw = ImageDraw.Draw(hull_img)
        hull_draw.polygon([tuple(p) for p in hull], fill=fill, outline=outline)
        hull_draw.line([tuple(p) for p in np.vstack([hull, hull[0]])], fill=outline, width=4, joint="curve")
        imagem.alpha_composite(hull_img)


def gerar_png(df: pd.DataFrame, saida: Path, saida_csv: Path, com_hulls: bool = False) -> None:
    fontes = tenta_fontes()
    width, height = 1800, 1240
    imagem = Image.new("RGBA", (width, height), "white")
    draw = ImageDraw.Draw(imagem)

    margin_left = 120
    margin_top = 170
    plot_w = 1240
    plot_h = 920
    plot_area = (margin_left, margin_top, margin_left + plot_w, margin_top + plot_h)
    painel_x0 = margin_left + plot_w + 40
    painel_x1 = width - 40
    painel_y0 = margin_top
    painel_y1 = margin_top + 420

    draw.text((40, 28), "PCA do resultado fuzzy", fill="#111111", font=fontes["title"])
    draw.text(
        (40, 68),
        "Visualizacao em PC1 x PC2 calculada apenas para leitura grafica do resultado, sem alterar o metodo fuzzy.",
        fill="#555555",
        font=fontes["subtitle"],
    )

    X = df[COLUNAS_PCA].to_numpy(dtype=float)
    componentes, variancia_explicada, autovetores = pca_2d(X)
    pca = pd.DataFrame(
        {
            "pc1": componentes[:, 0],
            "pc2": componentes[:, 1],
            "classificacao_fuzzy": df["classificacao_fuzzy"].values,
            "confianca_classificacao": df["confianca_classificacao"].values,
            "municipio": df["municipio"].values,
            "uf": df["uf"].values,
        }
    )
    saida_csv.parent.mkdir(parents=True, exist_ok=True)
    pca.to_csv(saida_csv, index=False, encoding="utf-8")

    min_x = float(pca["pc1"].min())
    max_x = float(pca["pc1"].max())
    min_y = float(pca["pc2"].min())
    max_y = float(pca["pc2"].max())
    pad_x = (max_x - min_x) * 0.08 or 1.0
    pad_y = (max_y - min_y) * 0.08 or 1.0
    bounds = (min_x - pad_x, max_x + pad_x, min_y - pad_y, max_y + pad_y)

    # fundo e eixos
    draw.rounded_rectangle(plot_area, radius=22, fill="#fafafa", outline="#d9d9d9", width=2)
    x0, y0, x1, y1 = plot_area
    zero_x, _ = project_points(pd.Series([0.0]), pd.Series([0.0]), bounds, plot_area)
    zero_x = float(zero_x[0])
    _, zero_y = project_points(pd.Series([0.0]), pd.Series([0.0]), bounds, plot_area)
    zero_y = float(zero_y[0])
    draw.line((x0, zero_y, x1, zero_y), fill="#bdbdbd", width=2)
    draw.line((zero_x, y0, zero_x, y1), fill="#bdbdbd", width=2)

    # grades
    for frac in [0.25, 0.5, 0.75]:
        gx = x0 + (x1 - x0) * frac
        gy = y0 + (y1 - y0) * frac
        draw.line((gx, y0, gx, y1), fill="#ececec", width=1)
        draw.line((x0, gy, x1, gy), fill="#ececec", width=1)

    xs, ys = project_points(pca["pc1"], pca["pc2"], bounds, plot_area)
    sizes = 18 + (pca["confianca_classificacao"].to_numpy() * 34)

    if com_hulls:
        draw_class_hulls(imagem, xs, ys, pca["classificacao_fuzzy"], CORES_CLASSES)

    for classe in ORDEM_CLASSES:
        mask = pca["classificacao_fuzzy"] == classe
        cor = CORES_CLASSES[classe]
        for x, y, s in zip(xs[mask], ys[mask], sizes[mask]):
            r = float(s) / 2.0
            draw.ellipse((x - r, y - r, x + r, y + r), fill=cor, outline="white", width=2)

    # setas de carga aproximadas
    for idx, nome in enumerate(COLUNAS_PCA):
        carga = autovetores[idx, :2]
        origem = (zero_x, zero_y)
        destino = (
            zero_x + float(carga[0]) * 220,
            zero_y - float(carga[1]) * 220,
        )
        draw_arrow(draw, origem, destino, "#444444", width=2)
        draw.text((destino[0] + 6, destino[1] + 2), nome, fill="#444444", font=fontes["tiny"])

    # legenda principal
    legend_x = 48
    legend_y = 122
    draw.text((legend_x, legend_y), "Classe fuzzy", fill="#222222", font=fontes["body"])
    for i, classe in enumerate(ORDEM_CLASSES):
        y = legend_y + 28 + i * 26
        draw.rectangle((legend_x, y + 2, legend_x + 16, y + 18), fill=CORES_CLASSES[classe], outline="white")
        draw.text((legend_x + 24, y), TITULOS_CLASSES[classe], fill="#222222", font=fontes["small"])

    # painel lateral
    draw.rounded_rectangle((painel_x0, painel_y0, painel_x1, painel_y1), radius=20, fill="white", outline="#d9d9d9", width=2)
    draw.text((painel_x0 + 16, painel_y0 + 16), "Resumo PCA", fill="#111111", font=fontes["body"])
    draw.text(
        (painel_x0 + 16, painel_y0 + 42),
        f"PC1: {variancia_explicada[0] * 100:.1f}%  |  PC2: {variancia_explicada[1] * 100:.1f}%",
        fill="#555555",
        font=fontes["small"],
    )

    resumo = (
        pca.groupby("classificacao_fuzzy", dropna=False)
        .agg(qtd_municipios=("municipio", "count"), confianca_media=("confianca_classificacao", "mean"))
        .reindex(ORDEM_CLASSES)
    )
    bloco_top = painel_y0 + 82
    for i, classe in enumerate(ORDEM_CLASSES):
        y0b = bloco_top + i * 125
        y1b = y0b + 104
        draw.rounded_rectangle((painel_x0 + 12, y0b, painel_x1 - 12, y1b), radius=16, fill="#fbfbfb", outline="#eeeeee", width=1)
        draw.rectangle((painel_x0 + 24, y0b + 16, painel_x0 + 46, y0b + 38), fill=CORES_CLASSES[classe], outline="white")
        draw.text((painel_x0 + 56, y0b + 14), TITULOS_CLASSES[classe], fill="#111111", font=fontes["body"])
        linha = resumo.loc[classe]
        draw.text((painel_x0 + 24, y0b + 48), f"municipios: {int(linha['qtd_municipios'])}", fill="#333333", font=fontes["small"])
        draw.text((painel_x0 + 24, y0b + 68), f"confianca media: {linha['confianca_media']:.3f}", fill="#333333", font=fontes["small"])

    # rótulos dos eixos
    draw.text((plot_area[0] + plot_w / 2 - 22, plot_area[3] + 24), f"PC1 ({variancia_explicada[0] * 100:.1f}%)", fill="#333333", font=fontes["body"])
    draw.text((14, plot_area[1] + plot_h / 2 - 20), f"PC2 ({variancia_explicada[1] * 100:.1f}%)", fill="#333333", font=fontes["body"])

    draw.text(
        (40, height - 40),
        "As setas indicam a direcao aproximada de cada variavel original no espaco de PCA.",
        fill="#666666",
        font=fontes["small"],
    )

    imagem.convert("RGB").save(saida)


def main() -> int:
    args = parse_args()
    df = carrega_dados(Path(args.classificacao))
    gerar_png(df, Path(args.output), Path(args.output_csv))
    gerar_png(df, Path(SAIDA_PNG_HULLS_PADRAO), Path(args.output_csv), com_hulls=True)
    print(f"PNG gerado: {Path(args.output)}")
    print(f"PNG com hulls gerado: {SAIDA_PNG_HULLS_PADRAO}")
    print(f"CSV PCA gerado: {Path(args.output_csv)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
