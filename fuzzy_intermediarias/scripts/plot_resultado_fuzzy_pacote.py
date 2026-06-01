#!/usr/bin/env python3
"""Gera um mapa resumido do resultado fuzzy das capitais intermediarias."""

from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import pandas as pd
from PIL import Image, ImageDraw, ImageFont


ROOT = Path(__file__).resolve().parents[1]
CLASSIFICACAO_PADRAO = ROOT / "outputs" / "classificacao_capitais_intermediarias_fuzzy.csv"
GEOMETRIAS_PADRAO = ROOT / "inputs" / "capitais_intermediarias_geometrias.gpkg"
SAIDA_PADRAO = ROOT / "outputs" / "plot_resultado_fuzzy_capitais_intermediarias.png"

ORDEM_CLASSES = [
    "muito_baixo",
    "baixo",
    "medio",
    "alto",
    "muito_alto",
]

CORES_CLASSES = {
    "muito_baixo": "#7f0000",
    "baixo": "#d7301f",
    "medio": "#fdae61",
    "alto": "#1a9850",
    "muito_alto": "#006837",
}

TITULOS_CLASSES = {
    "muito_baixo": "Muito baixo",
    "baixo": "Baixo",
    "medio": "Medio",
    "alto": "Alto",
    "muito_alto": "Muito alto",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gera um plot do resultado fuzzy das capitais intermediarias.")
    parser.add_argument("--classificacao", default=str(CLASSIFICACAO_PADRAO))
    parser.add_argument("--geometrias", default=str(GEOMETRIAS_PADRAO))
    parser.add_argument("--output", default=str(SAIDA_PADRAO))
    return parser.parse_args()


def carrega_dados(caminho_classificacao: Path, caminho_geometrias: Path) -> pd.DataFrame:
    classificacao = pd.read_csv(caminho_classificacao)
    classificacao["cod_mun"] = pd.to_numeric(classificacao["cod_mun"], errors="coerce").astype("Int64")

    gdf = gpd.read_file(caminho_geometrias)
    gdf["cod_mun"] = pd.to_numeric(gdf["CD_MUN"], errors="coerce").astype("Int64")

    if gdf["cod_mun"].isna().any():
        raise ValueError("Algumas geometrias nao possuem codigo municipal valido em CD_MUN.")

    merged = gdf.merge(
        classificacao,
        on="cod_mun",
        how="inner",
        validate="one_to_one",
    )
    if len(merged) != len(classificacao):
        raise ValueError("Nem todos os municipios da classificacao foram encontrados nas geometrias.")
    return merged


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
        "title": load(42),
        "subtitle": load(22),
        "body": load(18),
        "small": load(15),
        "tiny": load(13),
        "mono": load(14),
    }


def hex_to_rgb(hex_color: str) -> tuple[int, int, int]:
    hex_color = hex_color.lstrip("#")
    return tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))


def text_box(draw: ImageDraw.ImageDraw, xy: tuple[int, int], text: str, font, fill, pad: int = 0) -> tuple[int, int, int, int]:
    bbox = draw.multiline_textbbox(xy, text, font=font, spacing=4)
    return (bbox[0] - pad, bbox[1] - pad, bbox[2] + pad, bbox[3] + pad)


def plota_mapa(gdf: pd.DataFrame, saida: Path) -> None:
    saida.parent.mkdir(parents=True, exist_ok=True)

    fontes = tenta_fontes()
    largura, altura = 1800, 1300
    imagem = Image.new("RGB", (largura, altura), "white")
    draw = ImageDraw.Draw(imagem)

    margem_esq = 70
    margem_sup = 130
    margem_dir = 40
    margem_inf = 80
    painel_largura = 430
    gap = 35

    mapa_x0 = margem_esq
    mapa_y0 = margem_sup
    mapa_x1 = largura - margem_dir - painel_largura - gap
    mapa_y1 = altura - margem_inf

    painel_x0 = mapa_x1 + gap
    painel_y0 = margem_sup
    painel_x1 = largura - margem_dir
    painel_y1 = altura - margem_inf

    draw.text((margem_esq, 35), "Resultado do fuzzy das capitais intermediarias", fill="#111111", font=fontes["title"])
    draw.text(
        (margem_esq, 86),
        "119 municipios representados pelos centrioides geograficos e classificados por score final",
        fill="#444444",
        font=fontes["subtitle"],
    )

    pontos = gdf.geometry.representative_point()
    coords = pd.DataFrame({"x": pontos.x, "y": pontos.y, "classe": gdf["classificacao_fuzzy"].values})

    x_min, x_max = coords["x"].min(), coords["x"].max()
    y_min, y_max = coords["y"].min(), coords["y"].max()
    pad_x = (x_max - x_min) * 0.05 or 1.0
    pad_y = (y_max - y_min) * 0.05 or 1.0
    x_min -= pad_x
    x_max += pad_x
    y_min -= pad_y
    y_max += pad_y

    def project(x: float, y: float) -> tuple[float, float]:
        px = mapa_x0 + (x - x_min) / (x_max - x_min) * (mapa_x1 - mapa_x0)
        py = mapa_y1 - (y - y_min) / (y_max - y_min) * (mapa_y1 - mapa_y0)
        return px, py

    # Map panel background and frame
    draw.rounded_rectangle((mapa_x0, mapa_y0, mapa_x1, mapa_y1), radius=20, fill="#fafafa", outline="#d9d9d9", width=2)

    # Grid and axis ticks
    for frac in [0.0, 0.25, 0.5, 0.75, 1.0]:
        lon = x_min + (x_max - x_min) * frac
        lat = y_min + (y_max - y_min) * frac
        gx0, gy0 = project(lon, y_min)
        gx1, gy1 = project(lon, y_max)
        draw.line((gx0, mapa_y0, gx0, mapa_y1), fill="#e6e6e6", width=1)
        draw.line((mapa_x0, gy0, mapa_x1, gy0), fill="#e6e6e6", width=1)
        draw.text((gx0 - 25, mapa_y1 + 10), f"{lon:.1f}", fill="#666666", font=fontes["tiny"])
        draw.text((10, gy0 - 8), f"{lat:.1f}", fill="#666666", font=fontes["tiny"])

    draw.text((mapa_x0 + 10, mapa_y1 + 35), "Longitude", fill="#666666", font=fontes["small"])
    draw.text((10, mapa_y0 - 25), "Latitude", fill="#666666", font=fontes["small"])

    # Draw the municipality centroids
    for classe in ORDEM_CLASSES:
        subset = coords[coords["classe"] == classe]
        cor = hex_to_rgb(CORES_CLASSES[classe])
        for _, row in subset.iterrows():
            px, py = project(float(row["x"]), float(row["y"]))
            r = 8
            draw.ellipse((px - r, py - r, px + r, py + r), fill=cor, outline="white", width=2)

    # Legend
    legenda_x0 = mapa_x0 + 20
    legenda_y0 = mapa_y0 + 20
    legenda_w = 210
    legenda_h = 32 * len(ORDEM_CLASSES) + 35
    draw.rounded_rectangle(
        (legenda_x0, legenda_y0, legenda_x0 + legenda_w, legenda_y0 + legenda_h),
        radius=16,
        fill="white",
        outline="#d9d9d9",
        width=2,
    )
    draw.text((legenda_x0 + 16, legenda_y0 + 12), "Classe fuzzy", fill="#222222", font=fontes["body"])
    for idx, classe in enumerate(ORDEM_CLASSES):
        y = legenda_y0 + 42 + idx * 30
        cor = hex_to_rgb(CORES_CLASSES[classe])
        draw.rectangle((legenda_x0 + 16, y + 3, legenda_x0 + 34, y + 21), fill=cor, outline="white")
        draw.text((legenda_x0 + 46, y), TITULOS_CLASSES[classe], fill="#222222", font=fontes["small"])

    # Side panel with summary
    draw.rounded_rectangle((painel_x0, painel_y0, painel_x1, painel_y1), radius=20, fill="#ffffff", outline="#d9d9d9", width=2)
    draw.text((painel_x0 + 18, painel_y0 + 18), "Resumo por classe", fill="#111111", font=fontes["body"])
    draw.text((painel_x0 + 18, painel_y0 + 48), "Qtd. municipios, score medio e confianca media", fill="#666666", font=fontes["small"])

    resumo = (
        gdf.groupby("classificacao_fuzzy", dropna=False)
        .agg(
            qtd_municipios=("cod_mun", "count"),
            score_final_medio=("score_final", "mean"),
            confianca_media=("confianca_classificacao", "mean"),
        )
        .reindex(ORDEM_CLASSES)
    )

    bloco_top = painel_y0 + 88
    bloco_altura = 180
    for idx, classe in enumerate(ORDEM_CLASSES):
        y0 = bloco_top + idx * bloco_altura
        y1 = y0 + bloco_altura - 14
        cor = hex_to_rgb(CORES_CLASSES[classe])
        draw.rounded_rectangle((painel_x0 + 14, y0, painel_x1 - 14, y1), radius=16, fill="#fbfbfb", outline="#ececec", width=1)
        draw.rectangle((painel_x0 + 26, y0 + 18, painel_x0 + 52, y0 + 44), fill=cor, outline="white")
        draw.text((painel_x0 + 62, y0 + 16), TITULOS_CLASSES[classe], fill="#111111", font=fontes["body"])

        linha = resumo.loc[classe]
        textos = [
            f"municipios: {int(linha['qtd_municipios'])}",
            f"score medio: {linha['score_final_medio']:.3f}",
            f"confianca media: {linha['confianca_media']:.3f}",
        ]
        for j, texto in enumerate(textos):
            draw.text((painel_x0 + 26, y0 + 64 + j * 28), texto, fill="#333333", font=fontes["small"])

    # Bottom note
    nota = [
        "Leitura rapida:",
        "cores mais quentes indicam classes mais altas",
        "a posicao geografica usa centrioides municipais",
    ]
    nota_y = painel_y1 - 98
    draw.rounded_rectangle((painel_x0 + 14, nota_y, painel_x1 - 14, painel_y1 - 14), radius=14, fill="#f7f7f7", outline="#e2e2e2", width=1)
    for idx, texto in enumerate(nota):
        draw.text((painel_x0 + 26, nota_y + 12 + idx * 22), texto, fill="#444444", font=fontes["tiny"])

    imagem.save(saida)


def main() -> int:
    args = parse_args()
    gdf = carrega_dados(Path(args.classificacao), Path(args.geometrias))
    plota_mapa(gdf, Path(args.output))
    print(f"Plot gerado: {Path(args.output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
