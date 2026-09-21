#!/usr/bin/env python3
"""
Pipeline documentado de classificacao municipal por PCA + k-means.

O objetivo e transformar varias medidas municipais, que estao em escalas
diferentes, em grupos de municipios com perfis semelhantes. O processamento
segue esta ordem:

1. Le a base municipal consolidada e as bases complementares.
2. Faz os merges pela chave `cod_mun`.
3. Calcula indicadores derivados, como PIB per capita, empresas por 1.000
   habitantes e indicadores de distancia ao litoral.
4. Cria os quatro universos de analise e exclui capitais de UF e conurbados
   com capitais de UF.
5. Padroniza as variaveis do modelo para evitar que unidades de medida
   diferentes dominem a analise.
6. Aplica PCA e retém componentes ate atingir a variancia acumulada definida
   em `--variance-threshold`.
7. Aplica k-means++ sobre os escores dos componentes retidos.
8. Ordena os rotulos dos clusters pela renda media do chefe de familia, apenas
   para facilitar a leitura comparativa dos resultados.
9. Salva tabelas, metricas, diagnosticos, figuras e o relatorio QMD.

O modelo nao utiliza `regic_var56`. A variavel de densidade demografica urbana
entra no lugar da centralidade REGIC no teste descrito neste pacote.

Universos analisados:
1. Santa Catarina, todos os municipios, exceto capital de UF e conurbados com UF
2. Santa Catarina, classe A, exceto capital de UF e conurbados com UF
3. Brasil, todos os municipios, exceto capital de UF e conurbados com UF
4. Brasil, classe A, exceto capital de UF e conurbados com UF

O script gera, dentro de `outputs/`:
- CSVs com os municipios por cluster
- resumos por cluster
- componentes do PCA
- matriz de correlacao em CSV, MD e PNG
- analise exploratoria das variaveis
- GPKG opcional com as camadas dos clusters, quando uma malha for informada
- relatorio QMD consolidado
"""

from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

os.environ.setdefault("MPLCONFIGDIR", "/private/tmp/matplotlib")

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


# O pacote e a unidade de compartilhamento. Assim, os caminhos padrao
# continuam funcionando mesmo quando o repositorio e clonado em outra pasta.
PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = None
for pasta in Path(__file__).resolve().parents:
    if (pasta / "prata" / "processamento" / "merge_v28.csv").exists():
        REPO_ROOT = pasta
        break
if REPO_ROOT is None:
    raise FileNotFoundError("Nao foi possivel localizar a raiz do repositorio.")


@dataclass
class Universo:
    chave: str
    label: str
    filtro_classe_a: bool


UNIVERSOS = [
    Universo(
        chave="sc_todos_sem_capital_conurbados",
        label="Santa Catarina, todos os municipios, exceto capital de UF e conurbados com UF",
        filtro_classe_a=False,
    ),
    Universo(
        chave="sc_classe_a_sem_capital_conurbados",
        label="Santa Catarina, classe A, exceto capital de UF e conurbados com UF",
        filtro_classe_a=True,
    ),
    Universo(
        chave="br_todos_sem_capital_conurbados",
        label="Brasil, todos os municipios, exceto capital de UF e conurbados com UF",
        filtro_classe_a=False,
    ),
    Universo(
        chave="br_classe_a_sem_capital_conurbados",
        label="Brasil, classe A, exceto capital de UF e conurbados com UF",
        filtro_classe_a=True,
    ),
]


# Bases de entrada. Os merges abaixo usam sempre `cod_mun` como chave.
BASE_PADRAO = PACKAGE_ROOT / "inputs" / "merge_v28.csv"
CLASSIFICACAO_PADRAO = PACKAGE_ROOT / "inputs" / "municipios_brasil_abc_cnefe.csv"
DOMICILIOS_PADRAO = PACKAGE_ROOT / "inputs" / "ABC_CNEFE.csv"
RENDA_PADRAO = PACKAGE_ROOT / "inputs" / "Agregados_por_municipios_renda_responsavel_BR 2.csv"
DENSIDADE_PADRAO = PACKAGE_ROOT / "inputs" / "setores_urbanos_agregado_municipio.csv"
GEOMETRIA_PADRAO = None

OUTPUT_ROOT = PACKAGE_ROOT / "outputs"
DOCS_ROOT = PACKAGE_ROOT / "docs"
RELATORIO_PADRAO = DOCS_ROOT / "relatorio_pca_kmeans_densidade_urbana.qmd"
COMPARATIVO_PADRAO = OUTPUT_ROOT / "comparativo_modelos.csv"

# Colunas lidas diretamente da base municipal consolidada.
COLUNAS_BASE = [
    "cod_mun",
    "municipio",
    "pib_total",
    "pop_total",
    "dom_total",
    "empresas_total",
    "ambulatorios_sus_2026_02",
    "via_pav_pct",
    "indice_conectividade",
    "categoria_turistica_2019",
    "distancia_litoral",
]

# Variaveis efetivamente usadas no PCA. As demais colunas servem para
# identificacao, filtros, validacao ou leitura dos agrupamentos.
COLUNAS_MODELO = [
    "via_pav_pct",
    "indice_conectividade",
    "ambulatorios_sus_2026_02",
    "pib_pc",
    "categoria_turistica_score",
    "litoral_ate_50km",
    "litoral_50_100km",
    "litoral_100_500km",
    "litoral_acima_500km",
    "empresas_1k",
    "densidade_demografica_urbana",
    "renda_media_chefe",
]


def parse_args() -> argparse.Namespace:
    """Le os parametros de reproducao e os caminhos dos arquivos.

    Os valores padrao apontam para a estrutura do proprio pacote. Os
    argumentos permitem trocar uma fonte, testar outro valor de `k` ou salvar
    os resultados em outra pasta sem editar o codigo.
    """
    parser = argparse.ArgumentParser(
        description="Executa PCA + k-means nos quatro universos municipais."
    )
    parser.add_argument("--base", default=str(BASE_PADRAO), help="CSV municipal consolidado.")
    parser.add_argument("--classificacao", default=str(CLASSIFICACAO_PADRAO), help="CSV com categoria e classe ABC.")
    parser.add_argument("--domicilios", default=str(DOMICILIOS_PADRAO), help="CSV com domicilios CNEFE.")
    parser.add_argument("--renda", default=str(RENDA_PADRAO), help="CSV de renda com CD_MUN e V06006.")
    parser.add_argument("--densidade", default=str(DENSIDADE_PADRAO), help="CSV com densidade urbana municipal.")
    parser.add_argument(
        "--geometria",
        default=GEOMETRIA_PADRAO,
        help="Malha municipal opcional para gerar GeoPackages.",
    )
    parser.add_argument("--k", type=int, default=5, help="Numero de clusters do k-means.")
    parser.add_argument("--seed", type=int, default=42, help="Semente para o k-means++.")
    parser.add_argument(
        "--variance-threshold",
        type=float,
        default=0.90,
        help="Variancia acumulada minima para reter componentes do PCA.",
    )
    parser.add_argument("--output-root", default=str(OUTPUT_ROOT), help="Pasta dos resultados.")
    parser.add_argument("--report", default=str(RELATORIO_PADRAO), help="Caminho do relatorio QMD.")
    parser.add_argument("--comparativo", default=str(COMPARATIVO_PADRAO), help="CSV com metricas dos quatro universos.")
    return parser.parse_args()


def normalize_cod_mun(series: pd.Series) -> pd.Series:
    """Padroniza codigos IBGE para uma serie numerica nullable."""
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def uf_por_codigo(cod_mun: pd.Series) -> pd.Series:
    """Extrai a UF pelo prefixo de dois digitos do codigo IBGE municipal."""
    mapa = {
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
    prefixos = pd.to_numeric(cod_mun.astype(str).str.slice(0, 2), errors="coerce")
    return prefixos.map(mapa).astype("string")


def load_base(path: Path) -> pd.DataFrame:
    """Le somente as colunas necessarias da base municipal consolidada."""
    df = pd.read_csv(path, usecols=COLUNAS_BASE).copy()
    df["cod_mun"] = normalize_cod_mun(df["cod_mun"])
    for coluna in [c for c in COLUNAS_BASE if c not in {"cod_mun", "municipio", "categoria_turistica_2019"}]:
        df[coluna] = pd.to_numeric(df[coluna], errors="coerce")
    df["categoria_turistica_2019"] = df["categoria_turistica_2019"].astype("string")
    return df


def load_classificacao(path: Path) -> pd.DataFrame:
    """Le categoria territorial e classe ABC, mantendo uma linha por municipio."""
    df = pd.read_csv(path).copy()
    df = df.rename(columns={"geocodigo_municipio": "cod_mun", "nome_municipio": "nome_municipio_regic"})
    df["cod_mun"] = normalize_cod_mun(df["cod_mun"])
    df["uf"] = df["uf"].astype("string")
    df["categoria"] = df["categoria"].astype("string")
    df["classe_abc"] = df["classe_abc"].astype("string")
    return df[["cod_mun", "nome_municipio_regic", "uf", "categoria", "classe_abc"]].drop_duplicates(
        subset=["cod_mun"], keep="first"
    )


def load_domicilios(path: Path) -> pd.DataFrame:
    """Le o total de domicilios CNEFE por municipio."""
    df = pd.read_csv(path).copy()
    df = df.rename(columns={"codigo_mun": "cod_mun", "domicilios": "domicilios_cnefe", "GrupoABC": "classe_abc_domicilios"})
    df["cod_mun"] = normalize_cod_mun(df["cod_mun"])
    df["domicilios_cnefe"] = pd.to_numeric(df["domicilios_cnefe"], errors="coerce")
    df["classe_abc_domicilios"] = df["classe_abc_domicilios"].astype("string")
    return df[["cod_mun", "domicilios_cnefe", "classe_abc_domicilios"]]


def load_renda(path: Path) -> pd.DataFrame:
    """Le a renda media do chefe a partir de CD_MUN e V06006."""
    df = pd.read_csv(path, sep=";", encoding="latin1").copy()
    df = df.rename(columns={"CD_MUN": "cod_mun", "V06006": "renda_media_chefe"})
    if "cod_mun" not in df.columns or "renda_media_chefe" not in df.columns:
        raise ValueError("Nao foi possivel localizar CD_MUN e V06006 no CSV de renda.")
    df["cod_mun"] = normalize_cod_mun(df["cod_mun"])
    df["renda_media_chefe"] = pd.to_numeric(df["renda_media_chefe"], errors="coerce")
    return df[["cod_mun", "renda_media_chefe"]]


def load_densidade(path: Path) -> pd.DataFrame:
    """Le a densidade demografica urbana ja agregada por municipio."""
    df = pd.read_csv(path).copy()
    df = df.rename(columns={"CD_MUN": "cod_mun"})
    df["cod_mun"] = normalize_cod_mun(df["cod_mun"])
    if "densidade_demografica_urbana" not in df.columns:
        raise ValueError("Coluna densidade_demografica_urbana nao encontrada.")
    df["densidade_demografica_urbana"] = pd.to_numeric(df["densidade_demografica_urbana"], errors="coerce")
    return df[["cod_mun", "densidade_demografica_urbana"]]


def load_geometry(path: Path):
    """Le a malha municipal para produtos cartograficos opcionais."""
    import geopandas as gpd

    gdf = gpd.read_file(path)
    if "CD_MUN" not in gdf.columns:
        raise ValueError("Coluna CD_MUN nao encontrada na geometria.")
    gdf["cod_mun"] = pd.to_numeric(gdf["CD_MUN"], errors="coerce").astype("Int64")
    return gdf


def cria_base_enriquecida(args: argparse.Namespace) -> pd.DataFrame:
    """Monta a matriz municipal de trabalho por meio de merges controlados.

    A base principal e enriquecida com classificacao, domicilios, renda e
    densidade. Os merges sao um-para-um por `cod_mun`; por isso, duplicidades
    indicam problema na fonte e interrompem a execucao.
    """
    base = load_base(Path(args.base))
    classificacao = load_classificacao(Path(args.classificacao))
    domicilios = load_domicilios(Path(args.domicilios))
    renda = load_renda(Path(args.renda))
    densidade = load_densidade(Path(args.densidade))

    base = base.merge(classificacao, on="cod_mun", how="left", validate="one_to_one")
    base = base.merge(domicilios, on="cod_mun", how="left", validate="one_to_one")
    base = base.merge(renda, on="cod_mun", how="left", validate="one_to_one")
    base = base.merge(densidade, on="cod_mun", how="left", validate="one_to_one")

    base["classe_abc"] = base["classe_abc"].fillna(base["classe_abc_domicilios"])
    base["uf"] = base["uf"].fillna(uf_por_codigo(base["cod_mun"]))
    base["categoria"] = base["categoria"].fillna("demais municipios")
    base = base.drop(columns=["classe_abc_domicilios"])

    faltantes = base.loc[base["uf"].isna(), "cod_mun"].head(5).tolist()
    if faltantes:
        raise ValueError(f"UF ausente apos o merge para alguns municipios: {faltantes}")
    return base


def filtra_universo(df: pd.DataFrame, uf: str, classe_a: bool) -> pd.DataFrame:
    """Aplica UF, classe ABC e as regras de exclusao do universo."""
    universo = df.loc[df["uf"].astype("string").str.upper().eq(uf.upper())].copy() if uf else df.copy()
    if classe_a:
        universo = universo.loc[universo["classe_abc"].astype("string").str.upper().eq("A")].copy()
    universo = universo.loc[~universo["categoria"].astype("string").str.lower().eq("capital de uf")].copy()
    universo = universo.loc[~universo["categoria"].astype("string").str.lower().eq("conurbado com uf")].copy()
    return universo


def cria_indicadores(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula variaveis derivadas usadas pelo modelo.

    Inclui PIB per capita, empresas por 1.000 habitantes, escore turistico e
    quatro indicadores mutuamente exclusivos de distancia ao litoral.
    """
    trabalho = df.copy()
    pop = trabalho["pop_total"].replace(0, np.nan)
    dist_km = pd.to_numeric(trabalho["distancia_litoral"], errors="coerce") / 1000.0

    trabalho["pib_pc"] = trabalho["pib_total"] / pop
    trabalho["empresas_1k"] = 1000.0 * trabalho["empresas_total"] / pop

    mapa_categoria_turistica = {"A": 5.0, "B": 4.0, "C": 3.0, "D": 2.0, "E": 1.0, "SEM CATEGORIA": 0.0}
    trabalho["categoria_turistica_score"] = (
        trabalho["categoria_turistica_2019"].astype("string").str.strip().str.upper().map(mapa_categoria_turistica)
    )

    trabalho["litoral_ate_50km"] = ((dist_km >= 0) & (dist_km <= 50)).astype(float)
    trabalho["litoral_50_100km"] = ((dist_km > 50) & (dist_km <= 100)).astype(float)
    trabalho["litoral_100_500km"] = ((dist_km > 100) & (dist_km <= 500)).astype(float)
    trabalho["litoral_acima_500km"] = (dist_km > 500).astype(float)
    return trabalho


def padroniza(df: pd.DataFrame, colunas: list[str]) -> tuple[np.ndarray, pd.Index, pd.Series, pd.Series]:
    """Converte as variaveis para escores z antes do PCA.

    Linhas com algum valor ausente ou infinito sao retiradas apenas da matriz
    estatistica. A media e o desvio-padrao sao calculados no proprio universo.
    Desvios iguais a zero recebem 1 para evitar divisao por zero.
    """
    trabalho = df[colunas].replace([np.inf, -np.inf], np.nan).dropna().copy()
    medias = trabalho.mean()
    desvios = trabalho.std(ddof=0).replace(0, 1.0)
    z = ((trabalho - medias) / desvios).to_numpy(dtype=float)
    return z, trabalho.index, medias, desvios


def executa_pca(X: np.ndarray, colunas: list[str], variance_threshold: float) -> tuple[np.ndarray, pd.DataFrame, int]:
    """Executa PCA por decomposicao SVD e escolhe componentes por variancia.

    A SVD fornece os eixos principais, os escores dos municipios e os pesos
    (loadings) das variaveis. O numero retido e o primeiro PC cuja variancia
    acumulada alcanca o limite definido pelo usuario.
    """
    X_centrado = X - X.mean(axis=0, keepdims=True)
    _, singular_values, vt = np.linalg.svd(X_centrado, full_matrices=False)
    autovalores = (singular_values**2) / max(X.shape[0] - 1, 1)
    variancia_explicada = autovalores / autovalores.sum()
    variancia_acumulada = np.cumsum(variancia_explicada)
    n_componentes = int(np.searchsorted(variancia_acumulada, variance_threshold) + 1)

    componentes = vt[:n_componentes]
    scores = X_centrado @ componentes.T

    linhas = []
    for idx in range(n_componentes):
        linha = {
            "componente": f"PC{idx + 1}",
            "variancia_explicada": variancia_explicada[idx],
            "variancia_acumulada": variancia_acumulada[idx],
        }
        for j, coluna in enumerate(colunas):
            linha[coluna] = componentes[idx, j]
        linhas.append(linha)

    return scores, pd.DataFrame(linhas), n_componentes


def inicializa_kmeans_pp(X: np.ndarray, k: int, rng: np.random.Generator) -> np.ndarray:
    """Escolhe centroides iniciais usando a estrategia k-means++."""
    n = X.shape[0]
    centroides = np.empty((k, X.shape[1]), dtype=float)
    centroides[0] = X[rng.integers(0, n)]
    dist2 = np.sum((X - centroides[0]) ** 2, axis=1)
    for i in range(1, k):
        soma = dist2.sum()
        if soma <= 0:
            centroides[i:] = X[rng.integers(0, n, size=k - i)]
            break
        probs = dist2 / soma
        centroides[i] = X[rng.choice(n, p=probs)]
        dist2 = np.minimum(dist2, np.sum((X - centroides[i]) ** 2, axis=1))
    return centroides


def roda_kmeans(X: np.ndarray, k: int, seed: int, max_iter: int = 250) -> tuple[np.ndarray, np.ndarray, float]:
    """Agrupa os escores do PCA e retorna rotulos, centroides e inercia.

    Em cada iteracao, cada municipio e associado ao centroide mais proximo e
    os centroides sao recalculados pela media dos municipios associados. O
    processo para quando os centroides deixam de mudar ou chega ao limite de
    iteracoes.
    """
    if k < 2:
        raise ValueError("O numero de clusters deve ser pelo menos 2.")
    if X.shape[0] < k:
        raise ValueError("Ha menos observacoes validas do que clusters.")

    rng = np.random.default_rng(seed)
    centroides = inicializa_kmeans_pp(X, k, rng)

    for _ in range(max_iter):
        distancias = np.sum((X[:, None, :] - centroides[None, :, :]) ** 2, axis=2)
        labels = distancias.argmin(axis=1)

        novos = centroides.copy()
        for grupo in range(k):
            mascara = labels == grupo
            if mascara.any():
                novos[grupo] = X[mascara].mean(axis=0)
            else:
                novos[grupo] = X[rng.integers(0, X.shape[0])]
        if np.allclose(novos, centroides):
            centroides = novos
            break
        centroides = novos

    distancias = np.sum((X[:, None, :] - centroides[None, :, :]) ** 2, axis=2)
    labels = distancias.argmin(axis=1)
    inertia = float(np.sum(np.min(distancias, axis=1)))
    return labels, centroides, inertia


def silhouette_score(X: np.ndarray, labels: np.ndarray) -> float:
    """Calcula a silhueta media: maior indica melhor separacao relativa."""
    labels = np.asarray(labels)
    n = X.shape[0]
    if n < 3 or len(np.unique(labels)) < 2:
        return float("nan")
    dist = np.sqrt(((X[:, None, :] - X[None, :, :]) ** 2).sum(axis=2))
    sil = []
    for i in range(n):
        same = labels == labels[i]
        same[i] = False
        a = dist[i, same].mean() if same.any() else 0.0
        b = min(
            dist[i, labels == cl].mean()
            for cl in np.unique(labels)
            if cl != labels[i] and np.any(labels == cl)
        )
        sil.append((b - a) / max(a, b) if max(a, b) > 0 else 0.0)
    return float(np.mean(sil))


def davies_bouldin_score(X: np.ndarray, labels: np.ndarray) -> float:
    """Calcula Davies-Bouldin: menor indica grupos mais compactos e separados."""
    labels = np.asarray(labels)
    clusters = np.unique(labels)
    if len(clusters) < 2:
        return float("nan")
    centroides = np.array([X[labels == cl].mean(axis=0) for cl in clusters])
    dispersoes = np.array(
        [np.sqrt(((X[labels == cl] - centroide) ** 2).sum(axis=1)).mean() for cl, centroide in zip(clusters, centroides)]
    )
    dist_centroides = np.sqrt(((centroides[:, None, :] - centroides[None, :, :]) ** 2).sum(axis=2))
    np.fill_diagonal(dist_centroides, np.inf)
    r = (dispersoes[:, None] + dispersoes[None, :]) / dist_centroides
    return float(np.mean(np.max(r, axis=1)))


def calinski_harabasz_score(X: np.ndarray, labels: np.ndarray) -> float:
    """Calcula Calinski-Harabasz: maior indica melhor separacao relativa."""
    labels = np.asarray(labels)
    n = X.shape[0]
    clusters = np.unique(labels)
    k = len(clusters)
    if k < 2 or n <= k:
        return float("nan")
    overall = X.mean(axis=0)
    bss = 0.0
    wss = 0.0
    for cl in clusters:
        Xi = X[labels == cl]
        mu = Xi.mean(axis=0)
        bss += len(Xi) * float(((mu - overall) ** 2).sum())
        wss += float(((Xi - mu) ** 2).sum())
    return float((bss / (k - 1)) / (wss / (n - k)))


def reorganiza_clusters(base_valida: pd.DataFrame, labels: np.ndarray, coluna_ordem: str = "renda_media_chefe") -> pd.Series:
    """Troca os rotulos tecnicos do k-means por uma ordem interpretavel.

    O k-means nao cria uma ordem natural para os grupos. Por isso, os grupos
    sao ordenados pela media decrescente da renda media do chefe. Essa etapa
    nao altera os municipios de cada grupo, apenas seus numeros de exibicao.
    """
    trabalho = base_valida.copy()
    trabalho["cluster_id"] = labels
    ordem = (
        trabalho.groupby("cluster_id", as_index=False)[coluna_ordem]
        .mean()
        .sort_values(coluna_ordem, ascending=False)
        .reset_index(drop=True)
    )
    ordem["cluster_ordem"] = np.arange(1, len(ordem) + 1)
    mapa = dict(zip(ordem["cluster_id"], ordem["cluster_ordem"]))
    return pd.Series(labels, index=base_valida.index).map(mapa).astype(int)


def loadings_top(df_componentes: pd.DataFrame, colunas: list[str], n_top: int = 4) -> pd.DataFrame:
    linhas = []
    for _, row in df_componentes.iterrows():
        pesos = [(col, float(row[col])) for col in colunas]
        pesos.sort(key=lambda x: abs(x[1]), reverse=True)
        registro = {"componente": row["componente"]}
        for idx in range(n_top):
            if idx < len(pesos):
                registro[f"top_{idx + 1}_variavel"] = pesos[idx][0]
                registro[f"top_{idx + 1}_peso"] = pesos[idx][1]
            else:
                registro[f"top_{idx + 1}_variavel"] = ""
                registro[f"top_{idx + 1}_peso"] = np.nan
        linhas.append(registro)
    return pd.DataFrame(linhas)


def plot_correlation_matrix(corr: pd.DataFrame, output_png: Path, title: str) -> None:
    n = corr.shape[0]
    fig_w = max(10, 0.48 * n + 5)
    fig_h = max(8, 0.42 * n + 4)
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), constrained_layout=True)
    im = ax.imshow(corr.values, cmap="RdBu_r", vmin=-1, vmax=1)
    ax.set_xticks(range(n))
    ax.set_yticks(range(n))
    ax.set_xticklabels(corr.columns, rotation=45, ha="right", fontsize=8)
    ax.set_yticklabels(corr.index, fontsize=8)
    ax.set_title(title, fontsize=14, pad=14)

    for i in range(n):
        for j in range(n):
            valor = corr.iat[i, j]
            cor_texto = "white" if abs(valor) >= 0.60 else "black"
            ax.text(
                j,
                i,
                f"{valor:.2f}",
                ha="center",
                va="center",
                fontsize=7.5,
                color=cor_texto,
                bbox=dict(facecolor="white", alpha=0.35, edgecolor="none", boxstyle="round,pad=0.12"),
            )

    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.ax.tick_params(labelsize=8)
    fig.savefig(output_png, dpi=220, bbox_inches="tight")
    plt.close(fig)


def correlation_markdown(corr: pd.DataFrame) -> str:
    linhas = ["| Variavel | " + " | ".join(corr.columns) + " |", "|" + "---|" * (len(corr.columns) + 1)]
    for idx, row in corr.iterrows():
        valores = " | ".join(f"{v:.4f}" for v in row.values)
        linhas.append(f"| {idx} | {valores} |")
    return "\n".join(linhas)


def summary_to_md(df: pd.DataFrame, colunas: list[str]) -> str:
    linhas = ["| Variavel | Tipo | N | Faltantes | % Faltantes | Media | Desvio | Min | Q1 | Mediana | Q3 | Max | Zeros | Negativos | Outliers IQR | % Outliers |", "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    total = len(df)
    for coluna in colunas:
        s = pd.to_numeric(df[coluna], errors="coerce")
        q1 = s.quantile(0.25)
        q3 = s.quantile(0.75)
        iqr = q3 - q1
        lim_inf = q1 - 1.5 * iqr
        lim_sup = q3 + 1.5 * iqr
        outliers = ((s < lim_inf) | (s > lim_sup)).sum()
        linhas.append(
            "| {variavel} | {tipo} | {n} | {falt} | {pfalt:.2f} | {media:.4f} | {std:.4f} | {minv:.4f} | {q1v:.4f} | {med:.4f} | {q3v:.4f} | {maxv:.4f} | {zeros} | {neg} | {out} | {pout:.2f} |".format(
                variavel=coluna,
                tipo=str(df[coluna].dtype),
                n=int(s.notna().sum()),
                falt=int(s.isna().sum()),
                pfalt=100.0 * float(s.isna().sum()) / total if total else 0.0,
                media=float(s.mean()),
                std=float(s.std(ddof=0)),
                minv=float(s.min()),
                q1v=float(q1),
                med=float(s.median()),
                q3v=float(q3),
                maxv=float(s.max()),
                zeros=int((s == 0).sum()),
                neg=int((s < 0).sum()),
                out=int(outliers),
                pout=100.0 * float(outliers) / total if total else 0.0,
            )
        )
    return "\n".join(linhas)


def freq_md(df: pd.DataFrame, coluna: str) -> str:
    serie = df[coluna].astype("string").fillna("<NA>")
    counts = serie.value_counts(dropna=False).reset_index()
    counts.columns = [coluna, "contagem"]
    total = counts["contagem"].sum()
    counts["percentual"] = 100.0 * counts["contagem"] / total if total else 0.0
    linhas = [f"| {coluna} | contagem | percentual |", "|---|---:|---:|"]
    for _, row in counts.iterrows():
        linhas.append(f"| {row[coluna]} | {int(row['contagem'])} | {row['percentual']:.2f} |")
    return "\n".join(linhas)


def interpreta_cluster(base_valida: pd.DataFrame, model_cols: list[str]) -> pd.DataFrame:
    medias = base_valida.groupby("cluster_pca_sc")[model_cols].mean()
    desvio_global = base_valida[model_cols].std(ddof=0).replace(0, 1.0)
    z_medias = (medias - base_valida[model_cols].mean()) / desvio_global

    labels = {
        "via_pav_pct": "via pavimentada",
        "indice_conectividade": "conectividade",
        "ambulatorios_sus_2026_02": "ambulatórios SUS",
        "pib_pc": "PIB per capita",
        "categoria_turistica_score": "turismo",
        "litoral_ate_50km": "litoral ate 50 km",
        "litoral_50_100km": "litoral 50 a 100 km",
        "litoral_100_500km": "litoral 100 a 500 km",
        "litoral_acima_500km": "litoral acima de 500 km",
        "empresas_1k": "empresas por mil hab.",
        "densidade_demografica_urbana": "densidade demografica urbana",
        "renda_media_chefe": "renda media do chefe",
    }

    linhas = []
    for cluster, row in z_medias.iterrows():
        positivos = row.sort_values(ascending=False)
        top = positivos.head(4)
        nomes = []
        for var, val in top.items():
            nome = labels.get(var, var)
            nomes.append(f"{nome} ({val:+.2f} desvios)")
        res = "acima da media em " + ", ".join(nomes[:3]) if len(nomes) else "sem destaque claro"
        linhas.append(
            {
                "cluster": int(cluster),
                "top_1": nomes[0] if len(nomes) > 0 else "",
                "top_2": nomes[1] if len(nomes) > 1 else "",
                "top_3": nomes[2] if len(nomes) > 2 else "",
                "resumo": res,
            }
        )
    return pd.DataFrame(linhas).sort_values("cluster")


def monta_resumo_cluster(base_valida: pd.DataFrame, n_componentes: int, inertia: float) -> pd.DataFrame:
    resumo = (
        base_valida.groupby("cluster_pca_sc", as_index=False)
        .agg(
            qtd_municipios=("cod_mun", "count"),
            uf_principal=("uf", lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0]),
            categoria_principal=("categoria", lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0]),
            classe_abc_principal=("classe_abc", lambda s: s.mode().iat[0] if not s.mode().empty else s.iloc[0]),
            domicilios_cnefe_total=("domicilios_cnefe", "sum"),
            domicilios_cnefe_medio=("domicilios_cnefe", "mean"),
            pib_pc_medio=("pib_pc", "mean"),
            renda_media_chefe_media=("renda_media_chefe", "mean"),
            densidade_demografica_urbana_media=("densidade_demografica_urbana", "mean"),
            via_pav_media=("via_pav_pct", "mean"),
            conectividade_media=("indice_conectividade", "mean"),
            ambulatorios_media=("ambulatorios_sus_2026_02", "mean"),
            turismo_score_medio=("categoria_turistica_score", "mean"),
            empresas_1k_medio=("empresas_1k", "mean"),
            litoral_ate_50km_pct=("litoral_ate_50km", "mean"),
            litoral_50_100km_pct=("litoral_50_100km", "mean"),
            litoral_100_500km_pct=("litoral_100_500km", "mean"),
            litoral_acima_500km_pct=("litoral_acima_500km", "mean"),
        )
        .sort_values("cluster_pca_sc")
        .reset_index(drop=True)
    )
    resumo["n_componentes_pca"] = n_componentes
    resumo["inercia_kmeans"] = inertia
    resumo["percentual_municipios"] = 100.0 * resumo["qtd_municipios"] / resumo["qtd_municipios"].sum()
    return resumo


def plota_resultado(base: pd.DataFrame, output: Path, titulo: str) -> None:
    fig, axes = plt.subplots(1, 2, figsize=(15, 6), constrained_layout=True)
    cores = {1: "#0f766e", 2: "#2563eb", 3: "#f59e0b", 4: "#dc2626", 5: "#7c3aed", 6: "#059669"}
    pares = [(axes[0], "pc1", "pc2", "PC1 x PC2"), (axes[1], "pc1", "pc3", "PC1 x PC3")]

    for ax, x, y, title in pares:
        if y not in base.columns:
            ax.set_visible(False)
            continue
        for cluster, grupo in base.groupby("cluster_pca_sc"):
            ax.scatter(
                grupo[x],
                grupo[y],
                s=18,
                alpha=0.78,
                color=cores.get(int(cluster), "#111827"),
                label=f"Cluster {int(cluster)}",
                edgecolors="white",
                linewidths=0.35,
            )
        ax.set_title(title)
        ax.set_xlabel(x.upper())
        ax.set_ylabel(y.upper())
        ax.grid(alpha=0.18)
        ax.legend(frameon=False, fontsize=9)

    fig.suptitle(titulo, fontsize=15)
    fig.savefig(output, dpi=200, bbox_inches="tight")
    plt.close(fig)


def gera_analise_exploratoria(df: pd.DataFrame, model_cols: list[str]) -> dict[str, pd.DataFrame]:
    num = df[model_cols].apply(pd.to_numeric, errors="coerce")
    linhas = []
    total = len(num)
    for coluna in model_cols:
        s = num[coluna]
        q1 = s.quantile(0.25)
        q3 = s.quantile(0.75)
        iqr = q3 - q1
        lim_inf = q1 - 1.5 * iqr
        lim_sup = q3 + 1.5 * iqr
        outliers = ((s < lim_inf) | (s > lim_sup)).sum()
        linhas.append(
            {
                "variavel": coluna,
                "tipo": str(df[coluna].dtype),
                "n_obs": int(s.notna().sum()),
                "faltantes": int(s.isna().sum()),
                "pct_faltantes": round(100.0 * float(s.isna().sum()) / total if total else 0.0, 4),
                "media": s.mean(),
                "desvio_padrao": s.std(ddof=0),
                "min": s.min(),
                "q1": q1,
                "mediana": s.median(),
                "q3": q3,
                "max": s.max(),
                "zeros": int((s == 0).sum()),
                "negativos": int((s < 0).sum()),
                "outliers_iqr": int(outliers),
                "pct_outliers_iqr": round(100.0 * float(outliers) / total if total else 0.0, 4),
            }
        )

    resumo = pd.DataFrame(linhas)
    freq_cat = []
    for coluna in ["categoria", "classe_abc", "categoria_turistica_2019", "uf"]:
        if coluna in df.columns:
            serie = df[coluna].astype("string").fillna("<NA>")
            cont = serie.value_counts(dropna=False)
            for valor, qtd in cont.items():
                freq_cat.append(
                    {
                        "variavel": coluna,
                        "valor": valor,
                        "qtd": int(qtd),
                        "percentual": round(100.0 * qtd / len(df), 4) if len(df) else 0.0,
                    }
                )
    frequencias = pd.DataFrame(freq_cat)
    return {"resumo": resumo, "frequencias": frequencias}


def executa_modelo(base_universo: pd.DataFrame, cols_modelo: list[str], output_dir: Path, prefix_name: str, titulo_plot: str, k: int, seed: int, variance_threshold: float) -> dict:
    """Executa todas as etapas estatisticas e salva os produtos de um universo."""
    trabalho = base_universo.copy()
    z, idx_validos, _, _ = padroniza(trabalho, cols_modelo)
    base_valida = trabalho.loc[idx_validos].copy()

    scores, componentes_df, n_componentes = executa_pca(z, cols_modelo, variance_threshold)
    labels_raw, _, inertia = roda_kmeans(scores, k=k, seed=seed)
    cluster_ordem = reorganiza_clusters(base_valida, labels_raw)
    base_valida["cluster_pca_sc"] = cluster_ordem.to_numpy(dtype=int)
    for i in range(n_componentes):
        base_valida[f"pc{i + 1}"] = scores[:, i]

    sil = silhouette_score(scores, cluster_ordem.to_numpy(dtype=int))
    db = davies_bouldin_score(scores, cluster_ordem.to_numpy(dtype=int))
    ch = calinski_harabasz_score(scores, cluster_ordem.to_numpy(dtype=int))

    resumo = monta_resumo_cluster(base_valida, n_componentes, inertia)
    interpretacao = interpreta_cluster(base_valida, cols_modelo)
    exploratorio = gera_analise_exploratoria(base_valida, cols_modelo)
    corr = base_valida[cols_modelo].corr(method="pearson")

    out_dir = output_dir / prefix_name
    out_dir.mkdir(parents=True, exist_ok=True)

    base_saida = (
        base_valida[
            [
                "cluster_pca_sc",
                "cod_mun",
                "municipio",
                "uf",
                "categoria",
                "classe_abc",
                "domicilios_cnefe",
                "renda_media_chefe",
                "densidade_demografica_urbana",
            ]
        ]
        .rename(columns={"uf": "UF"})
        .sort_values(["cluster_pca_sc", "municipio"], ascending=[True, True])
        .reset_index(drop=True)
    )

    base_saida.to_csv(out_dir / f"{prefix_name}_municipios.csv", index=False, encoding="utf-8")
    resumo.to_csv(out_dir / f"{prefix_name}_resumo.csv", index=False, encoding="utf-8")
    componentes_df.to_csv(out_dir / f"{prefix_name}_componentes.csv", index=False, encoding="utf-8")
    interpretacao.to_csv(out_dir / f"{prefix_name}_cluster_interpretacao.csv", index=False, encoding="utf-8")
    exploratorio["resumo"].to_csv(out_dir / f"{prefix_name}_exploratorio_variaveis.csv", index=False, encoding="utf-8")
    exploratorio["frequencias"].to_csv(out_dir / f"{prefix_name}_frequencias_categoricas.csv", index=False, encoding="utf-8")
    corr.to_csv(out_dir / f"{prefix_name}_correlacao.csv", index=True, encoding="utf-8")
    plot_correlation_matrix(corr, out_dir / f"{prefix_name}_correlacao.png", f"Matriz de correlacao - {prefix_name}")
    (out_dir / f"{prefix_name}_correlacao.md").write_text(correlation_markdown(corr), encoding="utf-8")
    plota_resultado(base_valida, out_dir / f"{prefix_name}_eixos.png", titulo_plot)

    return {
        "base": base_valida,
        "resumo": resumo,
        "componentes": componentes_df,
        "interpretacao": interpretacao,
        "exploratorio": exploratorio["resumo"],
        "frequencias": exploratorio["frequencias"],
        "correlacao": corr,
        "plot_file": f"{prefix_name}_eixos.png",
        "corr_plot_file": f"{prefix_name}_correlacao.png",
        "prefix_name": prefix_name,
        "output_dir": out_dir,
        "metrics": {
            "n_municipios": int(len(base_valida)),
            "n_componentes": int(n_componentes),
            "silhouette": float(sil),
            "davies_bouldin": float(db),
            "calinski_harabasz": float(ch),
            "inercia": float(inertia),
        },
    }


def dataframe_to_md(df: pd.DataFrame, columns: list[str] | None = None) -> list[str]:
    if columns is not None:
        df = df[columns].copy()
    linhas = ["| " + " | ".join(df.columns) + " |", "|" + "---|" * len(df.columns)]
    for _, row in df.iterrows():
        vals = []
        for val in row.tolist():
            if isinstance(val, float):
                if math.isnan(val):
                    vals.append("")
                else:
                    vals.append(f"{val:.4f}")
            else:
                vals.append(str(val))
        linhas.append("| " + " | ".join(vals) + " |")
    return linhas


def escreve_relatorio(qmd_path: Path, resultados: dict[str, dict], universe_order: list[Universo], cols_modelo: list[str]) -> None:
    def chave_por_universo(u: Universo) -> str:
        return f"{u.chave}_solicitadas_renda"

    linhas: list[str] = [
        "---",
        'title: "Novo teste PCA + k-means com densidade urbana"',
        'subtitle: "Variaveis selecionadas + renda, sem REGIC e com densidade demografica urbana"',
        'author: "Preencher"',
        "date: today",
        "lang: pt-BR",
        "format:",
        "  html:",
        "    toc: true",
        "    number-sections: true",
        "    code-fold: false",
        "---",
        "",
        "## 1. Objetivo",
        "",
        "Este relatorio documenta um novo teste de PCA + k-means, sem alterar os resultados anteriores.",
        "Neste teste, a variavel de centralidade REGIC foi retirada e substituida pela densidade demografica urbana.",
        "O modelo final inclui as variaveis selecionadas + renda media do chefe de familia.",
        "",
        "## 2. Universos analisados",
        "",
        "- Santa Catarina, todos os municipios, exceto capital de UF e conurbados com UF;",
        "- Santa Catarina, classe A, exceto capital de UF e conurbados com UF;",
        "- Brasil, todos os municipios, exceto capital de UF e conurbados com UF;",
        "- Brasil, classe A, exceto capital de UF e conurbados com UF.",
        "",
        "## 3. Variaveis do modelo",
        "",
        "| Variavel | Descricao |",
        "|---|---|",
        "| `via_pav_pct` | percentual de domicilios com via pavimentada no entorno |",
        "| `indice_conectividade` | indice brasileiro de conectividade municipal normalizado |",
        "| `ambulatorios_sus_2026_02` | estabelecimentos com atendimento ambulatorial no SUS |",
        "| `pib_pc` | PIB per capita, calculado como `pib_total / pop_total` |",
        "| `categoria_turistica_score` | escore ordinal da categoria turistica |",
        "| `litoral_ate_50km` | indicador de ate 50 km do litoral |",
        "| `litoral_50_100km` | indicador entre 50 e 100 km do litoral |",
        "| `litoral_100_500km` | indicador entre 100 e 500 km do litoral |",
        "| `litoral_acima_500km` | indicador acima de 500 km do litoral |",
        "| `empresas_1k` | empresas por 1.000 habitantes |",
        "| `densidade_demografica_urbana` | densidade demografica urbana municipal |",
        "| `renda_media_chefe` | renda media do chefe de familia, derivada de `V06006` |",
        "",
        "## 4. Como o processamento foi feito",
        "",
        "1. A base municipal consolidada foi enriquecida com a classificacao ABC/CNEFE, a renda e a densidade urbana.",
        "2. Em cada universo, foram excluidos os municipios com categoria `capital de uf` e `conurbado com uf`.",
        "3. As variaveis do modelo foram padronizadas com media zero e desvio-padrao um.",
        "4. O PCA foi aplicado sobre as variaveis padronizadas.",
        "5. Foram retidos os componentes necessarios para atingir pelo menos 90% da variancia acumulada.",
        "6. O k-means foi rodado sobre os escores dos componentes retidos.",
        "7. A rotulagem final dos clusters foi ordenada pela media de `renda_media_chefe`.",
        "",
        "## 5. Densidade dos dados e inspeção exploratoria",
        "",
        "A tabela exploratoria de cada universo permite revisar faltantes, estatisticas descritivas, valores nulos e outliers pelo criterio do IQR.",
        "A matriz de correlacao ajuda a verificar colinearidade entre variaveis antes do PCA.",
        "",
    ]

    for u in universe_order:
        chave = chave_por_universo(u)
        r = resultados[chave]
        linhas += [
            f"## {u.chave.replace('_', ' ').upper()}",
            "",
            f"Universo com `{r['metrics']['n_municipios']}` municipios.",
            "",
            f"- `silhouette`: `{r['metrics']['silhouette']:.4f}`",
            f"- `Davies-Bouldin`: `{r['metrics']['davies_bouldin']:.4f}`",
            f"- `Calinski-Harabasz`: `{r['metrics']['calinski_harabasz']:.2f}`",
            f"- componentes retidos: `{r['metrics']['n_componentes']}`",
            "",
            "### 5.1 Exploracao das variaveis",
            "",
        ]
        linhas += dataframe_to_md(
            r["exploratorio"],
            [
                "variavel",
                "tipo",
                "n_obs",
                "faltantes",
                "pct_faltantes",
                "media",
                "desvio_padrao",
                "min",
                "mediana",
                "max",
                "outliers_iqr",
                "pct_outliers_iqr",
            ],
        )
        linhas += [
            "",
            "### 5.2 Frequencias categoricas",
            "",
        ]
        freq = r["frequencias"]
        if not freq.empty:
            for var in ["categoria", "classe_abc", "categoria_turistica_2019", "uf"]:
                bloco = freq.loc[freq["variavel"].eq(var)].copy()
                if not bloco.empty:
                    linhas += [f"#### {var}", ""]
                    linhas += dataframe_to_md(bloco, ["valor", "qtd", "percentual"])
                    linhas += [""]

        linhas += [
            "### 5.3 Matriz de correlacao",
            "",
            f"![](../outputs/{u.chave}_solicitadas_renda/{u.chave}_solicitadas_renda_correlacao.png){{width=92%}}",
            "",
            "### 5.4 Componentes retidos",
            "",
        ]
        linhas += dataframe_to_md(
            r["componentes"],
            ["componente", "variancia_explicada", "variancia_acumulada"] + cols_modelo,
        )
        linhas += [
            "",
            "### 5.5 Resumo dos clusters",
            "",
        ]
        linhas += dataframe_to_md(
            r["resumo"],
            [
                "cluster_pca_sc",
                "qtd_municipios",
                "percentual_municipios",
                "domicilios_cnefe_total",
                "pib_pc_medio",
                "renda_media_chefe_media",
                "densidade_demografica_urbana_media",
                "via_pav_media",
                "conectividade_media",
            ],
        )
        linhas += [
            "",
            "### 5.6 O que cada cluster uniu",
            "",
        ]
        linhas += dataframe_to_md(r["interpretacao"], ["cluster", "top_1", "top_2", "top_3", "resumo"])
        linhas += [
            "",
            "### 5.7 Eixos do PCA",
            "",
            f"![](../outputs/{u.chave}_solicitadas_renda/{u.chave}_solicitadas_renda_eixos.png){{width=92%}}",
            "",
        ]

    linhas += [
        "## 6. Leitura geral",
        "",
        "Os clusters devem ser lidos como agrupamentos de municipios com perfis semelhantes ao combinar urbanizacao, oferta de servicos, economia local, conectividade e renda.",
        "A presenca da densidade demografica urbana tende a reforcar o contraste entre nucleos mais compactos e municipios mais dispersos.",
        "A analise das matrizes de correlacao e dos loadings do PCA ajuda a entender se os componentes estao capturando centralidade, estrutura economica, litoralidade ou padroes urbanos mais difusos.",
        "",
    ]
    qmd_path.write_text("\n".join(linhas), encoding="utf-8")


def build_gpkg(geometry_path: Path, base_universo: pd.DataFrame, output_path: Path) -> None:
    import geopandas as gpd

    gdf = gpd.read_file(geometry_path)
    gdf["cod_mun"] = pd.to_numeric(gdf["CD_MUN"], errors="coerce").astype("Int64")
    merged = gdf.merge(base_universo, on="cod_mun", how="inner")

    if "UF" not in merged.columns and "uf" in merged.columns:
        merged = merged.rename(columns={"uf": "UF"})

    keep_cols = [
        "cod_mun",
        "municipio",
        "UF",
        "categoria",
        "classe_abc",
        "cluster_pca_sc",
        "domicilios_cnefe",
        "renda_media_chefe",
        "densidade_demografica_urbana",
        "geometry",
    ]
    result = gpd.GeoDataFrame(merged[keep_cols].copy(), geometry="geometry", crs=gdf.crs)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    result.to_file(output_path, layer="municipios_universo", driver="GPKG", mode="w")
    for cluster in sorted(result["cluster_pca_sc"].dropna().unique()):
        layer = result.loc[result["cluster_pca_sc"].eq(cluster)].copy()
        layer_name = f"cluster_{int(cluster)}"
        layer.to_file(output_path, layer=layer_name, driver="GPKG", mode="a")

    excluidos = merged.loc[
        merged["categoria"].astype("string").str.lower().isin(["capital de uf", "conurbado com uf"])
    ].copy()
    if not excluidos.empty:
        gpd.GeoDataFrame(excluidos[keep_cols].copy(), geometry="geometry", crs=gdf.crs).to_file(
            output_path, layer="excluidos_capital_conurbado", driver="GPKG", mode="a"
        )


def main() -> None:
    """Executa os quatro universos e grava o comparativo e o relatorio final."""
    args = parse_args()
    output_root = Path(args.output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    DOCS_ROOT.mkdir(parents=True, exist_ok=True)

    base = cria_base_enriquecida(args)

    resultados: dict[str, dict] = {}
    universos_def = {
        "sc_todos_sem_capital_conurbados": {"df": filtra_universo(base, "SC", False), "uf": "SC", "classe_a": False},
        "sc_classe_a_sem_capital_conurbados": {"df": filtra_universo(base, "SC", True), "uf": "SC", "classe_a": True},
        "br_todos_sem_capital_conurbados": {"df": filtra_universo(base, "", False), "uf": "", "classe_a": False},
        "br_classe_a_sem_capital_conurbados": {"df": filtra_universo(base, "", True), "uf": "", "classe_a": True},
    }

    for universo in UNIVERSOS:
        universo_df = cria_indicadores(universos_def[universo.chave]["df"])
        prefix_name = f"{universo.chave}_solicitadas_renda"
        titulo = f"{universo.label}: variaveis selecionadas + renda"
        resultado = executa_modelo(
            universo_df,
            COLUNAS_MODELO,
            output_root,
            prefix_name,
            titulo,
            k=args.k,
            seed=args.seed,
            variance_threshold=args.variance_threshold,
        )
        resultado["universo_label"] = universo.label
        resultado["universo_key"] = universo.chave
        resultados[f"{universo.chave}_solicitadas_renda"] = resultado

        if args.geometria:
            build_gpkg(
                Path(args.geometria),
                resultado["base"],
                output_root / prefix_name / f"{prefix_name}_camadas.gpkg",
            )

    comparativo = []
    for chave, resultado in resultados.items():
        comparativo.append(
            {
                "chave": chave,
                "universo": resultado["universo_label"],
                "n_municipios": resultado["metrics"]["n_municipios"],
                "n_componentes": resultado["metrics"]["n_componentes"],
                "silhouette": resultado["metrics"]["silhouette"],
                "davies_bouldin": resultado["metrics"]["davies_bouldin"],
                "calinski_harabasz": resultado["metrics"]["calinski_harabasz"],
                "inercia": resultado["metrics"]["inercia"],
                "cluster_counts": json.dumps(resultado["resumo"]["qtd_municipios"].tolist(), ensure_ascii=False),
            }
        )
    pd.DataFrame(comparativo).to_csv(Path(args.comparativo), index=False, encoding="utf-8")

    escreve_relatorio(Path(args.report), resultados, UNIVERSOS, COLUNAS_MODELO)

    for chave, resultado in resultados.items():
        m = resultado["metrics"]
        print(
            f"{chave}: n={m['n_municipios']} comps={m['n_componentes']} silhouette={m['silhouette']:.4f} "
            f"db={m['davies_bouldin']:.4f} ch={m['calinski_harabasz']:.2f}"
        )
    print(f"Comparativo salvo em: {args.comparativo}")
    print(f"Relatorio salvo em: {args.report}")


if __name__ == "__main__":
    main()
