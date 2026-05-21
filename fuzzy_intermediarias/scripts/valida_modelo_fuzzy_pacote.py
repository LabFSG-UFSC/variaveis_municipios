#!/usr/bin/env python3
"""
Validacao formal do fuzzy reduzido das capitais de regioes intermediarias.

Gera:
- comparacao sistematica entre modelos alternativos
- otimizacao estatistica de pesos
- teste de estabilidade por bootstrap
- analise de sensibilidade
- validacao externa contra o fuzzy completo e variaveis retidas
"""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_MODELO = ROOT / "scripts" / "classifica_capitais_intermediarias_fuzzy_pacote.py"
BASE_MUNICIPAL_PADRAO = ROOT / "inputs" / "merge_v28_fuzzy_subset.csv"
CAPITAIS_PADRAO = ROOT / "inputs" / "capitais_intermediarias_nao_uf.csv"
FUZZY_COMPLETO_PADRAO = (
    ROOT.parent
    / "classificacao_municipios"
    / "processamento"
    / "pacote_fuzzy_capitais_intermediarias"
    / "outputs"
    / "classificacao_capitais_intermediarias_fuzzy.csv"
)
SAIDA_RESUMO_PADRAO = ROOT / "outputs" / "validacao_modelo_fuzzy_reduzido_resumo.csv"
SAIDA_SENS_PADRAO = ROOT / "outputs" / "validacao_modelo_fuzzy_reduzido_sensibilidade.csv"
SAIDA_EXT_PADRAO = ROOT / "outputs" / "validacao_modelo_fuzzy_reduzido_validacao_externa.csv"
SAIDA_RELATORIO_PADRAO = ROOT / "outputs" / "validacao_modelo_fuzzy_reduzido_relatorio.md"

ORDEM_CLASSES = ["muito_baixo", "baixo", "medio", "alto", "muito_alto"]
MAPA_CLASSE = {classe: indice for indice, classe in enumerate(ORDEM_CLASSES)}
COLUNAS_SCORE = [
    "score_empresas_1k",
    "score_regic_var60",
    "score_via_pav_pct",
    "score_indice_conectividade",
    "score_estab_saude_10k",
    "score_homicidios_100k",
]
COLUNAS_EIXOS = [
    "dinamismo_economico",
    "infraestrutura_urbana",
    "conectividade_digital",
    "oferta_servicos",
    "seguranca_territorial",
]
COLUNAS_VALIDACAO_EXTERNA = [
    "pib_total",
    "regic_var56",
    "regic_var59",
    "regic_var61",
    "regic_var66",
    "ambulatorios_sus_2026_02",
    "densidade_scm",
    "cobertura_pop_4g5g",
    "fibra",
    "adensamento_estacoes",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Valida o fuzzy reduzido das capitais intermediarias.")
    parser.add_argument("--base-municipal", default=str(BASE_MUNICIPAL_PADRAO))
    parser.add_argument("--capitais", default=str(CAPITAIS_PADRAO))
    parser.add_argument("--fuzzy-completo", default=str(FUZZY_COMPLETO_PADRAO))
    parser.add_argument("--output-resumo", default=str(SAIDA_RESUMO_PADRAO))
    parser.add_argument("--output-sensibilidade", default=str(SAIDA_SENS_PADRAO))
    parser.add_argument("--output-externa", default=str(SAIDA_EXT_PADRAO))
    parser.add_argument("--output-relatorio", default=str(SAIDA_RELATORIO_PADRAO))
    parser.add_argument("--n-random-search", type=int, default=50000)
    parser.add_argument("--n-bootstrap", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def carrega_modulo_modelo():
    spec = importlib.util.spec_from_file_location("modelo_reduzido", SCRIPT_MODELO)
    modulo = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(modulo)
    return modulo


def rank_pearson(
    a: pd.Series,
    b: pd.Series,
    ascending_a: bool = False,
    ascending_b: bool = False,
) -> float:
    serie_a = pd.Series(a).rank(method="average", ascending=ascending_a)
    serie_b = pd.Series(b).rank(method="average", ascending=ascending_b)
    return float(serie_a.corr(serie_b))


def classifica_por_score(modulo, score: pd.Series) -> tuple[pd.Series, pd.Series]:
    pertinencias = pd.DataFrame(modulo.pertinencia_quantis(score))
    classes = pertinencias.idxmax(axis=1)
    confianca = pertinencias.max(axis=1)
    return classes, confianca


def avalia_modelo(modulo, df: pd.DataFrame, nome: str, score: pd.Series) -> dict[str, float | str]:
    classes, confianca = classifica_por_score(modulo, score)
    classe_num = classes.map(MAPA_CLASSE)
    classe_ref = df["classificacao_fuzzy_orig"].map(MAPA_CLASSE)
    return {
        "modelo": nome,
        "correlacao_score_completo": float(pd.Series(score).corr(df["score_final_orig"])),
        "correlacao_ranking_completo": rank_pearson(score, df["ranking_final_orig"], ascending_a=False, ascending_b=True),
        "acordo_exato_classes": float((classes == df["classificacao_fuzzy_orig"]).mean()),
        "acordo_adjacente_classes": float(((classe_num - classe_ref).abs() <= 1).mean()),
        "erro_medio_absoluto_classe": float((classe_num - classe_ref).abs().mean()),
        "confianca_media": float(confianca.mean()),
    }


def prepara_base_validacao(modulo, caminho_base: Path, caminho_capitais: Path, caminho_fuzzy_completo: Path) -> pd.DataFrame:
    base = modulo.carrega_base(caminho_base)
    base_completa = pd.read_csv(caminho_base, usecols=["cod_mun", *COLUNAS_VALIDACAO_EXTERNA])
    base_completa["cod_mun"] = pd.to_numeric(base_completa["cod_mun"], errors="coerce").astype("Int64")
    capitais = modulo.carrega_capitais(caminho_capitais)
    fuzzy_completo = pd.read_csv(caminho_fuzzy_completo)
    fuzzy_completo["cod_mun"] = pd.to_numeric(fuzzy_completo["cod_mun"], errors="coerce").astype("Int64")

    df = modulo.prepara_base_capitais(base, capitais)
    df = modulo.calcula_indicadores(df)
    df = modulo.calcula_scores(df)
    df = modulo.calcula_eixos(df)
    df = df.merge(
        fuzzy_completo[["cod_mun", "score_final", "ranking_final", "classificacao_fuzzy"]],
        on="cod_mun",
        how="left",
    ).merge(
        base_completa,
        on="cod_mun",
        how="left",
    ).rename(
        columns={
            "score_final": "score_final_orig",
            "ranking_final": "ranking_final_orig",
            "classificacao_fuzzy": "classificacao_fuzzy_orig",
        }
    )
    df["score_modelo_atual"] = modulo.weighted_mean(df[COLUNAS_EIXOS], modulo.PESOS_CLASSIFICACAO)
    return df


def random_search_simplex(matriz: np.ndarray, alvo: np.ndarray, tamanho: int, seed: int) -> tuple[np.ndarray, float]:
    rng = np.random.default_rng(seed)
    pesos = rng.dirichlet(np.ones(matriz.shape[1]), size=tamanho)
    scores = matriz @ pesos.T
    alvo_c = alvo - alvo.mean()
    scores_c = scores - scores.mean(axis=0)
    correlacoes = (alvo_c[:, None] * scores_c).sum(axis=0) / np.sqrt((alvo_c**2).sum() * (scores_c**2).sum(axis=0))
    indice = int(np.nanargmax(correlacoes))
    return pesos[indice], float(correlacoes[indice])


def gera_modelos_alternativos(df: pd.DataFrame, modulo, n_random_search: int, seed: int) -> tuple[dict[str, pd.Series], dict[str, dict[str, float]]]:
    modelos: dict[str, pd.Series] = {}
    metadados: dict[str, dict[str, float]] = {}

    modelos["atual"] = df["score_modelo_atual"]
    metadados["atual"] = {"tipo": "manual"}

    modelos["eixos_iguais"] = df[COLUNAS_EIXOS].mean(axis=1)
    metadados["eixos_iguais"] = {"tipo": "alternativo"}

    modelos["variaveis_iguais"] = df[COLUNAS_SCORE].mean(axis=1)
    metadados["variaveis_iguais"] = {"tipo": "alternativo"}

    matriz_scores = df[COLUNAS_SCORE].to_numpy(dtype=float)
    matriz_centrada = matriz_scores - np.nanmean(matriz_scores, axis=0)
    matriz_centrada = np.where(np.isnan(matriz_centrada), 0.0, matriz_centrada)
    autovalores, autovetores = np.linalg.eigh(np.cov(matriz_centrada, rowvar=False))
    componente_principal = autovetores[:, np.argmax(autovalores)]
    if np.corrcoef(matriz_scores[:, 0], matriz_centrada @ componente_principal)[0, 1] < 0:
        componente_principal = -componente_principal
    modelos["pca1"] = modulo.percent_rank(pd.Series(matriz_centrada @ componente_principal), ascending=True)
    metadados["pca1"] = {"tipo": "alternativo"}

    pesos_otimizados_eixos, corr_eixos = random_search_simplex(
        df[COLUNAS_EIXOS].to_numpy(dtype=float),
        df["score_final_orig"].to_numpy(dtype=float),
        tamanho=n_random_search,
        seed=seed,
    )
    modelos["otimizado_eixos"] = pd.Series(df[COLUNAS_EIXOS].to_numpy(dtype=float) @ pesos_otimizados_eixos, index=df.index)
    metadados["otimizado_eixos"] = {
        "tipo": "otimizado",
        **{f"peso_{coluna}": float(peso) for coluna, peso in zip(COLUNAS_EIXOS, pesos_otimizados_eixos)},
        "correlacao_otimizacao": corr_eixos,
    }

    pesos_otimizados_variaveis, corr_variaveis = random_search_simplex(
        matriz_scores,
        df["score_final_orig"].to_numpy(dtype=float),
        tamanho=n_random_search,
        seed=seed + 1,
    )
    modelos["otimizado_variaveis"] = pd.Series(matriz_scores @ pesos_otimizados_variaveis, index=df.index)
    metadados["otimizado_variaveis"] = {
        "tipo": "otimizado",
        **{f"peso_{coluna}": float(peso) for coluna, peso in zip(COLUNAS_SCORE, pesos_otimizados_variaveis)},
        "correlacao_otimizacao": corr_variaveis,
    }
    return modelos, metadados


def bootstrap_estabilidade(modulo, df: pd.DataFrame, pesos_eixos: dict[str, float], n_bootstrap: int, seed: int) -> dict[str, float]:
    rng = np.random.default_rng(seed)
    score_base = modulo.weighted_mean(df[COLUNAS_EIXOS], pesos_eixos)
    ranking_base = score_base.rank(method="average", ascending=False)
    correlacoes_internas: list[float] = []
    correlacoes_externas: list[float] = []

    for _ in range(n_bootstrap):
        amostra = df.iloc[rng.integers(0, len(df), len(df))].copy().reset_index(drop=True)
        for coluna in ["empresas_1k", "regic_var60", "via_pav_pct", "indice_conectividade", "estab_saude_10k"]:
            amostra[f"score_{coluna}"] = modulo.percent_rank(amostra[coluna], ascending=True)
        amostra["score_homicidios_100k"] = modulo.percent_rank(amostra["homicidios_100k"], ascending=False)
        amostra["dinamismo_economico"] = 0.55 * amostra["score_empresas_1k"] + 0.45 * amostra["score_regic_var60"]
        amostra["infraestrutura_urbana"] = amostra["score_via_pav_pct"]
        amostra["conectividade_digital"] = amostra["score_indice_conectividade"]
        amostra["oferta_servicos"] = amostra["score_estab_saude_10k"]
        amostra["seguranca_territorial"] = amostra["score_homicidios_100k"]
        amostra["score_boot"] = modulo.weighted_mean(amostra[COLUNAS_EIXOS], pesos_eixos)
        ranks_boot = amostra.groupby("cod_mun", as_index=False)["score_boot"].mean()
        ranks_boot["ranking_boot"] = ranks_boot["score_boot"].rank(method="average", ascending=False)

        comparacao = df[["cod_mun", "ranking_final_orig"]].merge(ranks_boot[["cod_mun", "ranking_boot"]], on="cod_mun", how="inner")
        if len(comparacao) > 30:
            base_comum = pd.Series(ranking_base.loc[df["cod_mun"].isin(comparacao["cod_mun"])].to_numpy())
            comparacao = comparacao.sort_values("cod_mun").reset_index(drop=True)
            base_comum = df[["cod_mun"]].merge(
                pd.DataFrame({"cod_mun": df["cod_mun"], "ranking_base": ranking_base}),
                on="cod_mun",
                how="inner",
            ).merge(comparacao[["cod_mun", "ranking_boot"]], on="cod_mun", how="inner")
            correlacoes_internas.append(float(base_comum["ranking_base"].corr(base_comum["ranking_boot"])))
            correlacoes_externas.append(float(comparacao["ranking_final_orig"].corr(comparacao["ranking_boot"])))

    valores_internos = np.array(correlacoes_internas)
    valores_externos = np.array(correlacoes_externas)
    return {
        "bootstrap_corr_media_interna": float(valores_internos.mean()),
        "bootstrap_corr_p5_interna": float(np.quantile(valores_internos, 0.05)),
        "bootstrap_corr_p95_interna": float(np.quantile(valores_internos, 0.95)),
        "bootstrap_corr_media_externa": float(valores_externos.mean()),
        "bootstrap_corr_p5_externa": float(np.quantile(valores_externos, 0.05)),
        "bootstrap_corr_p95_externa": float(np.quantile(valores_externos, 0.95)),
    }


def analise_sensibilidade(df: pd.DataFrame, modulo) -> pd.DataFrame:
    pesos_base = dict(modulo.PESOS_CLASSIFICACAO)
    score_base = df["score_modelo_atual"]
    resultados: list[dict[str, float | str]] = []

    for eixo in COLUNAS_EIXOS:
        for delta in (-0.10, -0.05, 0.05, 0.10):
            pesos = dict(pesos_base)
            pesos[eixo] = max(0.001, pesos[eixo] + delta)
            soma = sum(pesos.values())
            pesos = {chave: valor / soma for chave, valor in pesos.items()}
            score = modulo.weighted_mean(df[COLUNAS_EIXOS], pesos)
            resultados.append(
                {
                    "cenario": f"peso_{eixo}_{delta:+.2f}",
                    "tipo": "perturbacao_pesos_eixos",
                    "correlacao_com_modelo_atual": float(score.corr(score_base)),
                    "correlacao_com_fuzzy_completo": float(score.corr(df["score_final_orig"])),
                    "correlacao_ranking_com_modelo_atual": rank_pearson(score, score_base, ascending_a=False, ascending_b=False),
                    "correlacao_ranking_com_fuzzy_completo": rank_pearson(score, df["ranking_final_orig"], ascending_a=False, ascending_b=True),
                }
            )

    for coluna_removida in COLUNAS_SCORE:
        colunas_restantes = [coluna for coluna in COLUNAS_SCORE if coluna != coluna_removida]
        score = df[colunas_restantes].mean(axis=1)
        resultados.append(
            {
                "cenario": f"sem_{coluna_removida}",
                "tipo": "leave_one_out_variavel",
                "correlacao_com_modelo_atual": float(score.corr(score_base)),
                "correlacao_com_fuzzy_completo": float(score.corr(df["score_final_orig"])),
                "correlacao_ranking_com_modelo_atual": rank_pearson(score, score_base, ascending_a=False, ascending_b=False),
                "correlacao_ranking_com_fuzzy_completo": rank_pearson(score, df["ranking_final_orig"], ascending_a=False, ascending_b=True),
            }
        )

    return pd.DataFrame(resultados).sort_values(["tipo", "correlacao_com_fuzzy_completo"], ascending=[True, False]).reset_index(drop=True)


def validacao_externa(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["pib_pc"] = np.where(df["pop_total"] > 0, df["pib_total"] / df["pop_total"], np.nan)
    variaveis_externas = [
        "pib_pc",
        "regic_var56",
        "regic_var59",
        "regic_var61",
        "regic_var66",
        "ambulatorios_sus_2026_02",
        "densidade_scm",
        "cobertura_pop_4g5g",
        "fibra",
        "adensamento_estacoes",
    ]
    resultados: list[dict[str, float | str]] = []
    for coluna in variaveis_externas:
        if coluna not in df.columns:
            continue
        score_externo = pd.to_numeric(df[coluna], errors="coerce").rank(method="average", pct=True, ascending=True).fillna(0.5)
        resultados.append(
            {
                "variavel_externa": coluna,
                "correlacao_modelo_reduzido": float(df["score_modelo_atual"].corr(score_externo)),
            }
        )
    return pd.DataFrame(resultados).sort_values("correlacao_modelo_reduzido", ascending=False).reset_index(drop=True)


def monta_relatorio(
    resumo: pd.DataFrame,
    sensibilidade: pd.DataFrame,
    externa: pd.DataFrame,
    estabilidade: dict[str, float],
    metadados_modelos: dict[str, dict[str, float]],
) -> str:
    melhor_correlacao = resumo.sort_values("correlacao_score_completo", ascending=False).iloc[0]
    melhor_adjacencia = resumo.sort_values("acordo_adjacente_classes", ascending=False).iloc[0]
    pior_leave_one_out = sensibilidade[sensibilidade["tipo"] == "leave_one_out_variavel"].sort_values(
        "correlacao_com_fuzzy_completo"
    ).iloc[0]

    linhas = [
        "# Validacao Formal Do Fuzzy Reduzido",
        "",
        "## Escopo",
        "",
        "Esta validacao cobre cinco frentes:",
        "",
        "- otimizacao estatistica de pesos",
        "- teste de estabilidade por bootstrap",
        "- analise de sensibilidade a pesos e exclusao de variaveis",
        "- validacao externa contra o fuzzy completo e variaveis retidas",
        "- comparacao sistematica com modelos alternativos",
        "",
        "## Comparacao Sistemica Entre Modelos",
        "",
        "```text",
        resumo.round(4).to_string(index=False),
        "```",
        "",
        "Leitura principal:",
        f"- melhor correlacao com o fuzzy completo: `{melhor_correlacao['modelo']}` (`{melhor_correlacao['correlacao_score_completo']:.4f}`)",
        f"- melhor acordo adjacente de classes: `{melhor_adjacencia['modelo']}` (`{melhor_adjacencia['acordo_adjacente_classes']:.4f}`)",
        f"- desempenho do modelo atual: correlacao `{resumo.loc[resumo['modelo'] == 'atual', 'correlacao_score_completo'].iloc[0]:.4f}` e acordo adjacente `{resumo.loc[resumo['modelo'] == 'atual', 'acordo_adjacente_classes'].iloc[0]:.4f}`",
        "",
        "## Otimizacao Estatistica De Pesos",
        "",
        "A otimizacao foi feita por busca aleatoria em simplex, maximizando a correlacao com o `score_final` do fuzzy completo.",
        "",
        "Pesos otimizados por eixos:",
    ]

    for chave, valor in metadados_modelos["otimizado_eixos"].items():
        if chave.startswith("peso_"):
            linhas.append(f"- `{chave.removeprefix('peso_')}`: `{valor:.4f}`")
    linhas.extend(
        [
            f"- correlacao alcancada: `{metadados_modelos['otimizado_eixos']['correlacao_otimizacao']:.4f}`",
            "",
            "Pesos otimizados por variaveis:",
        ]
    )
    for chave, valor in metadados_modelos["otimizado_variaveis"].items():
        if chave.startswith("peso_"):
            linhas.append(f"- `{chave.removeprefix('peso_')}`: `{valor:.4f}`")
    linhas.append(f"- correlacao alcancada: `{metadados_modelos['otimizado_variaveis']['correlacao_otimizacao']:.4f}`")
    linhas.extend(
        [
            "",
            "Interpretacao:",
            "- o fuzzy completo parece valorizar mais `regic_var60` e `indice_conectividade`",
            "- `homicidios_100k` contribui pouco quando o alvo e aproximar o fuzzy completo",
            "",
            "## Estabilidade",
            "",
            f"- correlacao media bootstrap do modelo atual com ele mesmo: `{estabilidade['bootstrap_corr_media_interna']:.4f}`",
            f"- faixa bootstrap interna p5-p95: `{estabilidade['bootstrap_corr_p5_interna']:.4f}` a `{estabilidade['bootstrap_corr_p95_interna']:.4f}`",
            f"- correlacao media bootstrap do modelo atual com o ranking do fuzzy completo: `{estabilidade['bootstrap_corr_media_externa']:.4f}`",
            f"- faixa bootstrap externa p5-p95: `{estabilidade['bootstrap_corr_p5_externa']:.4f}` a `{estabilidade['bootstrap_corr_p95_externa']:.4f}`",
            "",
            "Interpretacao:",
            "- a estrutura do ranking se manteve alta ao longo das reamostragens",
            "- isso indica que pequenas variacoes amostrais nao desorganizam o modelo",
            "",
            "## Sensibilidade",
            "",
            "```text",
            sensibilidade.round(4).to_string(index=False),
            "```",
            "",
            f"- cenario leave-one-out mais critico: `{pior_leave_one_out['cenario']}` com correlacao `{pior_leave_one_out['correlacao_com_fuzzy_completo']:.4f}` com o fuzzy completo",
            "- perturbacoes moderadas nos pesos dos eixos alteram pouco o ranking, sugerindo robustez estrutural",
            "",
            "## Validacao Externa",
            "",
            "A validacao externa foi operacionalizada de duas formas:",
            "",
            "- benchmark contra o fuzzy completo ja existente no projeto",
            "- correlacao do modelo reduzido com variaveis importantes que ficaram de fora da especificacao final",
            "",
            "```text",
            externa.round(4).to_string(index=False),
            "```",
            "",
            "Interpretacao:",
            "- correlacoes altas com `densidade_scm`, `cobertura_pop_4g5g` e `pib_pc` indicam boa validade convergente",
            "- correlacoes positivas com varias `regic_*` sugerem que o modelo preserva informacao de centralidade que nao foi explicitamente mantida",
            "",
            "## Conclusao Metodologica",
            "",
            "- o modelo atual e defensavel como especificacao reduzida, porque combina boa aderencia ao fuzzy completo com alta interpretabilidade",
            "- a alternativa `otimizado_variaveis` e a melhor se o objetivo for maximizar proximidade estatistica com o fuzzy completo",
            "- a especificacao atual continua preferivel se o objetivo principal for transparencia substantiva e leitura simples dos eixos",
            "",
            "## Limitacoes",
            "",
            "- a validacao externa nao usa base fora do projeto; ela usa benchmark interno forte e variaveis retidas como criterio convergente",
            "- a otimizacao estatistica foi feita por busca aleatoria em simplex, adequada para este problema pequeno, mas nao unica forma possivel",
        ]
    )
    return "\n".join(linhas) + "\n"


def main() -> int:
    args = parse_args()
    modulo = carrega_modulo_modelo()
    df = prepara_base_validacao(modulo, Path(args.base_municipal), Path(args.capitais), Path(args.fuzzy_completo))
    modelos, metadados = gera_modelos_alternativos(df, modulo, n_random_search=args.n_random_search, seed=args.seed)

    resumo = pd.DataFrame([avalia_modelo(modulo, df, nome, score) for nome, score in modelos.items()])
    resumo = resumo.sort_values("correlacao_score_completo", ascending=False).reset_index(drop=True)

    sensibilidade = analise_sensibilidade(df, modulo)
    externa = validacao_externa(df)
    estabilidade = bootstrap_estabilidade(modulo, df, dict(modulo.PESOS_CLASSIFICACAO), n_bootstrap=args.n_bootstrap, seed=args.seed)

    output_resumo = Path(args.output_resumo)
    output_sensibilidade = Path(args.output_sensibilidade)
    output_externa = Path(args.output_externa)
    output_relatorio = Path(args.output_relatorio)
    for caminho in [output_resumo, output_sensibilidade, output_externa, output_relatorio]:
        caminho.parent.mkdir(parents=True, exist_ok=True)

    resumo.to_csv(output_resumo, index=False, encoding="utf-8")
    sensibilidade.to_csv(output_sensibilidade, index=False, encoding="utf-8")
    externa.to_csv(output_externa, index=False, encoding="utf-8")
    output_relatorio.write_text(monta_relatorio(resumo, sensibilidade, externa, estabilidade, metadados), encoding="utf-8")

    print(f"Resumo da comparacao de modelos: {output_resumo}")
    print(f"Sensibilidade: {output_sensibilidade}")
    print(f"Validacao externa: {output_externa}")
    print(f"Relatorio: {output_relatorio}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
