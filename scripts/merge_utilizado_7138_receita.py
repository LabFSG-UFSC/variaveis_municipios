#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script utilizado para enriquecer a base merge_completo_todos_mun.csv com:
- tabela7138.csv, via codigo do municipio;
- 0fcf5cfb-9b3d-45b8-80c8-00d6eb180ff6.csv, via nome do municipio.

O arquivo 7138 traz a taxa de escolarizacao.
O arquivo 0fcf5... traz as colunas de receita prevista e realizada.
"""

import argparse
import os
import re
import unicodedata
from datetime import datetime

import pandas as pd


class MergeConfig:
    """Configuracao padrao do merge."""

    def __init__(self):
        self.base_file = "../tabelas_processadas/merge_completo_todos_mun.csv"
        self.education_file = "../tabelas_mae/tabela7138.csv"
        self.revenue_file = "../tabelas_mae/0fcf5cfb-9b3d-45b8-80c8-00d6eb180ff6.csv"
        self.output_file = "../tabelas_processadas/merge_completo_todos_mun_7138_receita.csv"

        self.base_key_code = "Cód."
        self.base_city_col = "Município"
        self.education_key_code = "Cód."
        self.education_value_col = "Total"
        self.education_output_col = "taxa_escolarizacao_2024"
        self.education_skiprows = 4

        self.revenue_city_col = "Ente Municipal"
        self.revenue_value_cols = [
            "Valor Receita Prevista",
            "Valor Receita Realizada",
        ]
        self.revenue_skiprows = 0
        self.revenue_uf = "SC"

        self.encoding = "utf-8"
        self.verbose = True


def carregar_arquivo(caminho, encoding="utf-8", skiprows=0, verbose=True):
    """Carrega arquivo CSV com validacao basica."""

    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Arquivo nao encontrado: {caminho}")

    df = pd.read_csv(caminho, encoding=encoding, skiprows=skiprows)
    df.columns = df.columns.str.strip()

    if verbose:
        print(f"Arquivo carregado: {caminho}")
        if skiprows:
            print(f"   - Linhas ignoradas no inicio: {skiprows}")
        print(f"   - Linhas: {len(df)}")
        print(f"   - Colunas: {list(df.columns)}")

    return df


def verificar_colunas(df, colunas, nome_arquivo):
    """Valida se todas as colunas existem no DataFrame."""

    faltantes = [col for col in colunas if col not in df.columns]
    if faltantes:
        raise ValueError(
            f"Colunas ausentes em {nome_arquivo}: {faltantes}. "
            f"Disponiveis: {list(df.columns)}"
        )


def normalizar_codigo(valor):
    """Normaliza codigo de municipio para comparacao."""

    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def normalizar_nome_municipio(valor):
    """Normaliza nomes para merge por municipio."""

    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    texto = re.sub(r"\s+\([A-Z]{2}\)$", "", texto)
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = texto.upper()
    texto = texto.replace("'", " ")
    texto = texto.replace("-", " ")
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto or pd.NA


def extrair_uf(valor):
    """Extrai a UF do texto 'Municipio (UF)'."""

    if pd.isna(valor):
        return pd.NA

    match = re.search(r"\(([A-Z]{2})\)\s*$", str(valor))
    return match.group(1) if match else pd.NA


def preparar_merge_educacao(base_df, education_df, config):
    """Executa merge da tabela 7138 por codigo."""

    verificar_colunas(
        base_df,
        [config.base_key_code],
        "arquivo base",
    )
    verificar_colunas(
        education_df,
        [config.education_key_code, config.education_value_col],
        "tabela7138.csv",
    )

    base_tmp = base_df.copy()
    education_tmp = education_df.copy()

    base_tmp["_codigo_merge"] = base_tmp[config.base_key_code].map(normalizar_codigo)
    education_tmp["_codigo_merge"] = education_tmp[config.education_key_code].map(normalizar_codigo)

    education_tmp = education_tmp[["_codigo_merge", config.education_value_col]].copy()
    education_tmp = education_tmp.rename(
        columns={config.education_value_col: config.education_output_col}
    )

    duplicados = education_tmp["_codigo_merge"].dropna().duplicated().sum()
    if duplicados:
        print(
            f"Aviso: tabela7138.csv possui {duplicados} codigo(s) duplicado(s). "
            "Apenas a primeira ocorrencia sera mantida."
        )
        education_tmp = education_tmp.drop_duplicates(subset=["_codigo_merge"], keep="first")

    resultado = base_tmp.merge(education_tmp, on="_codigo_merge", how="left")
    correspondencias = resultado[config.education_output_col].notna().sum()

    print("\nMerge da tabela7138 concluido:")
    print(f"   - Linhas da base: {len(base_df)}")
    print(f"   - Correspondencias por codigo: {correspondencias}")
    print(f"   - Sem correspondencia: {len(base_df) - correspondencias}")

    return resultado


def preparar_merge_receita(base_df, revenue_df, config):
    """Executa merge do arquivo de receita por nome de municipio."""

    verificar_colunas(
        base_df,
        [config.base_city_col],
        "arquivo base",
    )
    verificar_colunas(
        revenue_df,
        [config.revenue_city_col] + config.revenue_value_cols,
        "arquivo de receita",
    )

    resultado = base_df.copy()
    resultado["_ordem_base"] = range(len(resultado))
    resultado["_municipio_merge"] = resultado[config.base_city_col].map(normalizar_nome_municipio)
    resultado["_uf_base"] = resultado[config.base_city_col].map(extrair_uf)

    revenue_tmp = revenue_df.copy()
    revenue_tmp["_municipio_merge"] = revenue_tmp[config.revenue_city_col].map(normalizar_nome_municipio)
    revenue_tmp = revenue_tmp[["_municipio_merge"] + config.revenue_value_cols].copy()

    duplicados = revenue_tmp["_municipio_merge"].dropna().duplicated().sum()
    if duplicados:
        print(
            f"Aviso: arquivo de receita possui {duplicados} municipio(s) duplicado(s). "
            "Apenas a primeira ocorrencia sera mantida."
        )
        revenue_tmp = revenue_tmp.drop_duplicates(subset=["_municipio_merge"], keep="first")

    mascara_uf = resultado["_uf_base"].eq(config.revenue_uf)
    base_uf = resultado.loc[mascara_uf].copy()
    base_outros = resultado.loc[~mascara_uf].copy()

    base_uf = base_uf.merge(revenue_tmp, on="_municipio_merge", how="left")
    correspondencias = base_uf[config.revenue_value_cols[0]].notna().sum()

    print("\nMerge do arquivo de receita concluido:")
    print(f"   - UF usada no merge por nome: {config.revenue_uf}")
    print(f"   - Linhas da base nessa UF: {len(base_uf)}")
    print(f"   - Correspondencias por nome: {correspondencias}")
    print(f"   - Sem correspondencia: {len(base_uf) - correspondencias}")

    for coluna in config.revenue_value_cols:
        if coluna not in base_outros.columns:
            base_outros[coluna] = pd.NA

    combinado = pd.concat([base_uf, base_outros], ignore_index=True)
    combinado = combinado.sort_values("_ordem_base").reset_index(drop=True)
    return combinado


def limpar_colunas_auxiliares(df):
    """Remove colunas temporarias do processamento."""

    colunas_aux = [
        col for col in ["_codigo_merge", "_municipio_merge", "_uf_base", "_ordem_base"]
        if col in df.columns
    ]
    if colunas_aux:
        df = df.drop(columns=colunas_aux)
    return df


def executar_merge(config):
    """Executa o fluxo completo."""

    print("\n" + "=" * 70)
    print("MERGE DA BASE MUNICIPAL COM ESCOLARIZACAO E RECEITA")
    print("=" * 70)
    print(f"Execucao: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 70 + "\n")

    base_df = carregar_arquivo(config.base_file, config.encoding, verbose=config.verbose)
    education_df = carregar_arquivo(
        config.education_file,
        config.encoding,
        skiprows=config.education_skiprows,
        verbose=config.verbose,
    )
    revenue_df = carregar_arquivo(
        config.revenue_file,
        config.encoding,
        skiprows=config.revenue_skiprows,
        verbose=config.verbose,
    )

    resultado = preparar_merge_educacao(base_df, education_df, config)
    resultado = preparar_merge_receita(resultado, revenue_df, config)
    resultado = limpar_colunas_auxiliares(resultado)

    print(f"\nSalvando arquivo: {config.output_file}")
    resultado.to_csv(config.output_file, index=False, encoding=config.encoding)

    print("\nProcesso concluido com sucesso.")
    print(f"Arquivo gerado: {config.output_file}")
    return 0


def main():
    """Ponto de entrada do script."""

    config = MergeConfig()

    parser = argparse.ArgumentParser(
        description="Merge da base municipal com tabela7138 e arquivo de receita"
    )
    parser.add_argument("--base", help="Arquivo base processado")
    parser.add_argument("--education", help="Arquivo tabela7138.csv")
    parser.add_argument("--revenue", help="Arquivo de receita 0fcf5...")
    parser.add_argument("--output", help="Arquivo final de saida")
    parser.add_argument("--revenue-uf", help="UF a usar no merge por nome do arquivo de receita")

    args = parser.parse_args()

    if args.base:
        config.base_file = args.base
    if args.education:
        config.education_file = args.education
    if args.revenue:
        config.revenue_file = args.revenue
    if args.output:
        config.output_file = args.output
    if args.revenue_uf:
        config.revenue_uf = args.revenue_uf

    raise SystemExit(executar_merge(config))


if __name__ == "__main__":
    main()
