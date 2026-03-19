#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para processar o arquivo de transferencias do Fundeb por municipio,
transformar os tipos de transferencia em colunas e fazer o merge por codigo IBGE
com a base processada anteriormente.
"""

import argparse
import os
import re
import unicodedata
from datetime import datetime

import pandas as pd


class MergeConfig:
    """Configuracao padrao do processamento."""

    def __init__(self):
        self.base_file = "../tabelas_processadas/merge_completo_todos_mun_7138_receita.csv"
        self.fundeb_file = "../tabelas_mae/transferências_para_municípios.csv"
        self.output_file = "../tabelas_processadas/merge_completo_todos_mun_7138_receita_fundeb.csv"

        self.base_key_col = "Cód."
        self.fundeb_key_col = "Código IBGE"
        self.fundeb_type_col = "Transferência"
        self.fundeb_value_col = "Valor Consolidado"
        self.fundeb_year_col = "Ano"
        self.fundeb_encoding = "latin1"
        self.fundeb_sep = ";"
        self.fundeb_year = 2025
        self.verbose = True


def carregar_csv(caminho, encoding="utf-8", sep=",", verbose=True):
    """Carrega um CSV com validacoes simples."""

    if not os.path.exists(caminho):
        raise FileNotFoundError(f"Arquivo nao encontrado: {caminho}")

    df = pd.read_csv(caminho, encoding=encoding, sep=sep)
    df.columns = df.columns.str.strip()

    if verbose:
        print(f"Arquivo carregado: {caminho}")
        print(f"   - Linhas: {len(df)}")
        print(f"   - Colunas: {list(df.columns)}")

    return df


def verificar_colunas(df, colunas, nome_arquivo):
    """Verifica se todas as colunas esperadas estao presentes."""

    faltantes = [col for col in colunas if col not in df.columns]
    if faltantes:
        raise ValueError(
            f"Colunas ausentes em {nome_arquivo}: {faltantes}. "
            f"Disponiveis: {list(df.columns)}"
        )


def normalizar_codigo(valor):
    """Normaliza codigo municipal."""

    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if texto.endswith(".0"):
        texto = texto[:-2]
    return texto or pd.NA


def moeda_brl_para_float(valor):
    """Converte moeda no formato brasileiro para float."""

    if pd.isna(valor):
        return pd.NA

    texto = str(valor).strip()
    if not texto:
        return pd.NA

    texto = texto.replace("R$", "").strip()
    texto = texto.replace(".", "").replace(",", ".")

    try:
        return float(texto)
    except ValueError:
        return pd.NA


def slugify_transferencia(valor):
    """Converte nome da transferencia em nome de coluna padrao."""

    texto = str(valor).strip().lower()
    texto = texto.replace("fundeb - ", "")
    texto = texto.replace("fundeb - ", "")
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    texto = texto.replace("/", " ")
    texto = texto.replace("-", " ")
    texto = re.sub(r"[^a-z0-9]+", "_", texto)
    texto = re.sub(r"_+", "_", texto).strip("_")
    return f"fundeb_{texto}"


def preparar_fundeb(df, config):
    """Filtra, limpa e pivota o arquivo Fundeb."""

    verificar_colunas(
        df,
        [
            config.fundeb_key_col,
            config.fundeb_type_col,
            config.fundeb_value_col,
            config.fundeb_year_col,
        ],
        "arquivo Fundeb",
    )

    fundeb = df.copy()
    fundeb["_codigo_merge"] = fundeb[config.fundeb_key_col].map(normalizar_codigo)
    fundeb["_valor_numerico"] = fundeb[config.fundeb_value_col].map(moeda_brl_para_float)

    if config.fundeb_year is not None:
        fundeb = fundeb[fundeb[config.fundeb_year_col] == config.fundeb_year].copy()

    fundeb["_coluna_transferencia"] = fundeb[config.fundeb_type_col].map(slugify_transferencia)

    tipos_encontrados = sorted(fundeb[config.fundeb_type_col].dropna().unique().tolist())
    print("\nTipos de transferencia encontrados no arquivo Fundeb:")
    for item in tipos_encontrados:
        print(f"   - {item}")

    pivot = fundeb.pivot_table(
        index="_codigo_merge",
        columns="_coluna_transferencia",
        values="_valor_numerico",
        aggfunc="sum",
    ).reset_index()

    pivot.columns.name = None

    print("\nPivot Fundeb concluido:")
    print(f"   - Codigos unicos no arquivo Fundeb: {pivot['_codigo_merge'].nunique()}")
    print(f"   - Colunas de transferencia geradas: {len(pivot.columns) - 1}")

    return pivot


def executar_merge(config):
    """Executa o processamento completo."""

    print("\n" + "=" * 72)
    print("PROCESSAMENTO DE TRANSFERENCIAS FUNDEB E MERGE COM BASE MUNICIPAL")
    print("=" * 72)
    print(f"Execucao: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 72 + "\n")

    base_df = carregar_csv(config.base_file, verbose=config.verbose)
    fundeb_df = carregar_csv(
        config.fundeb_file,
        encoding=config.fundeb_encoding,
        sep=config.fundeb_sep,
        verbose=config.verbose,
    )

    verificar_colunas(base_df, [config.base_key_col], "arquivo base")

    base = base_df.copy()
    base["_codigo_merge"] = base[config.base_key_col].map(normalizar_codigo)

    pivot_fundeb = preparar_fundeb(fundeb_df, config)
    resultado = base.merge(pivot_fundeb, on="_codigo_merge", how="left")

    colunas_fundeb = [col for col in pivot_fundeb.columns if col != "_codigo_merge"]
    correspondencias = resultado[colunas_fundeb].notna().any(axis=1).sum() if colunas_fundeb else 0

    print("\nMerge concluido:")
    print(f"   - Linhas da base: {len(base_df)}")
    print(f"   - Municipios com alguma transferencia Fundeb: {correspondencias}")
    print(f"   - Municipios sem correspondencia no Fundeb: {len(base_df) - correspondencias}")

    if "_codigo_merge" in resultado.columns:
        resultado = resultado.drop(columns=["_codigo_merge"])

    print(f"\nSalvando arquivo: {config.output_file}")
    resultado.to_csv(config.output_file, index=False)

    print("\nProcesso concluido com sucesso.")
    print(f"Arquivo gerado: {config.output_file}")
    return 0


def main():
    """Ponto de entrada."""

    config = MergeConfig()

    parser = argparse.ArgumentParser(
        description="Processa transferencias do Fundeb e faz merge por codigo IBGE"
    )
    parser.add_argument("--base", help="Arquivo base processado")
    parser.add_argument("--fundeb", help="Arquivo CSV de transferencias Fundeb")
    parser.add_argument("--output", help="Arquivo final de saida")
    parser.add_argument("--year", type=int, help="Ano a filtrar no arquivo Fundeb")

    args = parser.parse_args()

    if args.base:
        config.base_file = args.base
    if args.fundeb:
        config.fundeb_file = args.fundeb
    if args.output:
        config.output_file = args.output
    if args.year is not None:
        config.fundeb_year = args.year

    raise SystemExit(executar_merge(config))


if __name__ == "__main__":
    main()
