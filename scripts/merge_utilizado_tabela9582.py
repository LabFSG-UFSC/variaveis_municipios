#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Versao utilizada para fazer o merge entre:
- tabela9582.csv
- merge_completo.csv

Mantem a logica mais robusta usada nesta analise, incluindo:
- skiprows para arquivos com metadados no cabecalho;
- validacao basica de configuracao;
- normalizacao de chaves;
- tratamento mais seguro de erros;
- suporte a selecao de colunas por argumento.
"""

import argparse
import os
from datetime import datetime

import pandas as pd


class MergeConfig:
    """Classe para configurar os parâmetros do merge."""

    def __init__(self):
        self.file1 = "../tabelas_mae/tabela9582.csv"
        self.file2 = "../tabelas_mae/merge_completo.csv"
        self.output_file = "../tabelas_processadas/merge_completo_todos_mun.csv"
        self.key_file1 = "Cód."
        self.key_file2 = "Cód."
        self.columns_to_merge = ["Total"]
        self.encoding = "utf-8"
        self.file1_skiprows = 4
        self.file2_skiprows = 0
        self.how = "left"
        self.drop_duplicate_key = True
        self.show_stats = True
        self.stats_column = "Total"
        self.verbose = True


def carregar_arquivo(caminho, encoding, skiprows=0, verbose=True):
    """Carrega um arquivo CSV com verificação de existência."""

    if not os.path.exists(caminho):
        print(f"Erro: arquivo '{caminho}' nao encontrado.")
        return None

    try:
        df = pd.read_csv(caminho, encoding=encoding, skiprows=skiprows)
        df.columns = df.columns.str.strip()
        if verbose:
            print(f"Arquivo carregado: {caminho}")
            if skiprows:
                print(f"   - Linhas ignoradas no inicio: {skiprows}")
            print(f"   - Linhas: {len(df)}")
            print(f"   - Colunas: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"Erro ao ler arquivo '{caminho}': {e}")
        return None


def verificar_colunas(df, coluna, nome_arquivo):
    """Verifica se uma coluna existe no DataFrame."""

    if coluna not in df.columns:
        print(f"Erro: coluna '{coluna}' nao encontrada em {nome_arquivo}.")
        print(f"   Colunas disponiveis: {list(df.columns)}")
        return False
    return True


def normalizar_chave_serie(serie):
    """Normaliza valores de chave para reduzir falhas por espacos extras."""

    serie_normalizada = serie.astype("string").str.strip()
    return serie_normalizada.mask(serie_normalizada.eq(""), pd.NA)


def validar_configuracao(config):
    """Valida parâmetros básicos antes da execução."""

    if not config.file1 or not config.file2 or not config.output_file:
        print("Erro: file1, file2 e output_file devem ser informados.")
        return False

    if not config.key_file1 or not config.key_file2:
        print("Erro: as colunas-chave key_file1 e key_file2 devem ser informadas.")
        return False

    if config.columns_to_merge is not None and not isinstance(config.columns_to_merge, list):
        print("Erro: columns_to_merge deve ser uma lista de colunas ou None.")
        return False

    if config.file1_skiprows < 0 or config.file2_skiprows < 0:
        print("Erro: os valores de skiprows devem ser maiores ou iguais a zero.")
        return False

    return True


def exibir_estatisticas(df_resultado, stats_col):
    """Exibe estatísticas numéricas da coluna configurada."""

    serie_numerica = pd.to_numeric(df_resultado[stats_col], errors="coerce").dropna()

    if serie_numerica.empty:
        print(f"\nAviso: a coluna '{stats_col}' nao possui valores numericos validos para estatistica.")
        return

    print("\n" + "=" * 50)
    print(f"ESTATISTICAS - {stats_col}")
    print("=" * 50)
    print(f"Soma:     R$ {serie_numerica.sum():,.2f}")
    print(f"Media:    R$ {serie_numerica.mean():,.2f}")
    print(f"Mediana:  R$ {serie_numerica.median():,.2f}")
    print(f"Maximo:   R$ {serie_numerica.max():,.2f}")
    print(f"Minimo:   R$ {serie_numerica.min():,.2f}")
    print(f"Desvio padrao: R$ {serie_numerica.std():,.2f}")


def fazer_merge(config):
    """Função principal que faz o merge dos arquivos."""

    if not validar_configuracao(config):
        return 1

    print("\n" + "=" * 60)
    print("SCRIPT DE MERGE DE ARQUIVOS CSV")
    print("=" * 60)
    print(f"Execucao: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60 + "\n")

    print("Carregando arquivos...")
    df1 = carregar_arquivo(
        config.file1,
        config.encoding,
        skiprows=config.file1_skiprows,
        verbose=config.verbose,
    )
    if df1 is None:
        return 1

    df2 = carregar_arquivo(
        config.file2,
        config.encoding,
        skiprows=config.file2_skiprows,
        verbose=config.verbose,
    )
    if df2 is None:
        return 1

    print("\n" + "-" * 60)

    if not verificar_colunas(df1, config.key_file1, "arquivo 1"):
        return 1

    if not verificar_colunas(df2, config.key_file2, "arquivo 2"):
        return 1

    df1 = df1.copy()
    df2 = df2.copy()
    df1[config.key_file1] = normalizar_chave_serie(df1[config.key_file1])
    df2[config.key_file2] = normalizar_chave_serie(df2[config.key_file2])

    chaves_vazias_file1 = df1[config.key_file1].isna().sum()
    chaves_vazias_file2 = df2[config.key_file2].isna().sum()
    if chaves_vazias_file1:
        print(f"Aviso: {chaves_vazias_file1} registro(s) com chave vazia no arquivo 1.")
    if chaves_vazias_file2:
        print(f"Aviso: {chaves_vazias_file2} registro(s) com chave vazia no arquivo 2.")

    if config.columns_to_merge is not None:
        colunas_validas = []
        for col in config.columns_to_merge:
            if col in df1.columns:
                colunas_validas.append(col)
            else:
                print(f"Aviso: coluna '{col}' nao encontrada no arquivo 1. Coluna ignorada.")

        if config.key_file1 not in colunas_validas:
            colunas_validas.insert(0, config.key_file1)

        df1_clean = df1[colunas_validas].copy()
        print(f"\nColunas selecionadas do arquivo 1: {colunas_validas}")
    else:
        df1_clean = df1.copy()
        print("\nUsando todas as colunas do arquivo 1")

    duplicated_keys = df1_clean[config.key_file1].dropna().duplicated().sum()
    if duplicated_keys:
        print(
            f"Aviso: o arquivo 1 possui {duplicated_keys} chave(s) duplicada(s). "
            "Isso pode multiplicar linhas no resultado."
        )

    colunas_incorporadas = [col for col in df1_clean.columns if col != config.key_file1]
    coluna_referencia_match = colunas_incorporadas[0] if colunas_incorporadas else config.key_file1

    print(f"Chave de merge: '{config.key_file1}' (arquivo 1) x '{config.key_file2}' (arquivo 2)")
    print(f"Tipo de join: {config.how.upper()}")

    print("\nExecutando merge...")
    try:
        df_resultado = pd.merge(
            df2,
            df1_clean,
            left_on=config.key_file2,
            right_on=config.key_file1,
            how=config.how,
            suffixes=("", "_duplicada"),
        )
    except Exception as e:
        print(f"Erro ao executar o merge: {e}")
        return 1

    if (
        config.drop_duplicate_key
        and config.key_file1 != config.key_file2
        and config.key_file1 in df_resultado.columns
    ):
        df_resultado = df_resultado.drop(columns=[config.key_file1])

    total_linhas = len(df_resultado)
    correspondencias = df_resultado[coluna_referencia_match].notna().sum()

    print("\nResultado do merge:")
    print(f"   - Total de linhas: {total_linhas}")
    print(f"   - Correspondencias encontradas: {correspondencias}")
    print(f"   - Sem correspondencia: {total_linhas - correspondencias}")

    if correspondencias < total_linhas:
        coluna_chave = config.key_file2
        sem_corresp = df_resultado[df_resultado[coluna_referencia_match].isna()][coluna_chave].tolist()

        print(f"\nRegistros sem correspondencia ({len(sem_corresp)}):")
        for item in sem_corresp[:15]:
            print(f"   - {item}")
        if len(sem_corresp) > 15:
            print(f"   ... e mais {len(sem_corresp) - 15}")

    if config.show_stats:
        if config.stats_column in df_resultado.columns:
            exibir_estatisticas(df_resultado, config.stats_column)
        else:
            print(f"\nAviso: coluna de estatistica '{config.stats_column}' nao encontrada no resultado.")

    print(f"\nSalvando arquivo: {config.output_file}")
    try:
        df_resultado.to_csv(config.output_file, index=False, encoding=config.encoding)
    except Exception as e:
        print(f"Erro ao salvar arquivo '{config.output_file}': {e}")
        return 1

    print("\n" + "=" * 60)
    print("PROCESSO CONCLUIDO COM SUCESSO")
    print("=" * 60)
    return 0


def main():
    """Função principal com suporte a argumentos de linha de comando."""

    config = MergeConfig()

    parser = argparse.ArgumentParser(description="Script utilizado para o merge da tabela 9582")
    parser.add_argument("--file1", help="Primeiro arquivo (dados a incorporar)")
    parser.add_argument("--file2", help="Segundo arquivo (base principal)")
    parser.add_argument("--output", help="Arquivo de saída")
    parser.add_argument("--key1", help="Coluna chave no primeiro arquivo")
    parser.add_argument("--key2", help="Coluna chave no segundo arquivo")
    parser.add_argument("--skiprows1", type=int, help="Quantidade de linhas para ignorar no inicio do arquivo 1")
    parser.add_argument("--skiprows2", type=int, help="Quantidade de linhas para ignorar no inicio do arquivo 2")
    parser.add_argument("--columns1", help="Lista de colunas do arquivo 1 separadas por virgula")
    parser.add_argument("--stats-column", help="Coluna do resultado usada para estatisticas")
    parser.add_argument("--how", choices=["left", "right", "inner", "outer"], help="Tipo de join")

    args = parser.parse_args()

    if args.file1:
        config.file1 = args.file1
    if args.file2:
        config.file2 = args.file2
    if args.output:
        config.output_file = args.output
    if args.key1:
        config.key_file1 = args.key1
    if args.key2:
        config.key_file2 = args.key2
    if args.skiprows1 is not None:
        config.file1_skiprows = args.skiprows1
    if args.skiprows2 is not None:
        config.file2_skiprows = args.skiprows2
    if args.columns1:
        config.columns_to_merge = [col.strip() for col in args.columns1.split(",") if col.strip()]
    if args.stats_column:
        config.stats_column = args.stats_column
    if args.how:
        config.how = args.how

    raise SystemExit(fazer_merge(config))


if __name__ == "__main__":
    main()
