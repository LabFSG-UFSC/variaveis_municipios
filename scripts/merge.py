#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script genérico para fazer merge de dois arquivos CSV baseado em colunas-chave.
Você pode reutilizá-lo apenas alterando os parâmetros de configuração.
"""

import pandas as pd
import os
import argparse
from datetime import datetime

class MergeConfig:
    """Classe para configurar os parâmetros do merge"""
    
    def __init__(self):
        # CONFIGURAÇÕES PRINCIPAIS - ALTERE AQUI CONFORME NECESSÁRIO
        
        # Arquivos de entrada e saída
        self.file1 = "pib_regioes - apenas_regioes.csv"        # Primeiro arquivo (dados a serem incorporados)
        self.file2 = "resumo_ufs - TODAS_UFs.csv"              # Segundo arquivo (base principal)
        self.output_file = "resultado_merge.csv"                # Arquivo de saída
        
        # Colunas para merge
        self.key_file1 = "região imediata"                      # Coluna-chave no primeiro arquivo
        self.key_file2 = "Região Imediata (RGI)"                # Coluna-chave no segundo arquivo
        
        # Colunas a serem incorporadas do primeiro arquivo
        # Use None para pegar todas as colunas (exceto a chave)
        self.columns_to_merge = [
            'codigo_rgi', 'pib_total_rgi', 'pib_agricola_rgi', 
            'pib_industria_rgi', 'pib_servicos_rgi'
        ]  # Colunas específicas ou None para todas
        
        # Configurações adicionais
        self.encoding = 'utf-8'                                  # Encoding dos arquivos
        self.how = 'left'                                         # Tipo de join: 'left', 'right', 'inner', 'outer'
        self.drop_duplicate_key = True                            # Remove coluna chave duplicada do resultado
        
        # Estatísticas
        self.show_stats = True                                    # Mostrar estatísticas básicas
        self.stats_column = 'pib_total_rgi'                       # Coluna para estatísticas (se show_stats=True)
        
        # Debug
        self.verbose = True                                       # Mostrar informações detalhadas

def carregar_arquivo(caminho, encoding, verbose=True):
    """Carrega um arquivo CSV com verificação de existência"""
    
    if not os.path.exists(caminho):
        print(f"❌ Erro: Arquivo '{caminho}' não encontrado!")
        return None
    
    try:
        df = pd.read_csv(caminho, encoding=encoding)
        if verbose:
            print(f"✅ Arquivo carregado: {caminho}")
            print(f"   - Linhas: {len(df)}")
            print(f"   - Colunas: {list(df.columns)}")
        return df
    except Exception as e:
        print(f"❌ Erro ao ler arquivo '{caminho}': {e}")
        return None

def verificar_colunas(df, coluna, nome_arquivo):
    """Verifica se uma coluna existe no DataFrame"""
    
    if coluna not in df.columns:
        print(f"❌ Erro: Coluna '{coluna}' não encontrada em {nome_arquivo}")
        print(f"   Colunas disponíveis: {list(df.columns)}")
        return False
    return True

def fazer_merge(config):
    """Função principal que faz o merge dos arquivos"""
    
    print("\n" + "="*60)
    print("🔗 SCRIPT DE MERGE DE ARQUIVOS CSV")
    print("="*60)
    print(f"📅 Execução: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("="*60 + "\n")
    
    # Carrega os arquivos
    print("📂 Carregando arquivos...")
    df1 = carregar_arquivo(config.file1, config.encoding, config.verbose)
    if df1 is None:
        return
    
    df2 = carregar_arquivo(config.file2, config.encoding, config.verbose)
    if df2 is None:
        return
    
    print("\n" + "-"*60)
    
    # Verifica colunas-chave
    if not verificar_colunas(df1, config.key_file1, "arquivo 1"):
        return
    
    if not verificar_colunas(df2, config.key_file2, "arquivo 2"):
        return
    
    # Prepara o primeiro DataFrame para merge
    if config.columns_to_merge is not None:
        # Verifica se as colunas especificadas existem
        colunas_validas = []
        for col in config.columns_to_merge:
            if col in df1.columns:
                colunas_validas.append(col)
            else:
                print(f"⚠️  Aviso: Coluna '{col}' não encontrada no arquivo 1 - ignorada")
        
        # Adiciona a coluna chave se não estiver incluída
        if config.key_file1 not in colunas_validas:
            colunas_validas.insert(0, config.key_file1)
        
        df1_clean = df1[colunas_validas].copy()
        print(f"\n📋 Colunas selecionadas do arquivo 1: {colunas_validas}")
    else:
        df1_clean = df1.copy()
        print(f"\n📋 Usando todas as colunas do arquivo 1")
    
    print(f"🔑 Chave de merge: '{config.key_file1}' (arquivo 1) x '{config.key_file2}' (arquivo 2)")
    print(f"🔄 Tipo de join: {config.how.upper()}")
    
    # Faz o merge
    print("\n🔄 Executando merge...")
    df_resultado = pd.merge(
        df2,
        df1_clean,
        left_on=config.key_file2,
        right_on=config.key_file1,
        how=config.how,
        suffixes=('', '_duplicada')
    )
    
    # Remove coluna chave duplicada se configurado
    if config.drop_duplicate_key and config.key_file1 in df_resultado.columns:
        df_resultado = df_resultado.drop(columns=[config.key_file1])
    
    # Estatísticas do merge
    total_linhas = len(df_resultado)
    correspondencias = df_resultado[df_resultado.columns[-1]].notna().sum()  # Última coluna como referência
    
    print(f"\n📊 Resultado do merge:")
    print(f"   - Total de linhas: {total_linhas}")
    print(f"   - Correspondências encontradas: {correspondencias}")
    print(f"   - Sem correspondência: {total_linhas - correspondencias}")
    
    # Lista registros sem correspondência
    if correspondencias < total_linhas:
        coluna_chave = config.key_file2
        sem_corresp = df_resultado[df_resultado[df1_clean.columns[-1]].isna()][coluna_chave].tolist()
        
        print(f"\n⚠️  Registros sem correspondência ({len(sem_corresp)}):")
        for item in sem_corresp[:15]:  # Mostra até 15
            print(f"   - {item}")
        if len(sem_corresp) > 15:
            print(f"   ... e mais {len(sem_corresp) - 15}")
    
    # Estatísticas básicas (se configurado)
    if config.show_stats and config.stats_column in df_resultado.columns:
        stats_col = config.stats_column
        if df_resultado[stats_col].notna().any():
            print("\n" + "="*50)
            print(f"📈 ESTATÍSTICAS - {stats_col}")
            print("="*50)
            print(f"Soma:     R$ {df_resultado[stats_col].sum():,.2f}")
            print(f"Média:    R$ {df_resultado[stats_col].mean():,.2f}")
            print(f"Mediana:  R$ {df_resultado[stats_col].median():,.2f}")
            print(f"Máximo:   R$ {df_resultado[stats_col].max():,.2f}")
            print(f"Mínimo:   R$ {df_resultado[stats_col].min():,.2f}")
            print(f"Desvio padrão: R$ {df_resultado[stats_col].std():,.2f}")
    
    # Salva o resultado
    print(f"\n💾 Salvando arquivo: {config.output_file}")
    df_resultado.to_csv(config.output_file, index=False, encoding=config.encoding)
    
    print("\n" + "="*60)
    print("✅ PROCESSO CONCLUÍDO COM SUCESSO!")
    print("="*60)

def main():
    """Função principal com suporte a argumentos de linha de comando"""
    
    # Configuração padrão
    config = MergeConfig()
    
    # Suporte a argumentos de linha de comando (opcional)
    parser = argparse.ArgumentParser(description='Script genérico para merge de CSVs')
    parser.add_argument('--file1', help='Primeiro arquivo (dados a incorporar)')
    parser.add_argument('--file2', help='Segundo arquivo (base principal)')
    parser.add_argument('--output', help='Arquivo de saída')
    parser.add_argument('--key1', help='Coluna chave no primeiro arquivo')
    parser.add_argument('--key2', help='Coluna chave no segundo arquivo')
    parser.add_argument('--how', choices=['left', 'right', 'inner', 'outer'], 
                       help='Tipo de join')
    
    args = parser.parse_args()
    
    # Sobrescreve configurações com argumentos de linha de comando se fornecidos
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
    if args.how:
        config.how = args.how
    
    # Executa o merge
    fazer_merge(config)

if __name__ == "__main__":
    main()
