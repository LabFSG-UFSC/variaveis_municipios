# Jornada De Merges Das Variaveis Municipais

Este README centraliza a documentacao da pasta `scripts` e descreve a ordem de execucao do pipeline.

## Estrutura Do Projeto

- `../tabelas_mae/`: CSVs de entrada.
- `../tabelas_processadas/`: CSVs gerados pelos merges.
- `./`: scripts Python e esta documentacao.

## Dependencias

Todos os scripts usam Python 3 e `pandas`.

```bash
pip install pandas
```

## Visao Geral

Hoje a sequencia recomendada e:

1. usar `merge.py` quando for necessario um merge simples e reutilizavel;
2. executar `merge_utilizado_tabela9582.py` para acrescentar o total de empresas;
3. executar `merge_utilizado_7138_receita.py` para acrescentar escolarizacao e receita;
4. executar `merge_utilizado_fundeb_transferencias.py` para acrescentar as transferencias do Fundeb em colunas.

## Etapa 0: Script Generico

O arquivo `merge.py` e uma versao reutilizavel para merges simples entre dois CSVs.

Ele permite configurar:

- arquivo de origem;
- arquivo base;
- colunas-chave;
- colunas a incorporar;
- tipo de join;
- arquivo de saida.

Exemplo:

```bash
python3 merge.py \
  --file1 "arquivo_origem.csv" \
  --file2 "arquivo_base.csv" \
  --output "saida.csv" \
  --key1 "coluna_chave_origem" \
  --key2 "coluna_chave_base" \
  --how left
```

## Etapa 1: Total De Empresas

Script: `merge_utilizado_tabela9582.py`

Objetivo:

- ler `../tabelas_mae/tabela9582.csv`;
- fazer merge com `../tabelas_mae/merge_completo.csv`;
- gerar `../tabelas_processadas/merge_completo_todos_mun.csv`.

Diferencial desta etapa:

- usa `skiprows=4` porque `tabela9582.csv` tem linhas de metadados antes do cabecalho util.

Configuracao principal:

- chave em ambos os arquivos: `Cód.`
- coluna incorporada: `Total`
- tipo de join: `left`

Execucao:

```bash
python3 merge_utilizado_tabela9582.py
```

## Etapa 2: Escolarizacao E Receita

Script: `merge_utilizado_7138_receita.py`

Objetivo:

- ler `../tabelas_processadas/merge_completo_todos_mun.csv`;
- incorporar `../tabelas_mae/tabela7138.csv`;
- incorporar `../tabelas_mae/0fcf5cfb-9b3d-45b8-80c8-00d6eb180ff6.csv`;
- gerar `../tabelas_processadas/merge_completo_todos_mun_7138_receita.csv`.

Como o merge e feito:

- `tabela7138.csv` entra por codigo do municipio via `Cód.`;
- o arquivo `0fcf5...csv` entra por nome do municipio, porque nao possui codigo.

Regras especiais do merge por nome:

- normalizacao do nome do municipio;
- remocao de acentos;
- remocao do sufixo `(UF)` da base;
- restricao a UF `SC` para evitar colisoes entre municipios homonimos.

Colunas adicionadas:

- `taxa_escolarizacao_2024`
- `Valor Receita Prevista`
- `Valor Receita Realizada`

Execucao:

```bash
python3 merge_utilizado_7138_receita.py
```

## Etapa 3: Transferencias Do Fundeb

Script: `merge_utilizado_fundeb_transferencias.py`

Objetivo:

- ler `../tabelas_processadas/merge_completo_todos_mun_7138_receita.csv`;
- processar `../tabelas_mae/transferências_para_municípios.csv`;
- gerar `../tabelas_processadas/merge_completo_todos_mun_7138_receita_fundeb.csv`.

O que este script resolve:

- o arquivo do Fundeb vem em formato longo;
- cada linha representa um tipo de transferencia;
- o valor vem como moeda em formato brasileiro;
- o arquivo usa `latin1` e separador `;`.

Processamento aplicado:

1. leitura do CSV no formato correto;
2. filtro do ano de interesse;
3. conversao de `Valor Consolidado` para numero;
4. pivot das categorias de `Transferência` em colunas;
5. merge final por codigo do municipio.

Exemplos de colunas geradas:

- `fundeb_coun_vaaf`
- `fundeb_coun_vaar`
- `fundeb_coun_vaat`
- `fundeb_fpe`
- `fundeb_fpm`
- `fundeb_fti`
- `fundeb_icms`
- `fundeb_ipi_exp`
- `fundeb_ipva`
- `fundeb_itcmd`
- `fundeb_itr`

Execucao:

```bash
python3 merge_utilizado_fundeb_transferencias.py
```

## Ordem Recomendada

Se a ideia for reconstruir a base processada do zero:

1. partir de `../tabelas_mae/merge_completo.csv`;
2. rodar `merge_utilizado_tabela9582.py`;
3. rodar `merge_utilizado_7138_receita.py`;
4. rodar `merge_utilizado_fundeb_transferencias.py`.

## Saidas Da Jornada

Ao final das etapas atuais, os principais arquivos processados sao:

- `../tabelas_processadas/merge_completo_todos_mun.csv`
- `../tabelas_processadas/merge_completo_todos_mun_7138_receita.csv`
- `../tabelas_processadas/merge_completo_todos_mun_7138_receita_fundeb.csv`

## Regra Para Proximos Scripts

Este `readme.md` e o documento mestre da pasta `scripts`.

Quando um novo script de merge for criado, acrescente aqui:

1. nome do script;
2. objetivo da etapa;
3. arquivos de entrada;
4. chave de merge;
5. colunas adicionadas ou transformacoes realizadas;
6. arquivo de saida;
7. comando de execucao.

## Arquivos Relacionados

- `merge.py`
- `merge_utilizado_tabela9582.py`
- `merge_utilizado_7138_receita.py`
- `merge_utilizado_fundeb_transferencias.py`
