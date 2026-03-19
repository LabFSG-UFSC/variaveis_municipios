# variaveis_municipios

Repositorio de apoio ao processo de avaliacao em massa com merges de variaveis municipais.

## Estrutura

- `scripts/`: scripts Python do pipeline de processamento.
- `tabelas_mae/`: arquivos CSV de entrada usados como base dos merges.
- `tabelas_processadas/`: arquivos CSV gerados pelo pipeline.

## Pipeline

A sequencia recomendada para reconstruir a base processada e:

1. executar `scripts/merge_utilizado_tabela9582.py`;
2. executar `scripts/merge_utilizado_7138_receita.py`;
3. executar `scripts/merge_utilizado_fundeb_transferencias.py`.

O detalhamento de cada etapa, incluindo entradas, saidas e exemplos de execucao, esta em `scripts/readme.md`.
