# variaveis_municipios

Repositorio para organizacao, processamento e consolidacao de variaveis municipais usadas no contexto de avaliacao em massa.

O projeto estrutura os dados em camadas:

- `bronze/`: arquivos de origem e bases brutas;
- `prata/`: arquivos processados, pre-merges e versoes consolidadas do merge municipal;
- `ouro/`: camada reservada para produtos finais e derivados de maior valor analitico;
- `documentacao/`: scripts, regras de processamento e documentacao operacional do pipeline.

O objetivo central do repositorio e transformar diferentes fontes municipais em uma base consolidada, versionada por etapas de merge, com rastreabilidade das variaveis incorporadas ao longo do processo.

Entre as fontes trabalhadas no projeto estao bases do IBGE, receitas municipais, transferencias do Fundeb, SINISA, seguranca publica, homicidios municipais e arquivos da REGIC.

## Como Navegar

- Para entender o fluxo completo de processamento, consulte [`documentacao/readme.md`](documentacao/readme.md).
- Para executar ou revisar os scripts do pipeline, consulte `documentacao/scripts/`.
- Para inspecionar os dados de entrada e saida, use as pastas `bronze/` e `prata/`.

## Resultado Esperado

Ao final do pipeline, o repositorio produz tabelas municipais enriquecidas por sucessivas etapas de merge, permitindo analise comparativa entre municipios com um historico claro de quais variaveis foram adicionadas em cada versao.
