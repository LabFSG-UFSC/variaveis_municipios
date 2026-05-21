# Novo Fuzzy Das Capitais Intermediarias

## Objetivo

Esta pasta reune apenas os arquivos necessarios para reproduzir o novo fuzzy
reduzido das capitais de regioes intermediarias, com menos variaveis explicativas.

## Estrutura

- `inputs/merge_v28_fuzzy_subset.csv`
  - recorte da `merge_v28` contendo somente as colunas usadas no fuzzy
- `inputs/capitais_intermediarias_nao_uf.csv`
  - lista dos 119 municipios do universo analisado
- `inputs/capitais_intermediarias_geometrias.gpkg`
  - geometrias apenas dos 119 municipios, para gerar geopackages por classe
- `scripts/classifica_capitais_intermediarias_fuzzy_pacote.py`
  - roda a classificacao fuzzy reduzida
- `scripts/valida_modelo_fuzzy_pacote.py`
  - executa a validacao formal do modelo reduzido
- `scripts/gera_gpkg_capitais_intermediarias_fuzzy_pacote.py`
  - gera um geopackage para cada classe fuzzy
- `outputs/`
  - pasta de saida dos resultados

## O Que Cada Script Faz

### `scripts/classifica_capitais_intermediarias_fuzzy_pacote.py`

Funcao:

- executa o processamento principal do novo fuzzy reduzido das capitais de regioes intermediarias

Entradas:

- `inputs/merge_v28_fuzzy_subset.csv`
- `inputs/capitais_intermediarias_nao_uf.csv`

O que o script faz:

- filtra a base municipal para manter apenas os 119 municipios do universo analisado
- mantem um conjunto reduzido de variaveis explicativas:
  - `empresas_1k`
  - `regic_var60`
  - `via_pav_pct`
  - `indice_conectividade`
  - `estab_saude_10k`
  - `homicidios_100k`
- calcula `percent_rank` para os indicadores comparaveis
- agrega os indicadores em 5 eixos simplificados:
  - `dinamismo_economico`
  - `infraestrutura_urbana`
  - `conectividade_digital`
  - `oferta_servicos`
  - `seguranca_territorial`
- classifica os municipios em `muito_alto`, `alto`, `medio`, `baixo` e `muito_baixo`
- calcula tambem `score_final`, `ranking_final` e `confianca_classificacao`

Saidas:

- `outputs/classificacao_capitais_intermediarias_fuzzy.csv`
- `outputs/classificacao_capitais_intermediarias_fuzzy_resumo.csv`

## Explicacao Completa Do Calculo

O novo fuzzy foi montado para reduzir redundancia entre variaveis e manter um
modelo mais facil de interpretar. Em vez de usar muitos indicadores muito
parecidos entre si, ele trabalha com um conjunto menor de variaveis que
representam dimensoes diferentes da centralidade urbana.

## Fluxograma Do Processamento

```text
+-------------------------------------------------------------+
| 1. LEITURA DOS INSUMOS                                      |
| - inputs/merge_v28_fuzzy_subset.csv                         |
| - inputs/capitais_intermediarias_nao_uf.csv                 |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 2. DEFINICAO DO UNIVERSO                                    |
| - cruzar a base municipal com a lista de capitais           |
| - manter apenas os 119 municipios analisados                |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 3. VARIAVEIS BRUTAS                                         |
| - pop_total                                                 |
| - empresas_total                                            |
| - estab_total                                               |
| - vitimas_homicidio_2022                                    |
| - via_pav_pct                                               |
| - regic_var60                                               |
| - indice_conectividade                                      |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 4. INDICADORES DERIVADOS                                    |
| - empresas_1k = empresas_total / pop_total * 1000           |
| - estab_saude_10k = estab_total / pop_total * 10000         |
| - homicidios_100k = vitimas_homicidio_2022 / pop_total      |
|                      * 100000                               |
| - limitar via_pav_pct a 0-100                              |
| - limitar indice_conectividade a 0-100                     |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 5. PADRONIZACAO DOS INDICADORES                             |
| - aplicar percent_rank nas variaveis positivas              |
| - aplicar percent_rank invertido em homicidios_100k         |
| - gerar scores comparaveis entre os 119 municipios          |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 6. SCORES USADOS NO MODELO                                  |
| - score_empresas_1k                                         |
| - score_regic_var60                                         |
| - score_via_pav_pct                                         |
| - score_indice_conectividade                                |
| - score_estab_saude_10k                                     |
| - score_homicidios_100k                                     |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 7. EIXOS SINTETICOS                                         |
| - dinamismo_economico                                       |
| - infraestrutura_urbana                                     |
| - conectividade_digital                                     |
| - oferta_servicos                                           |
| - seguranca_territorial                                     |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 8. SCORE FINAL                                              |
| - media ponderada dos 5 eixos                              |
| - gera um valor continuo para cada municipio                |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 9. TRANSFORMACAO FUZZY                                      |
| - calcular q20, q35, q50, q65, q80 do score_final          |
| - gerar pertinencias:                                       |
|   muito_baixo, baixo, medio, alto, muito_alto              |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 10. CLASSIFICACAO FINAL                                     |
| - escolher a maior pertinencia                              |
| - definir classificacao_fuzzy                               |
| - calcular confianca_classificacao                          |
| - calcular ranking_final                                    |
+-------------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------------+
| 11. SAIDAS                                                  |
| - classificacao_capitais_intermediarias_fuzzy.csv           |
| - classificacao_capitais_intermediarias_fuzzy_resumo.csv    |
| - geopackages por classe fuzzy                              |
+-------------------------------------------------------------+
```

Em termos conceituais, o processo inteiro pode ser resumido assim:

`base bruta -> indicadores derivados -> scores comparaveis -> eixos sinteticos -> score final -> pertinencias fuzzy -> classe final`

Isso significa que o script nao classifica os municipios diretamente a partir
das colunas originais. Primeiro ele transforma os dados em medidas comparaveis,
depois resume essas medidas em poucos eixos analiticos e, por fim, converte o
resultado final continuo em categorias fuzzy.

### 1. Universo Analisado

O script nao classifica todos os municipios do Brasil.

Ele:

- le a base municipal em `inputs/merge_v28_fuzzy_subset.csv`
- le a lista de capitais intermediarias em `inputs/capitais_intermediarias_nao_uf.csv`
- faz um `merge` pela coluna `cod_mun`
- mantem apenas os 119 municipios presentes na lista de capitais intermediarias

Se algum municipio esperado nao estiver na base, o script gera erro.

### 2. Variaveis Brutas De Entrada

As colunas lidas da base estao em `COLUNAS_BASE`:

- `cod_mun`: codigo identificador do municipio
- `municipio`: nome do municipio
- `pop_total`: populacao total do municipio
- `empresas_total`: total de empresas no municipio
- `estab_total`: total de estabelecimentos de saude no municipio
- `vitimas_homicidio_2022`: numero de vitimas de homicidio registrado em 2022
- `via_pav_pct`: percentual de vias urbanas pavimentadas
- `regic_var60`: indicador da REGIC usado como proxy de centralidade funcional
- `indice_conectividade`: indicador sintetico de conectividade digital

Nem todas entram diretamente na classificacao. Parte delas serve para construir
indicadores derivados.

### 3. Indicadores Derivados

Depois de filtrar os municipios, o script cria tres indicadores derivados:

- `empresas_1k`
  - formula: `empresas_total / pop_total * 1000`
  - interpreta quantas empresas existem por mil habitantes

- `estab_saude_10k`
  - formula: `estab_total / pop_total * 10000`
  - interpreta quantos estabelecimentos de saude existem por dez mil habitantes

- `homicidios_100k`
  - formula: `vitimas_homicidio_2022 / pop_total * 100000`
  - interpreta a taxa de homicidios por cem mil habitantes

O script tambem padroniza:

- `via_pav_pct`
  - limitada ao intervalo de `0` a `100`

- `indice_conectividade`
  - limitado ao intervalo de `0` a `100`

As outras duas variaveis usadas diretamente sem derivacao sao:

- `regic_var60`
- `via_pav_pct`
- `indice_conectividade`

No fim, o fuzzy usa 6 variaveis explicativas:

- `empresas_1k`
- `regic_var60`
- `via_pav_pct`
- `indice_conectividade`
- `estab_saude_10k`
- `homicidios_100k`

### 4. Padronizacao Por Percent Rank

Como as variaveis estao em escalas diferentes, o script transforma cada uma em
um `percent_rank`.

Isso significa que cada municipio passa a receber uma posicao relativa entre os
119 municipios do universo analisado.

Exemplo:

- um municipio com `percent_rank = 0.90` em `empresas_1k` esta entre os maiores valores dessa variavel
- um municipio com `percent_rank = 0.10` esta entre os menores

O script faz isso assim:

- para variaveis em que valor alto e melhor:
  - usa `ascending=True`

- para variaveis em que valor alto e pior:
  - usa `ascending=False`

Variaveis tratadas como positivas:

- `empresas_1k`
- `regic_var60`
- `via_pav_pct`
- `indice_conectividade`
- `estab_saude_10k`

Variavel tratada como negativa:

- `homicidios_100k`

Isso faz com que, mesmo para homicidios, um score alto represente situacao
relativamente melhor no universo comparado.

Essa etapa e importante porque impede que uma variavel com numeros muito altos
ou com unidade diferente domine o calculo apenas por escala. Com o
`percent_rank`, todas passam a ser lidas como posicao relativa no conjunto dos
119 municipios.

### 5. Eixos Do Modelo

Depois da padronizacao, os scores sao agrupados em 5 eixos.

Cada eixo representa uma dimensao sintetica do municipio:

#### `dinamismo_economico`

Combina:

- `empresas_1k` com peso `0.55`
- `regic_var60` com peso `0.45`

Interpretacao:

- mede presenca de atividade economica e relevancia funcional ligada a REGIC

#### `infraestrutura_urbana`

Combina:

- `via_pav_pct` com peso `1.00`

Interpretacao:

- mede condicao basica de infraestrutura urbana fisica

#### `conectividade_digital`

Combina:

- `indice_conectividade` com peso `1.00`

Interpretacao:

- mede insercao digital e conectividade relativa

#### `oferta_servicos`

Combina:

- `estab_saude_10k` com peso `1.00`

Interpretacao:

- mede oferta relativa de servicos de saude

#### `seguranca_territorial`

Combina:

- `homicidios_100k` com peso `1.00`

Interpretacao:

- mede condicao territorial a partir da seguranca publica relativa

### 6. Como Cada Eixo E Calculado

Cada eixo e uma media ponderada dos scores que o compoem.

Formula geral:

`eixo = soma(score_variavel * peso_variavel) / soma(pesos_validos)`

O calculo usa apenas os pesos das variaveis que tiverem valor valido. Se alguma
variavel estiver ausente para um municipio, o peso dela nao entra no divisor.

Na pratica, isso torna o calculo mais robusto, porque evita que um valor ausente
zere artificialmente o eixo inteiro.

### 7. Score Final

Depois de calcular os 5 eixos, o script gera um `score_final`.

Pesos dos eixos:

- `dinamismo_economico`: `0.30`
- `infraestrutura_urbana`: `0.15`
- `conectividade_digital`: `0.20`
- `oferta_servicos`: `0.20`
- `seguranca_territorial`: `0.15`

Formula:

`score_final = soma(eixo * peso_do_eixo) / soma(pesos_validos)`

Esse `score_final` e o valor continuo principal do modelo.

Ele pode ser interpretado como uma sintese da posicao relativa do municipio no
conjunto das capitais intermediarias analisadas, ponderando simultaneamente
economia, infraestrutura, conectividade, servicos e seguranca territorial.

### 8. Transformacao Fuzzy Do Score Final

O script nao usa regras fuzzy complexas entre varios eixos como no modelo
original. Aqui a logica fuzzy foi simplificada.

Ele pega o `score_final` e calcula graus de pertinencia para 5 classes:

- `muito_baixo`
- `baixo`
- `medio`
- `alto`
- `muito_alto`

Essas pertinencias sao definidas por quantis da distribuicao observada do
`score_final`:

- `q20`
- `q35`
- `q50`
- `q65`
- `q80`

Ou seja, os limites nao sao fixos de antemao. Eles se ajustam ao conjunto dos
119 municipios.

### 9. Funcoes De Pertinencia

As classes sao calculadas com funcoes triangulares e trapezoidais.

Em termos intuitivos:

- `muito_baixo`
  - domina os menores `score_final`
  - perde forca entre `q20` e `q35`

- `baixo`
  - cresce de `q20` ate `q35`
  - atinge pico nessa faixa
  - cai de `q35` ate `q50`

- `medio`
  - cresce de `q35` ate `q50`
  - cai de `q50` ate `q65`

- `alto`
  - cresce de `q50` ate `q65`
  - cai de `q65` ate `q80`

- `muito_alto`
  - cresce de `q65` ate `q80`
  - domina acima de `q80`

Assim, cada municipio recebe cinco valores de pertinencia entre `0` e `1`.

Um ponto importante: um municipio pode ter pertinencia parcial em mais de uma
classe ao mesmo tempo. Isso e a parte propriamente fuzzy do metodo. Em vez de
uma fronteira rigida, o modelo admite zonas de transicao entre categorias.

### 10. Classe Final

A classificacao final e escolhida pela maior pertinencia:

- `classificacao_fuzzy = argmax(pert_muito_baixo, pert_baixo, pert_medio, pert_alto, pert_muito_alto)`

Tambem e gerada:

- `confianca_classificacao`
  - igual ao maior valor de pertinencia entre as 5 classes

Quanto maior essa confianca, mais claramente o municipio pertence a uma
categoria, sem ficar perto da fronteira entre duas classes.

### 11. Ranking Final

O script tambem cria:

- `ranking_final`

Esse ranking ordena os municipios por `score_final` em ordem decrescente.

Ou seja:

- `ranking_final = 1` corresponde ao maior `score_final`
- valores maiores de ranking correspondem a posicoes piores no ordenamento

### 12. O Que Vai Para O CSV Final

O arquivo `outputs/classificacao_capitais_intermediarias_fuzzy.csv` salva:

- identificacao do municipio
- classe fuzzy final
- confianca da classificacao
- `score_final`
- os 5 eixos sinteticos
- as principais variaveis que alimentaram o modelo

Essas colunas permitem:

- auditar o resultado
- comparar municipios
- entender por que um municipio ficou acima ou abaixo de outro

### 13. Resumo Por Classe

O arquivo `outputs/classificacao_capitais_intermediarias_fuzzy_resumo.csv`
agrega os resultados por categoria fuzzy e traz:

- quantidade de municipios por classe
- media do `score_final`
- media da `confianca_classificacao`

### 14. Logica Substantiva Do Modelo

Em termos conceituais, o modelo reduzido tenta responder:

- quais capitais intermediarias combinam dinamismo economico, infraestrutura,
  conectividade, oferta de servicos e melhor situacao territorial?

Como ele usa menos variaveis, o resultado fica:

- mais simples de interpretar
- menos dependente de blocos repetitivos de indicadores
- mais facil de explicar e revisar
- menos parecido com um sistema de regras muito complexo

O custo dessa simplificacao e que ele perde parte do detalhamento do modelo
original, mas ganha em legibilidade e transparencia.

## Passo A Passo Do Algoritmo

Esta secao descreve o algoritmo exatamente na ordem em que o script executa.

### Etapa 1. Ler os argumentos e definir caminhos

O script comeca carregando:

- caminho da base municipal
- caminho da lista de capitais intermediarias
- caminho do CSV final
- caminho do CSV de resumo

Se o usuario nao informar nada por linha de comando, ele usa os caminhos padrao
da propria pasta.

### Etapa 2. Ler a lista de capitais intermediarias

Funcao usada:

- `carrega_capitais`

O que acontece:

- le o arquivo CSV das capitais
- converte `cod_mun` para numero inteiro padronizado
- mantem apenas:
  - `cod_mun`
  - `municipio`
  - `uf`
- remove duplicatas

Objetivo:

- garantir o universo de referencia do modelo

### Etapa 3. Ler a base municipal

Funcao usada:

- `carrega_base`

O que acontece:

- le apenas as colunas listadas em `COLUNAS_BASE`
- converte `cod_mun` para inteiro padronizado
- converte as demais colunas para formato numerico

Objetivo:

- preparar a base com os campos necessarios para o calculo

### Etapa 4. Cruzar a base com o universo de municipios

Funcao usada:

- `prepara_base_capitais`

O que acontece:

- faz o `merge` da base municipal com a lista de capitais usando `cod_mun`
- mantem apenas os municipios presentes no universo analisado
- confere se o numero final de linhas bate com a lista esperada

Se nao bater:

- o script interrompe a execucao com erro

Objetivo:

- assegurar que nenhum municipio esperado ficou de fora

### Etapa 5. Construir os indicadores derivados

Funcao usada:

- `calcula_indicadores`

O que acontece:

- calcula `empresas_1k`
- calcula `estab_saude_10k`
- calcula `homicidios_100k`
- limita `via_pav_pct` ao intervalo `0-100`
- limita `indice_conectividade` ao intervalo `0-100`

Objetivo:

- transformar totais absolutos em medidas comparaveis por populacao
- evitar distorcoes por valores fora de faixa

### Etapa 6. Transformar variaveis em scores comparaveis

Funcao usada:

- `calcula_scores`

O que acontece:

- aplica `percent_rank` nas variaveis positivas
- aplica `percent_rank` invertido na variavel negativa
- cria colunas do tipo:
  - `score_empresas_1k`
  - `score_regic_var60`
  - `score_via_pav_pct`
  - `score_indice_conectividade`
  - `score_estab_saude_10k`
  - `score_homicidios_100k`

Objetivo:

- colocar todas as variaveis na mesma logica comparativa

### Etapa 7. Calcular os eixos sinteticos

Funcao usada:

- `calcula_eixos`

O que acontece:

- monta um pequeno conjunto de scores para cada eixo
- aplica a funcao `weighted_mean`
- gera as colunas:
  - `dinamismo_economico`
  - `infraestrutura_urbana`
  - `conectividade_digital`
  - `oferta_servicos`
  - `seguranca_territorial`

Objetivo:

- resumir o comportamento do municipio em grandes dimensoes analiticas

### Etapa 8. Calcular o score final

Funcao usada:

- `classifica_fuzzy`

Primeiro passo dentro dela:

- combinar os 5 eixos em um `score_final`

O que acontece:

- aplica uma nova media ponderada usando `PESOS_CLASSIFICACAO`

Objetivo:

- sintetizar o desempenho geral do municipio em um unico numero continuo

### Etapa 9. Calcular os quantis do score final

Funcao usada:

- `pertinencia_quantis`

O que acontece:

- calcula os pontos de corte `q20`, `q35`, `q50`, `q65` e `q80`

Objetivo:

- definir os limites das curvas fuzzy com base na distribuicao real dos dados

### Etapa 10. Calcular as pertinencias fuzzy

Funcao usada:

- `pertinencia_quantis`

O que acontece:

- para cada municipio, calcula:
  - `pert_muito_baixo`
  - `pert_baixo`
  - `pert_medio`
  - `pert_alto`
  - `pert_muito_alto`

Cada uma dessas colunas mede o grau de pertencimento do municipio a uma classe.

Objetivo:

- transformar um escore continuo em uma classificacao gradual, com zonas de
  transicao entre categorias

### Etapa 11. Escolher a classe dominante

Ainda dentro de `classifica_fuzzy`, o script:

- compara as 5 pertinencias de cada municipio
- escolhe a maior delas com `idxmax`
- remove o prefixo `pert_`
- grava o resultado em `classificacao_fuzzy`

Objetivo:

- produzir a classe final observavel pelo usuario

### Etapa 12. Calcular a confianca da classificacao

Ainda dentro de `classifica_fuzzy`, o script:

- pega o maior valor entre as 5 pertinencias
- grava esse valor em `confianca_classificacao`

Objetivo:

- indicar o quao nitida ou ambigua foi a classificacao do municipio

Interpretacao:

- confianca alta: municipio bem encaixado em uma categoria
- confianca baixa: municipio proximo da fronteira entre duas classes

### Etapa 13. Calcular o ranking final

Ainda dentro de `classifica_fuzzy`, o script:

- ordena os municipios por `score_final` em ordem decrescente
- atribui a colocacao em `ranking_final`

Objetivo:

- permitir leitura ordinal do resultado, alem da classe fuzzy

### Etapa 14. Ordenar a tabela final

Antes de salvar, o script ordena o resultado por:

- `ranking_final`
- `score_final`
- `municipio`

Objetivo:

- deixar a saida estavel e facil de ler

### Etapa 15. Gerar o resumo por categoria

Funcao usada:

- `gera_resumo`

O que acontece:

- agrupa por `classificacao_fuzzy`
- calcula:
  - quantidade de municipios
  - media do `score_final`
  - media da `confianca_classificacao`

Objetivo:

- criar uma visao sintetica da distribuicao final das classes

### Etapa 16. Salvar os resultados

No final da `main`, o script:

- cria a pasta de saida, se necessario
- salva o CSV principal
- salva o CSV resumo
- imprime no terminal os caminhos dos arquivos gerados

Objetivo:

- materializar o resultado do algoritmo em arquivos reutilizaveis

## Pseudofluxo Do Algoritmo

Uma forma compacta de ler o processo e:

```text
1. Ler lista de capitais intermediarias
2. Ler base municipal com colunas selecionadas
3. Filtrar a base para manter apenas os municipios do universo
4. Calcular indicadores derivados por habitante
5. Padronizar variaveis com percent_rank
6. Agregar os scores em 5 eixos tematicos
7. Agregar os 5 eixos em um score_final
8. Calcular quantis do score_final
9. Gerar pertinencias fuzzy para 5 classes
10. Escolher a classe de maior pertinencia
11. Calcular confianca_classificacao
12. Calcular ranking_final
13. Gerar tabela final e resumo por classe
14. Salvar os resultados em CSV
```

### `scripts/gera_gpkg_capitais_intermediarias_fuzzy_pacote.py`

Funcao:

- transforma o resultado tabular da classificacao fuzzy em arquivos geoespaciais

Entradas:

- `outputs/classificacao_capitais_intermediarias_fuzzy.csv`
- `inputs/capitais_intermediarias_geometrias.gpkg`

O que o script faz:

- junta a classificacao fuzzy com a malha geografica dos 119 municipios
- separa os municipios por categoria fuzzy
- gera um geopackage especifico para cada classe

Saidas:

- `outputs/gpkg_fuzzy_capitais_intermediarias/capitais_intermediarias_fuzzy_muito_alto.gpkg`
- `outputs/gpkg_fuzzy_capitais_intermediarias/capitais_intermediarias_fuzzy_alto.gpkg`
- `outputs/gpkg_fuzzy_capitais_intermediarias/capitais_intermediarias_fuzzy_medio.gpkg`
- `outputs/gpkg_fuzzy_capitais_intermediarias/capitais_intermediarias_fuzzy_baixo.gpkg`
- `outputs/gpkg_fuzzy_capitais_intermediarias/capitais_intermediarias_fuzzy_muito_baixo.gpkg`

### `scripts/valida_modelo_fuzzy_pacote.py`

Funcao:

- executa a validacao formal do fuzzy reduzido

O que o script faz:

- compara o modelo atual com modelos alternativos
- faz otimizacao estatistica de pesos contra o fuzzy completo
- testa estabilidade por bootstrap
- faz analise de sensibilidade a pesos e exclusao de variaveis
- mede validade convergente com variaveis que ficaram fora do modelo final

Saidas:

- `outputs/validacao_modelo_fuzzy_reduzido_resumo.csv`
- `outputs/validacao_modelo_fuzzy_reduzido_sensibilidade.csv`
- `outputs/validacao_modelo_fuzzy_reduzido_validacao_externa.csv`
- `outputs/validacao_modelo_fuzzy_reduzido_relatorio.md`

## Como Rodar

Classificacao fuzzy:

```bash
python3 scripts/classifica_capitais_intermediarias_fuzzy_pacote.py
```

Geopackages por classe:

```bash
python3 scripts/gera_gpkg_capitais_intermediarias_fuzzy_pacote.py
```

Validacao formal:

```bash
python3 scripts/valida_modelo_fuzzy_pacote.py
```

## Saidas Esperadas

Depois de rodar os scripts, a pasta `outputs/` deve conter:

- `classificacao_capitais_intermediarias_fuzzy.csv`
- `classificacao_capitais_intermediarias_fuzzy_resumo.csv`
- `gpkg_fuzzy_capitais_intermediarias/`
- `validacao_modelo_fuzzy_reduzido_resumo.csv`
- `validacao_modelo_fuzzy_reduzido_sensibilidade.csv`
- `validacao_modelo_fuzzy_reduzido_validacao_externa.csv`
- `validacao_modelo_fuzzy_reduzido_relatorio.md`

## Observacao

Este pacote foi reduzido para reproducao do fuzzy. Ele nao substitui a arvore
completa do projeto, mas concentra os insumos e scripts necessarios para esse
processamento especifico.
