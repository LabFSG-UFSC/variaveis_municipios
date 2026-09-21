# Modelo de Classificação de Municípios

## Estudo atual: PCA + k-means com densidade urbana

Este pacote contém somente os materiais da rodada atual do estudo de classificação de municípios por PCA + k-means. Modelos anteriores, bases brutas, GeoPackages, imagens, relatórios antigos e arquivos temporários permanecem fora desta pasta.

## O que está sendo compartilhado

- `scripts/roda_pca_kmeans_densidade_urbana.py`: script principal do processamento;
- `inputs/`: bases utilizadas pelo processamento;
- `docs/relatorio_pca_kmeans_densidade_urbana.qmd`: relatório completo do teste;
- `assets/`: arquivo de referência para renderização em DOCX;
- `outputs/figuras/`: figuras de correlação e dos eixos dos quatro universos;
- `outputs/csv/`: tabelas finais em formato textual, preparadas para versionamento no Git;
- `outputs/xlsx/`: planilhas Excel mantidas localmente, fora do commit.

As tabelas e figuras foram geradas a partir do mesmo processamento e usam os mesmos rótulos de cluster dos arquivos municipais do modelo atual.

## Bases de entrada incluídas

Foram copiadas para `inputs/` as bases efetivamente lidas pelo script:

- `merge_v28.csv`: base municipal consolidada;
- `municipios_brasil_abc_cnefe.csv`: classificação territorial e classe ABC;
- `ABC_CNEFE.csv`: domicílios CNEFE;
- `Agregados_por_municipios_renda_responsavel_BR 2.csv`: renda média do chefe de família;
- `setores_urbanos_agregado_municipio.csv`: área urbana e densidade urbana;
- `idhm_mun_2010.csv`: IDHM 2010 incorporado às matrizes de apoio;
- A malha municipal não foi incluída, porque é necessária apenas para gerar GeoPackages e não participa do cálculo do PCA ou do k-means.

Os arquivos de entrada incluídos ocupam aproximadamente 5 MB. Eles foram incluídos porque fazem parte da rastreabilidade do estudo e permitem repetir a montagem da matriz municipal.

## Modelo utilizado

O modelo é PCA + k-means++ com `k = 5` clusters e semente `42`.

Antes do PCA, as variáveis numéricas são padronizadas para média zero e desvio-padrão um. O PCA é calculado sobre a matriz padronizada e retém o menor número de componentes cuja variância acumulada atinge pelo menos 90%. O k-means++ é então aplicado aos escores dos componentes retidos, e não diretamente às variáveis originais.

Para facilitar a comparação entre os universos, a numeração dos clusters é ordenada pela média decrescente de `renda_media_chefe`. Portanto, o número do cluster é um rótulo ordenado para leitura dos resultados, não uma medida de distância ou de qualidade.

## Variáveis do modelo

Neste teste, `regic_var56` não foi utilizada. Foram utilizadas as seguintes variáveis:

| Variável | Significado |
|---|---|
| `via_pav_pct` | Percentual de domicílios com existência de via pavimentada no entorno. |
| `indice_conectividade` | Índice Brasileiro de Conectividade municipal normalizado. |
| `ambulatorios_sus_2026_02` | Número de estabelecimentos com atendimento ambulatorial no SUS, conforme a base municipal utilizada. |
| `pib_pc` | PIB per capita, calculado como `pib_total / pop_total`. |
| `categoria_turistica_score` | Escore ordinal da categoria turística municipal. |
| `litoral_ate_50km` | Indicador de município situado até 50 km do litoral. |
| `litoral_50_100km` | Indicador de município situado entre 50 e 100 km do litoral. |
| `litoral_100_500km` | Indicador de município situado entre 100 e 500 km do litoral. |
| `litoral_acima_500km` | Indicador de município situado a mais de 500 km do litoral. |
| `empresas_1k` | Número de empresas por 1.000 habitantes. |
| `densidade_demografica_urbana` | População urbana por km² de área urbana municipal. |
| `renda_media_chefe` | Renda média do chefe de família, derivada da variável `V06006` da base de renda. |

## Universos analisados e resultados

| Universo | Regra de seleção | Municípios | Componentes retidos | Tamanho dos clusters 1 a 5 |
|---|---|---:|---:|---|
| SC todos | Todos os municípios de SC, exceto capital de UF e conurbados com capital de UF | 291 | 8 | 12, 82, 106, 26, 65 |
| SC classe A | Municípios de SC classe A, exceto capital de UF e conurbados com capital de UF | 41 | 7 | 2, 31, 3, 3, 2 |
| BR todos | Todos os municípios do Brasil, exceto capitais de UF e conurbados com capitais de UF | 5.423 | 9 | 1.146, 1.070, 814, 582, 1.811 |
| BR classe A | Municípios do Brasil classe A, exceto capitais de UF e conurbados com capitais de UF | 825 | 8 | 3, 298, 122, 94, 308 |

## Tabelas em CSV

Os oito arquivos Excel foram convertidos para 16 CSVs porque o formato CSV não possui abas. Cada CSV recebe o nome do arquivo de origem, seguido pelo nome da aba, separado por `__`.

As tabelas detalhadas geram as abas `resumo_brasil`, `tabela_detalhada` e `municipios_atributos`. As tabelas de cruzamento geram a aba `tabela_cluster`.

O conjunto de CSVs das tabelas ocupa aproximadamente 514 KB, portanto permanece abaixo do limite de 10 MB considerado adequado para este repositório.

### Relação categoria x cluster detalhada

Arquivos de origem convertidos:

- `sc_todos_categoria_cluster_detalhada.xlsx`
- `sc_classe_a_categoria_cluster_detalhada.xlsx`
- `br_todos_categoria_cluster_detalhada.xlsx`
- `br_classe_a_categoria_cluster_detalhada.xlsx`

Cada arquivo contém três abas:

- `Resumo Brasil`: resumo de domicílios por cluster, com número e participação percentual;
- `Tabela detalhada`: relação das categorias territoriais com os clusters;
- `Municipios atributos`: município, cluster, categoria, classe ABC, UF e atributos usados na leitura do resultado.

### Relação cluster x categoria x classe ABC x UF

Arquivos:

- `sc_todos_cluster_categoria_abc_uf.xlsx`
- `sc_classe_a_cluster_categoria_abc_uf.xlsx`
- `br_todos_cluster_categoria_abc_uf.xlsx`
- `br_classe_a_cluster_categoria_abc_uf.xlsx`

Cada arquivo apresenta a quantidade de municípios e os domicílios CNEFE por combinação de cluster, categoria territorial, classe ABC e UF.

Os XLSX continuam disponíveis na pasta local `outputs/xlsx/` para consulta visual, mas foram adicionados ao `.gitignore`. Para o Git, devem ser usados apenas os CSVs correspondentes em `outputs/csv/`.

## Leitura dos resultados

Os clusters não representam categorias previamente definidas. Eles são formados pela combinação das variáveis após a padronização e a redução por PCA. Assim, municípios no mesmo cluster apresentam perfis relativamente semelhantes no conjunto analisado, considerando infraestrutura viária, conectividade, serviços ambulatoriais, atividade econômica, turismo, posição em relação ao litoral, densidade urbana e renda.

As categorias territoriais, a classe ABC, a UF e os domicílios CNEFE são informações de caracterização e leitura dos grupos. Elas não são usadas como variáveis do PCA, com exceção das variáveis listadas na seção anterior.

## Relatório

O relatório QMD apresenta o objetivo, os universos, as variáveis, a padronização, o critério de retenção de componentes, o funcionamento do k-means, a análise exploratória, as correlações, os loadings e a interpretação dos agrupamentos.

O arquivo pode ser aberto diretamente no Quarto. As figuras usadas no texto já estão em `outputs/figuras/`, e o arquivo de referência Times New Roman está em `assets/`.

## Reprodução

O script principal lê as bases diretamente da pasta `inputs/` deste pacote e grava os resultados na pasta `outputs/`. A geração de GeoPackages é opcional e pode ser ativada com o argumento `--geometria`, apontando para uma malha municipal externa.

Para executar a rotina a partir da raiz do repositório:

```bash
python3 "classificacao_municipios/documentacao_sc_classificacao/Modelo de Classificação de Municípios/scripts/roda_pca_kmeans_densidade_urbana.py"
```

As tabelas Excel são produtos finais locais. As versões CSV são as cópias destinadas ao Git, enquanto as bases de entrada e o script preservam a rastreabilidade do processamento.
