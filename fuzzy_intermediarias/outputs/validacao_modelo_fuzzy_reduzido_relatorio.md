# Validacao Formal Do Fuzzy Reduzido

## Escopo

Esta validacao cobre cinco frentes:

- otimizacao estatistica de pesos
- teste de estabilidade por bootstrap
- analise de sensibilidade a pesos e exclusao de variaveis
- validacao externa contra o fuzzy completo e variaveis retidas
- comparacao sistematica com modelos alternativos

## Comparacao Sistemica Entre Modelos

```text
             modelo  correlacao_score_completo  correlacao_ranking_completo  acordo_exato_classes  acordo_adjacente_classes  erro_medio_absoluto_classe  confianca_media
otimizado_variaveis                     0.9712                       0.9725                0.3025                    0.7227                      0.9748           0.8621
    otimizado_eixos                     0.9650                       0.9682                0.3109                    0.7395                      0.9496           0.8687
   variaveis_iguais                     0.9369                       0.9440                0.2857                    0.7647                      0.9496           0.8445
              atual                     0.9323                       0.9390                0.3109                    0.7647                      0.9244           0.8599
               pca1                     0.9291                       0.9381                0.3193                    0.7227                      0.9580           0.8513
       eixos_iguais                     0.9097                       0.9190                0.2941                    0.7227                      0.9832           0.8617
```

Leitura principal:
- melhor correlacao com o fuzzy completo: `otimizado_variaveis` (`0.9712`)
- melhor acordo adjacente de classes: `variaveis_iguais` (`0.7647`)
- desempenho do modelo atual: correlacao `0.9323` e acordo adjacente `0.7647`

## Otimizacao Estatistica De Pesos

A otimizacao foi feita por busca aleatoria em simplex, maximizando a correlacao com o `score_final` do fuzzy completo.

Pesos otimizados por eixos:
- `dinamismo_economico`: `0.5101`
- `infraestrutura_urbana`: `0.1719`
- `conectividade_digital`: `0.2292`
- `oferta_servicos`: `0.0811`
- `seguranca_territorial`: `0.0077`
- correlacao alcancada: `0.9650`

Pesos otimizados por variaveis:
- `score_empresas_1k`: `0.1365`
- `score_regic_var60`: `0.3199`
- `score_via_pav_pct`: `0.1493`
- `score_indice_conectividade`: `0.2187`
- `score_estab_saude_10k`: `0.1516`
- `score_homicidios_100k`: `0.0241`
- correlacao alcancada: `0.9712`

Interpretacao:
- o fuzzy completo parece valorizar mais `regic_var60` e `indice_conectividade`
- `homicidios_100k` contribui pouco quando o alvo e aproximar o fuzzy completo

## Estabilidade

- correlacao media bootstrap do modelo atual com ele mesmo: `0.9967`
- faixa bootstrap interna p5-p95: `0.9938` a `0.9983`
- correlacao media bootstrap do modelo atual com o ranking do fuzzy completo: `0.9366`
- faixa bootstrap externa p5-p95: `0.9198` a `0.9523`

Interpretacao:
- a estrutura do ranking se manteve alta ao longo das reamostragens
- isso indica que pequenas variacoes amostrais nao desorganizam o modelo

## Sensibilidade

```text
                         cenario                    tipo  correlacao_com_modelo_atual  correlacao_com_fuzzy_completo  correlacao_ranking_com_modelo_atual  correlacao_ranking_com_fuzzy_completo
       sem_score_homicidios_100k  leave_one_out_variavel                       0.9833                         0.9599                               0.9842                                 0.9612
       sem_score_estab_saude_10k  leave_one_out_variavel                       0.9774                         0.9392                               0.9756                                 0.9526
           sem_score_empresas_1k  leave_one_out_variavel                       0.9882                         0.9342                               0.9865                                 0.9444
           sem_score_via_pav_pct  leave_one_out_variavel                       0.9858                         0.9223                               0.9836                                 0.9317
  sem_score_indice_conectividade  leave_one_out_variavel                       0.9862                         0.9101                               0.9830                                 0.9197
           sem_score_regic_var60  leave_one_out_variavel                       0.9855                         0.8774                               0.9846                                 0.8883
peso_seguranca_territorial_-0.10 perturbacao_pesos_eixos                       0.9945                         0.9474                               0.9939                                 0.9494
peso_seguranca_territorial_-0.05 perturbacao_pesos_eixos                       0.9987                         0.9408                               0.9984                                 0.9469
      peso_oferta_servicos_-0.10 perturbacao_pesos_eixos                       0.9962                         0.9403                               0.9952                                 0.9502
  peso_dinamismo_economico_+0.10 perturbacao_pesos_eixos                       0.9989                         0.9402                               0.9977                                 0.9478
peso_conectividade_digital_+0.10 perturbacao_pesos_eixos                       0.9983                         0.9380                               0.9975                                 0.9451
      peso_oferta_servicos_-0.05 perturbacao_pesos_eixos                       0.9992                         0.9370                               0.9987                                 0.9446
  peso_dinamismo_economico_+0.05 perturbacao_pesos_eixos                       0.9997                         0.9367                               0.9992                                 0.9424
peso_conectividade_digital_+0.05 perturbacao_pesos_eixos                       0.9995                         0.9357                               0.9991                                 0.9434
peso_infraestrutura_urbana_+0.05 perturbacao_pesos_eixos                       0.9991                         0.9324                               0.9988                                 0.9391
peso_infraestrutura_urbana_+0.10 perturbacao_pesos_eixos                       0.9967                         0.9310                               0.9957                                 0.9386
peso_infraestrutura_urbana_-0.05 perturbacao_pesos_eixos                       0.9989                         0.9303                               0.9984                                 0.9377
peso_conectividade_digital_-0.05 perturbacao_pesos_eixos                       0.9994                         0.9275                               0.9988                                 0.9334
  peso_dinamismo_economico_-0.05 perturbacao_pesos_eixos                       0.9996                         0.9268                               0.9993                                 0.9336
      peso_oferta_servicos_+0.05 perturbacao_pesos_eixos                       0.9993                         0.9267                               0.9990                                 0.9323
peso_infraestrutura_urbana_-0.10 perturbacao_pesos_eixos                       0.9952                         0.9257                               0.9941                                 0.9342
peso_seguranca_territorial_+0.05 perturbacao_pesos_eixos                       0.9989                         0.9225                               0.9980                                 0.9311
peso_conectividade_digital_-0.10 perturbacao_pesos_eixos                       0.9974                         0.9208                               0.9956                                 0.9275
      peso_oferta_servicos_+0.10 perturbacao_pesos_eixos                       0.9975                         0.9206                               0.9963                                 0.9246
  peso_dinamismo_economico_-0.10 perturbacao_pesos_eixos                       0.9984                         0.9199                               0.9977                                 0.9270
peso_seguranca_territorial_+0.10 perturbacao_pesos_eixos                       0.9961                         0.9117                               0.9952                                 0.9229
```

- cenario leave-one-out mais critico: `sem_score_regic_var60` com correlacao `0.8774` com o fuzzy completo
- perturbacoes moderadas nos pesos dos eixos alteram pouco o ranking, sugerindo robustez estrutural

## Validacao Externa

A validacao externa foi operacionalizada de duas formas:

- benchmark contra o fuzzy completo ja existente no projeto
- correlacao do modelo reduzido com variaveis importantes que ficaram de fora da especificacao final

```text
        variavel_externa  correlacao_modelo_reduzido
           densidade_scm                      0.8530
      cobertura_pop_4g5g                      0.7482
                  pib_pc                      0.7232
ambulatorios_sus_2026_02                      0.5483
    adensamento_estacoes                      0.5109
             regic_var59                      0.5085
             regic_var56                      0.4798
             regic_var61                      0.4763
             regic_var66                      0.3793
                   fibra                      0.3638
```

Interpretacao:
- correlacoes altas com `densidade_scm`, `cobertura_pop_4g5g` e `pib_pc` indicam boa validade convergente
- correlacoes positivas com varias `regic_*` sugerem que o modelo preserva informacao de centralidade que nao foi explicitamente mantida

## Conclusao Metodologica

- o modelo atual e defensavel como especificacao reduzida, porque combina boa aderencia ao fuzzy completo com alta interpretabilidade
- a alternativa `otimizado_variaveis` e a melhor se o objetivo for maximizar proximidade estatistica com o fuzzy completo
- a especificacao atual continua preferivel se o objetivo principal for transparencia substantiva e leitura simples dos eixos

## Limitacoes

- a validacao externa nao usa base fora do projeto; ela usa benchmark interno forte e variaveis retidas como criterio convergente
- a otimizacao estatistica foi feita por busca aleatoria em simplex, adequada para este problema pequeno, mas nao unica forma possivel
