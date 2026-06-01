# Correlacao Das Variaveis Do Fuzzy

- municipios analisados: `119`
- variaveis na matriz: `12`
- maior correlacao positiva: `estab_saude_10k` x `oferta_servicos` = `0.9533`
- maior correlacao negativa: `homicidios_100k` x `seguranca_territorial` = `-0.9520`

## Matriz de correlacao

```text
                       empresas_1k  regic_var60  via_pav_pct  indice_conectividade  estab_saude_10k  homicidios_100k  dinamismo_economico  infraestrutura_urbana  conectividade_digital  oferta_servicos  seguranca_territorial  score_final
empresas_1k                 1.0000       0.3852       0.4539                0.6976           0.5940          -0.4700               0.8648                 0.5364                 0.7218           0.6638                 0.5087       0.8501
regic_var60                 0.3852       1.0000       0.2525                0.4556           0.0462          -0.1699               0.6796                 0.2690                 0.4857           0.0799                 0.1864       0.4606
via_pav_pct                 0.4539       0.2525       1.0000                0.5234           0.3715          -0.3230               0.4789                 0.8808                 0.5070           0.4312                 0.3300       0.6385
indice_conectividade        0.6976       0.4556       0.5234                1.0000           0.5380          -0.2561               0.7842                 0.5344                 0.9254           0.6096                 0.2986       0.8210
estab_saude_10k             0.5940       0.0462       0.3715                0.5380           1.0000          -0.4578               0.5200                 0.4375                 0.5306           0.9533                 0.4984       0.7404
homicidios_100k            -0.4700      -0.1699      -0.3230               -0.2561          -0.4578           1.0000              -0.4158                -0.4000                -0.3888          -0.4805                -0.9520      -0.6273
dinamismo_economico         0.8648       0.6796       0.4789                0.7842           0.5200          -0.4158               1.0000                 0.5476                 0.7906           0.5779                 0.4650       0.8840
infraestrutura_urbana       0.5364       0.2690       0.8808                0.5344           0.4375          -0.4000               0.5476                 1.0000                 0.5497           0.4982                 0.4179       0.7301
conectividade_digital       0.7218       0.4857       0.5070                0.9254           0.5306          -0.3888               0.7906                 0.5497                 1.0000           0.5928                 0.4365       0.8680
oferta_servicos             0.6638       0.0799       0.4312                0.6096           0.9533          -0.4805               0.5779                 0.4982                 0.5928           1.0000                 0.5005       0.8001
seguranca_territorial       0.5087       0.1864       0.3300                0.2986           0.4984          -0.9520               0.4650                 0.4179                 0.4365           0.5005                 1.0000       0.6740
score_final                 0.8501       0.4606       0.6385                0.8210           0.7404          -0.6273               0.8840                 0.7301                 0.8680           0.8001                 0.6740       1.0000
```

## Maiores correlacoes absolutas

```text
           variavel_a            variavel_b  correlacao_pearson  correlacao_absoluta
      estab_saude_10k       oferta_servicos              0.9533               0.9533
      homicidios_100k seguranca_territorial             -0.9520               0.9520
 indice_conectividade conectividade_digital              0.9254               0.9254
  dinamismo_economico           score_final              0.8840               0.8840
          via_pav_pct infraestrutura_urbana              0.8808               0.8808
conectividade_digital           score_final              0.8680               0.8680
          empresas_1k   dinamismo_economico              0.8648               0.8648
          empresas_1k           score_final              0.8501               0.8501
 indice_conectividade           score_final              0.8210               0.8210
      oferta_servicos           score_final              0.8001               0.8001
  dinamismo_economico conectividade_digital              0.7906               0.7906
 indice_conectividade   dinamismo_economico              0.7842               0.7842
      estab_saude_10k           score_final              0.7404               0.7404
infraestrutura_urbana           score_final              0.7301               0.7301
          empresas_1k conectividade_digital              0.7218               0.7218
```
