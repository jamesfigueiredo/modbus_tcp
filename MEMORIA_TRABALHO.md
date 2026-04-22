# Memoria de Trabalho - mudbus_tcp

Data: 22/04/2026

## Objetivo desta memoria
Registrar as decisoes e entregas feitas em conjunto para manter continuidade tecnica.

## O que foi feito
- Adicionado modulo de previsao para REA da ETE BSB:
  - `prediction/sarima_ete_bsb_reatores.py`
  - `prediction_ete_bsb_next_hour_reatores.py`
- Ajustada a saida operacional de REA para:
  - retorno somente da proxima hora
  - impressao da janela de 24h para auditoria
- Corrigido arquivo de dependencias:
  - `requirements.txt` (padrao)
  - `requeriments.txt` mantido como alias (`-r requirements.txt`)
- Integracao REA para escrita no CLP ETE SUL:
  - `IP_CLP = 172.16.51.30`
  - `RACK = 0`
  - `SLOT = 3`
  - `DB = 3`
  - `start_offset`: `14`, `18`, `22`, `26` (um por tag REA)
  - Correcao no cliente Modbus para escrita com `start_offset > 0` em `modbus/client.py`

## EEBs: melhoria e retorno ao original
- Versao melhorada criada e preservada em:
  - `prediction/sarima_eebs_improved.py`
  - `prediction_eebs_24h_improved.py`
- Versao original restaurada como padrao em:
  - `prediction/sarima.py`
  - `prediction_eebs_24h.py` continua usando `prediction.sarima.prediction_sarimax`

## Como executar
- Fluxo original EEBs:
  - `python prediction_eebs_24h.py`
- Fluxo melhorado EEBs (teste/controlado):
  - `python prediction_eebs_24h_improved.py`
- Fluxo REA (proxima hora + impressao 24h):
  - `python prediction_ete_bsb_next_hour_reatores.py`

## Proximos passos recomendados
- Comparar original vs improved por backtest (MAE/MAPE por tag).
- Definir criterio de promocao da versao improved para producao.
- Adicionar `DRY_RUN` para testes sem escrita em CLP.

## Regra de trabalho
- Atualizar este arquivo `MEMORIA_TRABALHO.md` a cada alteracao funcional acordada entre nos.

## Validacao pre-push (22/04/2026)
- `py_compile` executado nos principais scripts e modulos: OK.
- Diferencas no fluxo original EEB mantidas minimas:
  - `prediction/sarima.py` restaurado para logica original.
  - `prediction_eebs_24h.py` ganhou apenas guardas para `df` vazio/tag ausente.
- Ajuste funcional relevante em comunicacao Modbus:
  - `modbus/client.py` corrigido para escrita com `start_offset > 0`.
