# Resumo de execucao dos ETLs - 2026-07-02

Comando base:

```bash
TARGET_DB_SCHEMA=<schema> .venv/bin/etl-manager-v2 add <id> --interval 60 --force
TARGET_DB_SCHEMA=<schema> .venv/bin/etl-manager-v2 run <id>
```

## Resultado consolidado

| Schema | ID | Status | Observacao |
| --- | --- | --- | --- |
| webapp_110760000036 | 110760000036 | success | OK |
| webapp_110760000045 | 110760000045 | success | OK |
| webapp_110760000088 | 110760000088 | success | OK |
| webapp_110760000091 | 110760000091 | success | OK |
| webapp_110760000095 | 110760000095 | success | OK |
| webapp_110760000100 | 110760000100 | success | OK |
| webapp_110760000185 | 110760000185 | success | OK |
| webapp_110760000241 | 110760000241 | success | OK |
| webapp_110760000317 | 110760000317 | success | OK no retry apos timeout temporario |
| webapp_110760000369 | 110760000369 | success | OK |
| webapp_110760000411 | 110760000411 | success | OK |
| webapp_110760000412 | 110760000412 | success | OK |
| webapp_110760000460 | 110760000460 | success | OK |
| webapp_110760000525 | 110760000525 | success | OK |
| webapp_110760000549 | 110760000549 | success | OK |
| webapp_110760000596 | 110760000596 | success | OK |
| webapp_110760000611 | 110760000611 | success | OK |
| webapp_110760000767 | 110760000767 | interrupted | Execucao longa sem retorno; retry apos executemany tambem foi interrompido. Precisa carga isolada com progresso por tabela. |
| webapp_110760000780 | 110760000780 | success | OK |
| webapp_110760000902 | 110760000902 | success | OK |
| webapp_110760000938 | 110760000938 | success | OK |
| webapp_110760000995 | 110760000995 | success | OK |
| webapp_110760001004 | 110760001004 | success | OK |
| webapp_110760001052 | 110760001052 | success | OK |
| webapp_110760001163 | 110760001163 | success | OK |
| webapp_110760001223 | 110760001223 | success | OK |
| webapp_110760001227 | 110760001227 | success | OK |
| webapp_110760001246 | 110760001246 | success | OK |
| webapp_110760001255 | 110760001255 | success | OK |
| webapp_110760001263 | 110760001263 | success | OK |
| webapp_11760000596 | 11760000596 | failed | Origem MySQL nao existe: argus_11760000596 |

## Totais

- Success: 29
- Interrupted: 1
- Failed: 1
