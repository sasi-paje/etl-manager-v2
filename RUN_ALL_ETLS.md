# Rodar todos os ETLs no Supabase

Execute a partir da raiz do projeto:

```bash
cd /Users/jorgehbchaves/Dev/etl-manager-v2
source .venv/bin/activate

schemas=(
  webapp_110760000036
  webapp_110760000045
  webapp_110760000088
  webapp_110760000091
  webapp_110760000095
  webapp_110760000100
  webapp_110760000185
  webapp_110760000241
  webapp_110760000317
  webapp_110760000369
  webapp_110760000411
  webapp_110760000412
  webapp_110760000460
  webapp_110760000525
  webapp_110760000549
  webapp_110760000596
  webapp_110760000611
  webapp_110760000767
  webapp_110760000780
  webapp_110760000902
  webapp_110760000938
  webapp_110760000995
  webapp_110760001004
  webapp_110760001052
  webapp_110760001163
  webapp_110760001223
  webapp_110760001227
  webapp_110760001246
  webapp_110760001255
  webapp_110760001263
)

for schema in "${schemas[@]}"; do
  id="${schema#webapp_}"
  echo "=== $schema / $id ==="
  TARGET_DB_SCHEMA="$schema" etl-manager-v2 add "$id" --interval 60 --force
  TARGET_DB_SCHEMA="$schema" etl-manager-v2 run "$id"
done
```

Versao sem ativar o ambiente virtual:

```bash
cd /Users/jorgehbchaves/Dev/etl-manager-v2

schemas=(
  webapp_110760000036
  webapp_110760000045
  webapp_110760000088
  webapp_110760000091
  webapp_110760000095
  webapp_110760000100
  webapp_110760000185
  webapp_110760000241
  webapp_110760000317
  webapp_110760000369
  webapp_110760000411
  webapp_110760000412
  webapp_110760000460
  webapp_110760000525
  webapp_110760000549
  webapp_110760000596
  webapp_110760000611
  webapp_110760000767
  webapp_110760000780
  webapp_110760000902
  webapp_110760000938
  webapp_110760000995
  webapp_110760001004
  webapp_110760001052
  webapp_110760001163
  webapp_110760001223
  webapp_110760001227
  webapp_110760001246
  webapp_110760001255
  webapp_110760001263
)

for schema in "${schemas[@]}"; do
  id="${schema#webapp_}"
  echo "=== $schema / $id ==="
  TARGET_DB_SCHEMA="$schema" .venv/bin/etl-manager-v2 add "$id" --interval 60 --force
  TARGET_DB_SCHEMA="$schema" .venv/bin/etl-manager-v2 run "$id"
done
```
