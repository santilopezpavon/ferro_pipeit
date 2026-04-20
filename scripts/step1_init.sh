#!/bin/bash
set -e

echo "[$TASK_NAME] Iniciando..."

mkdir -p "$(dirname "$OUT_RAW_DATA")"
env

echo "id,item,cantidad" > "$OUT_RAW_DATA"
echo "1,Servidor,5"     >> "$OUT_RAW_DATA"
echo "2,Switch,12"      >> "$OUT_RAW_DATA"
echo "3,Router,3"       >> "$OUT_RAW_DATA"

echo "[$TASK_NAME] Archivo creado en: $OUT_RAW_DATA"
echo "[$TASK_NAME] Done."