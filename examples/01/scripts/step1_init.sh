#!/bin/bash
set -e

echo "[$PIPEIT_TASK_NAME_ID] Iniciando..."

mkdir -p "$(dirname "$PIPEIT_OUT_RAW_DATA")"

echo "id,item,cantidad" > "$PIPEIT_OUT_RAW_DATA"
echo "1,Servidor,5"     >> "$PIPEIT_OUT_RAW_DATA"
echo "2,Switch,12"      >> "$PIPEIT_OUT_RAW_DATA"
echo "3,Router,3"       >> "$PIPEIT_OUT_RAW_DATA"

echo "[$PIPEIT_TASK_NAME_ID] Archivo creado en: $PIPEIT_OUT_RAW_DATA"
echo "[$PIPEIT_TASK_NAME_ID] Done."