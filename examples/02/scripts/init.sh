#!/usr/bin/env bash

echo "TASK: $PIPEIT_TASK_NAME_ID"

mkdir -p "$(dirname "$PIPEIT_OUT_RAW_DATA")"
echo "line1" > "$PIPEIT_OUT_RAW_DATA"
echo "line2" >> "$PIPEIT_OUT_RAW_DATA"