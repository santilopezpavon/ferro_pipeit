#!/usr/bin/env bash

echo "TASK: $PIPEIT_TASK_NAME_ID"

raw="$PIPEIT_IN_RAW_DATA"
out="$PIPEIT_OUT_SUMMARY"

echo "---- RAW ----" > "$out"
cat "$raw" >> "$out"

echo "" >> "$out"
