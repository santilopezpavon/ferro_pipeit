#!/usr/bin/env bash

echo "TASK: $PIPEIT_TASK_NAME_ID"

raw="$PIPEIT_IN_RAW_DATA"
report="$PIPEIT_IN_REPORT"
out="$PIPEIT_OUT_SUMMARY"

echo "---- RAW ----" > "$out"
cat "$raw" >> "$out"

echo "" >> "$out"
echo "---- REPORT ----" >> "$out"
cat "$report" >> "$out"