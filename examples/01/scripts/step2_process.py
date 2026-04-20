import os

task_name  = os.environ.get("TASK_NAME")
input_csv  = os.environ.get("IN_RAW_DATA")
output_txt = os.environ.get("OUT_REPORT")

print(f"[{task_name}] Starting...")
print(f"[{task_name}] Leyendo: {input_csv}")

with open(input_csv, 'r') as f:
    lines = f.readlines()

total = len(lines) - 1  # sin cabecera

os.makedirs(os.path.dirname(output_txt), exist_ok=True)
with open(output_txt, 'w') as f:
    f.write(f"total_items={total}\n")
    f.write(f"status=OK_TRANSFORMADO\n")
    f.write(f"autor=Santi\n")

print(f"[{task_name}] Reporte escrito en: {output_txt}")
print(f"[{task_name}] Done.")