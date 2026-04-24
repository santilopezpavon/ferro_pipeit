import os

print("TASK:", os.environ.get("PIPEIT_TASK_NAME_ID"))

input_path = os.environ["PIPEIT_IN_RAW_DATA"]
output_path = os.environ["PIPEIT_OUT_PROCESSED"]

with open(input_path) as f:
    data = f.readlines()

data = [line.upper() for line in data]

with open(output_path, "w") as f:
    f.writelines(data)