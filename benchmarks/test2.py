import re

running_aspect_pattern = re.compile(r"Running Aspect:\w+ Column:\w+")

test_lines = [
    "Running Aspect:unit_positions Column:frame",
    "Running Aspect:unit_positions Column:unit_id",
    "  Running Aspect:unit_positions Column:unit_def_id  ",
    "Random line",
]

for line in test_lines:
    if running_aspect_pattern.match(line.strip()):
        print(f"MATCH: {line}")
    else:
        print(f"NO MATCH: {line}")