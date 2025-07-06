# testcolsreorder.py
import re
import tambak_cache

def reorder_log_inplace(filename):
    running_aspect_pattern = re.compile(r"Running Aspect:\S+ Column:\S+")
    stage_pattern = re.compile(r"--- STAGE 1:")

    with open(filename, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Extract running aspect lines (contents only)
    running_aspect_lines = [line for line in lines if running_aspect_pattern.match(line.strip())]

    # Find all stage line indices
    stage_line_indices = [i for i, line in enumerate(lines) if stage_pattern.search(line)]

    output_lines = []
    running_aspect_idx = 0
    i = 0
    while i < len(lines):
        # Before outputting a stage line, insert running aspect line if available
        if i in stage_line_indices:
            if running_aspect_idx < len(running_aspect_lines):
                output_lines.append(running_aspect_lines[running_aspect_idx])
                running_aspect_idx += 1
            output_lines.append(lines[i])
            i += 1
        else:
            # Skip running aspect lines here since they're handled separately
            if running_aspect_pattern.match(lines[i].strip()):
                i += 1
                continue
            output_lines.append(lines[i])
            i += 1

    # If any running aspect lines remain (more than stages), append them at end
    while running_aspect_idx < len(running_aspect_lines):
        output_lines.append(running_aspect_lines[running_aspect_idx])
        running_aspect_idx += 1

    # Overwrite the original file with reordered content
    with open(filename + "reorder", "w", encoding="utf-8") as fw:
        fw.writelines(output_lines)

if __name__ == "__main__":
    filename = f"test_cols_{tambak_cache.__version__}.txt"
    reorder_log_inplace(filename)
