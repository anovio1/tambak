# In: test_mpk_compression.py

import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import time
import sys
import logging
import json
import zstandard
import re

logger = logging.getLogger(__name__)
try:
    import tambak_cache
except ImportError:
    logger.error(
        "Could not import 'tambak_cache'. Make sure it is installed correctly."
    )
    sys.exit(1)
logger.setLevel(logging.DEBUG)  # Set logger level globally

FILENAME = f"test_cols_{tambak_cache.__version__}.txt"

# Console handler: DEBUG and above
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
console_formatter = logging.Formatter("%(message)s")
console_handler.setFormatter(console_formatter)

# File handler: INFO and above
file_handler = logging.FileHandler(FILENAME, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("%(message)s")
file_handler.setFormatter(file_formatter)

# Remove any existing handlers
if logger.hasHandlers():
    logger.handlers.clear()

# Add new handlers
logger.addHandler(console_handler)
logger.addHandler(file_handler)

TUBUIN_PROCESSOR_PATH = "V:/Github/tubuin-processor/src"
sys.path.append(TUBUIN_PROCESSOR_PATH)

try:
    from tubuin_processor.core.decoder import stream_decode_aspect
    from tubuin_processor.core.value_transformer import stream_transform_aspect
except ImportError as e:
    logger.error(
        f"Failed to import from tubuin-processor. Check the TUBUIN_PROCESSOR_PATH. Error: {e}"
    )
    sys.exit(1)

ASPECT_NAMES = ["unit_positions", "unit_events", "damage_log"]


def reorder_log_inplace(filename):
    running_aspect_pattern = re.compile(r"Running Aspect:\S+ Column:\S+")
    stage_pattern = re.compile(r"--- STAGE 1:")

    with open(filename, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # Extract running aspect lines (contents only)
    running_aspect_lines = [
        line for line in lines if running_aspect_pattern.match(line.strip())
    ]

    # Find all stage line indices
    stage_line_indices = [
        i for i, line in enumerate(lines) if stage_pattern.search(line)
    ]

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
    with open(filename, "w", encoding="utf-8") as fw:
        fw.writelines(output_lines)


def write_tambak_frame(output_path: pathlib.Path, compressed_columns: dict):
    # ... (function is correct and unchanged) ...
    header = b"PHX1"
    toc = {}
    data_blobs = []
    current_offset = 0
    for name, data in compressed_columns.items():
        toc[name] = {"offset": current_offset, "size": len(data)}
        current_offset += len(data)
        data_blobs.append(data)
    toc_json_bytes = json.dumps(toc, indent=2).encode("utf-8")
    toc_len_bytes = len(toc_json_bytes).to_bytes(4, "little")
    with open(output_path, "wb") as f:
        f.write(header)
        f.write(toc_len_bytes)
        f.write(toc_json_bytes)
        for blob in data_blobs:
            f.write(blob)
    logger.debug(f"✅ Successfully wrote tambak frame to: {output_path}")


def main(aspect_name):
    MPK_FILE_PATH = pathlib.Path(f"./{aspect_name}.mpk")
    PHX_OUTPUT_PATH = pathlib.Path(f"./{aspect_name}.phx")
    PARQUET_OUTPUT_PATH = pathlib.Path(f"./{aspect_name}.parquet")
    # ... (Loading and transforming data is the same) ...
    if not MPK_FILE_PATH.exists():
        logger.error(f"MPK file not found at '{MPK_FILE_PATH}'")
        return

    logger.debug(
        f"--- Starting MPK Compression Test for: {MPK_FILE_PATH.name} (Aspect: {aspect_name}) ---"
    )
    mpk_bytes = MPK_FILE_PATH.read_bytes()
    logger.debug(f"  - Loaded {len(mpk_bytes):,} bytes from disk.")
    logger.debug("  - Decoding and transforming records...")
    start_time = time.perf_counter()
    try:
        raw_model_stream = stream_decode_aspect(aspect_name, mpk_bytes)
        clean_model_stream = stream_transform_aspect(
            aspect_name, raw_model_stream, skip_on_error=True
        )
        clean_data_list = [record.model_dump() for record in clean_model_stream]
    except Exception as e:
        logger.error(
            f"An error occurred during the decode/transform phase: {e}", exc_info=True
        )
        return
    transform_time = time.perf_counter() - start_time
    logger.debug(
        f"  - Transformation complete in {transform_time:.2f}s. Found {len(clean_data_list):,} records."
    )

    if not clean_data_list:
        logger.warning("  - No data to compress after transformation. Exiting.")
        return

    logger.debug("  - Creating PyArrow Table...")
    try:
        arrow_table = pa.Table.from_pylist(clean_data_list)
    except Exception as e:
        logger.error(f"Failed to create Arrow Table. Error: {e}", exc_info=True)
        return

    # --- BENCHMARKING ---
    logger.debug("  - Running all benchmarks...")

    all_column_results = {}
    tambak_compressed_columns = {}
    zstd_compressor = zstandard.ZstdCompressor(level=3)
    tambak_cache.enable_verbose_logging(FILENAME)

    for column_name in arrow_table.column_names:
        # ... (This loop is unchanged, it populates all_column_results) ...
        column_array = arrow_table.column(column_name)
        if isinstance(column_array, pa.ChunkedArray):
            column_array = column_array.combine_chunks()
        all_column_results[column_name] = {}
        try:
            analysis_result = tambak_cache.compress_analyze(column_array)
            logger.info(f"\nRunning Aspect:{aspect} Column:{column_name}\n")
            for handler in logger.handlers:
                handler.flush()
            tambak_compressed_columns[column_name] = analysis_result["artifact"]
            all_column_results[column_name]["tambak_size"] = len(
                analysis_result["artifact"]
            )
            all_column_results[column_name]["plan"] = analysis_result["plan"]
        except Exception:
            all_column_results[column_name]["tambak_size"] = "N/A"
            all_column_results[column_name]["plan"] = "Failed"
        try:
            column_data_bytes = b"".join(
                b.to_pybytes() for b in column_array.buffers() if b is not None
            )
            zstd_bytes = zstd_compressor.compress(column_data_bytes)
            all_column_results[column_name]["zstd_size"] = len(zstd_bytes)
        except Exception:
            all_column_results[column_name]["zstd_size"] = "N/A"

    # --- CALCULATE TOTALS AND FILE SIZES ---

    # 1. On-Disk File Sizes
    write_tambak_frame(PHX_OUTPUT_PATH, tambak_compressed_columns)
    tambak_frame_size = PHX_OUTPUT_PATH.stat().st_size

    zstd_on_mpk_bytes = zstd_compressor.compress(mpk_bytes)
    zstd_on_mpk_size = len(zstd_on_mpk_bytes)

    try:
        pq.write_table(
            arrow_table, PARQUET_OUTPUT_PATH, compression="ZSTD", compression_level=3
        )
        parquet_file_size = PARQUET_OUTPUT_PATH.stat().st_size
    except Exception as e:
        logger.error(f"Failed to write Parquet file: {e}")
        parquet_file_size = -1

    # 2. Columnar Data Totals
    total_tambak_data_size = sum(
        v.get("tambak_size", 0)
        for v in all_column_results.values()
        if isinstance(v.get("tambak_size"), int)
    )
    total_zstd_columnar_size = sum(
        v.get("zstd_size", 0)
        for v in all_column_results.values()
        if isinstance(v.get("zstd_size"), int)
    )

    parquet_col_sizes = {}
    total_parquet_columnar_size = -1
    if parquet_file_size != -1:
        parquet_file_metadata = pq.read_metadata(PARQUET_OUTPUT_PATH)
        for i in range(parquet_file_metadata.num_row_groups):
            row_group = parquet_file_metadata.row_group(i)
            for j in range(row_group.num_columns):
                column_meta = row_group.column(j)
                col_name = column_meta.path_in_schema
                if col_name not in parquet_col_sizes:
                    parquet_col_sizes[col_name] = 0
                parquet_col_sizes[col_name] += column_meta.total_compressed_size
        total_parquet_columnar_size = sum(parquet_col_sizes.values())

    # --- FINAL REPORTING ---
    # print("\n" + "="*80)
    print(
        f"\n--- tambak {tambak_cache.__version__}.0 Columns {aspect_name} ---".center(
            80
        )
    )
    # print("="*80)

    # print("\n" + "="*80)
    # print("--- 📊 TOTAL ON-DISK FILE SIZE COMPARISON ---".center(80))
    # print("--- (The 'CEO' View: Which final file is smallest?) ---".center(80))
    # print("="*80)
    # print(f"  - Original MPK File:         {len(mpk_bytes):>15,} bytes (100.00%)")
    # print(f"  - Zstd on original MPK:      {zstd_on_mpk_size:>15,} bytes ({zstd_on_mpk_size/len(mpk_bytes)*100:6.2f}%)")
    if parquet_file_size != -1:
        pass
        # print(f"  - Parquet (Zstd) File:       {parquet_file_size:>15,} bytes ({parquet_file_size/len(mpk_bytes)*100:6.2f}%)")
    # print(f"  - tambak Frame File (.phx): {tambak_frame_size:>15,} bytes ({tambak_frame_size/len(mpk_bytes)*100:6.2f}%)")
    # print("="*80)

    # print("\n" + "="*80)
    # print("--- 📈 TOTAL COLUMNAR DATA SIZE COMPARISON ---".center(80))
    # print("--- (The 'Engineering' View: How effective is our logic?) ---".center(80))
    # print("="*80)
    if total_parquet_columnar_size != -1:
        # print(f"  - Parquet Columnar Data:     {total_parquet_columnar_size:>15,} bytes (100.00%)")
        ratio = total_zstd_columnar_size / total_parquet_columnar_size * 100
        # print(f"  - Zstd-per-Column Data:      {total_zstd_columnar_size:>15,} bytes ({ratio:6.2f}%)")
        ratio = total_tambak_data_size / total_parquet_columnar_size * 100
        # print(f"  - tambak Columnar Data:     {total_tambak_data_size:>15,} bytes ({ratio:6.2f}%)")
    # print("="*80)

    # print("\n--- 🔬 PER-COLUMN DIAGNOSTICS ---")
    # print(f'{"Column":<20} {"tambak":>12} {"Zstd":>12} {"Parquet*":>12} {"Plan"}')
    # print(f'{"-"*20} {"-"*12} {"-"*12} {"-"*12} {"-"*40}')
    for name, results in all_column_results.items():
        p_size = results.get("tambak_size", "N/A")
        z_size = results.get("zstd_size", "N/A")
        pq_size = parquet_col_sizes.get(name, "N/A")
        plan = results.get("plan", "N/A")
        p_str = f"{p_size:,}" if isinstance(p_size, int) else str(p_size)
        z_str = f"{z_size:,}" if isinstance(z_size, int) else str(z_size)
        pq_str = f"{pq_size:,}" if isinstance(pq_size, int) else str(pq_size)
        # print(f"{name:<20} {p_str:>12} {z_str:>12} {pq_str:>12} {plan}")
    # print("-" * 80)
    # print("*Parquet size is the on-disk compressed size for that column's data chunks.")


if __name__ == "__main__":
    for i, aspect in enumerate(ASPECT_NAMES):
        if i == 0:
            with open(FILENAME, "w", encoding="utf-8") as fw:
                fw.write("")
        main(aspect)
    reorder_log_inplace(FILENAME)
