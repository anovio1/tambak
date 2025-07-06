# In: test_mpk_compression.py

import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import time
import sys
import logging
import json
import zstandard

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

try:
    import tambak_cache
except ImportError:
    logger.error(
        "Could not import 'tambak_cache'. Make sure it is installed correctly."
    )
    sys.exit(1)

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


def main(aspect_name):
    MPK_FILE_PATH = pathlib.Path(f"./{aspect_name}.mpk")
    PHX_OUTPUT_PATH = pathlib.Path(f"./{aspect_name}.phx")
    PARQUET_OUTPUT_PATH = pathlib.Path(f"./{aspect_name}.parquet")

    if not MPK_FILE_PATH.exists():
        logger.error(f"MPK file not found at '{MPK_FILE_PATH}'")
        return

    logger.info(
        f"--- Starting MPK Compression Test for: {MPK_FILE_PATH.name} (Aspect: {aspect_name}) ---"
    )
    mpk_bytes = MPK_FILE_PATH.read_bytes()
    logger.info(f"  - Loaded {len(mpk_bytes):,} bytes from disk.")
    logger.info("  - Decoding and transforming records...")
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
    logger.info(
        f"  - Transformation complete in {transform_time:.2f}s. Found {len(clean_data_list):,} records."
    )

    if not clean_data_list:
        logger.warning("  - No data to compress after transformation. Exiting.")
        return

    logger.info("  - Creating PyArrow Table...")
    try:
        arrow_table = pa.Table.from_pylist(clean_data_list)
    except Exception as e:
        logger.error(f"Failed to create Arrow Table. Error: {e}", exc_info=True)
        return

    # --- BENCHMARKING ---
    logger.info("  - Running all benchmarks...")

    # --- tambak Compression (NEW & IMPROVED METHOD) ---
    logger.info("    - Compressing with tambak stateful writer...")
    tambak_frame_size = -1
    tambak_compress_start = time.perf_counter()
    try:
        # 1. Create a RecordBatchReader from the table.
        source_reader = arrow_table.to_reader()

        # 2. Open the output file handle in binary write mode.
        with open(PHX_OUTPUT_PATH, "wb") as f:
            # 3. Create a tambak Compressor, passing it the file handle and a config.
            config = tambak_cache.CompressorConfig()
            compressor = tambak_cache.Compressor(f, config)

            # 4. Tell the compressor to run. This is the big moment.
            compressor.compress(source_reader)

        tambak_compress_time = time.perf_counter() - tambak_compress_start
        logger.info(
            f"    - tambak compression successful in {tambak_compress_time:.2f}s."
        )
        tambak_frame_size = PHX_OUTPUT_PATH.stat().st_size

    except Exception as e:
        logger.error(f"    - tambak compression FAILED: {e}", exc_info=True)

    # --- Zstd & Per-Column Diagnostics (UNCHANGED LOGIC FOR COMPARISON) ---
    all_column_results = {}
    zstd_compressor = zstandard.ZstdCompressor(level=3)

    for column_name in arrow_table.column_names:
        column_array = arrow_table.column(column_name)
        if isinstance(column_array, pa.ChunkedArray):
            column_array = column_array.combine_chunks()
        all_column_results[column_name] = {}
        try:
            analysis_result = tambak_cache.compress_analyze(column_array)
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

            zstd_compress_start = time.perf_counter()
            zstd_bytes = zstd_compressor.compress(column_data_bytes)
            zstd_compress_time = time.perf_counter() - zstd_compress_start
            logger.info(
                f"    - Zstd compression successful in {zstd_compress_time:.2f}s."
            )
            all_column_results[column_name]["zstd_size"] = len(zstd_bytes)
        except Exception:
            all_column_results[column_name]["zstd_size"] = "N/A"

    # --- CALCULATE TOTALS AND FILE SIZES ---

    # 1. On-Disk File Sizes
    zstd_on_mpk_bytes = zstd_compressor.compress(mpk_bytes)
    zstd_on_mpk_size = len(zstd_on_mpk_bytes)

    try:

        parquet_compress_start = time.perf_counter()
        pq.write_table(
            arrow_table, PARQUET_OUTPUT_PATH, compression="ZSTD", compression_level=3
        )
        parquet_compress_time = time.perf_counter() - parquet_compress_start
        logger.info(
            f"    - Parquet compression successful in {parquet_compress_time:.2f}s."
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
    print("\n" + "=" * 80)
    print(f"--- tambak {tambak_cache.__version__} {aspect_name} ---".center(80))
    print("=" * 80)

    print("\n" + "=" * 80)
    print("--- 📊 TOTAL ON-DISK FILE SIZE COMPARISON ---".center(80))
    print("--- (The 'CEO' View: Which final file is smallest?) ---".center(80))
    print("=" * 80)
    print(f"  - Original MPK File:         {len(mpk_bytes):>15,} bytes (100.00%)")
    print(
        f"  - Zstd on original MPK:      {zstd_on_mpk_size:>15,} bytes ({zstd_on_mpk_size/len(mpk_bytes)*100:6.2f}%)"
    )
    if parquet_file_size != -1:
        print(
            f"  - Parquet (Zstd) File:       {parquet_file_size:>15,} bytes ({parquet_file_size/len(mpk_bytes)*100:6.2f}%)"
        )
    if tambak_frame_size != -1:
        print(
            f"  - tambak Frame File (.phx): {tambak_frame_size:>15,} bytes ({tambak_frame_size/len(mpk_bytes)*100:6.2f}%)"
        )
    print("=" * 80)

    print("\n" + "=" * 80)
    print("--- 📈 TOTAL COLUMNAR DATA SIZE COMPARISON ---".center(80))
    print("--- (The 'Engineering' View: How effective is our logic?) ---".center(80))
    print("=" * 80)
    if total_parquet_columnar_size != -1:
        print(
            f"  - Parquet Columnar Data:     {total_parquet_columnar_size:>15,} bytes (100.00%)"
        )
        ratio = total_zstd_columnar_size / total_parquet_columnar_size * 100
        print(
            f"  - Zstd-per-Column Data:      {total_zstd_columnar_size:>15,} bytes ({ratio:6.2f}%)"
        )
        ratio = total_tambak_data_size / total_parquet_columnar_size * 100
        print(
            f"  - tambak Columnar Data:     {total_tambak_data_size:>15,} bytes ({ratio:6.2f}%)"
        )
    print("=" * 80)

    print("\n--- 🔬 PER-COLUMN DIAGNOSTICS ---")
    print(f'{"Column":<20} {"tambak":>12} {"Zstd":>12} {"Parquet*":>12} {"Plan"}')
    print(f'{"-"*20} {"-"*12} {"-"*12} {"-"*12} {"-"*40}')
    for name, results in all_column_results.items():
        p_size = results.get("tambak_size", "N/A")
        z_size = results.get("zstd_size", "N/A")
        pq_size = parquet_col_sizes.get(name, "N/A")
        plan = results.get("plan", "N/A")
        p_str = f"{p_size:,}" if isinstance(p_size, int) else str(p_size)
        z_str = f"{z_size:,}" if isinstance(z_size, int) else str(z_size)
        pq_str = f"{pq_size:,}" if isinstance(pq_size, int) else str(pq_size)
        print(f"{name:<20} {p_str:>12} {z_str:>12} {pq_str:>12} {plan}")
    print("-" * 80)
    print("*Parquet size is the on-disk compressed size for that column's data chunks.")


if __name__ == "__main__":
    for aspect in ASPECT_NAMES:
        main(aspect)
