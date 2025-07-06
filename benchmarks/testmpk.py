# In: test_mpk.py

import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import time
import sys
import logging
import json
import zstandard

ASPECT_NAMES = [
    "unit_positions",
    "unit_events",
    "damage_log",
    "commands_log",
    "construction_log",
    "scouting_log",
    "team_stats",
    "unit_economy",
    "unit_state_snapshots"
]

import csv # Make sure to add this import at the top of your script

def save_summary_csv(all_results, filename):
    """
    Saves a summary of the on-disk file size comparison to a CSV file.

    Args:
        all_results (list[dict]): A list of result dictionaries, one for each aspect.
        filename (str): The path to the output CSV file.
    """
    if not all_results:
        print("No results to save to CSV.")
        return

    # --- Dynamically determine all possible column headers ---
    # This ensures that if a strategy is added or fails for one aspect,
    # the CSV columns remain consistent.
    
    # Base headers are always present
    headers = ["Aspect", "Original MPK (bytes)", "Original MPK (%)"]
    
    # Standard formats
    standard_formats = {
        "zstd_on_mpk_size": "Zstd on original MPK",
        "parquet_file_size": "Parquet (Zstd) File",
        "tambak_frame_size": "tambak Stitched File (.phx)" # This is the stateless one
    }
    
    # Discover all unique tambak strategies from the results
    tambak_strategies = set()
    for result in all_results:
        tambak_strategies.update(result.get('tambak_strategy_sizes', {}).keys())
    
    # Sort for consistent ordering
    sorted_strategies = sorted(list(tambak_strategies))

    # Build the final header list
    for key, name in standard_formats.items():
        headers.extend([f"{name} (bytes)", f"{name} (%)"])
        
    for strategy_name in sorted_strategies:
        label = strategy_name
        if strategy_name == "per_batch_relinearize":
            label = "relin"
        headers.extend([f"tambak {label} File (.phx) (bytes)", f"tambak {label} File (.phx) (%)"])

    # --- Write data to CSV ---
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)

            for result in all_results:
                row = [result['aspect_name']]
                original_size = result.get('mpk_bytes_len')

                if original_size is None or original_size == 0:
                    # If there's no original, we can't calculate percentages
                    row.extend(['N/A'] * (len(headers) - 1))
                    writer.writerow(row)
                    continue
                
                # Original MPK
                row.extend([original_size, "100.00"])

                # Helper to add size and percentage to the row
                def add_cells(size_key):
                    size = result.get(size_key, -1)
                    if size is not None and size > 0:
                        percentage = f"{(size / original_size * 100):.2f}"
                        row.extend([size, percentage])
                    else:
                        row.extend(["N/A", "N/A"])

                # Add cells for standard formats
                for key in standard_formats:
                    add_cells(key)
                
                # Add cells for dynamic tambak strategies
                strategy_sizes = result.get('tambak_strategy_sizes', {})
                for strategy_name in sorted_strategies:
                    size = strategy_sizes.get(strategy_name, -1)
                    if size is not None and size > 0:
                         percentage = f"{(size / original_size * 100):.2f}"
                         row.extend([size, percentage])
                    else:
                        row.extend(["N/A", "N/A"])

                writer.writerow(row)

        logger.info(f"✅ Successfully wrote summary CSV to: {filename}")

    except IOError as e:
        logger.error(f"Failed to write CSV file: {e}", exc_info=True)

def save_and_print_report(
    aspect_name,
    tambak_cache_version,
    mpk_bytes_len,
    zstd_on_mpk_size,
    parquet_file_size,
    tambak_strategy_sizes,
    tambak_frame_size,
    total_parquet_columnar_size,
    total_zstd_columnar_size,
    total_tambak_data_size,
    all_column_results,
    parquet_col_sizes,
):

    filename = f"test_results_{tambak_cache.__version__}.txt"
    with open(filename, "a", encoding="utf-8") as f:

        def dual_print(*args, **kwargs):
            print(*args, **kwargs)  # print to console
            print(*args, **kwargs, file=f)  # print to file

        left_len = 40

        dual_print("\n" + "=" * 80)
        dual_print(f"--- tambak {tambak_cache_version} {aspect_name} ---".center(80))
        dual_print("=" * 80)

        dual_print("\n" + "=" * 80)
        dual_print("--- 📊 TOTAL ON-DISK FILE SIZE COMPARISON ---".center(80))
        dual_print("--- (The 'CEO' View: Which final file is smallest?) ---".center(80))
        dual_print("=" * 80)
        dual_print(
            f"{'  - Original MPK File:':<{left_len}} {mpk_bytes_len:>15,} bytes (100.00%)"
        )
        dual_print(
            f"{'  - Zstd on original MPK:':<{left_len}} {zstd_on_mpk_size:>15,} bytes ({zstd_on_mpk_size/mpk_bytes_len*100:6.2f}%)"
        )
        if parquet_file_size != -1:
            dual_print(
                f"{'  - Parquet (Zstd) File:':<{left_len}} {parquet_file_size:>15,} bytes ({parquet_file_size/mpk_bytes_len*100:6.2f}%)"
            )
        dual_print(
            f"{'  - tambak Stitched File (.phx):':<{left_len}} {tambak_frame_size:>15,} bytes ({tambak_frame_size/mpk_bytes_len*100:6.2f}%)"
        )
        for strategy_name in tambak_strategy_sizes:
            size = tambak_strategy_sizes[strategy_name]
            if size is None or size <= 0 or strategy_name is None:
                continue
            label = strategy_name
            if strategy_name == "per_batch_relinearize":
                label = "relin"
            print_string = f"  - tambak {label} File (.phx):"
            dual_print(
                f"{print_string:<{left_len}} {size:>15,} bytes ({size/mpk_bytes_len*100:6.2f}%)"
            )
        dual_print("=" * 80)

        dual_print("\n" + "=" * 80)
        dual_print("--- 📈 TOTAL COLUMNAR DATA SIZE COMPARISON ---".center(80))
        dual_print(
            "--- (The 'Engineering' View: How effective is our logic?) ---".center(80)
        )
        dual_print("=" * 80)
        if total_parquet_columnar_size != -1:
            dual_print(
                f"  - Parquet Columnar Data:     {total_parquet_columnar_size:>15,} bytes (100.00%)"
            )
            ratio = total_zstd_columnar_size / total_parquet_columnar_size * 100
            dual_print(
                f"  - Zstd-per-Column Data:      {total_zstd_columnar_size:>15,} bytes ({ratio:6.2f}%)"
            )
            ratio = total_tambak_data_size / total_parquet_columnar_size * 100
            dual_print(
                f"  - tambak Columnar Data:     {total_tambak_data_size:>15,} bytes ({ratio:6.2f}%)"
            )
        dual_print("=" * 80)

        dual_print("\n--- 🔬 PER-COLUMN DIAGNOSTICS ---")
        dual_print(
            f'{"Column":<20} {"tambak":>12} {"Zstd":>12} {"Parquet*":>12} {"Plan"}'
        )
        dual_print(f'{"-"*20} {"-"*12} {"-"*12} {"-"*12} {"-"*40}')
        for name, results in all_column_results.items():
            p_size = results.get("tambak_size", "N/A")
            z_size = results.get("zstd_size", "N/A")
            pq_size = parquet_col_sizes.get(name, "N/A")
            plan = results.get("plan", "N/A")
            p_str = f"{p_size:,}" if isinstance(p_size, int) else str(p_size)
            z_str = f"{z_size:,}" if isinstance(z_size, int) else str(z_size)
            pq_str = f"{pq_size:,}" if isinstance(pq_size, int) else str(pq_size)
            dual_print(f"{name:<20} {p_str:>12} {z_str:>12} {pq_str:>12} {plan}")
        dual_print("-" * 80)
        dual_print(
            "*Parquet size is the on-disk compressed size for that column's data chunks."
        )


# ... (All setup and helper functions are the same) ...
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
    logger.info(f"✅ Successfully wrote tambak frame to: {output_path}")


def main(aspect_name):
    MPK_FILE_PATH = pathlib.Path(f"./{aspect_name}.mpk")
    PHX_OUTPUT_PATH = pathlib.Path(f"./{aspect_name}.phx")
    PARQUET_OUTPUT_PATH = pathlib.Path(f"./{aspect_name}.parquet")
    # ... (Loading and transforming data is the same) ...
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

    all_column_results = {}
    tambak_compressed_columns = {}
    zstd_compressor = zstandard.ZstdCompressor(level=3)

    for column_name in arrow_table.column_names:
        # ... (This loop is unchanged, it populates all_column_results) ...
        column_array = arrow_table.column(column_name)
        if isinstance(column_array, pa.ChunkedArray):
            column_array = column_array.combine_chunks()
        all_column_results[column_name] = {}
        try:

            analysis_result = tambak_cache.compress_analyze(column_array)
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

    strategies_to_test = ["none", "per_batch_relinearize"]  # , "partitioned"]
    tambak_strategy_sizes = {}

    for strategy_name in strategies_to_test:
        logger.info(f"  -> Testing stateful tambak strategy: '{strategy_name}'...")
        phx_output_path = pathlib.Path(f"./{aspect_name}_{strategy_name}.phx")

        # 1. Build a dictionary of keyword arguments for the Compressor.
        # This replaces the old `CompressorConfig` object.
        compressor_kwargs = {}

        if strategy_name == "partitioned":
            key = "unit_id" if "unit_id" in arrow_table.column_names else None
            if not key:
                logger.warning("     -> Skipping 'partitioned': 'unit_id' column missing.")
                tambak_strategy_sizes[strategy_name] = -1
                continue
            
            compressor_kwargs["time_series_strategy"] = "partitioned"
            compressor_kwargs["partition_key_column"] = key
            # We can also configure other parameters like flush rows now.
            compressor_kwargs["partition_flush_rows"] = 50_000 

        elif strategy_name == "per_batch_relinearize":
            # This logic for finding the right column names is preserved.
            key = "unit_id" if "unit_id" in arrow_table.column_names else None
            ts = "frame" if "frame" in arrow_table.column_names else None
            if aspect_name == "damage_log":
                key = "victim_unit_id" if "victim_unit_id" in arrow_table.column_names else None
            if aspect_name == "construction_log":
                key = "builder_unit_id" if "builder_unit_id" in arrow_table.column_names else None
                
            if not key or not ts:
                logger.warning(f"     -> Skipping 'per_batch_relinearize': 'unit_id' or 'frame' missing.")
                tambak_strategy_sizes[strategy_name] = -1
                continue

            compressor_kwargs["time_series_strategy"] = "per_batch_relinearize"
            compressor_kwargs["stream_id_column"] = key
            compressor_kwargs["timestamp_column"] = ts
            
        elif strategy_name == "none":
            # For the default "none" strategy, we don't need to add any special kwargs.
            # The default values in Rust will be used.
            compressor_kwargs["time_series_strategy"] = "none"

        # 2. Run the stateful compressor with the new API.
        try:
            with open(phx_output_path, "wb") as f:
                # The `**` operator unpacks our dictionary into keyword arguments.
                compressor = tambak_cache.Compressor(f, **compressor_kwargs)
                reader = pa.RecordBatchReader.from_batches(arrow_table.schema, arrow_table.to_batches())
                compressor.compress(reader)

            # 3. Record the final on-disk size.
            tambak_strategy_sizes[strategy_name] = phx_output_path.stat().st_size
            logger.info(f"     -> Generated '{phx_output_path.name}': {tambak_strategy_sizes[strategy_name]:,} bytes")
        except Exception as e:
            logger.error(f"     -> FAILED to generate file for strategy '{strategy_name}': {e}", exc_info=True)
            tambak_strategy_sizes[strategy_name] = -1

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
    save_and_print_report(
        aspect_name=aspect_name,
        tambak_cache_version=tambak_cache.__version__,
        mpk_bytes_len=len(mpk_bytes),
        zstd_on_mpk_size=zstd_on_mpk_size,
        parquet_file_size=parquet_file_size,
        tambak_strategy_sizes=tambak_strategy_sizes,
        tambak_frame_size=tambak_frame_size,
        total_parquet_columnar_size=total_parquet_columnar_size,
        total_zstd_columnar_size=total_zstd_columnar_size,
        total_tambak_data_size=total_tambak_data_size,
        all_column_results=all_column_results,
        parquet_col_sizes=parquet_col_sizes,
    )
    results_for_csv = {
        "aspect_name": aspect_name,
        "mpk_bytes_len": len(mpk_bytes),
        "zstd_on_mpk_size": zstd_on_mpk_size,
        "parquet_file_size": parquet_file_size,
        "tambak_frame_size": tambak_frame_size,
        "tambak_strategy_sizes": tambak_strategy_sizes,
    }
    
    return results_for_csv # <-- Just return the data
    


if __name__ == "__main__":
    filename = f"test_results_{tambak_cache.__version__}.txt"
    csv_summary_filename = f"test_results_summary_{tambak_cache.__version__}.csv"

    # Clear file once
    all_results_for_csv = []
    with open(filename, "w", encoding="utf-8") as f:
        pass
    for aspect in ASPECT_NAMES:
        result_from_main = main(aspect)
        
        if result_from_main:
            all_results_for_csv.append(result_from_main)

    if all_results_for_csv:
        save_summary_csv(all_results_for_csv, csv_summary_filename)
