# In: test_mpk_compression.py

import pathlib
import pyarrow as pa
import time
import sys
import logging
import json
import zstandard # <-- NEW: Import the zstandard library

# --- Configure logging to see warnings ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# This assumes your phoenix_cache library is installed and importable
try:
    import phoenix_cache
except ImportError:
    logger.error("Could not import 'phoenix_cache'. Make sure it is installed correctly.")
    sys.exit(1)

# --- IMPORTANT ---
# Add the processor's 'src' directory to the Python path.
TUBUIN_PROCESSOR_PATH = "V:/Github/tubuin-processor/src" # Correct path to 'src'
sys.path.append(TUBUIN_PROCESSOR_PATH)

try:
    from tubuin_processor.core.decoder import stream_decode_aspect
    from tubuin_processor.core.value_transformer import stream_transform_aspect
except ImportError as e:
    logger.error(f"Failed to import from tubuin-processor. Check the TUBUIN_PROCESSOR_PATH. Error: {e}")
    sys.exit(1)

# --- CONFIGURATION ---
ASPECT_NAME = "unit_positions" 
MPK_FILE_PATH = pathlib.Path(f"./{ASPECT_NAME}.mpk")
PHX_OUTPUT_PATH = pathlib.Path(f"./{ASPECT_NAME}.phx") # <-- NEW: Output path for our format
ZSTD_OUTPUT_PATH = pathlib.Path(f"./{ASPECT_NAME}.zst") # <-- NEW: Output path for our format

# --- NEW: Function to create our .phx frame file ---
def write_phoenix_frame(output_path: pathlib.Path, compressed_columns: dict):
    """
    Serializes compressed columns into a simple .phx frame format.
    Format: [Header (4B)] [ToC Length (4B)] [JSON ToC] [Data Blobs]
    """
    header = b'PHX1'
    toc = {}
    data_blobs = []
    
    current_offset = 0
    for name, data in compressed_columns.items():
        toc[name] = {
            "offset": current_offset,
            "size": len(data)
        }
        current_offset += len(data)
        data_blobs.append(data)
        
    toc_json_bytes = json.dumps(toc, indent=2).encode('utf-8')
    toc_len_bytes = len(toc_json_bytes).to_bytes(4, 'little')

    with open(output_path, 'wb') as f:
        f.write(header)
        f.write(toc_len_bytes)
        f.write(toc_json_bytes)
        for blob in data_blobs:
            f.write(blob)
    
    logger.info(f"✅ Successfully wrote Phoenix frame to: {output_path}")
    

def write_zstd_baseline(output_path: pathlib.Path, original_bytes: bytes):
    """Compresses the original bytes with Zstd and writes to a file."""
    logger.info(f"  - Calculating and writing Zstd baseline...")
    try:
        zstd_compressor = zstandard.ZstdCompressor(level=3) # Using level 3 for a good balance
        zstd_compressed_bytes = zstd_compressor.compress(original_bytes)
        
        output_path.write_bytes(zstd_compressed_bytes)
        logger.info(f"✅ Successfully wrote Zstd baseline to: {output_path}")
        return zstd_compressed_bytes
    except Exception as e:
        logger.error(f"Failed to create Zstd baseline file. Error: {e}")
        return None

def main():
    """
    Main function to load, process, and compress a single MPK file.
    """
    
    if not MPK_FILE_PATH.exists():
        logger.error(f"MPK file not found at '{MPK_FILE_PATH}'")
        return

    logger.info(f"--- Starting MPK Compression Test for: {MPK_FILE_PATH.name} (Aspect: {ASPECT_NAME}) ---")

    # 1. INGESTION
    mpk_bytes = MPK_FILE_PATH.read_bytes()
    logger.info(f"  - Loaded {len(mpk_bytes):,} bytes from disk.")

    # 2. DECODE & TRANSFORM
    logger.info("  - Decoding and transforming records...")
    start_time = time.perf_counter()
    try:
        raw_model_stream = stream_decode_aspect(ASPECT_NAME, mpk_bytes)
        clean_model_stream = stream_transform_aspect(ASPECT_NAME, raw_model_stream, skip_on_error=True)
        clean_data_list = [record.model_dump() for record in clean_model_stream]
    except Exception as e:
        logger.error(f"An error occurred during the decode/transform phase: {e}", exc_info=True)
        return
    transform_time = time.perf_counter() - start_time
    logger.info(f"  - Transformation complete in {transform_time:.2f}s. Found {len(clean_data_list):,} records.")

    if not clean_data_list:
        logger.warning("  - No data to compress after transformation. Exiting.")
        return

    logger.info("  - Creating PyArrow Table...")
    try:
        arrow_table = pa.Table.from_pylist(clean_data_list)
    except Exception as e:
        logger.error(f"Failed to create Arrow Table. Error: {e}", exc_info=True)
        return

    # 4. COMPRESSION & BENCHMARKING
    logger.info("  - Compressing each column and calculating baselines...")

    # Dictionaries to store results
    phoenix_compressed_columns = {}
    all_column_results = {} # A single dictionary to hold all results per column

    zstd_compressor = zstandard.ZstdCompressor(level=3)

    for column_name in arrow_table.column_names:
        column_array = arrow_table.column(column_name)
        if isinstance(column_array, pa.ChunkedArray):
            column_array = column_array.combine_chunks()

        # Initialize results for this column
        all_column_results[column_name] = {
            "phoenix_size": "N/A (Failed)",
            "zstd_size": "N/A (Skipped)",
            "plan": "N/A"
        }

        # --- PHOENIX PATH ---
        try:
            logger.info(f"    - [Phoenix] Compressing column: '{column_name}'...")
            
            # --- THIS IS THE KEY CHANGE ---
            # Use compress_analyze to get the plan and other metadata
            analysis_result = phoenix_cache.compress_analyze(column_array)
            phoenix_bytes = analysis_result['artifact']
            
            phoenix_compressed_columns[column_name] = phoenix_bytes
            all_column_results[column_name]["phoenix_size"] = len(phoenix_bytes)
            all_column_results[column_name]["plan"] = analysis_result['plan']
            all_column_results[column_name]["original_type"] = analysis_result['original_type']

        except Exception as e:
            logger.warning(f"    - ⚠️  [Phoenix] SKIPPING column '{column_name}': {e}")
            continue # Skip to next column if Phoenix fails

        # --- ZSTD COLUMNAR BASELINE PATH ---
        try:
            logger.info(f"    - [Zstd] Compressing column: '{column_name}'...")
            column_data_bytes = b"".join(b.to_pybytes() for b in column_array.buffers() if b is not None)
            zstd_bytes = zstd_compressor.compress(column_data_bytes)
            all_column_results[column_name]["zstd_size"] = len(zstd_bytes)

        except Exception as e:
            logger.error(f"    - ❌ [Zstd] Failed for column '{column_name}': {e}")
            all_column_results[column_name]["zstd_size"] = "N/A (Failed)"
        
    # --- FINAL AGGREGATION AND REPORTING ---
    write_phoenix_frame(PHX_OUTPUT_PATH, phoenix_compressed_columns)

    total_phoenix_size = sum(v['phoenix_size'] for v in all_column_results.values() if isinstance(v['phoenix_size'], int))
    total_zstd_columnar_size = sum(v['zstd_size'] for v in all_column_results.values() if isinstance(v['zstd_size'], int))

    print("\n--- 📊 SIMULATION RESULTS (Columnar vs. Columnar) ---")
    print(f"  - Total Zstd Columnar Size:  {total_zstd_columnar_size:,} bytes")
    print(f"  - Total Phoenix Columnar Size: {total_phoenix_size:,} bytes")
    print("------------------------------------------------------\n")

    if total_phoenix_size < total_zstd_columnar_size:
        savings = total_zstd_columnar_size - total_phoenix_size
        improvement = (savings / total_zstd_columnar_size) * 100
        print(f"🏆 SUCCESS: Phoenix pipeline is {improvement:.2f}% smaller than Zstd-per-column.")
    else:
        penalty = total_phoenix_size - total_zstd_columnar_size
        worse_by = (penalty / total_zstd_columnar_size) * 100
        print(f"⚠️  INSIGHT: Phoenix pipeline is {worse_by:.2f}% larger than Zstd-per-column.")
        print("    (This suggests the planner's heuristics need improvement for this data type).")

    # --- NEW, ENHANCED PER-COLUMN PRINTOUT ---
    print("\n--- 🔬 PER-COLUMN BREAKDOWN ---")
    # Header
    print(f'{"Column":<20} {"Phoenix":>12} {"Zstd":>12} {"Diff":>12} {"%":>8} {"Plan"} {"Original Type"}')
    print(f'{"-"*20} {"-"*12} {"-"*12} {"-"*12} {"-"*8} {"-"*20}')

    for name, results in all_column_results.items():
        p_size = results.get("phoenix_size")
        z_size = results.get("zstd_size")
        plan = results.get("plan", "N/A")
        original_type = results.get("original_type", "N/A")

        # Format for printing, handling N/A cases
        p_str = f"{p_size:,}" if isinstance(p_size, int) else str(p_size)
        z_str = f"{z_size:,}" if isinstance(z_size, int) else str(z_size)
        
        if isinstance(p_size, int) and isinstance(z_size, int):
            diff = p_size - z_size
            diff_str = f"{diff:,}"
            pct = (p_size / z_size * 100) if z_size > 0 else 0
            pct_str = f"{pct:.2f}%"
        else:
            diff_str = "N/A"
            pct_str = "N/A"
            
        print(f"{name:<20} {p_str:>12} {z_str:>12} {diff_str:>12} {pct_str:>8} {plan} {original_type}")
    
    print("-" * 80)

if __name__ == "__main__":
    main()