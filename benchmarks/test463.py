# File: test463.py

import sys
import pathlib
import logging
import pyarrow as pa
import pandas as pd  # Optional, for pretty printing

# --- Setup ---
try:
    import tambak_cache
except ImportError:
    logging.error(
        "Could not import tambak_cache. Ensure it's installed or the path is correct."
    )
    sys.exit(1)

TUBUIN_PROCESSOR_PATH = (
    "V:/Github/tubuin-processor/src"  #! IMPORTANT: Configure this path
)
sys.path.append(TUBUIN_PROCESSOR_PATH)
try:
    from tubuin_processor.core.decoder import stream_decode_aspect
    from tubuin_processor.core.value_transformer import stream_transform_aspect
except ImportError as e:
    logging.error(
        f"Failed to import from tubuin-processor. Check TUBUIN_PROCESSOR_PATH. Error: {e}"
    )
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("RoundtripValidator")


def validate_roundtrip(aspect_name: str, strategy_name: str):
    """
    Performs a full compress/decompress roundtrip and validates data integrity
    for a given compression strategy.
    """
    logger.info(
        f"\n{'='*80}\n--- Validating Aspect: '{aspect_name}' with Strategy: '{strategy_name}' ---\n{'='*80}"
    )

    mpk_file_path = pathlib.Path(f"./{aspect_name}.mpk")
    phx_output_path = pathlib.Path(f"./{aspect_name}_{strategy_name}.phx")

    if not mpk_file_path.exists():
        logger.error(f"Source MPK file not found at '{mpk_file_path}'. Skipping.")
        return

    # --- 1. Load Data from Source of Truth ---
    logger.info(f"Step 1: Loading source data from '{mpk_file_path.name}'...")
    mpk_bytes = mpk_file_path.read_bytes()
    raw_model_stream = stream_decode_aspect(aspect_name, mpk_bytes)
    clean_model_stream = stream_transform_aspect(
        aspect_name, raw_model_stream, skip_on_error=True
    )
    clean_data_list = [record.model_dump() for record in clean_model_stream]

    if not clean_data_list:
        logger.warning("No data found after transformation. Skipping.")
        return

    original_table = pa.Table.from_pylist(clean_data_list)
    logger.info(f"-> Source data loaded: {original_table.num_rows} rows.")

    # --- 2. Configure and Compress Data ---
    logger.info(f"Step 2: Compressing data to '{phx_output_path.name}'...")

    # ==============================================================================
    # New Configuration
    # ==============================================================================
    compressor_kwargs = {}

    # --- STRATEGY SELECTION LOGIC ---
    if strategy_name == "partitioned":
        key = "unit_id" if "unit_id" in original_table.column_names else None
        if not key:
            logger.warning(
                "-> Cannot use 'partitioned' strategy: 'unit_id' column missing. Skipping."
            )
            return

        compressor_kwargs["time_series_strategy"] = "partitioned"
        compressor_kwargs["partition_key_column"] = key
        compressor_kwargs["partition_flush_rows"] = 50_000

    elif strategy_name == "per_batch_relinearize":
        key = "unit_id" if "unit_id" in original_table.column_names else None
        ts = "frame" if "frame" in original_table.column_names else None
        if not key or not ts:
            logger.warning(
                "-> Cannot use 'per_batch_relinearize' strategy: 'unit_id' or 'frame' missing. Skipping."
            )
            return

        compressor_kwargs["time_series_strategy"] = "per_batch_relinearize"
        compressor_kwargs["stream_id_column"] = key
        compressor_kwargs["timestamp_column"] = ts

    elif strategy_name == "none":
        compressor_kwargs["time_series_strategy"] = "none"
    else:
        logger.error(f"Unknown strategy: {strategy_name}")
        return

    try:
        with open(phx_output_path, "wb") as f:
            # dictionary of keyword arguments, unpacked with the `**` operator.
            compressor = tambak_cache.Compressor(f, **compressor_kwargs)

            reader = pa.RecordBatchReader.from_batches(
                original_table.schema, original_table.to_batches()
            )
            compressor.compress(reader)
        logger.info("-> Compression complete.")
    except Exception as e:
        logger.error(f"-> FAILED during compression phase: {e}", exc_info=True)
        return

    # --- 3. Decompress Data from Disk ---
    logger.info(f"Step 3: Decompressing data from '{phx_output_path.name}'...")

    try:
        with open(phx_output_path, "rb") as f:
            decompressor = tambak_cache.Decompressor(f)

            # --- DECOMPRESSION LOGIC PER STRATEGY ---
            if strategy_name == "partitioned":
                all_tables = [
                    partition_reader.read_all()
                    for _, partition_reader in decompressor.partitions()
                ]
                decompressed_table = (
                    pa.concat_tables(all_tables)
                    if all_tables
                    else pa.Table.from_pylist([])
                )
            else:  # For 'none' and 'per_batch_relinearize'
                reader = decompressor.batched()
                decompressed_table = reader.read_all()

        logger.info(
            f"-> Decompression complete. Reconstructed table with {decompressed_table.num_rows} rows."
        )
    except Exception as e:
        logger.error(f"-> FAILED during decompression phase: {e}", exc_info=True)
        return

    # --- 4. Validate Data Integrity ---
    logger.info("Step 4: Validating data integrity...")

    # --- VALIDATION LOGIC PER STRATEGY ---
    if strategy_name == "partitioned":
        # Partitioning breaks row order, so we must sort both tables to compare.
        logger.info(
            "-> Sorting tables for comparison (partitioning does not preserve order)."
        )
        sort_keys = [f.name for f in original_table.schema]
        original_to_compare = original_table.sort_by(
            [(key, "ascending") for key in sort_keys]
        )
        decompressed_to_compare = decompressed_table.sort_by(
            [(key, "ascending") for key in sort_keys]
        )
    else:
        # Streaming strategies preserve row order. No sort needed.
        logger.info("-> Comparing tables directly (streaming preserves order).")
        original_to_compare = original_table
        decompressed_to_compare = decompressed_table

    if original_to_compare.equals(decompressed_to_compare):
        logger.info("✅ SUCCESS: Roundtrip validation passed. Data is identical.")
    else:
        logger.error(
            "❌ FAILURE: Roundtrip validation failed. Data differs after decompression."
        )


if __name__ == "__main__":
    ASPECT_NAMES = ["unit_positions", "unit_events", "damage_log"]
    STRATEGIES_TO_TEST = [
        "none",
        "per_batch_relinearize",
        # "partitioned",
    ]

    for aspect in ASPECT_NAMES:
        for strategy in STRATEGIES_TO_TEST:
            validate_roundtrip(aspect, strategy)
