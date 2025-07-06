import pathlib
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import time
import sys
import logging
import json
import zstandard

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

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

try:
    import tambak_cache
except ImportError:
    logger.error("Could not import 'tambak_cache'. Make sure it is installed correctly.")
    sys.exit(1)

zstd_compressor = zstandard.ZstdCompressor(level=3)

ASPECT_NAMES = ["unit_positions", "damage_log", "unit_events"]
MAX_ROWS = None  # or whatever fixed limit you want

def simulate_multiplexed_compression(
    table,
    compress_fn,
    unit_id_col='unit_id',
    data_cols=None,
    max_units=None,
    max_rows_per_unit=None
):
    if data_cols is None:
        # default all columns except unit_id
        data_cols = [c for c in table.column_names if c != unit_id_col]

    total_compressed_size = 0
    unique_units = table[unit_id_col].unique().to_pylist()

    if max_units is not None:
        unique_units = unique_units[:max_units]

    logger.info(f"Simulating multiplexed compression on {len(unique_units)} units...")

    for unit in unique_units:
        mask = pc.equal(table[unit_id_col], unit)
        filtered_table = table.filter(mask)

        # Limit rows per unit
        if max_rows_per_unit is not None and filtered_table.num_rows > max_rows_per_unit:
            filtered_table = filtered_table.slice(0, max_rows_per_unit)

        for col_name in data_cols:
            array = filtered_table[col_name]
            if isinstance(array, pa.ChunkedArray):
                array = array.combine_chunks()
            try:
                compressed = compress_fn(array)
                total_compressed_size += len(compressed)
            except Exception as e:
                logger.warning(f"Compression failed for unit {unit} col {col_name}: {e}")

    return total_compressed_size

def get_raw_bytes_no_multiplex(table, data_cols=None):
    if data_cols is None:
        data_cols = table.column_names
    all_bytes = []
    for col_name in data_cols:
        arr = table[col_name]
        if isinstance(arr, pa.ChunkedArray):
            arr = arr.combine_chunks()
        buffers = arr.buffers()
        for buf in buffers:
            if buf is not None:
                all_bytes.append(buf.to_pybytes())
    return b"".join(all_bytes)

def zstd_compress_no_multiplex(table, data_cols=None):
    raw_bytes = get_raw_bytes_no_multiplex(table, data_cols)
    return zstd_compressor.compress(raw_bytes)

def tambak_compress(array):
    result = tambak_cache.compress_analyze(array)
    return result['artifact']


def zstd_compress(array):
    data_bytes = b"".join(b.to_pybytes() for b in array.buffers() if b is not None)
    return zstd_compressor.compress(data_bytes)


def main(aspect_name):
    MPK_FILE_PATH = pathlib.Path(f"./{aspect_name}.mpk")
    if not MPK_FILE_PATH.exists():
        logger.error(f"MPK file not found at '{MPK_FILE_PATH}'")
        return

    logger.info(f"Starting compression test for: {MPK_FILE_PATH.name} (Aspect: {aspect_name})")

    mpk_bytes = MPK_FILE_PATH.read_bytes()
    logger.info(f"Loaded {len(mpk_bytes):,} bytes from disk.")

    # Assuming you have your decoding & transforming here
    # For example (replace with your actual decoder):
    raw_model_stream = stream_decode_aspect(aspect_name, mpk_bytes)
    clean_model_stream = stream_transform_aspect(aspect_name, raw_model_stream, skip_on_error=True)
    clean_data_list = [record.model_dump() for record in clean_model_stream]

    # # For testing: Let's fake it with dummy data or you plug your real decoded data here
    # # clean_data_list = [...]

    # # Example: create dummy data to simulate structure
    # # For your real use, replace with actual cleaned data
    # import random
    # num_records = 10000
    
    # clean_data_list = []

    # # for i in range(num_records):
    # #     clean_data_list.append({
    # #         "unit_id": i % 50,  # 50 units
    # #         "x": random.randint(0, 1000),
    # #         "y": random.randint(0, 1000),
    # #         "z": random.randint(0, 1000),
    # #         "frame": i,
    # #     })
            
    # num_units = 50
    # num_frames = num_records
    # unit_positions = {uid: {'x': 0, 'y': 0, 'z': 0} for uid in range(num_units)}

    # for frame in range(num_frames):
    #     for uid in range(num_units):
    #         unit_positions[uid]['x'] += random.uniform(-1, 1)
    #         unit_positions[uid]['y'] += random.uniform(-1, 1)
    #         unit_positions[uid]['z'] += random.uniform(-1, 1)
    #         clean_data_list.append({
    #             'unit_id': uid,
    #             'x': unit_positions[uid]['x'],
    #             'y': unit_positions[uid]['y'],
    #             'z': unit_positions[uid]['z'],
    #             'frame': frame,
    #         })

    arrow_table = pa.Table.from_pylist(clean_data_list)


    # # Get first 50 rows as a PyArrow Table slice
    # top_50_table = arrow_table.slice(0, 100)
    # # Convert to Python dicts (list of dicts) for easy printing
    # top_50_list = top_50_table.to_pylist()
    # # Print nicely
    # for row in top_50_list:
    #     print(row)


    # Limit rows for both compressors:
    print(f"length before max rows slice: {arrow_table.num_rows}")
    if MAX_ROWS:
        if arrow_table.num_rows > MAX_ROWS:
            arrow_table = arrow_table.slice(0, MAX_ROWS)
    print(f" length after max rows slice: {arrow_table.num_rows}")
    

    logger.info(f"\n")
    print(f"Aspect {aspect_name}")
    # Full compress entire columns (baseline)
    tambak_total_size = 0
    zstd_total_size = 0
    for col_name in arrow_table.column_names:
        array = arrow_table[col_name]
        if isinstance(array, pa.ChunkedArray):
            array = array.combine_chunks()
        try:
            tambak_result = tambak_compress(array)
            tambak_total_size += len(tambak_result)
        except Exception:
            pass
        try:
            zstd_bytes = zstd_compress(array)
            zstd_total_size += len(zstd_bytes)
        except Exception:
            pass

    logger.info(f"Full tambak compress total size: {tambak_total_size:,} bytes")
    logger.info(f"Full Zstd compress total size: {zstd_total_size:,} bytes")

    # # Multiplexed compress, limiting to first 10 units and 200 rows each
    # multiplexed_tambak_size = simulate_multiplexed_compression(
    #     arrow_table,
    #     tambak_compress,
    #     unit_id_col='unit_id',
    #     data_cols=['x', 'y', 'z', 'frame'],
    #     max_units=1000,
    #     max_rows_per_unit=None,
    # )

    # multiplexed_zstd_size = simulate_multiplexed_compression(
    #     arrow_table,
    #     zstd_compress,
    #     unit_id_col='unit_id',
    #     data_cols=['x', 'y', 'z', 'frame'],
    #     max_units=100,
    #     max_rows_per_unit=None,
    # )
    # === Surgical patch: reorder by unit_id and frame ===
    sort_keys = [("unit_id", "ascending"), ("frame", "ascending")]
    if aspect_name == "damage_log":
        sort_keys = [("victim_unit_id", "ascending"), ("frame", "ascending")]

    # Get sort order indices (remap) - original indices after reorder
    remap_indices = pc.sort_indices(arrow_table, sort_keys=sort_keys).to_numpy()

    # Reorder the table by remap indices
    reordered_table = arrow_table.take(pa.array(remap_indices))

    print(f" length after reorder: {reordered_table.num_rows}")
    # Log first 20 remap indices for sanity check
    logger.info(f"Remap indices (first 20): {remap_indices[:20]}")
    
    batch_tambak_size = 0
    batch_zstd_size = 0
    BATCH_SIZE = 10_000
    num_rows = reordered_table.num_rows

    for batch_start in range(0, num_rows, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, num_rows)
        batch_table = reordered_table.slice(batch_start, batch_end - batch_start)

        for col_name in batch_table.column_names:
            arr = batch_table[col_name]
            if isinstance(arr, pa.ChunkedArray):
                arr = arr.combine_chunks()
            try:
                tambak_result = tambak_compress(arr)
                batch_tambak_size += len(tambak_result)
            except Exception:
                logger.warning(f"tambak compression failed for column: {col_name} in batch {batch_start}-{batch_end}")
            try:
                zstd_bytes = zstd_compress(arr)
                batch_zstd_size += len(zstd_bytes)
            except Exception:
                logger.warning(f"Zstd compression failed for column: {col_name} in batch {batch_start}-{batch_end}")

    logger.info(f"Batch-wise Reordered tambak compress total size: {batch_tambak_size:,} bytes")
    logger.info(f"Batch-wise Reordered Zstd compress total size: {batch_zstd_size:,} bytes")

    # Compress full reordered columns at once
    multiplexed_tambak_size = 0
    multiplexed_zstd_size = 0
    # for col_name in ['x', 'y', 'z', 'frame']:
    for col_name in reordered_table.column_names:
        arr = reordered_table[col_name]
        if isinstance(arr, pa.ChunkedArray):
            arr = arr.combine_chunks()
        try:
            tambak_result = tambak_compress(arr)
            multiplexed_tambak_size += len(tambak_result)
        except Exception:
            logger.warning(f"tambak compression failed for column: {col_name}")
        try:
            zstd_bytes = zstd_compress(arr)
            multiplexed_zstd_size += len(zstd_bytes)
        except Exception:
            logger.warning(f"Zstd compression failed for column: {col_name}")

    logger.info(f"Reordered tambak compress total size: {multiplexed_tambak_size:,} bytes")
    logger.info(f"Reordered Zstd compress total size: {multiplexed_zstd_size:,} bytes")

    try:
        zstd_no_multiplex_bytes = zstd_compress_no_multiplex(arrow_table, data_cols=arrow_table.column_names)
        zstd_no_multiplex_size = len(zstd_no_multiplex_bytes)
    except Exception as e:
        logger.warning(f"Zstd no multiplex compression failed: {e}")
        zstd_no_multiplex_size = -1

    logger.info(f"Multiplexed tambak compress total size (None units, 200 rows each): {multiplexed_tambak_size:,} bytes")
    logger.info(f"Multiplexed Zstd compress total size (None units, 200 rows each): {multiplexed_zstd_size:,} bytes")
    logger.info(f"Zstd no multiplex compress total size: {zstd_no_multiplex_size:,} bytes")

if __name__ == "__main__":
    for aspect in ASPECT_NAMES:
        main(aspect)
