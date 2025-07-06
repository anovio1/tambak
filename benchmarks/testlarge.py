import math
import random
import zstandard as zstd
import pyarrow as pa
import tambak_cache  # The name of your Rust module
import time


def run_zstd_only_test(test_name: str, original_array: pa.Array):
    """
    Compress the raw data buffer (and null bitmap) directly with zstd only, to compare
    against the tambak_cache compression pipeline.
    """
    print(f"\n--- Starting ZSTD Only Test Case: {test_name} ---")
    print(f"Data Type: {original_array.type}, Length: {len(original_array)}")

    # Get buffers: [0]=null bitmap, [1]=data
    buffers = original_array.buffers()
    null_buf = buffers[0]
    data_buf = buffers[1]

    null_bytes = null_buf.to_pybytes() if null_buf else b""
    data_bytes = data_buf.to_pybytes()

    # Concatenate to be fair (include null bitmap)
    full_bytes = null_bytes + data_bytes

    original_size = len(full_bytes)

    # Compress with zstd
    t0 = time.perf_counter()
    cctx = zstd.ZstdCompressor(level=3)
    compressed = cctx.compress(full_bytes)
    t1 = time.perf_counter()
    compressed_size = len(compressed)

    ratio = compressed_size / original_size
    percent = ratio * 100

    print(f"Original size (bytes): {original_size}")
    print(f"Compressed size (bytes): {compressed_size}")
    print(f"Compression ratio: {ratio:.5f}x")
    print(f"                 %: {percent:.2f}%")
    print(f"⏱ ZSTD compression time: {(t1 - t0) * 1000:.2f} ms")
    print("-" * 20)


def run_compression_test(test_name: str, original_array: pa.Array):
    """
    A generic function to run a full compress/decompress roundtrip test.

    Args:
        test_name: A descriptive name for the test case.
        original_array: The PyArrow Array to be tested.
    """
    print(f"\n--- Starting Test Case: {test_name} ---")
    print(f"Data Type: {original_array.type}, Length: {len(original_array)}")

    # For small arrays, print the data. For large ones, just show a snippet.
    if len(original_array) < 20:
        print("Original Array:")
        print(" ".join(map(str, (v.as_py() for v in original_array))))
    else:
        print("Original Array (snippet):")
        print(" ".join(map(str, (v.as_py() for v in original_array[:10]))))

    # Calculate original size, accounting for the validity bitmap.
    original_size_bytes = original_array.nbytes
    print(f"Original data size (bytes): {original_size_bytes}")
    print("-" * 20)

    # --- THIS IS THE KEY CHANGE ---
    # We will get the artifact from the analytics function and use it for all subsequent steps.
    compressed_artifact_bytes = None

    # 1. COMPRESSION
    try:
        t0 = time.perf_counter()
        stats = tambak_cache.compress_analyze(original_array)
        compressed_artifact_bytes = stats["artifact"]  # Assign the artifact here
        t1 = time.perf_counter()

        print(f"\n✅ Compression successful!: {test_name}")
        # Analytics
        print("--- Analytics Breakdown ---")
        # --- ADD THIS LINE ---
        print(f"  - Compression Plan: {stats['plan']}")
        print(f"  -  Original Data Size:       {original_size_bytes:>10} bytes")
        print(f"  - Total Artifact Size:       {stats['total_size']:>10} bytes")
        print(f"  -     Metadata (Header) Size:{stats['header_size']:>10} bytes")
        print(f"  -     Compressed Data Size:  {stats['data_size']:>10} bytes")

        if original_size_bytes > 0 and stats["total_size"] > 0:
            ratio_artifact = stats["total_size"] / original_size_bytes
            percent_artifact = ratio_artifact * 100
            print(f"  -  Artifact : Original, Ratio: {ratio_artifact:.5f}x")
            print(f"  -                           %: {percent_artifact:.2f}%")

            ratio_data = (
                stats["data_size"] / original_size_bytes
                if stats["data_size"] > 0
                else 0
            )
            percent_data = ratio_data * 100
            print(f"  - Data Only : Original, Ratio: {ratio_data:.5f}x")
            print(f"  -                           %: {percent_data:.2f}%")
        print(f"⏱ Compression time: {(t1 - t0) * 1000:.2f} ms")
        print("-" * 20)

    except Exception as e:
        print(f"❌ FAILURE: An error occurred during analytics compression: {e}")
        return

    # 2. DECOMPRESSION
    try:
        # The type string must match what the Rust dispatcher expects (e.g., "Int64")
        # We can derive this from the pyarrow type object.
        t2 = time.perf_counter()

        RUST_TYPE_MAP = {
            "int8": "Int8", "int16": "Int16", "int32": "Int32", "int64": "Int64",
            "uint8": "UInt8", "uint16": "UInt16", "uint32": "UInt32", "uint64": "UInt64",
            "float": "Float32",
            "double": "Float64",
            "bool": "Boolean",
        }

        type_name = str(original_array.type)
        original_type_str = RUST_TYPE_MAP.get(type_name)

        if original_type_str is None:
            raise ValueError(f"Unsupported pyarrow type for tambak_cache: {type_name}")

        decompressed_array = tambak_cache.decompress(
            compressed_artifact_bytes
        )

        t3 = time.perf_counter()
        print("Decompression successful!")
        print(f"⏱ Decompression time: {(t3 - t2) * 1000:.2f} ms")

    except Exception as e:
        print(f"❌ FAILURE: An error occurred during decompression: {e}")
        return

    # 3. VERIFICATION
    print("--- Verification ---")
    if original_array.equals(decompressed_array):
        print(
            f"✅ SUCCESS: Decompressed array is identical to the original for '{test_name}'."
        )
    else:
        print(
            f"❌ FAILURE: Decompressed array does NOT match the original for '{test_name}'."
        )

    if len(original_array) < 20:
        # Convert to Python values and string representations
        orig_values = [v.as_py() for v in original_array]
        decomp_values = [v.as_py() for v in decompressed_array]

        # Combine to compute max width
        all_strs = [str(val) for val in orig_values + decomp_values]
        field_width = max(len(s) for s in all_strs) if len(all_strs) > 0 else 5

        print("Original Array:")
        print(" ".join(f"{str(val):>{field_width}}" for val in orig_values))

        print("Decompressed Array:")
        print(" ".join(f"{str(val):>{field_width}}" for val in decomp_values))
    else:
        print("Original Array (snippet):")
        print(" ".join(map(str, (v.as_py() for v in original_array[:10]))))

        print("Decompressed Array (snippet):")
        print(" ".join(map(str, (v.as_py() for v in decompressed_array[:10]))))


if __name__ == "__main__":
    # --- Test Case 1: Small-scale example ---
    # This demonstrates correctness but has poor compression ratio due to metadata overhead.
    small_data = [100, 110, 125, 120, None, 110, 90, 85, None, 95]
    small_array = pa.array(small_data, type=pa.int64())
    run_compression_test("1_Small Scale Sanity Check", small_array)
    run_zstd_only_test("ZSTD ONLY: 1_Small Scale Sanity Check", small_array)

    # --- Test Case 2: Large-scale time-series data ---
    # This demonstrates the real-world effectiveness of the compression pipeline.
    # The data has strong locality, which is ideal for delta encoding.
    large_data = [1000 + (i % 50) + (-1) ** i for i in range(10_000)]
    large_data[100] = None  # Add some nulls
    large_data[5000] = None
    large_array = pa.array(large_data, type=pa.int64())
    run_compression_test("2_Large Scale Time-Series", large_array)
    run_zstd_only_test("ZSTD ONLY: 2_Large Scale Time-Series", large_array)

    # --- Test Case 3: Large-scale chaotic data (hard to compress) ---
    import random

    random.seed(42)  # for reproducibility

    large_crazy_data = []
    for i in range(5000_000):
        if random.random() < 0.01:  # ~1% nulls sprinkled randomly
            large_crazy_data.append(None)
        else:
            # Mix huge jumps, negative numbers, and random integers
            if random.random() < 0.3:
                large_crazy_data.append(random.randint(-1_000_000, 1_000_000))
            elif random.random() < 0.5:
                large_crazy_data.append(random.randint(-100, 100))
            else:
                large_crazy_data.append(random.randint(0, 10_000))

    large_crazy_array = pa.array(large_crazy_data, type=pa.int64())
    run_compression_test("3_Large Scale Chaotic / Pathological Data", large_crazy_array)
    # --- Compare: raw ZSTD-only compression on the same chaotic data ---
    run_zstd_only_test("ZSTD ONLY: 3_Large Scale Chaotic / Pathological Data", large_crazy_array)

    # --- Test Case 4: Data with low cardinality ---
    # This should trigger the RLE kernel to be used effectively.
    low_cardinality_data = [10, 10, 10, 10, 20, 20, 20, 20, 10, 10] * 1000
    low_cardinality_array = pa.array(low_cardinality_data, type=pa.int32())
    run_compression_test("4_Low Cardinality Data", low_cardinality_array)
    run_zstd_only_test("ZSTD ONLY: 4_Low Cardinality Data", small_array)

    # --- Test Case 4b: Signed Data with low cardinality ---
    # This should trigger the RLE kernel to be used effectively.
    low_cardinality_data = [-10, -10, -10, -10, 20, 20, 20, 20, 10, 10] * 1000
    low_cardinality_array = pa.array(low_cardinality_data, type=pa.int32())
    run_compression_test("4b_Low Cardinality Signed Data", low_cardinality_array)
    run_zstd_only_test("ZSTD ONLY: 4b_Low Cardinality Signed Data", small_array)

    # --- Test Case 5: Large Data with low cardinality ---
    # This should trigger the RLE kernel to be used effectively.
    random.seed(42)  # for reproducibility

    N = 5_000_000
    unique_values = [10, 20, 30, 40, 50]

    low_cardinality_large_data = [random.choice(unique_values) for _ in range(N)]
    low_cardinality_large_array = pa.array(low_cardinality_large_data, type=pa.int32())

    run_compression_test("4_Large Low Cardinality Randomized Data", low_cardinality_large_array)
    run_zstd_only_test("ZSTD ONLY: 4_Large Low Cardinality Randomized Data", low_cardinality_large_array)
    
    # --- Test Case 6: Empty Array ---
    # An important edge case to verify.
    empty_array = pa.array([], type=pa.int64())
    run_compression_test("6_Empty Array", empty_array)
    run_zstd_only_test("ZSTD ONLY: 6_Empty Array", small_array)

    # --- Test Case 7: All-Null Array ---
    # Another important edge case.
    all_null_array = pa.array([None, None, None, None] * 100, type=pa.int16())
    run_compression_test("7_All-Null Array", all_null_array)
    run_zstd_only_test("ZSTD ONLY: 7_All-Null Array", small_array)

    # --- Test Case 8: Smooth Float Time-Series (Ideal for Delta -> Shuffle) ---
    # This data simulates sensor readings or stock prices. The values are highly
    # correlated, so the deltas will be small. Shuffling the bytes of these small
    # floats will create highly compressible streams.
    N = 1_000_000
    smooth_floats = [
        None if i == 1000 or i == 500000 else
        100.0 + math.sin(i / 50.0) * 10 + (random.random() - 0.5) * 0.1
        for i in range(N)
    ]
    smooth_float_array = pa.array(smooth_floats, type=pa.float64())
    run_compression_test("8_Smooth Float Time-Series (f64)", smooth_float_array)
    run_zstd_only_test("ZSTD ONLY: 8_Smooth Float Time-Series (f64)", smooth_float_array)

    # --- Test Case 9: Chaotic/Random Float Data (Pathological Case) ---
    # This data has no correlation. The deltas will be large and random.
    # We expect our pipeline to perform similarly to, or slightly worse than,
    # raw Zstd, as our transforms won't find any structure to exploit.
    chaotic_floats = [random.uniform(-1e6, 1e6) for _ in range(N)]
    chaotic_float_array = pa.array(chaotic_floats, type=pa.float32())
    run_compression_test("9_Chaotic Random Float Data (f32)", chaotic_float_array)
    run_zstd_only_test("ZSTD ONLY: 9_Chaotic Random Float Data (f32)", chaotic_float_array)

    # --- Test Case 10: Low Cardinality Float Data ---
    # This tests how the pipeline handles data with very few unique values,
    # such as categorical data represented as floats. The deltas will be mostly
    # 0.0 or a few other constants, which should compress well after shuffling.
    unique_float_values = [0.0, 100.5, -999.99, 42.42]
    low_card_floats = [random.choice(unique_float_values) for _ in range(N)]
    low_card_float_array = pa.array(low_card_floats, type=pa.float32())
    run_compression_test("10_Low Cardinality Float Data (f32)", low_card_float_array)
    run_zstd_only_test("ZSTD ONLY: 10_Low Cardinality Float Data (f32)", low_card_float_array)