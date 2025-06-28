import pyarrow as pa
import phoenix_cache  # The name of your Rust module


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
        stats = phoenix_cache.compress_analyze(original_array)
        compressed_artifact_bytes = stats["artifact"]  # Assign the artifact here

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
            print(f"  -                           %: {ratio_data:.2f}%")
        print("-" * 20)

    except Exception as e:
        print(f"❌ FAILURE: An error occurred during analytics compression: {e}")
        return

    # 2. DECOMPRESSION
    try:
        # The type string must match what the Rust dispatcher expects (e.g., "Int64")
        # We can derive this from the pyarrow type object.
        original_type_str = str(original_array.type).capitalize().replace("t", "t")

        decompressed_array = phoenix_cache.decompress(
            compressed_artifact_bytes, original_type_str
        )
        print("Decompression successful!")

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
        field_width = max(len(s) for s in all_strs)

        print("Original Array:")
        print(
            " ".join(
                f"{str(val):>{field_width}}" for val in orig_values
            )
        )

        print("Decompressed Array:")
        print(
            " ".join(
                f"{str(val):>{field_width}}" for val in decomp_values
            )
        )
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

    # --- Test Case 2: Large-scale time-series data ---
    # This demonstrates the real-world effectiveness of the compression pipeline.
    # The data has strong locality, which is ideal for delta encoding.
    large_data = [1000 + (i % 50) + (-1) ** i for i in range(10_000)]
    large_data[100] = None  # Add some nulls
    large_data[5000] = None
    large_array = pa.array(large_data, type=pa.int64())
    run_compression_test("2_Large Scale Time-Series", large_array)

    # --- Test Case 3: Data with low cardinality ---
    # This should trigger the RLE kernel to be used effectively.
    low_cardinality_data = [10, 10, 10, 10, 20, 20, 20, 20, 10, 10] * 1000
    low_cardinality_array = pa.array(low_cardinality_data, type=pa.int32())
    run_compression_test("3_Low Cardinality Data", low_cardinality_array)

    # --- Test Case 4: Empty Array ---
    # An important edge case to verify.
    empty_array = pa.array([], type=pa.int64())
    run_compression_test("4_Empty Array", empty_array)

    # --- Test Case 5: All-Null Array ---
    # Another important edge case.
    all_null_array = pa.array([None, None, None, None] * 100, type=pa.int16())
    run_compression_test("5_All-Null Array", all_null_array)
