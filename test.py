import pyarrow as pa
# This is the name of your Rust module as defined in lib.rs and Cargo.toml
import phoenix_cache

def run_test():
    """
    Tests the full compress/decompress roundtrip for the phoenix_cache library.
    """
    print("--- Starting Compression Test ---")

    # 1. Create a PyArrow Array to compress.
    #    This array has a mix of positive/negative values and nulls.
    original_data = [100, 110, 125, 120, None, 110, 90, 85, None, 95]
    # The type must match one of the types your library supports (e.g., Int64)
    original_array = pa.array(original_data, type=pa.int64())

    print(f"Original Array ({original_array.type}):")
    print(original_array)
    print(f"Original data size (bytes): {original_array.nbytes}")
    print("-" * 20)

    # 2. Call the Rust `compress` function.
    #    The FFI layer will handle the conversion from PyArrow Array to Rust.
    try:
        compressed_bytes = phoenix_cache.compress(original_array)
        print("Compression successful!")
        print(f"Compressed size (bytes): {len(compressed_bytes)}")
        
        compression_ratio = original_array.nbytes / len(compressed_bytes)
        print(f"Compression Ratio: {compression_ratio:.2f}x")
        print("-" * 20)

    except Exception as e:
        print(f"An error occurred during compression: {e}")
        return

    # 3. Call the Rust `decompress` function.
    #    We need to pass the compressed bytes and the *original* data type as a string.
    try:
        # The type string must match what the Rust dispatcher expects (e.g., "Int64")
        original_type_str = "Int64"
        decompressed_array = phoenix_cache.decompress(compressed_bytes, original_type_str)
        
        print("Decompression successful!")
        print(f"Decompressed Array ({decompressed_array.type}):")
        print(decompressed_array)
        print("-" * 20)

    except Exception as e:
        print(f"An error occurred during decompression: {e}")
        return

    # 4. Verify the result.
    #    The `equals` method in PyArrow checks for both value and type equality.
    print("--- Verification ---")
    if original_array.equals(decompressed_array):
        print("✅ SUCCESS: Decompressed array is identical to the original.")
    else:
        print("❌ FAILURE: Decompressed array does NOT match the original.")
        print("Original:")
        print(original_array)
        print("Decompressed:")
        print(decompressed_array)

if __name__ == "__main__":
    run_test()