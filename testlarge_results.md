--- Starting Test Case: 1_Small Scale Sanity Check ---
Data Type: int64, Length: 10
Original Array:
100 110 125 120 None 110 90 85 None 95
Original data size (bytes): 82
--------------------

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"rle"}
  - Current Type: Boolean
  - Input Buf (first 16 bytes): Some([239, 2])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zstd","params":{"level":19}}
  - Current Type: Boolean
  - Input Buf (first 16 bytes): Some([239, 1, 2, 1])

[CHECKPOINT 1] Orchestrator -> Planner

[CHECKPOINT 1] valid_data_bytes = bytemuck::cast_slice(&valid_data_vec)
  - Type: Int64
  - Bytes Sent to ExePlannerutor (first 50): [100, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 125, 0, 0, 0, 0, 0, 0, 0, 120, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 90, 0, 0, 0, 0, 0, 0, 0, 85, 0]...

[CHECKPOINT 2] Planner -> Orchestrator: pipeline_json = planner::plan_pipeline
  - Pipeline JSON: [{"op":"delta","params":{"order":1}},{"op":"zigzag"},{"op":"shuffle"},{"op":"zstd","params":{"level":3}}]

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"delta","params":{"order":1}}
  - Current Type: Int64
  - Input Buf (first 16 bytes): Some([100, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zigzag"}
  - Current Type: Int64
  - Input Buf (first 16 bytes): Some([100, 0, 0, 0, 0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"shuffle"}
  - Current Type: UInt64
  - Input Buf (first 16 bytes): Some([200, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zstd","params":{"level":3}}
  - Current Type: UInt64
  - Input Buf (first 16 bytes): Some([200, 20, 30, 9, 19, 39, 9, 20, 0, 0, 0, 0, 0, 0, 0, 0])

[CHECKPOINT 3] Orchestrator -> Executor

[CHECKPOINT 3] compress_primitive_array
  - Type: Int64
  - Bytes Sent to Executor (first 50): [100, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 125, 0, 0, 0, 0, 0, 0, 0, 120, 0, 0, 0, 0, 0, 0, 0, 110, 0, 0, 0, 0, 0, 0, 0, 90, 0, 0, 0, 0, 0, 0, 0, 85, 0]...
  - Plan Sent to Executor: [{"op":"delta","params":{"order":1}},{"op":"zigzag"},{"op":"shuffle"},{"op":"zstd","params":{"level":3}}]

✅ Compression successful!: 1_Small Scale Sanity Check
--- Analytics Breakdown ---
  - Compression Plan: [{"op":"delta","params":{"order":1}},{"op":"zigzag"},{"op":"shuffle"},{"op":"zstd","params":{"level":3}}]
  - Total Artifact Size: 166 bytes
  - Metadata (Header) Size: 142 bytes
  - Compressed Data Size: 24 bytes
  - Ratio (Artifact : Original): 2.02439x
  -     % (Artifact : Original): 202.44%
  - Ratio (Data Only : Original): 0.29268x
  -     % (Data Only : Original): 0.29%
--------------------
Decompression successful!
--- Verification ---
❌ FAILURE: Decompressed array does NOT match the original for '1_Small Scale Sanity Check'.
Original Array:
100 110 125 120 None 110 90 85 None 95
Decompressed Array:
100 110 125 120 None 110 90 85 95 None

--- Starting Test Case: 2_Large Scale Time-Series ---
Data Type: int64, Length: 10000
Original Array (snippet):
1001 1000 1003 1002 1005 1004 1007 1006 1009 1008
Original data size (bytes): 81250
--------------------

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"rle"}
  - Current Type: Boolean
  - Input Buf (first 16 bytes): Some([255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 239, 255, 255, 255])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zstd","params":{"level":19}}
  - Current Type: Boolean
  - Input Buf (first 16 bytes): Some([255, 12, 239, 1, 255, 228, 4, 254, 1, 255, 240, 4])

[CHECKPOINT 1] Orchestrator -> Planner

[CHECKPOINT 1] valid_data_bytes = bytemuck::cast_slice(&valid_data_vec)
  - Type: Int64
  - Bytes Sent to ExePlannerutor (first 50): [233, 3, 0, 0, 0, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0, 235, 3, 0, 0, 0, 0, 0, 0, 234, 3, 0, 0, 0, 0, 0, 0, 237, 3, 0, 0, 0, 0, 0, 0, 236, 3, 0, 0, 0, 0, 0, 0, 239, 3]...

[CHECKPOINT 2] Planner -> Orchestrator: pipeline_json = planner::plan_pipeline
  - Pipeline JSON: [{"op":"delta","params":{"order":1}},{"op":"zigzag"},{"op":"shuffle"},{"op":"zstd","params":{"level":3}}]

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"delta","params":{"order":1}}
  - Current Type: Int64
  - Input Buf (first 16 bytes): Some([233, 3, 0, 0, 0, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zigzag"}
  - Current Type: Int64
  - Input Buf (first 16 bytes): Some([233, 3, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"shuffle"}
  - Current Type: UInt64
  - Input Buf (first 16 bytes): Some([210, 7, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zstd","params":{"level":3}}
  - Current Type: UInt64
  - Input Buf (first 16 bytes): Some([210, 1, 6, 1, 6, 1, 6, 1, 6, 1, 6, 1, 6, 1, 6, 1])

[CHECKPOINT 3] Orchestrator -> Executor

[CHECKPOINT 3] compress_primitive_array
  - Type: Int64
  - Bytes Sent to Executor (first 50): [233, 3, 0, 0, 0, 0, 0, 0, 232, 3, 0, 0, 0, 0, 0, 0, 235, 3, 0, 0, 0, 0, 0, 0, 234, 3, 0, 0, 0, 0, 0, 0, 237, 3, 0, 0, 0, 0, 0, 0, 236, 3, 0, 0, 0, 0, 0, 0, 239, 3]...
  - Plan Sent to Executor: [{"op":"delta","params":{"order":1}},{"op":"zigzag"},{"op":"shuffle"},{"op":"zstd","params":{"level":3}}]

✅ Compression successful!: 2_Large Scale Time-Series
--- Analytics Breakdown ---
  - Compression Plan: [{"op":"delta","params":{"order":1}},{"op":"zigzag"},{"op":"shuffle"},{"op":"zstd","params":{"level":3}}]
  - Total Artifact Size: 198 bytes
  - Metadata (Header) Size: 150 bytes
  - Compressed Data Size: 48 bytes
  - Ratio (Artifact : Original): 0.00244x
  -     % (Artifact : Original): 0.24%
  - Ratio (Data Only : Original): 0.00059x
  -     % (Data Only : Original): 0.00%
--------------------

thread '<unnamed>' panicked at C:\Users\Ace\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\arrow-buffer-50.0.0\src\buffer\boolean.rs:55:9:
assertion failed: total_len <= bit_len
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
Traceback (most recent call last):
  File "V:\Github\RustPythonTest\testlarge.py", line 111, in <module>
    run_compression_test("2_Large Scale Time-Series", large_array)
  File "V:\Github\RustPythonTest\testlarge.py", line 68, in run_compression_test
    decompressed_array = phoenix_cache.decompress(compressed_artifact_bytes, original_type_str)
                         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
pyo3_runtime.PanicException: assertion failed: total_len <= bit_len