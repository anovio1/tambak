(.venv) PS V:\Github\RustPythonTest> python testlarge.py

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
  -  Original Data Size:               82 bytes
  - Total Artifact Size:              166 bytes
  -     Metadata (Header) Size:       142 bytes
  -     Compressed Data Size:          24 bytes
  -  Artifact : Original, Ratio: 2.02439x
  -                           %: 202.44%
  - Data Only : Original, Ratio: 0.29268x
  -                           %: 0.29%
--------------------
❌ FAILURE: An error occurred during decompression: Internal logic error (this is a bug): Invalid argument error: Need at least 80 bytes in buffers[0] in array of type Int64, but got 64

--- Starting Test Case: 3_Low Cardinality Data ---
Data Type: int32, Length: 10000
Original Array (snippet):
10 10 10 10 20 20 20 20 10 10
Original data size (bytes): 40000
--------------------

[CHECKPOINT 1] Orchestrator -> Planner

[CHECKPOINT 1] valid_data_bytes = bytemuck::cast_slice(&valid_data_vec)
  - Type: Int32
  - Bytes Sent to ExePlannerutor (first 50): [10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 20, 0, 0, 0, 20, 0, 0, 0, 20, 0, 0, 0, 20, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 10, 0]...

[CHECKPOINT 2] Planner -> Orchestrator: pipeline_json = planner::plan_pipeline
  - Pipeline JSON: [{"op":"delta","params":{"order":1}},{"op":"rle"},{"op":"zstd","params":{"level":3}}]

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"delta","params":{"order":1}}
  - Current Type: Int32
  - Input Buf (first 16 bytes): Some([10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"rle"}
  - Current Type: Int32
  - Input Buf (first 16 bytes): Some([10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zstd","params":{"level":3}}
  - Current Type: Int32
  - Input Buf (first 16 bytes): Some([10, 0, 0, 0, 1, 0, 0, 0, 0, 3, 10, 0, 0, 0, 1, 0])

[CHECKPOINT 3] Orchestrator -> Executor

[CHECKPOINT 3] compress_primitive_array
  - Type: Int32
  - Bytes Sent to Executor (first 50): [10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 20, 0, 0, 0, 20, 0, 0, 0, 20, 0, 0, 0, 20, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 10, 0, 0, 0, 10, 0]...
  - Plan Sent to Executor: [{"op":"delta","params":{"order":1}},{"op":"rle"},{"op":"zstd","params":{"level":3}}]

✅ Compression successful!: 3_Low Cardinality Data
--- Analytics Breakdown ---
  - Compression Plan: [{"op":"delta","params":{"order":1}},{"op":"rle"},{"op":"zstd","params":{"level":3}}]
  -  Original Data Size:            40000 bytes
  - Total Artifact Size:              146 bytes
  -     Metadata (Header) Size:       109 bytes
  -     Compressed Data Size:          37 bytes
  -  Artifact : Original, Ratio: 0.00365x
  -                           %: 0.36%
  - Data Only : Original, Ratio: 0.00093x
  -                           %: 0.00%
--------------------
Decompression successful!
--- Verification ---
✅ SUCCESS: Decompressed array is identical to the original for '3_Low Cardinality Data'.

--- Starting Test Case: 4_Empty Array ---
Data Type: int64, Length: 0
Original Array:

Original data size (bytes): 0
--------------------

✅ Compression successful!: 4_Empty Array
--- Analytics Breakdown ---
  - Compression Plan: []
  -  Original Data Size:                0 bytes
  - Total Artifact Size:               26 bytes
  -     Metadata (Header) Size:        26 bytes
  -     Compressed Data Size:           0 bytes
--------------------
Decompression successful!
--- Verification ---
✅ SUCCESS: Decompressed array is identical to the original for '4_Empty Array'.

--- Starting Test Case: 5_All-Null Array ---
Data Type: int16, Length: 400
Original Array (snippet):
None None None None None None None None None None
Original data size (bytes): 850
--------------------

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"rle"}
  - Current Type: Boolean
  - Input Buf (first 16 bytes): Some([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zstd","params":{"level":19}}
  - Current Type: Boolean
  - Input Buf (first 16 bytes): Some([0, 50])

✅ Compression successful!: 5_All-Null Array
--- Analytics Breakdown ---
  - Compression Plan: []
  -  Original Data Size:              850 bytes
  - Total Artifact Size:               37 bytes
  -     Metadata (Header) Size:        37 bytes
  -     Compressed Data Size:           0 bytes
  -  Artifact : Original, Ratio: 0.04353x
  -                           %: 4.35%
  - Data Only : Original, Ratio: 0.00000x
  -                           %: 0.00%
--------------------

thread '<unnamed>' panicked at C:\Users\Ace\.cargo\registry\src\index.crates.io-1949cf8c6b5b557f\arrow-buffer-50.0.0\src\buffer\boolean.rs:55:9:
assertion failed: total_len <= bit_len
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
Traceback (most recent call last):
  File "V:\Github\RustPythonTest\testlarge.py", line 154, in <module>
    run_compression_test("5_All-Null Array", all_null_array)
  File "V:\Github\RustPythonTest\testlarge.py", line 74, in run_compression_test
    decompressed_array = phoenix_cache.decompress(
                         ^^^^^^^^^^^^^^^^^^^^^^^^^
pyo3_runtime.PanicException: assertion failed: total_len <= bit_len