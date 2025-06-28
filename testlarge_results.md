(.venv) PS V:\Github\RustPythonTest> python testlarge.py

--- Starting Test Case: 1_Small Scale Sanity Check ---
Data Type: int64, Length: 10
Original Array:
100 110 125 120 None 110 90 85 None 95
Original data size (bytes): 82
--------------------

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zstd","params":{"level":19}}       
  - Current Type: Boolean
  - Input Buf (first 16 bytes): Some([239, 2])

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
  - Total Artifact Size:              164 bytes
  -     Metadata (Header) Size:       140 bytes
  -     Compressed Data Size:          24 bytes
  -  Artifact : Original, Ratio: 2.00000x
  -                           %: 200.00%
  - Data Only : Original, Ratio: 0.29268x
  -                           %: 0.29%
--------------------
❌ FAILURE: An error occurred during decompression: Internal logic error (this is a bug): Invalid argument error: Need at least 80 bytes in buffers[0] in array of type Int64, but got 64

--- Starting Test Case: 2_Large Scale Time-Series ---
Data Type: int64, Length: 10000
Original Array (snippet):
1001 1000 1003 1002 1005 1004 1007 1006 1009 1008
Original data size (bytes): 81250
--------------------

[CHECKPOINT 4] Executor Loop: for op_config in pipeline.iter
  - Executing Op: {"op":"zstd","params":{"level":19}}
  - Current Type: Boolean
  - Input Buf (first 16 bytes): Some([255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 239, 255, 255, 255])

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
  -  Original Data Size:            81250 bytes
  - Total Artifact Size:              206 bytes
  -     Metadata (Header) Size:       158 bytes
  -     Compressed Data Size:          48 bytes
  -  Artifact : Original, Ratio: 0.00254x
  -                           %: 0.25%
  - Data Only : Original, Ratio: 0.00059x
  -                           %: 0.00%
--------------------
❌ FAILURE: An error occurred during decompression: Internal logic error (this is a bug): Invalid argument error: Need at least 80000 bytes in buffers[0] in array of type Int64, but got 79984

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
Original Array (snippet):
10 10 10 10 20 20 20 20 10 10
Decompressed Array (snippet):
10 10 10 10 20 20 20 20 10 10

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
Traceback (most recent call last):
  File "V:\Github\RustPythonTest\testlarge.py", line 149, in <module>
    run_compression_test("4_Empty Array", empty_array)
  File "V:\Github\RustPythonTest\testlarge.py", line 101, in run_compression_test
    field_width = max(len(s) for s in all_strs)
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: max() arg is an empty sequence