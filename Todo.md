# Python Orchestrator

**Python Orchestrator Concerns:**

* **Null/NA Handling:**
  The orchestrator must explicitly manage null values since the Rust kernels do not handle them. This includes creating and maintaining a validity bitmap that tracks which values are null.

* **Data Splitting:**
  Input data should be split into two streams before compression: a raw values stream (with nulls removed or replaced) and a null bitmap stream. Each stream is compressed separately.

* **Compression Pipeline Coordination:**
  The orchestrator sequences compression stages, invoking Rust kernels (bitpack, LEB128, RLE, etc.) on the non-null data and compressing the null bitmap independently, typically with a simple method like RLE.

* **Reconstruction on Decompression:**
  During decompression, the orchestrator first decompresses the null bitmap and the compressed value stream. It then reconstructs the full data by re-inserting nulls according to the bitmap, ensuring data integrity.

* **Type and Schema Awareness:**
  The orchestrator manages schema information including data types and nullability, which influences how data is split and processed.

* **Error Handling and Validation:**
  It verifies data consistency (e.g., matching lengths between bitmaps and values), handles corrupted or truncated data gracefully, and propagates errors with meaningful messages.

* **Performance Considerations:**
  The orchestrator balances overhead of splitting/merging data with compression gains, aiming to keep runtime and memory efficient.

