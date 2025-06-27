use crate::error::PhoenixError;
use num_traits::PrimInt;
use pyo3::PyResult;

/// Unsafely reinterprets a byte slice as a slice of a primitive integer type.
/// # Safety
/// The caller MUST guarantee that the byte slice has the correct alignment and
/// length for the target type `T`. This is a zero-copy view.
pub unsafe fn bytes_to_typed_slice<'a, T: PrimInt>(bytes: &'a [u8]) -> PyResult<&'a [T]> {
    let size = std::mem::size_of::<T>();
    if bytes.len() % size != 0 {
        return Err(PhoenixError::BufferMismatch(bytes.len(), size).into());
    }
    let ptr = bytes.as_ptr() as *const T;
    let len = bytes.len() / size;
    Ok(std::slice::from_raw_parts(ptr, len))
}

/// Converts a slice of primitive integers into a `Vec<u8>`.
/// This involves a copy. Assumes Little-Endian.
pub fn typed_slice_to_bytes<T: PrimInt>(data: &[T]) -> Vec<u8> {
    data.iter().flat_map(|&val| val.to_le_bytes()).collect()
}