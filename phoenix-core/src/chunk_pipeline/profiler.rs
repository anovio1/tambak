
use crate::error::PhoenixError;
use crate::types::PhoenixDataType;
use crate::utils::safe_bytes_to_typed_slice;

use ndarray::{s, Array1};
use num_traits::{ToPrimitive};

pub fn find_stride_by_autocorrelation(
    bytes: &[u8],
    dtype: PhoenixDataType,
) -> Result<usize, PhoenixError> {
    macro_rules! to_f64_vec {
        ($T:ty, $bytes:expr) => {{
            safe_bytes_to_typed_slice::<$T>($bytes)?
                .iter()
                .filter_map(|&x| x.to_f64())
                .collect::<Vec<f64>>()
        }};
    }

    let data_f64 = match dtype {
        PhoenixDataType::Int8 => to_f64_vec!(i8, bytes),
        PhoenixDataType::Int16 => to_f64_vec!(i16, bytes),
        PhoenixDataType::Int32 => to_f64_vec!(i32, bytes),
        PhoenixDataType::Int64 => to_f64_vec!(i64, bytes),
        PhoenixDataType::UInt8 => to_f64_vec!(u8, bytes),
        PhoenixDataType::UInt16 => to_f64_vec!(u16, bytes),
        PhoenixDataType::UInt32 => to_f64_vec!(u32, bytes),
        PhoenixDataType::UInt64 => to_f64_vec!(u64, bytes),
        PhoenixDataType::Float32 => to_f64_vec!(f32, bytes),
        PhoenixDataType::Float64 => to_f64_vec!(f64, bytes),
        PhoenixDataType::Boolean => return Ok(1), // Autocorrelation not meaningful for booleans
    };

    let calculate = |data: &[f64]| -> Option<usize> {
        let n = data.len();
        if n < 8 {
            return None;
        }
        let data_arr = Array1::from_vec(data.to_vec());
        let mean = data_arr.mean()?;
        let centered_data = data_arr - mean;
        let variance = centered_data.dot(&centered_data);
        if variance < 1e-9 {
            return None;
        }
        let mut best_lag = 0;
        let mut max_corr = -1.0;
        let upper_bound = (n / 4).max(3).min(256);
        for lag in 1..upper_bound {
            let acf = centered_data
                .slice(s![..n - lag])
                .dot(&centered_data.slice(s![lag..]));
            if acf > max_corr {
                max_corr = acf;
                best_lag = lag;
            }
        }
        const CORRELATION_THRESHOLD: f64 = 0.25;
        if (max_corr / variance) > CORRELATION_THRESHOLD {
            Some(best_lag)
        } else {
            None
        }
    };

    Ok(calculate(&data_f64).unwrap_or(1))
}


//  TO CHECK: DOES find_stride_by_autocorrelation need to support I64 OR OTHER TYPES?
// /// Analyzes a data sample to find a dominant period or cycle using autocorrelation.
// ///
// /// # Returns
// /// `Some(stride)` if a significant correlation is found for a specific lag,
// /// otherwise `None`.
// // This is the NEW public-facing function that the planner will call.
// pub fn find_stride_by_autocorrelation(bytes: &[u8], type_str: &str) -> Result<usize, PhoenixError> {
//     // --- START: CORRECTED MACROS ---
//     // Helper macro for SIGNED integers
//     macro_rules! signed_to_f64_vec {
//         ($T:ty) => {{
//             safe_bytes_to_typed_slice::<$T>(bytes)?
//                 .iter()
//                 .map(|&x| x.to_i64().unwrap_or(0) as f64) // Step 1: to_i64, Step 2: as f64
//                 .collect::<Vec<f64>>()
//         }};
//     }

//     // Helper macro for UNSIGNED integers
//     macro_rules! unsigned_to_f64_vec {
//         ($T:ty) => {{
//             safe_bytes_to_typed_slice::<$T>(bytes)?
//                 .iter()
//                 .map(|&x| x.to_u64().unwrap_or(0) as f64) // Step 1: to_u64, Step 2: as f64
//                 .collect::<Vec<f64>>()
//         }};
//     }
//     // --- END: CORRECTED MACROS ---

//     let data_f64 = match type_str {
//         "Int8" => signed_to_f64_vec!(i8),
//         "Int16" => signed_to_f64_vec!(i16),
//         "Int32" => signed_to_f64_vec!(i32),
//         "Int64" => signed_to_f64_vec!(i64),
//         "UInt8" => unsigned_to_f64_vec!(u8),
//         "UInt16" => unsigned_to_f64_vec!(u16),
//         "UInt32" => unsigned_to_f64_vec!(u32),
//         "UInt64" => unsigned_to_f64_vec!(u64),
//         // For floats, we can cast directly.
//         "Float32" => safe_bytes_to_typed_slice::<f32>(bytes)?
//             .iter()
//             .map(|&x| x as f64)
//             .collect(),
//         "Float64" => safe_bytes_to_typed_slice::<f64>(bytes)?.to_vec(),
//         _ => return Ok(1), // Default to stride 1 if type is not numeric
//     };

//     // Call the statistical engine and return its result, or a default stride of 1.
//     Ok(calculate_autocorrelation(&data_f64).unwrap_or(1))
// }

// // The private `calculate_autocorrelation` function remains unchanged.
// fn calculate_autocorrelation(data: &[f64]) -> Option<usize> {
//     // ... same implementation as before ...
//     let n = data.len();
//     if n < 8 {
//         return None;
//     }
//     let data_arr = Array1::from_vec(data.to_vec());
//     let mean = data_arr.mean()?;
//     let centered_data = data_arr - mean;
//     let variance = centered_data.dot(&centered_data);
//     if variance < 1e-9 {
//         return None;
//     }
//     let mut best_lag = 0;
//     let mut max_corr = -1.0;
//     let upper_bound = (n / 4).max(3).min(256);
//     for lag in 2..upper_bound {
//         let acf = centered_data
//             .slice(s![..n - lag])
//             .dot(&centered_data.slice(s![lag..]));
//         if acf > max_corr {
//             max_corr = acf;
//             best_lag = lag;
//         }
//     }
//     const CORRELATION_THRESHOLD: f64 = 0.25;
//     if (max_corr / variance) > CORRELATION_THRESHOLD {
//         Some(best_lag)
//     } else {
//         None
//     }
// }