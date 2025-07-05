//! This module provides observability and diagnostics capabilities for the v4.0 planner.
//!
//! A system this adaptive requires visibility into its decision-making process.
//! This module provides structured logging hooks to make the planner's behavior
//! transparent and debuggable. The `log_metric!` macro is the primary tool.
//!
//! It is a zero-cost abstraction: the `#[cfg(debug_assertions)]` attribute ensures
//! that the macro and all calls to it are completely compiled out of release builds,
//! imposing no performance penalty in production.

/// Logs a structured key-value metric string to stdout, only in debug builds.
///
/// Logs a structured key-value metric string to stdout, only in debug builds.
///
/// # Example
/// ```
/// use phoenix_cache::log_metric;
/// let stride = 4;
/// log_metric!("event"="discover_structure", "outcome"="FixedStride", "stride"=&stride);
/// ```
#[macro_export]
macro_rules! log_metric {
    ($($key:literal = $value:expr),+ $(,)?) => {
        #[cfg(debug_assertions)]
        {
            // Collect each pair as a JSON string fragment
            let mut parts = Vec::new();
            $(
                parts.push(format!("\"{}\": \"{}\"", $key, $value));
            )+

            let output = format!("PHOENIX_METRIC: {{ {} }}", parts.join(", "));
            println!("{}", output);
        }
    };
}
