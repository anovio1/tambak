// use num_traits::PrimInt;

// /// Trait to link a signed integer type to its unsigned counterpart.
// pub trait HasUnsigned {
//     type Unsigned: PrimInt;
// }

// /// Trait to link an unsigned integer type to its signed counterpart.
// pub trait HasSigned {
//     type Signed: PrimInt;
// }

// // Implementations for common primitive integer types

// impl HasUnsigned for i8 { type Unsigned = u8; }
// impl HasUnsigned for i16 { type Unsigned = u16; }
// impl HasUnsigned for i32 { type Unsigned = u32; }
// impl HasUnsigned for i64 { type Unsigned = u64; }
// impl HasUnsigned for i128 { type Unsigned = u128; }

// impl HasSigned for u8 { type Signed = i8; }
// impl HasSigned for u16 { type Signed = i16; }
// impl HasSigned for u32 { type Signed = i32; }
// impl HasSigned for u64 { type Signed = i64; }
// impl HasSigned for u128 { type Signed = i128; }



//! This module defines shared traits used across different kernels.

/// A trait that maps a signed integer type to its unsigned counterpart.
pub trait HasUnsigned {
    type Unsigned;
}

/// A trait that maps an unsigned integer type to its signed counterpart.
pub trait HasSigned {
    type Signed;
}

// Implement the traits for all primitive integer types.
macro_rules! impl_signed_unsigned_pair {
    ($S:ty, $U:ty) => {
        impl HasUnsigned for $S {
            type Unsigned = $U;
        }
        impl HasSigned for $U {
            type Signed = $S;
        }
    };
}

impl_signed_unsigned_pair!(i8, u8);
impl_signed_unsigned_pair!(i16, u16);
impl_signed_unsigned_pair!(i32, u32);
impl_signed_unsigned_pair!(i64, u64);
impl_signed_unsigned_pair!(i128, u128);