pub(crate) mod identifier;
mod serializer;
mod unsupported;

pub use serializer::{to_bytes, to_string, Serializer};
