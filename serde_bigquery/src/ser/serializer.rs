use std::io;
use std::iter::FromIterator;

use serde::{ser, Serialize};

use crate::error::{Error, Result};
use crate::ser::struct_serializer::StructSerializer;
use crate::ser::typed_serializer::TypedSerializer;
use crate::ser::unsupported::UnsupportedSerializer;
use crate::types::Type;

pub struct Serializer<W> {
    pub(crate) writer: W,
}

/// Serialize value to String
pub fn to_string<T>(value: &T) -> Result<String>
where
    T: ?Sized + Serialize,
{
    to_bytes(value).map(|v| String::from_utf8(v).unwrap())
}

/// Serialize value to bytes
pub fn to_bytes<T>(value: &T) -> Result<Vec<u8>>
where
    T: ?Sized + Serialize,
{
    let mut serializer = Serializer { writer: Vec::new() };
    value.serialize(&mut serializer)?;
    Ok(serializer.writer)
}

impl<W: io::Write> Serializer<W> {
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    pub(crate) fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.writer.write_all(buf).map_err(Error::io)
    }

    pub(crate) fn write_str(&mut self, s: &str) -> Result<()> {
        self.write(s.as_bytes())
    }

    pub(crate) fn write_fmt(&mut self, fmt: std::fmt::Arguments<'_>) -> Result<()> {
        self.writer.write_fmt(fmt).map_err(Error::io)
    }

    pub(crate) fn serialize<T>(&mut self, value: &T) -> Result<Type>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }
}

impl<'a, W: io::Write> ser::Serializer for &'a mut Serializer<W> {
    type Ok = Type;
    type Error = Error;

    type SerializeSeq = SeqSerializer<'a, W>;
    type SerializeTuple = StructSerializer<'a, W>;
    type SerializeTupleStruct = StructSerializer<'a, W>;
    type SerializeTupleVariant = UnsupportedSerializer;
    type SerializeMap = StructSerializer<'a, W>;
    type SerializeStruct = StructSerializer<'a, W>;
    type SerializeStructVariant = UnsupportedSerializer;

    fn serialize_bool(self, v: bool) -> Result<Type> {
        self.write(if v { b"TRUE" } else { b"FALSE" })
            .map(|_| Type::Bool)
    }

    fn serialize_i8(self, v: i8) -> Result<Type> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<Type> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Type> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Type> {
        self.write_str(&v.to_string()).map(|_| Type::Number)
    }

    fn serialize_u8(self, v: u8) -> Result<Type> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<Type> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Type> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<Type> {
        self.write_str(&v.to_string()).map(|_| Type::Number)
    }

    fn serialize_f32(self, v: f32) -> Result<Type> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Type> {
        self.write_str(&v.to_string()).map(|_| Type::Number)
    }

    fn serialize_char(self, v: char) -> Result<Type> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<Type> {
        // TODO: handle escape sequences (")
        self.write_fmt(format_args!("\"{}\"", v))
            .map(|_| Type::String)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Type> {
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#string_and_bytes_literals
        // TODO: (nice to have) use printable characters directly where possible
        self.write(b"b\"")?;
        self.write_str(&String::from_iter(
            v.iter().map(|b| format!("\\x{:02x}", b)),
        ))?;
        self.write(b"\"").map(|_| Type::Bytes)
    }

    fn serialize_none(self) -> Result<Type> {
        self.write(b"NULL").map(|_| Type::Any)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Type>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Type> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Type> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Type> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<Type>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Type>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.write(b"[")
            .map(move |_| SeqSerializer::with_serializer(self))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        if len > 0 {
            self.write(b"STRUCT(")
                .map(move |_| StructSerializer::with_serializer(self))
        } else {
            Err(Error::EmptyStruct)
        }
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_tuple(len)
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(Error::UnsupportedType)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        self.write(b"STRUCT(")
            .map(move |_| StructSerializer::with_serializer(self))
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_tuple(len)
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(Error::UnsupportedType)
    }
}

pub struct SeqSerializer<'a, W> {
    serializer: &'a mut Serializer<W>,
    has_elements: bool,
    element_type: Type,
}

impl<'a, W> SeqSerializer<'a, W> {
    fn with_serializer(serializer: &'a mut Serializer<W>) -> Self {
        Self {
            serializer,
            has_elements: false,
            element_type: Type::Any,
        }
    }

    pub(crate) fn with_element_type(self, element_type: Type) -> Self {
        Self {
            element_type,
            ..self
        }
    }
}

impl<'a, W: io::Write> ser::SerializeSeq for SeqSerializer<'a, W> {
    type Ok = Type;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        if self.has_elements {
            self.serializer.write(b",")?;
        } else {
            self.has_elements = true;
        }
        let mut typed_serializer =
            TypedSerializer::with_serializer(self.serializer, &self.element_type);
        let element_type = value.serialize(&mut typed_serializer)?;
        let new_element_type = self.element_type.merge(&element_type);
        if let Some(merged_element_type) = new_element_type {
            self.element_type = merged_element_type;
            Ok(())
        } else {
            Err(Error::UnexpectedType {
                expected: self.element_type.clone(),
                found: element_type,
            })
        }
    }

    fn end(self) -> Result<Type> {
        self.serializer
            .write(b"]")
            .map(|_| Type::Array(Box::new(self.element_type)))
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;
    use serde::ser::{SerializeSeq, SerializeStruct, Serializer};
    use serde_bytes::Bytes;
    use serde_derive::Serialize;

    #[test]
    fn test_simple_vals() {
        assert_eq!(to_string(&false).unwrap(), "FALSE");
        assert_eq!(to_string(&true).unwrap(), "TRUE");
        assert_eq!(to_string(&42).unwrap(), "42");
        assert_eq!(to_string(&1.25).unwrap(), "1.25");
    }

    #[test]
    fn test_simple_strings() {
        assert_eq!(to_string(&"foo").unwrap(), r#""foo""#);
    }

    #[test]
    fn test_simple_bytes() {
        assert_eq!(to_string(Bytes::new(b"foo")).unwrap(), r#"b"\x66\x6f\x6f""#);
    }

    #[test]
    fn test_optional_none() {
        let x: Option<u32> = None;
        assert_eq!(to_string(&x).unwrap(), "NULL");
    }

    #[test]
    fn test_vec_simple() {
        let v = vec![1, 2, 3];
        let expected = r#"[1,2,3]"#;
        assert_eq!(to_string(&v).unwrap(), expected);
    }

    #[test]
    fn test_vec_complex() {
        #[derive(Serialize)]
        struct Element {
            a: u32,
            b: f64,
        }

        let v = vec![Element { a: 1, b: 2.5 }, Element { a: 3, b: 10.5 }];
        let expected = r#"[STRUCT(1 AS `a`,2.5 AS `b`),STRUCT(3 AS `a`,10.5 AS `b`)]"#;
        assert_eq!(to_string(&v).unwrap(), expected);
    }

    #[test]
    fn test_vec_complex_single_field() {
        #[derive(Serialize)]
        struct Element {
            a: u32,
        }

        let v = vec![Element { a: 1 }, Element { a: 3 }];
        let expected = r#"[STRUCT(1 AS `a`),STRUCT(3 AS `a`)]"#;
        assert_eq!(to_string(&v).unwrap(), expected);
    }

    #[test]
    fn test_struct() {
        #[derive(Serialize)]
        struct Test {
            int: u32,
            seq: Vec<&'static str>,
        }

        let test = Test {
            int: 1,
            seq: vec!["a", "b"],
        };
        let expected = r#"STRUCT(1 AS `int`,["a","b"] AS `seq`)"#;
        assert_eq!(to_string(&test).unwrap(), expected);
    }

    #[test]
    fn test_empty_struct() {
        let mut serializer = super::Serializer::new(io::sink());
        let s = serializer.serialize_map(None).unwrap();
        assert!(s.end().is_err());
    }

    #[test]
    fn test_array_type_checking() {
        let mut serializer = super::Serializer::new(io::sink());
        let mut seq_serializer = serializer.serialize_seq(None).unwrap();
        seq_serializer.serialize_element(&1).unwrap();
        assert!(seq_serializer.serialize_element("boom").is_err());
    }

    #[test]
    fn test_array_deeper_type_checking() {
        #[derive(Serialize)]
        struct Foo {
            x: u32,
        }

        #[derive(Serialize)]
        struct Bar {
            x: &'static str,
        }

        let mut serializer = super::Serializer::new(io::sink());
        let mut seq_serializer = serializer.serialize_seq(None).unwrap();
        seq_serializer.serialize_element(&Foo { x: 42 }).unwrap();
        assert!(seq_serializer
            .serialize_element(&Bar { x: "boom" })
            .is_err());
    }
}
