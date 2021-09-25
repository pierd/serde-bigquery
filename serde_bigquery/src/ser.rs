use std::iter::FromIterator;
use std::io;

use serde::{ser, Serialize};

use crate::error::{Error, Result};

// TODO: check types - all structs in an array has to have the same files
// TODO: ensure struct/map fields are serialized in the same order (BigQuery doesn't care about field name annotations after the first struct)

pub struct Serializer<W> {
    writer: W,
}

pub fn to_string<T>(value: &T) -> Result<String>
where
    T: ?Sized + Serialize,
{
    to_bytes(value).map(|v| String::from_utf8(v).unwrap())
}

pub fn to_bytes<T>(value: &T) -> Result<Vec<u8>>
where
    T: ?Sized + Serialize,
{
    let mut serializer = Serializer {
        writer: Vec::new(),
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.writer)
}


impl<W> Serializer<W>
where
    W: io::Write,
{
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.writer.write_all(buf).map_err(|err| err.into())
    }

    fn write_str(&mut self, s: &str) -> Result<()> {
        self.write(s.as_bytes())
    }

    fn write_fmt(&mut self, fmt: std::fmt::Arguments<'_>) -> Result<()> {
        self.writer.write_fmt(fmt).map_err(|err| err.into())
    }
}


impl<'a, W> ser::Serializer for &'a mut Serializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<()> {
        self.write(if v { b"TRUE" } else { b"FALSE" })
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        self.serialize_i64(i64::from(v))
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.write_str(&v.to_string())
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        self.serialize_u64(u64::from(v))
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        self.write_str(&v.to_string())
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        self.serialize_f64(f64::from(v))
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.write_str(&v.to_string())
    }

    fn serialize_char(self, v: char) -> Result<()> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        // TODO: handle escape sequences (")
        self.write_fmt(format_args!("\"{}\"", v))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#string_and_bytes_literals
        // TODO: (nice to have) use printable characters directly where possible
        self.write(b"b\"")?;
        self.write_str(&String::from_iter(
            v.iter().map(|b| format!("\\x{:02x}", b)),
        ))?;
        self.write(b"\"")
    }

    fn serialize_none(self) -> Result<()> {
        self.write(b"NULL")
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<()> {
        self.serialize_bool(true)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        self.serialize_unit()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<()> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<()>
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
    ) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        self.write(b"[").map(|_| self)
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        if len > 0 {
            self.write(b"STRUCT(").map(|_| self)
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
        self.write(b"STRUCT(").map(|_| self)
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
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

impl<'a, W> ser::SerializeSeq for &'a mut Serializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        // FIXME: trailing comma
        self.write(b",")
    }

    fn end(self) -> Result<()> {
        self.write(b"]")
    }
}

impl<'a, W> ser::SerializeTuple for &'a mut Serializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        // FIXME: trailing comma
        self.write(b",")
    }

    fn end(self) -> Result<()> {
        // TODO: emit Err(Error::EmptyStruct)
        self.write(b")")
    }
}

impl<'a, W> ser::SerializeTupleStruct for &'a mut Serializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        // FIXME: trailing comma
        self.write(b",")
    }

    fn end(self) -> Result<()> {
        // TODO: emit Err(Error::EmptyStruct)
        self.write(b")")
    }
}

impl<'a, W> ser::SerializeTupleVariant for &'a mut Serializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn end(self) -> Result<()> {
        Err(Error::UnsupportedType)
    }
}

impl<'a, W> ser::SerializeMap for &'a mut Serializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        // TODO: this isn't actually implemented

        key.serialize(&mut **self)?;
        // FIXME: trailing comma
        self.write(b",")
    }

    // It doesn't make a difference whether the colon is printed at the end of
    // `serialize_key` or at the beginning of `serialize_value`. In this case
    // the code is a bit simpler having it here.
    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.write(b":")?;
        value.serialize(&mut **self)
    }

    // TODO: (nice to have) implement serialize_entry

    fn end(self) -> Result<()> {
        // TODO: emit Err(Error::EmptyStruct)
        self.write(b")")
    }
}

impl<'a, W> ser::SerializeStruct for &'a mut Serializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(&mut **self)?;
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#identifiers
        // FIXME: handle ` in key
        // FIXME: handle empty key
        // FIXME: trailing comma
        self.write_fmt(format_args!(" AS `{}`,", key))
    }

    fn end(self) -> Result<()> {
        // TODO: emit Err(Error::EmptyStruct)
        self.write(b")")
    }
}

impl<'a, W> ser::SerializeStructVariant for &'a mut Serializer<W>
where
    W: io::Write,
{
    type Ok = ();
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn end(self) -> Result<()> {
        Err(Error::UnsupportedType)
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;
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
}
