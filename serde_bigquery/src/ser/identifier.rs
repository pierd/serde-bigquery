use std::fmt::Write;

use serde::{ser, Serialize};

use crate::{
    error::{Error, Result},
    types,
};

use super::unsupported::UnsupportedSerializer;

///
/// https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#identifiers
pub fn format_as_identifier(s: &str) -> String {
    // FIXME: handle ` in key
    // FIXME: handle empty key
    let mut result = String::new();
    write!(result, "`{}`", s).unwrap();
    result
}

pub fn to_identifier<T>(value: &T) -> Result<String>
where
    T: ?Sized + Serialize,
{
    let mut serializer = IdentifierSerializer {
        output: String::new(),
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.output)
}

struct IdentifierSerializer {
    output: String,
}

impl ser::Serializer for &mut IdentifierSerializer {
    type Ok = types::Type;
    type Error = Error;

    type SerializeSeq = UnsupportedSerializer;
    type SerializeTuple = UnsupportedSerializer;
    type SerializeTupleStruct = UnsupportedSerializer;
    type SerializeTupleVariant = UnsupportedSerializer;
    type SerializeMap = UnsupportedSerializer;
    type SerializeStruct = UnsupportedSerializer;
    type SerializeStructVariant = UnsupportedSerializer;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Bool))
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok> {
        Err(Error::InvalidIdentifierType(types::Type::Number))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok> {
        self.serialize_str(&v.to_string())
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok> {
        self.output.write_str(v).map_err(Error::FormattingError)?;
        Ok(types::Type::String)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok> {
        if let Ok(s) = std::str::from_utf8(v) {
            self.serialize_str(s)
        } else {
            Err(Error::InvalidIdentifierType(types::Type::Bytes))
        }
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        Ok(types::Type::String)
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        Ok(types::Type::String)
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok> {
        self.serialize_str(name)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(self, _name: &'static str, value: &T) -> Result<Self::Ok>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(Error::InvalidIdentifierType(types::Type::Array(Box::new(
            types::Type::Any,
        ))))
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_map(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_map(Some(len))
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serialize_map(Some(len))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        Err(Error::InvalidIdentifierType(types::Type::Struct(vec![])))
    }

    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.serialize_map(Some(len))
    }
}
