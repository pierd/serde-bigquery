use serde::{ser, Serialize};

use crate::error::{Error, Result};
use crate::types;

pub struct UnsupportedSerializer;

impl ser::Serializer for UnsupportedSerializer {
    type Ok = types::Type;
    type Error = Error;

    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_none(self) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_some<T: ?Sized>(self, _value: &T) -> Result<Self::Ok>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn serialize_unit(self) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Err(Error::UnsupportedType)
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Err(Error::UnsupportedType)
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(Error::UnsupportedType)
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
        Err(Error::UnsupportedType)
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        Err(Error::UnsupportedType)
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

impl ser::SerializeSeq for UnsupportedSerializer {
    type Ok = types::Type;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, _value: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn end(self) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }
}

impl ser::SerializeTuple for UnsupportedSerializer {
    type Ok = types::Type;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, _value: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn end(self) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }
}

impl ser::SerializeTupleStruct for UnsupportedSerializer {
    type Ok = types::Type;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn end(self) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }
}

impl ser::SerializeTupleVariant for UnsupportedSerializer {
    type Ok = types::Type;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, _value: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn end(self) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }
}

impl ser::SerializeMap for UnsupportedSerializer {
    type Ok = types::Type;
    type Error = Error;

    fn serialize_key<T: ?Sized>(&mut self, _key: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn serialize_value<T: ?Sized>(&mut self, _value: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn end(self) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }
}

impl ser::SerializeStruct for UnsupportedSerializer {
    type Ok = types::Type;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn end(self) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }
}

impl ser::SerializeStructVariant for UnsupportedSerializer {
    type Ok = types::Type;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, _key: &'static str, _value: &T) -> Result<()>
    where
        T: Serialize,
    {
        Err(Error::UnsupportedType)
    }

    fn end(self) -> Result<Self::Ok> {
        Err(Error::UnsupportedType)
    }
}
