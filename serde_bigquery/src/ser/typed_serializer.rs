use std::io;

use serde::{ser, Serialize};

use crate::error::{Error, Result};
use crate::types::CheckType;
use crate::{ser::serializer::Serializer, types::Type};

pub struct TypedSerializer<'a, W> {
    serializer: &'a mut Serializer<W>,
    expected_type: &'a Type,
}

impl<'a, W> TypedSerializer<'a, W> {
    pub(crate) fn with_serializer(
        serializer: &'a mut Serializer<W>,
        expected_type: &'a Type,
    ) -> Self {
        Self {
            serializer,
            expected_type,
        }
    }
}

impl<'a, W: io::Write> ser::Serializer for &'a mut TypedSerializer<'a, W> {
    type Ok = Type;
    type Error = Error;

    type SerializeSeq = <&'a mut Serializer<W> as ser::Serializer>::SerializeSeq;
    type SerializeTuple = <&'a mut Serializer<W> as ser::Serializer>::SerializeTuple;
    type SerializeTupleStruct = <&'a mut Serializer<W> as ser::Serializer>::SerializeTupleStruct;
    type SerializeTupleVariant = <&'a mut Serializer<W> as ser::Serializer>::SerializeTupleVariant;
    type SerializeMap = <&'a mut Serializer<W> as ser::Serializer>::SerializeMap;
    type SerializeStruct = <&'a mut Serializer<W> as ser::Serializer>::SerializeStruct;
    type SerializeStructVariant =
        <&'a mut Serializer<W> as ser::Serializer>::SerializeStructVariant;

    fn serialize_bool(self, v: bool) -> Result<Type> {
        self.serializer
            .serialize_bool(v)
            .check_type(self.expected_type)
    }

    fn serialize_i8(self, v: i8) -> Result<Type> {
        self.serializer
            .serialize_i8(v)
            .check_type(self.expected_type)
    }

    fn serialize_i16(self, v: i16) -> Result<Type> {
        self.serializer
            .serialize_i16(v)
            .check_type(self.expected_type)
    }

    fn serialize_i32(self, v: i32) -> Result<Type> {
        self.serializer
            .serialize_i32(v)
            .check_type(self.expected_type)
    }

    fn serialize_i64(self, v: i64) -> Result<Type> {
        self.serializer
            .serialize_i64(v)
            .check_type(self.expected_type)
    }

    fn serialize_u8(self, v: u8) -> Result<Type> {
        self.serializer
            .serialize_u8(v)
            .check_type(self.expected_type)
    }

    fn serialize_u16(self, v: u16) -> Result<Type> {
        self.serializer
            .serialize_u16(v)
            .check_type(self.expected_type)
    }

    fn serialize_u32(self, v: u32) -> Result<Type> {
        self.serializer
            .serialize_u32(v)
            .check_type(self.expected_type)
    }

    fn serialize_u64(self, v: u64) -> Result<Type> {
        self.serializer
            .serialize_u64(v)
            .check_type(self.expected_type)
    }

    fn serialize_f32(self, v: f32) -> Result<Type> {
        self.serializer
            .serialize_f32(v)
            .check_type(self.expected_type)
    }

    fn serialize_f64(self, v: f64) -> Result<Type> {
        self.serializer
            .serialize_f64(v)
            .check_type(self.expected_type)
    }

    fn serialize_char(self, v: char) -> Result<Type> {
        self.serializer
            .serialize_char(v)
            .check_type(self.expected_type)
    }

    fn serialize_str(self, v: &str) -> Result<Type> {
        self.serializer
            .serialize_str(v)
            .check_type(self.expected_type)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Type> {
        self.serializer
            .serialize_bytes(v)
            .check_type(self.expected_type)
    }

    fn serialize_none(self) -> Result<Type> {
        self.serializer
            .serialize_none()
            .check_type(self.expected_type)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Type>
    where
        T: ?Sized + Serialize,
    {
        self.serializer
            .serialize_some(value)
            .check_type(self.expected_type)
    }

    fn serialize_unit(self) -> Result<Type> {
        self.serializer
            .serialize_unit()
            .check_type(self.expected_type)
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Type> {
        self.serializer
            .serialize_unit_struct(name)
            .check_type(self.expected_type)
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Type> {
        self.serializer
            .serialize_unit_variant(name, variant_index, variant)
            .check_type(self.expected_type)
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<Type>
    where
        T: ?Sized + Serialize,
    {
        self.serializer
            .serialize_newtype_struct(name, value)
            .check_type(self.expected_type)
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Type>
    where
        T: ?Sized + Serialize,
    {
        self.serializer
            .serialize_newtype_variant(name, variant_index, variant, value)
            .check_type(self.expected_type)
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        match self.expected_type {
            Type::Any => self.serializer.serialize_seq(len),
            Type::Array(ref element_type) => {
                let element_type = *element_type.clone();
                self.serializer
                    .serialize_seq(len)
                    .map(move |ss| ss.with_element_type(element_type))
            }
            _ => Err(Error::UnexpectedType {
                expected: self.expected_type.clone(),
                found: Type::any_array(),
            }),
        }
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        match self.expected_type {
            Type::Any => self.serializer.serialize_tuple(len),
            Type::Struct(ref fields) => self
                .serializer
                .serialize_tuple(len)
                .map(move |ss| ss.with_expected_fields(fields)),
            _ => Err(Error::UnexpectedType {
                expected: self.expected_type.clone(),
                found: Type::Struct(vec![]),
            }),
        }
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        match self.expected_type {
            Type::Any => self.serializer.serialize_tuple_struct(name, len),
            Type::Struct(ref fields) => self
                .serializer
                .serialize_tuple_struct(name, len)
                .map(move |ss| ss.with_expected_fields(fields)),
            _ => Err(Error::UnexpectedType {
                expected: self.expected_type.clone(),
                found: Type::Struct(vec![]),
            }),
        }
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        self.serializer
            .serialize_tuple_variant(name, variant_index, variant, len)
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        match self.expected_type {
            Type::Any => self.serializer.serialize_map(len),
            Type::Struct(ref fields) => self
                .serializer
                .serialize_map(len)
                .map(move |ss| ss.with_expected_fields(fields)),
            _ => Err(Error::UnexpectedType {
                expected: self.expected_type.clone(),
                found: Type::Struct(vec![]),
            }),
        }
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        match self.expected_type {
            Type::Any => self.serializer.serialize_struct(name, len),
            Type::Struct(ref fields) => self
                .serializer
                .serialize_struct(name, len)
                .map(move |ss| ss.with_expected_fields(fields)),
            _ => Err(Error::UnexpectedType {
                expected: self.expected_type.clone(),
                found: Type::Struct(vec![]),
            }),
        }
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant> {
        self.serializer
            .serialize_struct_variant(name, variant_index, variant, len)
    }
}
