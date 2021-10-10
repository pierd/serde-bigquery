use std::{collections::HashMap, io};

use serde::{ser, Serialize};

use crate::error::{Error, Result};
use crate::{
    ser::{
        identifier::{format_as_identifier, to_identifier},
        serializer::Serializer,
    },
    types::{Field, Type},
};

pub struct StructSerializer<'a, W> {
    serializer: &'a mut Serializer<W>,
    fields: Vec<Field>,
    pending_key: Option<String>,
    fields_buffer: Option<FieldsBuffer<'a>>,
}

impl<'a, W> StructSerializer<'a, W> {
    pub(crate) fn with_serializer(serializer: &'a mut Serializer<W>) -> Self {
        Self {
            serializer,
            fields: Vec::new(),
            pending_key: None,
            fields_buffer: None,
        }
    }

    pub(crate) fn with_expected_fields(self, expected_fields: &'a [Field]) -> Self {
        Self {
            fields_buffer: Some(FieldsBuffer::with_expected_fields(expected_fields)),
            ..self
        }
    }
}

impl<'a, W: io::Write> StructSerializer<'a, W> {
    fn serialize_field<T>(&mut self, key: Option<&str>, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let mut decision = FieldsBufferDecision::Expected;
        if let Some(ref mut fields_buffer) = self.fields_buffer {
            decision = fields_buffer.decide(key, value)?;
        }

        match decision {
            FieldsBufferDecision::Expected => {
                if !self.fields.is_empty() {
                    self.serializer.write(b",")?;
                }
                let field_type = self.serializer.serialize(value)?;

                if let Some(key) = key {
                    if !key.is_empty() {
                        self.serializer
                            .write_fmt(format_args!(" AS {}", format_as_identifier(key)))?;
                    }
                }

                self.fields.push(Field::with_type_and_name(
                    field_type,
                    key.map(|name| name.to_string()),
                ));

                Ok(())
            }
            FieldsBufferDecision::Buffered => Ok(()),
        }
    }

    fn serialize_struct_end(self) -> Result<Type> {
        let Self {
            serializer,
            mut fields,
            fields_buffer,
            ..
        } = self;

        // serialized potentially buffered fields
        if let Some(fields_buffer) = fields_buffer {
            for (field, serialized) in fields_buffer.drain() {
                if !fields.is_empty() {
                    serializer.write(b",")?;
                }
                serializer.write(&serialized)?;

                if let Some(ref key) = field.field_name {
                    if !key.is_empty() {
                        serializer.write_fmt(format_args!(" AS {}", format_as_identifier(key)))?;
                    }
                }

                fields.push(field.clone());
            }
        }

        if fields.is_empty() {
            Err(Error::EmptyStruct)
        } else {
            serializer.write(b")").map(|_| Type::Struct(fields))
        }
    }
}

impl<'a, W: io::Write> ser::SerializeTuple for StructSerializer<'a, W> {
    type Ok = Type;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_field(None, value)
    }

    fn end(self) -> Result<Self::Ok> {
        self.serialize_struct_end()
    }
}

impl<'a, W: io::Write> ser::SerializeTupleStruct for StructSerializer<'a, W> {
    type Ok = Type;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_field(None, value)
    }

    fn end(self) -> Result<Self::Ok> {
        self.serialize_struct_end()
    }
}

impl<'a, W: io::Write> ser::SerializeMap for StructSerializer<'a, W> {
    type Ok = Type;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        assert!(self.pending_key.is_none());
        self.pending_key = Some(to_identifier(key)?);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let mut key = None;
        std::mem::swap(&mut key, &mut self.pending_key);
        self.serialize_field(key.as_deref(), value)
    }

    fn serialize_entry<K: ?Sized, V: ?Sized>(&mut self, key: &K, value: &V) -> Result<()>
    where
        K: Serialize,
        V: Serialize,
    {
        self.serialize_field(Some(&to_identifier(key)?), value)
    }

    fn end(self) -> Result<Self::Ok> {
        self.serialize_struct_end()
    }
}

impl<'a, W: io::Write> ser::SerializeStruct for StructSerializer<'a, W> {
    type Ok = Type;
    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_field(Some(key), value)
    }

    fn end(self) -> Result<Self::Ok> {
        self.serialize_struct_end()
    }
}

enum FieldsBufferDecision {
    Buffered,
    Expected,
}

struct FieldsBuffer<'a> {
    expected_fields: &'a [Field],
    fields_buffer: HashMap<Field, Vec<u8>>,
}

impl<'a> FieldsBuffer<'a> {
    fn with_expected_fields(expected_fields: &'a [Field]) -> Self {
        Self {
            expected_fields,
            fields_buffer: HashMap::new(),
        }
    }

    fn buffer<T>(&mut self, key: &str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize,
    {
        let mut serializer = Serializer { writer: Vec::new() };
        let field_type = value.serialize(&mut serializer)?;
        if self
            .fields_buffer
            .insert(
                Field::with_type_and_name(field_type, Some(key.to_string())),
                serializer.writer,
            )
            .is_some()
        {
            Err(Error::DuplicateStructField(key.to_string()))
        } else {
            Ok(())
        }
    }

    fn decide<T>(&mut self, key: Option<&str>, value: &T) -> Result<FieldsBufferDecision>
    where
        T: ?Sized + Serialize,
    {
        // check if what we've got matches the first expected field
        if let Some((head, tail)) = self.expected_fields.split_first() {
            match (head.field_name.as_ref(), key) {
                (None, _) | (_, None) => {
                    self.expected_fields = tail;
                    Ok(FieldsBufferDecision::Expected)
                }
                (Some(expected_name), Some(name)) => {
                    if expected_name == name {
                        self.expected_fields = tail;
                        Ok(FieldsBufferDecision::Expected)
                    } else {
                        self.buffer(name, value)
                            .map(|_| FieldsBufferDecision::Buffered)
                    }
                }
            }
        } else {
            Err(Error::UnexpectedStructField(Field::with_name(
                key.map(|s| s.to_string()),
            )))
        }
    }

    fn drain(self) -> impl Iterator<Item = (&'a Field, Vec<u8>)> {
        let Self {
            expected_fields,
            mut fields_buffer,
        } = self;
        expected_fields.iter().map(move |field| {
            if let Some(serialized) = fields_buffer.remove(field) {
                (field, serialized)
            } else {
                (field, b"NULL".to_vec())
            }
        })
    }
}
