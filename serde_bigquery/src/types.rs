use std::fmt::Write;

use crate::error::{Error, Result};
use crate::ser::identifier::format_as_identifier;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Field {
    pub field_type: Type,
    pub field_name: Option<String>,
}

impl Field {
    pub fn with_type_and_name(field_type: Type, field_name: Option<String>) -> Self {
        Self {
            field_type,
            field_name,
        }
    }

    pub fn with_name(field_name: Option<String>) -> Self {
        Self::with_type_and_name(Type::Any, field_name)
    }

    fn merge(&self, other: &Self) -> Option<Self> {
        self.field_type
            .merge(&other.field_type)
            .map(|field_type| Field {
                field_type,
                field_name: match (self.field_name.as_ref(), other.field_name.as_ref()) {
                    (Some(n), _) => Some(n.to_string()),
                    (_, n) => n.map(|s| s.to_string()),
                },
            })
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(ref field_name) = self.field_name {
            f.write_str(&format_as_identifier(field_name))?;
            f.write_char(' ')?
        }
        f.write_fmt(format_args!("{}", self.field_type))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Type {
    Any,
    Bool,
    Number,
    String,
    Bytes,
    Struct(Vec<Field>),
    Array(Box<Type>),
}

impl Type {
    pub fn any_array() -> Self {
        Self::Array(Box::new(Self::Any))
    }

    pub fn matches(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Any, _) => true,
            (_, Self::Any) => true,
            (Self::Bool, Self::Bool) => true,
            (Self::Number, Self::Number) => true,
            (Self::String, Self::String) => true,
            (Self::Bytes, Self::Bytes) => true,
            (Self::Struct(fields), Self::Struct(other_fields)) => {
                fields.len() == other_fields.len()
                    && fields
                        .iter()
                        .zip(other_fields)
                        .all(|(f1, f2)| f1.field_type.matches(&f2.field_type))
            }
            (Self::Array(type_self), Self::Array(type_other)) => type_self.matches(type_other),
            _ => false,
        }
    }

    pub fn merge(&self, other: &Self) -> Option<Self> {
        match (self, other) {
            (Self::Any, _) => Some(other.clone()),
            (_, Self::Any) => Some(self.clone()),
            (Self::Bool, Self::Bool) => Some(Self::Bool),
            (Self::Number, Self::Number) => Some(Self::Number),
            (Self::String, Self::String) => Some(Self::String),
            (Self::Bytes, Self::Bytes) => Some(Self::Bytes),
            (Self::Struct(fields), Self::Struct(other_fields)) => {
                if fields.len() == other_fields.len() {
                    fields
                        .iter()
                        .zip(other_fields)
                        .map(|(f1, f2)| f1.merge(f2))
                        .collect::<Option<Vec<Field>>>()
                        .map(Self::Struct)
                } else {
                    None
                }
            }
            (Self::Array(type_self), Self::Array(type_other)) => type_self
                .merge(type_other)
                .map(|t| Self::Array(Box::new(t))),
            _ => None,
        }
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Any => f.write_char('?'),
            Type::Bool => f.write_str("BOOL"),
            Type::Number => f.write_str("DOUBLE"), // it can also be any numerical type but let's assume it's DOUBLE
            Type::String => f.write_str("STRING"),
            Type::Bytes => f.write_str("BYTES"),
            Type::Struct(fields) => {
                let mut first_field = true;
                f.write_str("STRUCT<")?;
                for field in fields {
                    if first_field {
                        first_field = false;
                    } else {
                        f.write_str(", ")?;
                    }
                    f.write_fmt(format_args!("{}", field))?;
                }
                f.write_str(">")
            }
            Type::Array(t) => write!(f, "ARRAY<{}>", t),
        }
    }
}

pub trait CheckType {
    fn check_type(self, expected: &Type) -> Result<Type>;
}

impl CheckType for Result<Type> {
    fn check_type(self, expected: &Type) -> Result<Type> {
        match self {
            Ok(ref found) => {
                if expected.matches(found) {
                    self
                } else {
                    Err(Error::UnexpectedType {
                        expected: expected.clone(),
                        found: found.clone(),
                    })
                }
            }
            _ => self,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_matches_any() {
        assert!(Type::Any.matches(&Type::Bool));
        assert!(Type::Any.matches(&Type::Number));
        assert!(Type::Any.matches(&Type::String));
        assert!(Type::Any.matches(&Type::Bytes));
        assert!(Type::Any.matches(&Type::Struct(vec![])));
        assert!(Type::Any.matches(&Type::Array(Box::new(Type::Any))));

        assert!(Type::Bool.matches(&Type::Any));
        assert!(Type::Number.matches(&Type::Any));
        assert!(Type::String.matches(&Type::Any));
        assert!(Type::Bytes.matches(&Type::Any));
        assert!(Type::Struct(vec![]).matches(&Type::Any));
        assert!(Type::Array(Box::new(Type::Any)).matches(&Type::Any));
    }

    #[test]
    fn test_matches_same() {
        for t in [
            Type::Bool,
            Type::Number,
            Type::String,
            Type::Bytes,
            Type::Struct(vec![]),
            Type::Array(Box::new(Type::Bool)),
        ] {
            assert!(t.matches(&t));
        }
    }
}
