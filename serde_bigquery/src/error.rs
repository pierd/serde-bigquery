use std::fmt::{self, Display};

use serde::ser;

use crate::types::Type;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Message(String),
    IOError(std::io::Error),
    FormattingError(std::fmt::Error),
    UnsupportedType,
    EmptyStruct,
    InvalidIdentifierType(Type),
    UnexpectedType(Type, Type),
}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Message(msg) => formatter.write_str(msg),
            Error::IOError(err) => formatter.write_fmt(format_args!("I/O error: {}", err)),
            Error::FormattingError(err) => {
                formatter.write_fmt(format_args!("Formatting error: {}", err))
            }
            Error::UnsupportedType => formatter.write_str("unsupported type"),
            Error::EmptyStruct => formatter.write_str("empty struct"),
            Error::InvalidIdentifierType(t) => {
                formatter.write_fmt(format_args!("invalid identifier type: {}", t))
            }
            Error::UnexpectedType(expected, found) => formatter.write_fmt(format_args!(
                "unexpected type: {} expected: {}",
                found, expected
            )),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IOError(err)
    }
}

impl From<std::fmt::Error> for Error {
    fn from(err: std::fmt::Error) -> Self {
        Error::FormattingError(err)
    }
}
