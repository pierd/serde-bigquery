use std::fmt::{self, Display};

use serde::ser;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Message(String),
    UnsupportedType,
    EmptyStruct,
}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(match self {
            Error::Message(msg) => msg,
            Error::UnsupportedType => "unsupported type",
            Error::EmptyStruct => "empty struct",
        })
    }
}

impl std::error::Error for Error {}
