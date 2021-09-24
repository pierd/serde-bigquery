use std::fmt::{self, Display};

use serde::ser;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Message(String),
    IOError(std::io::Error),
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
        match self {
            Error::Message(msg) => formatter.write_str(msg),
            Error::IOError(err) => formatter.write_fmt(format_args!("I/O error: {}", err)),
            Error::UnsupportedType => formatter.write_str("unsupported type"),
            Error::EmptyStruct => formatter.write_str("empty struct"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IOError(err)
    }
}
