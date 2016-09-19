use std::error::Error;
use std::fmt;

mod sql_light_index;
pub use index::sql_light_index::*;

#[derive(Debug)]
pub enum IndexError {
    Fatal(String, Option<Box<IndexError>>),
}

impl Error for IndexError {
    fn description(&self) -> &str {
        match *self {
            IndexError::Fatal(ref _s, ref _e) => "Unrecoverable fatal error",
        }
    }
    fn cause(&self) -> Option<&Error> {
        match *self {
            IndexError::Fatal(ref _s, None) => None,
            IndexError::Fatal(ref _s, Some(ref e)) => Some(e.as_ref()),
        }
    }
}

impl fmt::Display for IndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            IndexError::Fatal(ref s, None) => write!(f, "Fatal error: {}", s)?,
            IndexError::Fatal(ref s, Some(ref e)) => write!(f, "{}, caused by: {}", s, e)?,
        }
        Ok(())
    }
}
