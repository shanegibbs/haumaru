use std::error::Error;
use std::fmt;

use {Node, Storage};

#[derive(Debug)]
pub enum LocalStorageError {
}

impl Error for LocalStorageError {
    fn description(&self) -> &str {
        "LocalStorageError"
    }
}

impl fmt::Display for LocalStorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "MyError");
        Ok(())
    }
}

pub struct LocalStorage;

impl LocalStorage {
    pub fn new() -> Self {
        LocalStorage {}
    }
}

impl Storage for LocalStorage {
    fn send(&self, n: Node) -> Result<Node, Box<Error>> {
        Ok(n)
    }
}
