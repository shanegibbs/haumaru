use std::io::Error as IoError;
use std::io::Write;

use crypto::sha2::Sha256;
use crypto::digest::Digest;

pub struct Hasher {
    hash: Sha256,
}

impl Hasher {
    pub fn new() -> Self {
        Hasher { hash: Sha256::new() }
    }
    pub fn result(&mut self) -> Vec<u8> {
        let mut bytes = [0u8; 32];
        self.hash.result(&mut bytes);
        let mut vec = Vec::with_capacity(32);
        vec.append(&mut bytes.to_vec());
        vec
    }
}

impl Write for Hasher {
    fn write(&mut self, buf: &[u8]) -> Result<usize, IoError> {
        self.hash.input(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<(), IoError> {
        Ok(())
    }
}
