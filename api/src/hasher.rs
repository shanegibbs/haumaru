use std::io::Error as IoError;
use std::io::Write;

use crypto::sha2::Sha256;
use crypto::md5::Md5;
use crypto::digest::Digest;

pub struct Hasher {
    md5: Md5,
    sha256: Sha256,
}

impl Hasher {
    pub fn new() -> Self {
        Hasher {
            md5: Md5::new(),
            sha256: Sha256::new(),
        }
    }
    pub fn result(&mut self) -> (Vec<u8>, Vec<u8>) {
        let mut bytes = [0u8; 16];
        self.md5.result(&mut bytes);
        let mut md5_vec = Vec::with_capacity(32);
        md5_vec.append(&mut bytes.to_vec());

        let mut bytes = [0u8; 32];
        self.sha256.result(&mut bytes);
        let mut sha256_vec = Vec::with_capacity(32);
        sha256_vec.append(&mut bytes.to_vec());

        (md5_vec, sha256_vec)
    }
}

impl Write for Hasher {
    fn write(&mut self, buf: &[u8]) -> Result<usize, IoError> {
        self.md5.input(buf);
        self.sha256.input(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<(), IoError> {
        Ok(())
    }
}
