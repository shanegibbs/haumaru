mod local_storage;
mod s3_storage;

pub use storage::local_storage::*;
pub use storage::s3_storage::*;

use std::path::PathBuf;

use std::io;
use std::io::{Read, Cursor};
use std::fs::File;
use std::vec::Vec;
use Node;

pub enum SendRequestReader {
    InMemory(Cursor<Vec<u8>>),
    Disk(File),
}

impl Read for SendRequestReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let reader: &mut Read = match *self {
            SendRequestReader::InMemory(ref mut cur) => cur,
            SendRequestReader::Disk(ref mut file) => file,
        };
        reader.read(buf)
    }
}

pub struct SendRequest {
    md5: Vec<u8>,
    sha256: Vec<u8>,
    node: Node,
    reader: SendRequestReader,
    size: u64,
}

impl SendRequest {
    pub fn new(md5: Vec<u8>,
               sha256: Vec<u8>,
               node: Node,
               reader: SendRequestReader,
               size: u64)
               -> Self {
        SendRequest {
            md5: md5,
            sha256: sha256,
            node: node,
            reader: reader,
            size: size,
        }
    }
    pub fn node(&self) -> &Node {
        &self.node
    }
}

pub fn hash_dir(hash: &String) -> PathBuf {
    let mut path = PathBuf::new();
    path.push(hash[0..2].to_string());
    path.push(hash[2..4].to_string());
    path
}

pub fn hash_path(hash: &String) -> PathBuf {
    let mut path = PathBuf::new();
    path.push(hash_dir(hash));
    path.push(hash[4..].to_string());
    path
}
