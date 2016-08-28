use std::error::Error;
use std::fmt;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use std::path::PathBuf;
use std::fs::File;
use std::io;
use std::io::{Read, copy};
use std::fs::{create_dir_all, rename};
use rustc_serialize::hex::ToHex;

use {EngineConfig, Node, Storage};
use storage::{hash_dir, hash_path};

#[derive(Debug)]
pub enum LocalStorageError {
    Generic(String),
    Io(String, io::Error),
}

impl Error for LocalStorageError {
    fn description(&self) -> &str {
        "LocalStorageError"
    }
}

impl fmt::Display for LocalStorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Nice LocalStorageError error here").unwrap();
        Ok(())
    }
}

pub struct LocalStorage {
    target: String,
}

impl LocalStorage {
    pub fn new(config: &EngineConfig) -> Result<Self, LocalStorageError> {
        let mut storage_path = PathBuf::new();
        storage_path.push(config.working());
        storage_path.push("store");

        if !storage_path.exists() {
            create_dir_all(&storage_path)
                .map_err(|e| {
                    LocalStorageError::Generic(format!("Unable to create storage path {:?}: {}",
                                                       storage_path,
                                                       e))
                })?;
        }
        if !storage_path.is_dir() {
            return Err(LocalStorageError::Generic(format!("Storage path is not a directory: \
                                                           {:?}",
                                                          storage_path)));
        }
        Ok(LocalStorage { target: storage_path.to_str().unwrap().to_string() })
    }
}

impl Storage for LocalStorage {
    fn send(&self, hash: &[u8], mut ins: Box<Read>) -> Result<(), Box<Error>> {
        // fn send(&self, base: String, mut n: Node) -> Result<Node, Box<Error>> {

        let hex = hash.to_hex();

        let mut hash_filename = PathBuf::new();
        hash_filename.push(&self.target);
        hash_filename.push(hash_path(&hex));

        if hash_filename.exists() {
            debug!("Already have {}", hex);
            return Ok(());
        }

        debug!("Sending {:?}", hash);

        let mut dst_path = PathBuf::new();
        dst_path.push(&self.target);
        dst_path.push("_");

        debug!("Writing to {:?}", dst_path);

        // move into final name
        let mut dir = PathBuf::new();
        dir.push(&self.target);
        dir.push(hash_dir(&hex));
        debug!("Creating dir {:?}", dir);
        create_dir_all(&dir)
            .map_err(|e| {
                LocalStorageError::Generic(format!("Failed to create dir {:?}: {}", dir, e))
            })?;

        debug!("Writing to {:?}", dst_path);
        let mut dst_file = File::create(&dst_path)?;
        copy(&mut ins, &mut dst_file)
            .map_err(|e| LocalStorageError::Io(format!("Failed writing to: {:?}", dst_path), e))?;

        debug!("Moving new hash to {:?}", hash_filename);
        rename(dst_path, &hash_filename)
            .map_err(|e| {
                LocalStorageError::Generic(format!("Failed to rename to {:?}: {}",
                                                   hash_filename,
                                                   e))
            })?;

        Ok(())
    }

    fn retrieve(&self, hash: &[u8]) -> Result<Option<Box<Read>>, Box<Error>> {
        let hex = hash.to_hex();

        let mut hash_filename = PathBuf::new();
        hash_filename.push(&self.target);
        hash_filename.push(hash_path(&hex));

        Ok(Some(box File::open(hash_filename)?))
    }

    fn verify(&self, node: Node) -> Result<Option<Node>, Box<Error>> {
        trace!("store.verify {:?}", node);

        let hex = node.hash_string();
        let mut hash_filename = PathBuf::new();
        hash_filename.push(&self.target);
        hash_filename.push(hash_path(&hex));

        if !hash_filename.exists() {
            error!("Hash missing: {}", hex);
            return Ok(Some(node));
        }

        let mut src_file = File::open(hash_filename)?;
        let mut hasher = Sha256::new();

        let mut buffer = [0; 65536];

        loop {
            let read = src_file.read(&mut buffer[..])?;
            if read == 0 {
                break;
            }

            trace!("Read {} bytes", read);
            hasher.input(&buffer[0..read]);
        }

        let mut bytes = [0u8; 32];
        hasher.result(&mut bytes);
        let mut vec = Vec::with_capacity(32);
        vec.append(&mut bytes.to_vec());

        if vec != node.hash().clone().expect("can not validate without hash") {
            error!("Hash checksum failed: {}", hex);
            return Ok(Some(node));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod test {
    extern crate env_logger;

    use super::*;
    use std::fs::{File, create_dir_all, remove_dir_all};
    use std::io::{Cursor, Read};
    use std::path::PathBuf;
    use {EngineConfig, Storage};

    #[test]
    fn send_empty_file() {
        let name = "local_storage_send_empty_file";

        // begin setup
        let test_dir = format!("target/test/{}", name);
        let _ = remove_dir_all(&test_dir);
        create_dir_all(&test_dir).expect("mkdir test_dir");
        let path = PathBuf::from(test_dir.clone()).canonicalize().expect("canonicalize test_dir");
        // end setup

        let config = EngineConfig::new(test_dir.clone());

        let hash = vec![227, 176, 196, 66, 152, 252, 28, 20, 154, 251, 244, 200, 153, 111, 185,
                        36, 39, 174, 65, 228, 100, 155, 147, 76, 164, 149, 153, 27, 120, 82, 184,
                        85];
        let cursor = Cursor::new(vec![]);

        let storage = LocalStorage::new(&config).expect("new local storage");
        storage.send(&hash, box cursor).expect("Send stream");

        let mut hash_filename = path.clone();
        hash_filename.push("store");
        hash_filename.push("e3");
        hash_filename.push("b0");
        hash_filename.push("c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

        let mut f = File::open(hash_filename).expect("hash_filename exist");
        let mut s = String::new();
        f.read_to_string(&mut s).expect("read hash_filename");
        assert_eq!(s, "");
    }

    #[test]
    fn send_small_file() {
        let name = "local_storage_send_small_file";

        // begin setup
        let test_dir = format!("target/test/{}", name);
        let _ = remove_dir_all(&test_dir);
        create_dir_all(&test_dir).expect("mkdir test_dir");
        let path = PathBuf::from(test_dir.clone()).canonicalize().expect("canonicalize test_dir");
        // end setup

        let config = EngineConfig::new(test_dir.clone());

        let hash = vec![116, 231, 229, 187, 157, 34, 214, 219, 38, 191, 118, 148, 109, 64, 255,
                        243, 234, 159, 3, 70, 184, 132, 253, 6, 148, 146, 15, 204, 250, 209, 94,
                        51];
        let content = "0123456789abcdefghijklmnopqrstuvwxyz";
        let cursor = Cursor::new(content.to_string().into_bytes());

        let storage = LocalStorage::new(&config).expect("new local storage");
        storage.send(&hash, box cursor).expect("Send stream");

        let mut hash_filename = path.clone();
        hash_filename.push("store");
        hash_filename.push("74");
        hash_filename.push("e7");
        hash_filename.push("e5bb9d22d6db26bf76946d40fff3ea9f0346b884fd0694920fccfad15e33");

        let mut f = File::open(hash_filename).expect("hash_filename exist");
        let mut s = String::new();
        f.read_to_string(&mut s).expect("read hash_filename");
        assert_eq!(s, content);
    }
}
