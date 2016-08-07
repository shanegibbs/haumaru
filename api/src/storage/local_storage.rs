use std::error::Error;
use std::fmt;
use crypto::digest::Digest;
use crypto::sha2::Sha256;
use rustc_serialize::hex::ToHex;
use std::path::PathBuf;
use std::fs::File;
use std::io;
use std::io::{Read, Write};
use std::fs::{create_dir_all, rename};

use {Node, Storage};

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
    pub fn new(target: String) -> Result<Self, LocalStorageError> {
        let pb = PathBuf::from(&target);
        if !pb.exists() {
            return Err(LocalStorageError::Generic(format!("Storage path does not exist: {}",
                                                          target)));
        }
        if !pb.is_dir() {
            return Err(LocalStorageError::Generic(format!("Storage path is not a directory: {}",
                                                          target)));
        }
        Ok(LocalStorage { target: target })
    }
}

fn hash_dir(hash: &String) -> PathBuf {
    let mut path = PathBuf::new();
    path.push(hash[0..2].to_string());
    path.push(hash[2..4].to_string());
    path
}

fn hash_path(hash: &String) -> PathBuf {
    let mut path = PathBuf::new();
    path.push(hash_dir(hash));
    path.push(hash[4..].to_string());
    path
}

impl Storage for LocalStorage {
    fn send(&self, base: &String, mut n: Node) -> Result<Node, Box<Error>> {
        use std::io::Cursor;

        debug!("Sending {:?}", n);

        let mut path = PathBuf::new();
        path.push(base);
        path.push(&n.path);

        let mut dst_path = PathBuf::new();
        dst_path.push(&self.target);
        dst_path.push("_");

        debug!("Hashing {:?} to {:?}", path, dst_path);
        let mut hasher = Sha256::new();

        let mut buffer: Vec<u8> = vec![];
        let cursor = Cursor::new(&mut buffer);

        {
            let mut src_file = File::open(path)?;
            let mut dst_file = File::create(&dst_path)?;

            let mut buffer = [0; 4096];

            loop {
                let read = src_file.read(&mut buffer[..])?;
                if read == 0 {
                    break;
                }

                trace!("Read {} bytes", read);
                hasher.input(&buffer[0..read]);
                dst_file.write(&buffer[0..read])
                    .map_err(|e| {
                        LocalStorageError::Io(format!("Failed writing to: {:?}", dst_path), e)
                    })?;
            }
        }

        // calc hash
        let mut bytes = [0u8; 32];
        hasher.result(&mut bytes);
        let mut vec = Vec::with_capacity(32);
        vec.append(&mut bytes.to_vec());
        n.hash = Some(vec);

        // hex string
        let hex_b = n.hash.as_ref().unwrap().clone();
        let hex_slice = hex_b.as_slice();
        let hex: String = hex_slice.to_hex();

        // move into final name
        let mut dir = PathBuf::new();
        dir.push(&self.target);
        dir.push(hash_dir(&hex));
        debug!("Creating dir {:?}", dir);
        create_dir_all(&dir)
            .map_err(|e| {
                LocalStorageError::Generic(format!("Failed to create dir {:?}: {}", dir, e))
            })?;
        let mut hash_filename = PathBuf::new();
        hash_filename.push(&self.target);
        hash_filename.push(hash_path(&hex));

        if hash_filename.exists() {
            debug!("Already have {}", hex);
        } else {
            debug!("Moving new hash to {:?}", hash_filename);
            rename(dst_path, &hash_filename)
                .map_err(|e| {
                    LocalStorageError::Generic(format!("Failed to rename to {:?}: {}",
                                                       hash_filename,
                                                       e))
                })?;
        }

        Ok(n)
    }
}

#[cfg(test)]
mod test {
    extern crate env_logger;

    use super::*;
    use std::fs::{File, create_dir_all, remove_dir_all};
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use {Node, Storage};
    use time::Timespec;

    #[test]
    fn send_empty_file() {
        let name = "local_storage_send_empty_file";

        // begin setup
        let test_dir = format!("target/test/{}", name);
        let _ = remove_dir_all(&test_dir);
        create_dir_all(&test_dir).unwrap();
        let path = PathBuf::from(test_dir.clone()).canonicalize().unwrap();
        // end setup

        let mut filename = path.clone();
        filename.push("a");
        let content = "";

        {
            let mut f = File::create(&filename).unwrap();
            f.write_all(content.as_bytes()).unwrap();
        }

        let storage = LocalStorage::new(test_dir.clone()).unwrap();
        let node = storage.send(&test_dir, Node::new_file("a", Timespec::new(10, 0), 0, 490))
            .unwrap();

        let mut hash_filename = path.clone();
        hash_filename.push("e3");
        hash_filename.push("b0");
        hash_filename.push("c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

        let mut f = File::open(hash_filename).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        assert_eq!(s, "");
        assert_eq!(Some(vec![227, 176, 196, 66, 152, 252, 28, 20, 154, 251, 244, 200, 153, 111,
                             185, 36, 39, 174, 65, 228, 100, 155, 147, 76, 164, 149, 153, 27,
                             120, 82, 184, 85]),
                   node.hash)

    }

    #[test]
    fn send_small_file() {
        let name = "local_storage_send_small_file";

        // begin setup
        let test_dir = format!("target/test/{}", name);
        let _ = remove_dir_all(&test_dir);
        create_dir_all(&test_dir).unwrap();
        let path = PathBuf::from(test_dir.clone()).canonicalize().unwrap();
        // end setup

        let mut filename = path.clone();
        filename.push("a");
        let content = "0123456789abcdefghijklmnopqrstuvwxyz";

        {
            let mut f = File::create(&filename).unwrap();
            f.write_all(content.clone().as_bytes()).unwrap();
        }

        let storage = LocalStorage::new(test_dir.clone()).unwrap();
        let node = storage.send(&test_dir, Node::new_file("a", Timespec::new(10, 0), 0, 490))
            .unwrap();

        let mut hash_filename = path.clone();
        hash_filename.push("74");
        hash_filename.push("e7");
        hash_filename.push("e5bb9d22d6db26bf76946d40fff3ea9f0346b884fd0694920fccfad15e33");

        let mut f = File::open(hash_filename).unwrap();
        let mut s = String::new();
        f.read_to_string(&mut s).unwrap();
        assert_eq!(s, content);
        assert_eq!(Some(vec![116, 231, 229, 187, 157, 34, 214, 219, 38, 191, 118, 148, 109, 64,
                             255, 243, 234, 159, 3, 70, 184, 132, 253, 6, 148, 146, 15, 204, 250,
                             209, 94, 51]),
                   node.hash)
    }

}