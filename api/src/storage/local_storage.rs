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
        debug!("Sending {:?}", n);

        let mut path = PathBuf::new();
        path.push(base);
        path.push(&n.path);

        let mut dst_path = PathBuf::new();
        dst_path.push(&self.target);
        dst_path.push("_");

        debug!("Hashing {:?} to {:?}", path, dst_path);
        let mut hasher = Sha256::new();

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
