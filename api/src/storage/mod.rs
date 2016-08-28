mod local_storage;
mod s3_storage;

pub use storage::local_storage::*;
pub use storage::s3_storage::*;

use std::path::PathBuf;

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
