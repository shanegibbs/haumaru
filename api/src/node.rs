use time::{now, Timespec};
use rustc_serialize::hex::ToHex;

#[derive(Debug, Clone, PartialEq)]
pub struct Node {
    /// The backup key. Not the absolute path.
    path: String,
    kind: NodeKind,
    mtime: Timespec,
    size: u64,
    mode: u32,
    deleted: bool,
    hash: Option<Vec<u8>>,
    backup_set: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NodeKind {
    File,
    Dir,
}

impl Node {
    pub fn new<S>(path: S, kind: NodeKind, mtime: Timespec, size: u64, mode: u32) -> Self
        where S: Into<String>
    {
        Node {
            path: path.into(),
            kind: kind,
            mtime: mtime,
            size: size,
            mode: mode,
            deleted: false,
            hash: None,
            backup_set: None,
        }
    }
    pub fn new_file<S>(path: S, mtime: Timespec, size: u64, mode: u32) -> Self
        where S: Into<String>
    {
        Self::new(path, NodeKind::File, mtime, size, mode)
    }
    pub fn new_dir<S>(path: S, mtime: Timespec, mode: u32) -> Self
        where S: Into<String>
    {
        Self::new(path, NodeKind::Dir, mtime, 0, mode)
    }
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn hash(&self) -> &Option<Vec<u8>> {
        &self.hash
    }
    pub fn set_hash(&mut self, hash: Vec<u8>) {
        assert_eq!(32, hash.len(), "hash size");
        self.hash = Some(hash);
    }
    pub fn with_hash(mut self, hash: Vec<u8>) -> Self {
        assert_eq!(32, hash.len(), "hash size");
        self.hash = Some(hash);
        self
    }
    pub fn kind(&self) -> NodeKind {
        self.kind.clone()
    }
    pub fn mtime(&self) -> &Timespec {
        &self.mtime
    }
    #[cfg(test)]
    pub fn set_mtime(&mut self, mtime: Timespec) {
        self.mtime = mtime;
    }
    pub fn mode(&self) -> u32 {
        self.mode
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn deleted(&self) -> bool {
        self.deleted
    }
    pub fn as_deleted(mut self) -> Self {
        self.deleted = true;
        self.size = 0;
        self.mode = 0;
        self.mtime = now().to_timespec();
        self.hash = None;
        self
    }
    pub fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }
    pub fn is_dir(&self) -> bool {
        self.kind == NodeKind::Dir
    }
    pub fn is_file(&self) -> bool {
        self.kind == NodeKind::File
    }
    pub fn has_hash(&self) -> bool {
        self.hash.is_some()
    }
    pub fn hash_string(&self) -> String {
        let hex_b = self.hash().as_ref().expect("hash missing").clone();
        let hex_slice = hex_b.as_slice();
        hex_slice.to_hex()
    }
    pub fn backup_set(&self) -> Option<u64> {
        self.backup_set.clone()
    }
    pub fn set_backup_set(&mut self, backup_set: u64) {
        self.backup_set = Some(backup_set);
    }
    pub fn with_backup_set(mut self, backup_set: u64) -> Self {
        self.backup_set = Some(backup_set);
        self
    }
    pub fn validate(&self) {
        if let Some(ref hash) = self.hash.as_ref() {
            assert_eq!(32, hash.len(), "hash size: {:?}", self);
        }
        if self.kind == NodeKind::File {
            if !self.deleted && self.hash.is_none() {
                panic!("Non-deleted file node has no hash: {:?}", self);
            }
            if self.deleted && self.hash.is_some() {
                panic!("Deleted file node has hash: {:?}", self);
            }
            if self.deleted && self.mode() != 0 {
                panic!("Deleted file node has mode: {:?}", self);
            }
        } else if self.kind == NodeKind::Dir {
            if self.hash.is_some() {
                panic!("Dir has hash: {:?}", self);
            }
            assert_eq!(0, self.size, "Dir has file size");
        }
        if self.backup_set.is_none() {
            panic!("Node has no backup_set: {:?}", self);
        }
    }
}

#[cfg(test)]
mod test {
    extern crate env_logger;

    use super::*;
    use time::Timespec;

    #[test]
    fn validate_file() {
        let n = Node::new_file("a", Timespec::new(10, 0), 1024, 500)
            .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31])
            .with_backup_set(5);
        n.validate();
    }

    #[test]
    #[should_panic]
    fn missing_backup_set() {
        let n = Node::new_file("a", Timespec::new(10, 0), 1024, 500)
            .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
        n.validate();
    }

    #[test]
    #[should_panic]
    fn missing_hash() {
        let n = Node::new_file("a", Timespec::new(10, 0), 1024, 500).with_backup_set(5);
        n.validate();
    }

}