use std::slice::Iter;

use Node;

pub struct BackupSetController {
    current: Option<BackupSet>,
}

/// Holds all records of a backup set and then persists to the index on close.
pub struct BackupSet {
    index: u64,
    in_memory: Vec<Node>,
}

impl BackupSetController {
    pub fn new() -> Self {
        BackupSetController { current: None }
    }
    pub fn open(&mut self, index: u64) {
        if self.current.is_some() {
            panic!("backup set already open");
        }
        self.current = Some(BackupSet::new(index));
    }
    pub fn flush(&mut self) -> BackupSet {
        if self.current.is_none() {
            panic!("no backup set open");
        }
        self.current.take().unwrap()
    }
    pub fn close(&mut self) {
        self.current = None;
    }
    pub fn get(&mut self) -> Option<&mut BackupSet> {
        self.current.as_mut()
    }
}

impl BackupSet {
    fn new(index: u64) -> Self {
        BackupSet {
            index: index,
            in_memory: vec![],
        }
    }
    pub fn index(&self) -> u64 {
        self.index
    }
    pub fn insert(&mut self, node: Node) {
        self.in_memory.push(node);
    }
    pub fn iter(&mut self) -> Iter<Node> {
        self.in_memory.iter()
    }
}
