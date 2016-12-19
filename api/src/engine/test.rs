extern crate env_logger;

use std::io::Cursor;
use std::collections::HashSet;
use rusqlite::Connection;
use time::Timespec;

use index::SqlLightIndex;
use storage::LocalStorage;
use engine::DefaultEngine;
use {Node, Index, Engine, EngineConfig};

fn test_list(key: &str, f: &Fn(&mut Index)) -> String {
    let _ = env_logger::init();

    let conn = Connection::open_in_memory().expect("conn");
    let mut index = SqlLightIndex::new(conn).expect("index");
    let config = EngineConfig::new_detached("target/test/list_file");
    let store = LocalStorage::new(&config).expect("store");

    expect!(index.create_backup_set(0), "create backup set");
    f(&mut index);
    expect!(index.close_backup_set(), "close backup set");

    let mut engine = DefaultEngine::new(config, HashSet::new(), index, store).expect("new engine");
    let mut cur = Cursor::new(Vec::new());
    engine.list(key, None, &mut cur).expect("list");
    String::from_utf8(cur.into_inner()).expect("from_utf8")
}

#[test]
fn list_root_empty() {
    let output = test_list("", &|_index| {});
    assert_eq!("", output.as_str());
}

#[test]
fn list_root() {
    let output = test_list("",
                           &|index| {
        index.insert(Node::new_file("a", Timespec::new(10, 0), 1024, 500)
                .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                                18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31])
                .with_backup_set(5))
            .expect("insert");
    });
    assert_eq!("-rwxrw-r-- 1024B Dec 31 18:00 a\n", output.as_str());
}

#[test]
fn list_file() {
    let output = test_list("a",
                           &|index| {
        index.insert(Node::new_file("a", Timespec::new(10, 0), 1024, 500)
                .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                                18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31])
                .with_backup_set(5))
            .expect("insert");
    });
    assert_eq!("Name:   a\nSize:   1024 bytes\nTime:   Dec 31 18:00 -0600\nSHA256: \
                000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f\n",
               output.as_str());
}

#[test]
fn list_dir() {
    let output = test_list("a",
                           &|index| {
        index.insert(Node::new_dir("a", Timespec::new(10, 0), 500).with_backup_set(5))
            .expect("insert dir");
        index.insert(Node::new_dir("a/dir", Timespec::new(10, 0), 488).with_backup_set(5))
            .expect("insert dir");
        index.insert(Node::new_file("a/file", Timespec::new(10, 0), 1024, 420)
                .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                                18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31])
                .with_backup_set(5))
            .expect("insert_file");
    });
    assert_eq!("drwxr-x--- 0B Dec 31 18:00 a/dir\n-rw-r--r-- 1024B Dec 31 18:00 a/file\n",
               output.as_str());
}

#[test]
fn list_empty_dir() {
    let output = test_list("a",
                           &|index| {
                               index.insert(Node::new_dir("a", Timespec::new(10, 0), 500)
                                       .with_backup_set(5))
                                   .expect("insert dir");
                           });
    assert_eq!("", output.as_str());
}
