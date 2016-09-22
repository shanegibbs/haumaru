#[macro_use]
extern crate log;
extern crate env_logger;
extern crate haumaru_api;
extern crate rusqlite;

use std::fs::{File, remove_file, create_dir_all, remove_dir, remove_dir_all};
use rusqlite::Connection;
use std::path::PathBuf;
use std::collections::HashSet;
use std::io::{Read, Write, Cursor};
use log::{LogRecord, LogLevelFilter, LogLevel};
use env_logger::LogBuilder;
use std::env;

use haumaru_api::{Engine, Index, Record, NodeKind};
use haumaru_api::engine::*;
use haumaru_api::filesystem::Change;
use haumaru_api::storage::*;
use haumaru_api::index::SqlLightIndex;

fn setup_logging(default_log_str: &str) {

    let format = |record: &LogRecord| {
        let v: Vec<u8> = vec![];
        let mut buf = Cursor::new(v);

        write!(buf,
               "{}",
               match record.level() {
                   LogLevel::Error => "\x1b[31m",
                   LogLevel::Warn => "\x1b[33m",
                   LogLevel::Info => "\x1b[34m",
                   LogLevel::Debug => "\x1b[36m",
                   LogLevel::Trace => "\x1b[36m",
               })
            .unwrap();

        write!(buf, "{}", record.level()).unwrap();
        if record.level() == LogLevel::Warn || record.level() == LogLevel::Info {
            write!(buf, " ").unwrap();
        }
        write!(buf, "\x1b[0m ").unwrap();
        write!(buf, "{} ", record.location().module_path()).unwrap();
        write!(buf, "{}", record.args()).unwrap();
        return String::from_utf8(buf.into_inner()).unwrap();
    };

    let mut builder = LogBuilder::new();
    builder.format(format).filter(None, LogLevelFilter::Info);

    if let Ok(l) = env::var("LOG") {
        builder.parse(&l);
    } else {
        builder.parse(default_log_str);
    }

    let _ = builder.init();
}

fn test_change<'a, F>(name: &str, f: F) -> Vec<Record>
    where F: Fn(&mut DefaultEngine<SqlLightIndex, LocalStorage>, PathBuf)
{
    setup_logging("off");

    // sqlite
    let conn = Connection::open_in_memory().expect("conn");
    let index = SqlLightIndex::new(conn).unwrap();

    // delete and re-create test path
    let test_dir = format!("target/test/{}", name);
    let _ = remove_dir_all(&test_dir);
    create_dir_all(&test_dir).unwrap();
    let path = PathBuf::from(test_dir.clone()).canonicalize().unwrap();

    let mut working_path = path.clone();
    working_path.push("working");

    let mut files_path = path.clone();
    files_path.push("files");
    create_dir_all(&files_path).unwrap();

    let config = EngineConfig::new(working_path.to_str().unwrap().to_string())
        .with_path(files_path.to_str().unwrap().to_string());

    let store = LocalStorage::new(&config).unwrap();

    {
        let mut engine = DefaultEngine::new(config, HashSet::new(), index.clone(), store).unwrap();
        f(&mut engine, files_path);
        engine.wait_for_queue_drain();
    }

    index.dump()
}

fn write_file(path: PathBuf, name: &str, content: &str) -> PathBuf {
    let mut filename = path.clone();
    filename.push(name);
    let mut file = match File::create(filename.clone()) {
        Ok(f) => f,
        Err(e) => {
            panic!(format!("Unable to create file {:?}: {}", filename, e));
        }
    };
    let filename = filename.as_path().canonicalize().unwrap();
    file.write_all(content.as_bytes()).unwrap();
    filename
}

#[test]
fn process_change_transient() {
    let name = "process_change_transient";
    let dump = test_change(name, |engine, path| {
        let mut filename = path.clone();
        filename.push("a");
        let change = Change::new(filename);
        engine.process_change(3, change).unwrap();
    });
    let v: Vec<Record> = vec![];
    assert_eq!(v, dump);
}

#[test]
fn process_change_new_file() {
    let name = "process_change_new_file";

    let dump = test_change(name, |engine, path| {
        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        engine.process_change(3, Change::new(filename)).unwrap();
    });

    let v: Vec<Record> = vec![Record::new(NodeKind::File, "a".into(), 3, 420)];
    assert_eq!(v, dump);
}

#[test]
fn process_change_update_file() {
    let name = "process_change_update_file";

    let dump = test_change(name, |engine, path| {
        {
            let filename = write_file(path.clone(), "a", "abc");
            debug!("Created {:?}", filename);
            engine.process_change(3, Change::new(filename)).unwrap();
        }

        {
            let filename = write_file(path.clone(), "a", "1234");
            debug!("Created {:?}", filename);
            engine.process_change(3, Change::new(filename)).unwrap();
        }
    });

    let v: Vec<Record> = vec![Record::new(NodeKind::File, "a".into(), 3, 420),
                              Record::new(NodeKind::File, "a".into(), 4, 420)];
    assert_eq!(v, dump);
}

#[test]
fn process_change_delete_file() {
    let name = "process_change_delete_file";

    let dump = test_change(name, |engine, path| {
        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        engine.process_change(3, Change::new(filename.clone())).unwrap();
        engine.wait_for_queue_drain();

        remove_file(filename.clone()).unwrap();
        debug!("Deleted {:?}", filename);
        engine.process_change(3, Change::new(filename)).unwrap();

    });

    let v: Vec<Record> = vec![Record::new(NodeKind::File, "a".into(), 3, 420),
                              Record::new(NodeKind::File, "a".into(), 0, 0).deleted()];
    assert_eq!(v, dump);
}

#[test]
fn process_change_skip_dir_update() {
    let name = "process_change_skip_dir_update";

    let dump = test_change(name, |engine, path| {
        let mut subdir = path.clone();
        subdir.push("subdir");

        create_dir_all(subdir.clone()).unwrap();
        debug!("Created {:?}", subdir);
        subdir = subdir.canonicalize().unwrap();

        // TODO

        engine.process_change(3, Change::new(subdir.clone())).unwrap();
        engine.wait_for_queue_drain();

        let filename = write_file(subdir.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        engine.process_change(4, Change::new(filename.clone())).unwrap();
        engine.process_change(4, Change::new(subdir.clone())).unwrap();

    });

    let v: Vec<Record> = vec![Record::new(NodeKind::Dir, "subdir".into(), 0, 493),
                              Record::new(NodeKind::File, "subdir/a".into(), 3, 420)];
    assert_eq!(v, dump);
}

#[test]
fn process_change_file_then_dir() {
    let name = "process_change_file_then_dir";

    let dump = test_change(name, |engine, path| {

        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created {:?}", filename);
        engine.process_change(3, Change::new(filename.clone())).unwrap();
        engine.wait_for_queue_drain();

        remove_file(filename.clone()).unwrap();
        debug!("Deleted {:?}", filename);

        create_dir_all(filename.clone()).unwrap();
        debug!("Created {:?}", filename);
        engine.process_change(4, Change::new(filename.clone())).unwrap();
    });

    let v: Vec<Record> = vec![Record::new(NodeKind::File, "a".into(), 3, 420),
                              Record::new(NodeKind::Dir, "a".into(), 0, 493)];
    assert_eq!(v, dump);
}

#[test]
fn process_change_dir_then_file() {
    let name = "process_change_dir_then_file";

    let dump = test_change(name, |engine, path| {

        let mut n = path.clone();
        n.push("a");
        create_dir_all(n.clone()).unwrap();
        debug!("Created Dir {:?}", n);
        engine.process_change(3, Change::new(n.clone())).unwrap();
        engine.wait_for_queue_drain();

        remove_dir(n.clone()).unwrap();
        debug!("Deleted Dir {:?}", n);

        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created File {:?}", filename);
        engine.process_change(4, Change::new(filename.clone())).unwrap();
    });

    let v: Vec<Record> = vec![Record::new(NodeKind::Dir, "a".into(), 0, 493),
                              Record::new(NodeKind::File, "a".into(), 3, 420)];
    assert_eq!(v, dump);
}

#[test]
fn process_change_deleted_recreated_file() {
    let name = "process_change_deleted_recreated_file";

    let dump = test_change(name, |engine, path| {

        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created File {:?}", filename);
        engine.process_change(3, Change::new(filename.clone())).unwrap();
        engine.wait_for_queue_drain();

        remove_file(filename.clone()).unwrap();
        debug!("Deleted {:?}", filename);
        engine.process_change(4, Change::new(filename.clone())).unwrap();
        engine.wait_for_queue_drain();

        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created File {:?}", filename);
        engine.process_change(5, Change::new(filename.clone())).unwrap();
        engine.wait_for_queue_drain();

    });

    let v: Vec<Record> = vec![Record::new(NodeKind::File, "a".into(), 3, 420),
                              Record::new(NodeKind::File, "a".into(), 0, 0).deleted(),
                              Record::new(NodeKind::File, "a".into(), 3, 420)];
    assert_eq!(v, dump);
}

#[test]
fn scan_new_file() {
    let name = "scan_new_file";

    let dump = test_change(name, |engine, path| {

        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        engine.scan(5).unwrap();
    });

    let v: Vec<Record> = vec![Record::new(NodeKind::File, "a".into(), 3, 420)];
    assert_eq!(v, dump);
}

#[test]
fn scan_new_dir() {
    let name = "scan_new_dir";

    let dump = test_change(name, |engine, path| {

        let mut n = path.clone();
        n.push("a");
        create_dir_all(n.clone()).unwrap();
        debug!("Created {:?}", n);

        engine.scan(5).unwrap();
    });

    let v: Vec<Record> = vec![Record::new(NodeKind::Dir, "a".into(), 0, 493)];
    assert_eq!(v, dump);
}

#[test]
fn scan_updated_file() {
    let name = "scan_updated_file";

    let dump = test_change(name, |engine, path| {

        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created {:?}", filename);
        engine.scan(5).unwrap();

        let filename = write_file(path.clone(), "a", "abcd");
        debug!("Created {:?}", filename);
        engine.scan(6).unwrap();

    });

    let v: Vec<Record> = vec![Record::new(NodeKind::File, "a".into(), 3, 420),
                              Record::new(NodeKind::File, "a".into(), 4, 420)];
    assert_eq!(v, dump);
}

#[test]
fn scan_delete_last_file() {
    let name = "scan_delete_last_file";

    let dump = test_change(name, |engine, path| {

        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created {:?}", filename);
        engine.scan(5).unwrap();

        remove_file(filename.clone()).unwrap();
        debug!("Deleted {:?}", filename);
        engine.scan(6).unwrap();

    });

    let v: Vec<Record> = vec![Record::new(NodeKind::File, "a".into(), 3, 420),
                              Record::new(NodeKind::File, "a".into(), 0, 0).deleted()];
    assert_eq!(v, dump);
}

#[test]
fn scan_deleted_file() {
    let name = "scan_deleted_file";

    let dump = test_change(name, |engine, path| {

        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        let filename = write_file(path.clone(), "b", "abc");
        debug!("Created {:?}", filename);

        engine.scan(5).unwrap();

        remove_file(filename.clone()).unwrap();
        debug!("Deleted {:?}", filename);
        engine.scan(6).unwrap();

    });

    let v: Vec<Record> = vec![Record::new(NodeKind::File, "a".into(), 3, 420),
                              Record::new(NodeKind::File, "b".into(), 3, 420),
                              Record::new(NodeKind::File, "b".into(), 0, 0).deleted()];
    assert_eq!(v, dump);
}

#[test]
fn restore_file_from_root() {
    let name = "restore_file_from_root";
    test_change(name, |engine, path| {
        let filename = write_file(path.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        engine.scan(5).unwrap();

        let mut restore_path = path.clone();
        restore_path.push("restore");
        create_dir_all(&restore_path).expect("mkdir restore");
        let restore_path_str = &restore_path.to_str().expect("Path to_str");

        engine.restore("a", None, restore_path_str).expect("engine restore");

        let mut restored_file = restore_path.clone();
        restored_file.push("a");

        let mut f = File::open(restored_file).expect("open a");
        let mut content = String::new();
        f.read_to_string(&mut content).expect("read_to_string");
        assert_eq!(content, "abc");
    });
}

#[test]
fn restore_file_from_dir() {
    let name = "restore_file_from_dir";
    test_change(name, |engine, path| {

        let mut dir = path.clone();
        dir.push("dir");
        create_dir_all(dir.clone()).unwrap();
        debug!("Created {:?}", dir);

        let filename = write_file(dir.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        engine.scan(5).unwrap();

        let mut restore_path = path.clone();
        restore_path.push("restore");
        create_dir_all(&restore_path).expect("mkdir restore");
        let restore_path_str = &restore_path.to_str().expect("Path to_str");

        engine.restore("dir/a", None, restore_path_str).expect("engine restore");

        let mut restored_file = restore_path.clone();
        restored_file.push("a");

        let mut f = File::open(restored_file).expect("open a");
        let mut content = String::new();
        f.read_to_string(&mut content).expect("read_to_string");
        assert_eq!(content, "abc");
    });
}

#[test]
fn restore_dir_from_root() {
    let name = "restore_dir_from_root";
    test_change(name, |engine, path| {

        let mut dir = path.clone();
        dir.push("dir");
        create_dir_all(dir.clone()).unwrap();
        debug!("Created {:?}", dir);

        let filename = write_file(dir.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        engine.scan(5).unwrap();

        let mut restore_path = path.clone();
        restore_path.push("restore");
        create_dir_all(&restore_path).expect("mkdir restore");
        let restore_path_str = &restore_path.to_str().expect("Path to_str");

        engine.restore("dir", None, restore_path_str).expect("engine restore");

        let mut restored_file = restore_path.clone();
        restored_file.push("dir");
        restored_file.push("a");

        let mut f = File::open(restored_file).expect("open a");
        let mut content = String::new();
        f.read_to_string(&mut content).expect("read_to_string");
        assert_eq!(content, "abc");
    });
}

#[test]
fn restore_dir_from_dir() {
    let name = "restore_dir_from_dir";
    test_change(name, |engine, path| {

        let mut dir = path.clone();
        dir.push("dirA");
        dir.push("dirB");
        create_dir_all(dir.clone()).unwrap();
        debug!("Created {:?}", dir);

        let filename = write_file(dir.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        engine.scan(5).unwrap();

        let mut restore_path = path.clone();
        restore_path.push("restore");
        create_dir_all(&restore_path).expect("mkdir restore");
        let restore_path_str = &restore_path.to_str().expect("Path to_str");

        engine.restore("dirA/dirB", None, restore_path_str).expect("engine restore");

        let mut restored_file = restore_path.clone();
        restored_file.push("dirB");
        restored_file.push("a");

        let mut f = File::open(restored_file).expect("open a");
        let mut content = String::new();
        f.read_to_string(&mut content).expect("read_to_string");
        assert_eq!(content, "abc");
    });
}

#[test]
fn full_restore() {
    let name = "full_restore";
    test_change(name, |engine, path| {

        let mut dir = path.clone();
        dir.push("dirA");
        dir.push("dirB");
        create_dir_all(dir.clone()).unwrap();
        debug!("Created {:?}", dir);

        let filename = write_file(dir.clone(), "a", "abc");
        debug!("Created {:?}", filename);

        let filename = write_file(dir.clone(), "b", "def");
        debug!("Created {:?}", filename);

        let filename = write_file(dir.clone(), "c", "ghi");
        debug!("Created {:?}", filename);

        engine.scan(5).unwrap();

        let mut restore_path = path.clone();
        restore_path.push("restore");
        create_dir_all(&restore_path).expect("mkdir restore");
        let restore_path_str = &restore_path.to_str().expect("Path to_str");

        engine.restore("", None, restore_path_str).expect("engine restore");

        {
            let mut restored_file = restore_path.clone();
            restored_file.push("dirA");
            restored_file.push("dirB");
            restored_file.push("a");

            let mut f = File::open(restored_file).expect("open a");
            let mut content = String::new();
            f.read_to_string(&mut content).expect("read_to_string");
            assert_eq!(content, "abc");
        }

    });
}
