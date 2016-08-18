//! DB Schema
//!
//! `path` Table
//!  id(SERIAL), path(TEXT)
//!
//! `node` Table
//! id(SERIAL), parent_id(INTEGER), path_id(INTEGER), type, mtime(INTEGER),
//!     size, mode, deleted, hash
//!

use std::error::Error;
use std::fmt;
use time::Timespec;
use rusqlite::{Connection, Statement, Row};
use rusqlite::Error as SqlError;
use rusqlite::types::Value;
use std::path::Path;
use std::convert::{TryFrom, TryInto};

use {EngineConfig, Node, NodeKind, Index, Record};

#[derive(Debug)]
pub enum SqlLightIndexError {
    Connect(String, SqlError),
    CreateTable(String, SqlError),
    CreateStatement(String, SqlError),
    IllegalArgument(String, Option<Node>),
    FailedStatement(String, SqlError),
    FailedNodeStatement(String, Node, SqlError),
    NodeParse(String, Box<Error>),
    Other(String),
}

impl SqlLightIndexError {
    fn other<T>(s: String) -> Result<T, Box<Error>> {
        Err(Box::new(SqlLightIndexError::Other(s)))
    }
}

impl Error for SqlLightIndexError {
    fn description(&self) -> &str {
        "SqlLightIndexError"
    }
}

impl fmt::Display for SqlLightIndexError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            SqlLightIndexError::Other(ref s) => write!(f, "SQLiteIndex Error: {}", s),
            _ => write!(f, "Nice SqlLightIndexError"),
        }
    }
}

static CREATE_TABLE_BACKUP_SET_SQL: &'static str = "
    CREATE TABLE IF NOT EXISTS backup_set (
    id INTEGER PRIMARY KEY,
    at INTEGER NOT NULL
    )";

static INSERT_BACKUP_SET_SQL: &'static str = "INSERT INTO backup_set (at) VALUES (?)";

static CREATE_TABLE_PATH_SQL: &'static str = "
    CREATE TABLE IF NOT EXISTS path (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE
    )";

static CREATE_INDEX_PATH_SQL: &'static str = "
    CREATE INDEX IF NOT EXISTS path_path_index
    ON path (path);
    ";

static SELECT_PATH_SQL: &'static str = "SELECT id FROM path WHERE path = ?";

static INSERT_PATH_SQL: &'static str = "INSERT INTO path (path) VALUES (?)";

static CREATE_TABLE_NODE_SQL: &'static str = "
    CREATE TABLE IF NOT EXISTS node (
    id INTEGER PRIMARY KEY,
    backup_set_id INTEGER NOT NULL,
    parent_id INTEGER NOT NULL,
    path_id INTEGER NOT NULL,
    kind CHAR(1) NOT NULL,
    mtime INTEGER NOT NULL,
    size BIGINT,
    mode INTEGER,
    deleted BOOLEAN NOT NULL,
    hash BLOB
    )";

static CREATE_INDEX_NODE_PATH_ID_SQL: &'static str = "
    CREATE INDEX IF NOT EXISTS node_path_id_index
    ON node (path_id);
    ";

static CREATE_INDEX_NODE_PARENT_ID_SQL: &'static str = "
    CREATE INDEX IF NOT EXISTS node_parent_id_index
    ON node (parent_id);
    ";

static CREATE_INDEX_NODE_BACKUP_SET_ID_SQL: &'static str = "
    CREATE INDEX IF NOT EXISTS node_backup_set_id_index
    ON node (backup_set_id);
    ";

static INSERT_NODE_SQL: &'static str = "
    INSERT INTO node
    (backup_set_id, parent_id, path_id, kind, mtime, size, mode, deleted, hash)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

static GET_ALL_HASHABLE_QUERY_SQL: &'static str = "
    SELECT *
    FROM node
    INNER JOIN path
    ON path.id = node.path_id
    WHERE node.hash is not null
    ORDER BY node.id DESC";

static GET_LATEST_QUERY_SQL: &'static str = "
    SELECT *
    FROM node
    INNER JOIN path
    ON path.id = node.path_id
    WHERE path.path = ?
    ORDER BY node.id DESC
    LIMIT 1";

static LIST_PATH_QUERY_SQL: &'static str = "
    SELECT node.id as id, path.path, backup_set_id, node.kind, node.mtime, node.size, node.mode,
        node.deleted, node.hash
    FROM node
    INNER JOIN path
        ON path.id = node.path_id
    WHERE node.id IN (
        SELECT MAX(node.id)
        FROM node INNER JOIN path as parent_path
            ON node.parent_id = parent_path.id
        WHERE parent_path.path = ?
        GROUP BY path_id
    )
    ORDER BY path.path ASC";

static DUMP_NODES_QUERY_SQL: &'static str = "
    SELECT node.id as node_id, path.id as path_id,
    kind, path, mtime, size, mode, deleted, hash
    FROM node
    INNER JOIN path
    ON path.id = node.path_id
    ORDER BY path.path, node.id ASC";

pub struct SqlLightIndex<'a> {
    conn: &'a Connection,
    insert_path: Statement<'a>,
    select_path: Statement<'a>,
    insert_node: Statement<'a>,
    get_all_hashable: Statement<'a>,
    get_latest: Statement<'a>,
    list_path: Statement<'a>,
    insert_backup_set: Statement<'a>,
}

impl<'a> SqlLightIndex<'a> {
    pub fn open_database(config: &EngineConfig) -> Result<Connection, SqlLightIndexError> {
        let mut db_path = config.abs_working();
        db_path.push("haumaru.idx");

        Ok(Connection::open(&db_path)
            .map_err(|e| {
                SqlLightIndexError::Connect(format!("Failed to open database {:?}", db_path), e)
            })?)
    }
    pub fn new(conn: &'a Connection) -> Result<Self, SqlLightIndexError> {

        conn.execute(CREATE_TABLE_BACKUP_SET_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("backup_set".to_string(), e))?;

        conn.execute(CREATE_TABLE_PATH_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("path".to_string(), e))?;

        let insert_backup_set = try!(conn.prepare(INSERT_BACKUP_SET_SQL)
            .map_err(|e| SqlLightIndexError::CreateStatement("insert_backup_set".to_string(), e)));

        conn.execute(CREATE_INDEX_PATH_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("path_index".to_string(), e))?;

        let select_path = try!(conn.prepare(SELECT_PATH_SQL)
            .map_err(|e| SqlLightIndexError::CreateStatement("select_path".to_string(), e)));

        let insert_path = try!(conn.prepare(INSERT_PATH_SQL)
            .map_err(|e| SqlLightIndexError::CreateStatement("insert_path".to_string(), e)));

        try!(conn.execute(CREATE_TABLE_NODE_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("node".to_string(), e)));

        conn.execute(CREATE_INDEX_NODE_BACKUP_SET_ID_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("node_backup_set".to_string(), e))?;

        conn.execute(CREATE_INDEX_NODE_PATH_ID_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("node_index".to_string(), e))?;

        conn.execute(CREATE_INDEX_NODE_PARENT_ID_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("node_parent".to_string(), e))?;

        let insert_node = try!(conn.prepare(INSERT_NODE_SQL)
            .map_err(|e| SqlLightIndexError::CreateStatement("insert_node".to_string(), e)));

        let get_all_hashable = conn.prepare(GET_ALL_HASHABLE_QUERY_SQL)
            .map_err(|e| SqlLightIndexError::CreateStatement("get_all_hashable".to_string(), e))?;

        let get_latest = try!(conn.prepare(GET_LATEST_QUERY_SQL)
            .map_err(|e| SqlLightIndexError::CreateStatement("get_latest".to_string(), e)));

        let list_path = conn.prepare(LIST_PATH_QUERY_SQL)
            .map_err(|e| SqlLightIndexError::CreateStatement("last_path".to_string(), e))?;

        Ok(SqlLightIndex {
            conn: conn,
            insert_path: insert_path,
            select_path: select_path,
            insert_node: insert_node,
            get_all_hashable: get_all_hashable,
            get_latest: get_latest,
            list_path: list_path,
            insert_backup_set: insert_backup_set,
        })
    }

    fn get_path_id<S>(&mut self, path: S) -> Result<i64, Box<Error>>
        where S: Into<String>
    {
        let path = path.into();
        let mut rows = try!(self.select_path.query(&[&path]));
        while let Some(result_row) = rows.next() {
            let result_row = try!(result_row);
            match result_row.get_checked(0) {
                Ok(Value::Integer(i)) => return Ok(i),
                Ok(n) => {
                    return SqlLightIndexError::other(format!("Wrong type: {:?}", n));
                }
                Err(e) => {
                    error!("Unable to get ID: {}", e);
                    return Err(Box::new(e));
                }
            }
        }

        Ok(try!(self.insert_path.insert(&[&path])))
    }

    pub fn dump_records(&self) {
        let mut stmt = self.conn.prepare(DUMP_NODES_QUERY_SQL).unwrap();
        let mut rows = stmt.query(&[]).unwrap();

        while let Some(row) = rows.next() {
            let row = row.unwrap();
            let id = get_string_from_row(&row, "node_id");
            let path = get_string_from_row(&row, "path");
            let size = get_u64_from_row(&row, "size");
            let mtime: u64 = get_u64_from_row(&row, "mtime");
            let kind = get_string_from_row(&row, "kind");
            let mode = get_u32_from_row(&row, "mode");
            let deleted = get_bool_from_row(&row, "deleted");

            println!("{} {} {} {} {} {} {}",
                     id,
                     path,
                     size,
                     mtime,
                     kind,
                     mode,
                     deleted);

        }
    }
}

impl<'a> Index for SqlLightIndex<'a> {
    fn visit_all_hashable(&mut self,
                          f: &mut FnMut(Node) -> Result<(), Box<Error>>)
                          -> Result<(), Box<Error>> {
        trace!("Listing all hashable");

        let mut rows = self.get_all_hashable
            .query(&[])
            .map_err(|e| {
                SqlLightIndexError::FailedStatement(format!("list_all_hashable failed"), e)
            })?;

        while let Some(row) = rows.next() {
            let row = row?;
            f(row.try_into()?)?;
        }

        Ok(())
    }

    fn latest(&mut self, path: String) -> Result<Option<Node>, Box<Error>> {
        let mut rows = self.get_latest.query(&[&path]).unwrap();
        let row = rows.next();
        if row.is_none() {
            debug!("No record found for key {:?}", path);
            return Ok(None);
        }
        let row = row.unwrap().unwrap();
        let node: Node = row.try_into()?;
        node.validate();
        Ok(Some(node))
    }

    fn insert(&mut self, node: Node) -> Result<Node, Box<Error>> {
        debug!("Inserting {:?}", node);
        node.validate();
        // path_id, kind, mtime, size, mode, deleted, hash

        if node.is_file() {
            let ref node = node;
            if !node.has_hash() && !node.deleted() {
                let msg = "File node missing hash".into();
                let node = Some(node.clone());
                return Err(box SqlLightIndexError::IllegalArgument(msg, node));
            }
            if node.deleted() {
                if node.has_hash() {
                    let msg = "Deleted file can not have hash".into();
                    let node = Some(node.clone());
                    return Err(box SqlLightIndexError::IllegalArgument(msg, node));
                }
            } else {
                if let Some(ref v) = *node.hash() {
                    if v.is_empty() {
                        let msg = "File node hash is empty".into();
                        let node = Some(node.clone());
                        return Err(box SqlLightIndexError::IllegalArgument(msg, node));
                    }
                }
            }
        }

        {
            let path = Path::new(node.path());
            let parent_path = match path.parent() {
                Some(p) => p,
                None => {
                    let msg = "Unable to get parent path".into();
                    let node = Some(node.clone());
                    return Err(box SqlLightIndexError::IllegalArgument(msg, node));
                }
            };
            let parent_path_str = parent_path.to_str().unwrap();

            let id = try!(self.get_path_id(node.path().clone()));
            let parent_id = self.get_path_id(parent_path_str)?;

            debug!("Path id={:?}, key={}", id, node.path());

            let kind;
            let mut size = None;

            match node.kind() {
                NodeKind::File => {
                    kind = "F";
                    size = Some(node.size() as i64);
                }
                NodeKind::Dir => {
                    kind = "D";
                }
            }

            let mode = node.mode() as i64;

            let backup_set_id = node.backup_set().expect("node backup_set") as i64;

            self.insert_node
                .execute(&[&backup_set_id,
                           &parent_id,
                           &id,
                           &kind,
                           &node.mtime().sec,
                           &size,
                           &mode,
                           &node.deleted(),
                           node.hash()])
                .map_err(|e| {
                    SqlLightIndexError::FailedNodeStatement(format!("Insert"), node.clone(), e)
                })?;
        }
        Ok(node)
    }

    fn create_backup_set(&mut self, timestamp: i64) -> Result<u64, Box<Error>> {
        Ok(self.insert_backup_set.insert(&[&timestamp])? as u64)
    }

    fn dump(&self) -> Vec<Record> {
        let mut vec = vec![];

        let mut stmt = self.conn.prepare(DUMP_NODES_QUERY_SQL).unwrap();
        let mut rows = stmt.query(&[]).unwrap();

        while let Some(row) = rows.next() {
            let row = row.unwrap();
            // let id = get_string_from_row(&row, "node_id");
            let path = get_string_from_row(&row, "path");
            let size = get_u64_from_row(&row, "size");
            let kind = match get_string_from_row(&row, "kind").as_ref() {
                "D" => NodeKind::Dir,
                "F" => NodeKind::File,
                n => panic!("Unknown kind: {:?}", n),
            };
            let mode = get_u32_from_row(&row, "mode");
            let deleted = get_bool_from_row(&row, "deleted");

            vec.push(Record {
                kind: kind,
                path: path,
                size: size,
                mode: mode,
                deleted: deleted,
            });
        }

        vec
    }

    fn list(&mut self, path: String) -> Result<Vec<Node>, Box<Error>> {
        trace!("Listing path {}", path);

        let mut rows = self.list_path
            .query(&[&path])
            .map_err(|e| {
                SqlLightIndexError::FailedStatement(format!("list_path failed for {}", path), e)
            })?;

        let mut v = vec![];
        while let Some(row_result) = rows.next() {
            let row = row_result.unwrap();
            let node: Node = row.try_into()?;
            node.validate();
            v.push(node);
        }

        Ok(v)
    }
}

impl<'a, 'stmt> TryFrom<Row<'a, 'stmt>> for Node {
    type Err = Box<Error>;

    fn try_from(row: Row<'a, 'stmt>) -> Result<Self, Self::Err> {
        let path_str: String = row.get("path");

        let mtime: i64 = match row.get_checked("mtime") {
            Ok(Value::Integer(i)) => i,
            Ok(n) => {
                return SqlLightIndexError::other(format!("Wrong type for mtime: {:?}", n));
            }
            Err(e) => {
                error!("Unable to get mtime: {}", e);
                return Err(Box::new(e));
            }
        };

        // let id = get_u64_from_row(&row, "id");
        let backup_set_id = get_u64_from_row(&row, "backup_set_id");
        let size = get_u64_from_row(&row, "size");
        let mode = get_u32_from_row(&row, "mode");

        let kind_char = get_string_from_row(&row, "kind");

        let mut node = match kind_char.as_ref() {
                "F" => Node::new_file(path_str, Timespec::new(mtime, 0), size, mode),
                "D" => Node::new_dir(path_str, Timespec::new(mtime, 0), mode),
                k => return SqlLightIndexError::other(format!("Unknown kind: {}", k)),
            }
            .with_backup_set(backup_set_id);

        let deleted = get_bool_from_row(&row, "deleted");
        if deleted {
            node.set_deleted(true);
        }

        match row.get_checked("hash")? {
            Value::Blob(b) => {
                trace!("Setting hash");
                node = node.with_hash(b)
            }
            Value::Null => trace!("Hash is Null"),
            v => return SqlLightIndexError::other(format!("node.hash is not blob type: {:?}", v)),
        }

        trace!("Building {:?}", node);
        node.validate();

        Ok(node)
    }
}

fn get_string_from_row(row: &Row, name: &str) -> String {
    match row.get_checked(name) {
        Ok(Value::Integer(i)) => i.to_string(),
        Ok(Value::Text(t)) => t,
        Ok(n) => format!("{:?}", n),
        Err(e) => {
            panic!(format!("Unable to get col {} from row: {:?}", name, e));
        }
    }
}

fn get_u64_from_row(row: &Row, name: &str) -> u64 {
    match row.get_checked(name) {
        Ok(Value::Integer(i)) => i as u64,
        Ok(Value::Null) => 0,
        Ok(n) => panic!(format!("Unable to get col {}. Was {:?}", name, n)),
        Err(e) => panic!(format!("Unable to get col {} from row: {:?}", name, e)),
    }
}

fn get_u32_from_row(row: &Row, name: &str) -> u32 {
    match row.get_checked(name) {
        Ok(Value::Integer(i)) => i as u32,
        Ok(n) => panic!(format!("Unable to get col {}. Was {:?}", name, n)),
        Err(e) => panic!(format!("Unable to get col {} from row: {:?}", name, e)),
    }
}

fn get_bool_from_row(row: &Row, name: &str) -> bool {
    match row.get_checked(name) {
        Ok(Value::Integer(i)) => i == 1,
        Ok(n) => panic!(format!("Unable to get col {}. Was {:?}", name, n)),
        Err(e) => panic!(format!("Unable to get col {} from row: {:?}", name, e)),
    }
}

#[cfg(test)]
mod test {
    extern crate env_logger;

    use super::*;
    use rusqlite::Connection;
    use time::Timespec;
    use {Node, Index, NodeKind};

    #[test]
    fn insert_file() {
        let _ = env_logger::init();
        let conn = Connection::open_in_memory().unwrap();
        let mut index = SqlLightIndex::new(&conn).unwrap();

        let mtime = Timespec::new(10, 0);
        let mut n = Node::new_file("a", mtime, 1024, 500).with_backup_set(5);
        n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);

        index.insert(n).unwrap();
    }

    #[test]
    fn delete_file() {
        let _ = env_logger::init();
        let conn = Connection::open_in_memory().unwrap();
        let mut index = SqlLightIndex::new(&conn).unwrap();

        let mtime = Timespec::new(10, 0);
        let mut n = Node::new_file("a", mtime, 1024, 500).with_backup_set(5);
        n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
        let n = n.as_deleted();
        let mtime = n.mtime();

        index.insert(n.clone()).unwrap();
        let mut latest = index.latest("a".to_string()).expect("ok").expect("some");
        latest.set_mtime(mtime.clone());
        assert_eq!(n, latest);
    }

    #[test]
    fn update_node() {
        let _ = env_logger::init();
        let conn = Connection::open_in_memory().unwrap();
        let mut index = SqlLightIndex::new(&conn).unwrap();

        let n = Node::new_file("a", Timespec::new(10, 0), 1024, 500)
            .with_backup_set(5)
            .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);

        index.insert(n.clone()).unwrap();

        let n = Node::new_file("a", Timespec::new(11, 0), 1024, 500)
            .with_backup_set(6)
            .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
        index.insert(n).unwrap();
    }

    #[test]
    fn get_latest_file() {
        let _ = env_logger::init();
        let conn = Connection::open_in_memory().unwrap();
        let mut index = SqlLightIndex::new(&conn).unwrap();

        let mtime = Timespec::new(10, 0);
        let mut n = Node::new_file("a", mtime, 1024, 500).with_backup_set(5);
        n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);

        index.insert(n).unwrap();

        let n = index.latest("a".to_string()).unwrap();
        assert!(n.is_some());
        let n = n.unwrap();

        assert_eq!("a", n.path());
        assert_eq!(&Timespec::new(10, 0), n.mtime());
        assert_eq!(500, n.mode());
        assert_eq!(1024, n.size());
    }

    #[test]
    fn get_latest_dir() {
        let _ = env_logger::init();
        let conn = Connection::open_in_memory().unwrap();
        let mut index = SqlLightIndex::new(&conn).unwrap();

        let mtime = Timespec::new(10, 0);
        let n = Node::new_dir("a", mtime, 500).with_backup_set(5);

        index.insert(n).unwrap();

        let n = index.latest("a".to_string()).unwrap();
        assert!(n.is_some());
        let n = n.unwrap();

        assert_eq!(Some(5), n.backup_set());
        assert_eq!("a", n.path());
        assert_eq!(&Timespec::new(10, 0), n.mtime());
        assert_eq!(500, n.mode());
        assert_eq!(NodeKind::Dir, n.kind());
    }

    #[test]
    fn list() {
        let _ = env_logger::init();
        let conn = Connection::open_in_memory().unwrap();
        let mut index = SqlLightIndex::new(&conn).unwrap();

        let mtime = Timespec::new(10, 0);
        let dir = Node::new_dir("dir", mtime, 500).with_backup_set(5);
        index.insert(dir).unwrap();

        let file_a = Node::new_file("dir/a", mtime, 3, 500)
            .with_backup_set(5)
            .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
        index.insert(file_a.clone()).unwrap();

        let list = index.list("dir".to_string()).unwrap();
        let expected: Vec<Node> = vec![file_a];

        assert_eq!(expected, list);
    }

    #[test]
    fn list_dir_only() {
        let _ = env_logger::init();
        let conn = Connection::open_in_memory().unwrap();
        let mut index = SqlLightIndex::new(&conn).unwrap();

        let mtime = Timespec::new(10, 0);
        let n = Node::new_dir("a", mtime, 500).with_backup_set(5);

        index.insert(n.clone()).unwrap();

        let list = index.list("".to_string()).unwrap();

        let expected: Vec<Node> = vec![n];
        assert_eq!(expected, list);
    }

}
