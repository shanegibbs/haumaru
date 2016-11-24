#![allow(warnings)]
//! DB Schema
//!
//! `path` Table
//!  id(SERIAL), path(TEXT)
//!
//! `node` Table
//! id(SERIAL), parent_id(INTEGER), path_id(INTEGER), type, mtime(INTEGER),
//!     size, mode, deleted, hash
//!


use {EngineConfig, Index, Node, NodeKind, Record};
use index::{BackupSetController, IndexError};
use rusqlite::{CachedStatement, Connection, Row};
use rusqlite::Error as SqlError;
use rusqlite::types::Value;
use std::convert::{TryFrom, TryInto};
use std::error::Error;
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex};
use time::Timespec;

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
    WHERE node.hash is not null and path.path like ?
    ORDER BY path.path, node.backup_set_id ASC";

static GET_LATEST_QUERY_SQL: &'static str = "
    SELECT *
    FROM node
    INNER JOIN path
    ON path.id = node.path_id
    WHERE path.path = ?
    ORDER BY node.id DESC
    LIMIT 1";

static GET_FROM_QUERY_SQL: &'static str = "
    SELECT *
    FROM node
    INNER JOIN path
        ON path.id = node.path_id
    INNER JOIN backup_set
        ON node.backup_set_id = backup_set.id
    WHERE path.path = ?
        AND backup_set.at <= ?
    ORDER BY node.id DESC
    LIMIT 1";

static LIST_LATEST_QUERY_SQL: &'static str = "
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

static LIST_FROM_QUERY_SQL: &'static str = "
    SELECT node.id as id, path.path, backup_set_id, node.kind, node.mtime, node.size, node.mode,
        node.deleted, node.hash
    FROM node
    INNER JOIN path
        ON path.id = node.path_id
    INNER JOIN backup_set
        ON node.backup_set_id = backup_set.id
    WHERE node.id IN (
        SELECT MAX(node.id)
        FROM node INNER JOIN path as parent_path
            ON node.parent_id = parent_path.id
        WHERE parent_path.path = ?
            AND backup_set.at <= ?
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

pub struct SqlLightIndex {
    conn: Arc<Mutex<Connection>>,
    controller: Arc<Mutex<BackupSetController>>,
}

impl Clone for SqlLightIndex {
    fn clone(&self) -> Self {
        SqlLightIndex {
            conn: self.conn.clone(),
            controller: self.controller.clone(),
        }
    }
}

impl SqlLightIndex {
    pub fn open_database(config: &EngineConfig) -> Result<Connection, SqlLightIndexError> {
        let mut db_path = config.abs_working();
        db_path.push("haumaru.idx");

        Ok(Connection::open(&db_path).map_err(|e| {
                SqlLightIndexError::Connect(format!("Failed to open database {:?}", db_path), e)
            })?)
    }
    pub fn new(conn: Connection) -> Result<Self, SqlLightIndexError> {

        conn.execute(CREATE_TABLE_BACKUP_SET_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("backup_set".to_string(), e))?;

        conn.execute(CREATE_TABLE_PATH_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("path".to_string(), e))?;

        conn.execute(CREATE_INDEX_PATH_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("path_index".to_string(), e))?;

        conn.execute(CREATE_TABLE_NODE_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("node".to_string(), e))?;

        conn.execute(CREATE_INDEX_NODE_BACKUP_SET_ID_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("node_backup_set".to_string(), e))?;

        conn.execute(CREATE_INDEX_NODE_PATH_ID_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("node_index".to_string(), e))?;

        conn.execute(CREATE_INDEX_NODE_PARENT_ID_SQL, &[])
            .map_err(|e| SqlLightIndexError::CreateTable("node_parent".to_string(), e))?;

        Ok(SqlLightIndex {
            conn: Arc::new(Mutex::new(conn)),
            controller: Arc::new(Mutex::new(BackupSetController::new())),
        })
    }

    fn insert_path<'conn>(&self, conn: &'conn Connection) -> CachedStatement<'conn> {
        conn.prepare_cached(INSERT_PATH_SQL).expect("insert_path query")
    }

    fn select_path<'conn>(&self, conn: &'conn Connection) -> CachedStatement<'conn> {
        conn.prepare_cached(SELECT_PATH_SQL).expect("select_path query")
    }

    fn insert_node<'conn>(&self, conn: &'conn Connection) -> CachedStatement<'conn> {
        conn.prepare_cached(INSERT_NODE_SQL).expect("insert_node query")
    }

    fn get_all_hashable<'conn>(&self, conn: &'conn Connection) -> CachedStatement<'conn> {
        conn.prepare_cached(GET_ALL_HASHABLE_QUERY_SQL).expect("get_all_hashable query")
    }

    fn get_latest<'conn>(&self, conn: &'conn Connection) -> CachedStatement<'conn> {
        conn.prepare_cached(GET_LATEST_QUERY_SQL).expect("get_latest query")
    }

    fn get_from<'conn>(&self, conn: &'conn Connection) -> CachedStatement<'conn> {
        conn.prepare_cached(GET_FROM_QUERY_SQL).expect("get_from query")
    }

    fn list_latest<'conn>(&self, conn: &'conn Connection) -> CachedStatement<'conn> {
        conn.prepare_cached(LIST_LATEST_QUERY_SQL).expect("list_latest query")
    }

    fn list_from<'conn>(&self, conn: &'conn Connection) -> CachedStatement<'conn> {
        conn.prepare_cached(LIST_FROM_QUERY_SQL).expect("list_from query")
    }

    fn insert_backup_set<'conn>(&self, conn: &'conn Connection) -> CachedStatement<'conn> {
        conn.prepare_cached(INSERT_BACKUP_SET_SQL).expect("insert_backup_set query")
    }

    fn get_path_id<S>(&mut self, path: S) -> Result<i64, IndexError>
        where S: Into<String>
    {
        let conn = self.conn.lock().expect("conn lock");
        let path = path.into();
        {
            let mut select_path = self.select_path(&conn);
            let mut rows = select_path.query(&[&path])
                .map_err(|e| IndexError::Fatal(format!("Select path failed: {}", e), None))?;
            while let Some(result_row) = rows.next() {
                let result_row =
                    result_row.map_err(|e| {
                            IndexError::Fatal(format!("Failed to get result row: {}", e), None)
                        })?;
                match result_row.get_checked(0) {
                    Ok(Value::Integer(i)) => return Ok(i),
                    Ok(n) => {
                        return Err(IndexError::Fatal(format!("Wrong type: {:?}", n), None));
                    }
                    Err(e) => {
                        return Err(IndexError::Fatal(format!("Unable to get path ID: {}", e),
                                                     None));
                    }
                }
            }
        }

        let mut stmt = self.insert_path(&conn);
        Ok(stmt.insert(&[&path])
            .map_err(|e| IndexError::Fatal(format!("Insert query failed: {}", e), None))?)
    }

    fn persist(&mut self, node: &Node) -> Result<(), IndexError> {
        debug!("Inserting {:?}", node);
        node.validate();
        // path_id, kind, mtime, size, mode, deleted, hash

        if node.is_file() {
            let ref node = node;
            if !node.has_hash() && !node.deleted() {
                let msg = "File node missing hash";
                let node = Some(node.clone());
                return Err(IndexError::Fatal(format!("{}: {:?}", msg, node), None));
            }
            if node.deleted() {
                if node.has_hash() {
                    let msg = "Deleted file can not have hash";
                    let node = Some(node.clone());
                    return Err(IndexError::Fatal(format!("{}: {:?}", msg, node), None));
                }
            } else {
                if let Some(ref v) = *node.hash() {
                    if v.is_empty() {
                        let msg = "File node hash is empty";
                        let node = Some(node.clone());
                        return Err(IndexError::Fatal(format!("{}: {:?}", msg, node), None));
                    }
                }
            }
        }

        {
            let path = Path::new(node.path());
            let parent_path = match path.parent() {
                Some(p) => p,
                None => {
                    let msg = "Unable to get parent path";
                    let node = Some(node.clone());
                    return Err(IndexError::Fatal(format!("{}: {:?}", msg, node), None));
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

            let conn = self.conn.lock().expect("conn lock");
            self.insert_node(&conn)
                .execute(&[&backup_set_id,
                           &parent_id,
                           &id,
                           &kind,
                           &node.mtime().sec,
                           &size,
                           &mode,
                           &node.deleted(),
                           node.hash()])
                .map_err(|e| IndexError::Fatal(format!("Insert node query failed: {}", e), None))?;
        }
        Ok(())
    }

    pub fn dump_records(&self) {
        let conn = self.conn.lock().expect("conn lock");
        let mut stmt = conn.prepare(DUMP_NODES_QUERY_SQL).unwrap();
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

impl Index for SqlLightIndex {
    fn visit_all_hashable(&mut self,
                          like: String,
                          f: &mut FnMut(Node) -> Result<(), IndexError>)
                          -> Result<(), IndexError> {
        trace!("Listing all hashable");

        let like = {
            if like.is_empty() {
                "%".to_owned()
            } else {
                format!("%{}%", like)
            }
        };

        let conn = self.conn.lock().expect("conn lock");
        let mut get_all_hashable = self.get_all_hashable(&conn);
        let mut rows = get_all_hashable.query(&[&like])
            .map_err(|e| IndexError::Fatal(format!("list_all_hashable failed: {}", e), None))?;

        while let Some(row) = rows.next() {
            let row =
                row.map_err(|e| IndexError::Fatal(format!("Failed to get next row: {}", e), None))?;
            f(row.try_into()?)?;
        }

        Ok(())
    }

    fn insert(&mut self, node: Node) -> Result<(), IndexError> {
        let mut ctrl = expect!(self.controller.lock(), "backup_set lock");
        let mut backup_set = expect!(ctrl.get(), "backup set");
        backup_set.insert(node);
        Ok(())
    }

    fn get(&mut self, path: String, from: Option<Timespec>) -> Result<Option<Node>, IndexError> {
        let conn = expect!(self.conn.lock(), "conn lock");
        let mut get_latest = self.get_latest(&conn);
        let mut get_from = self.get_from(&conn);
        let mut rows = match from {
            None => expect!(get_latest.query(&[&path]), "get_latest_query"),
            Some(t) => expect!(get_from.query(&[&path, &t.sec]), "get_from_query"),
        };
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

    fn create_backup_set(&mut self, timestamp: i64) -> Result<u64, IndexError> {
        let conn = self.conn.lock().expect("conn lock");
        let mut stmt = self.insert_backup_set(&conn);
        let index = stmt.insert(&[&timestamp])
            .map_err(|e| {
                IndexError::Fatal(format!("Failed to create backup set: {}", e), None)
            })? as u64;

        let mut ctrl = self.controller.lock().expect("backup_set lock");
        ctrl.open(index);

        info!("Opened backup set {}", index);

        Ok(index)
    }

    fn close_backup_set(&mut self) -> Result<(), IndexError> {
        let mut backup_set = {
            let mut ctrl = self.controller.lock().expect("backup_set lock");
            ctrl.flush()
        };

        info!("Closing backup set {}", backup_set.index());

        // persist all nodes in backup_set
        for node in backup_set.iter() {
            self.persist(node)?;
        }

        info!("Backup set {} closed", backup_set.index());

        Ok(())
    }

    fn dump(&self) -> Vec<Record> {
        let mut vec = vec![];
        let conn = self.conn.lock().expect("conn lock");

        let mut stmt = conn.prepare(DUMP_NODES_QUERY_SQL).unwrap();
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

    fn list(&mut self, path: String, from: Option<Timespec>) -> Result<Vec<Node>, IndexError> {
        trace!("Listing path {}", path);
        let conn = self.conn.lock().expect("conn lock");

        let mut query;
        let mut rows = match from {
                None => {
                    query = self.list_latest(&conn);
                    query.query(&[&path])
                }
                Some(t) => {
                    query = self.list_from(&conn);
                    query.query(&[&path, &t.sec])
                }
            }.map_err(|e| IndexError::Fatal(format!("list failed for {}: {}", path, e), None))?;

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
    type Err = IndexError;

    fn try_from(row: Row<'a, 'stmt>) -> Result<Self, Self::Err> {
        let path_str: String = row.get("path");

        let mtime: i64 = match row.get_checked("mtime") {
            Ok(Value::Integer(i)) => i,
            Ok(n) => {
                return Err(IndexError::Fatal(format!("Wrong type for mtime: {:?}", n), None));
            }
            Err(e) => {
                error!("Unable to get mtime: {}", e);
                return Err(IndexError::Fatal(format!("Unable to get mtime: {}", e), None));
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
                k => return Err(IndexError::Fatal(format!("Unknown kind: {}", k), None)),
            }
            .with_backup_set(backup_set_id);

        let deleted = get_bool_from_row(&row, "deleted");
        if deleted {
            node.set_deleted(true);
        }

        match row.get_checked("hash")
            .map_err(|e| IndexError::Fatal(format!("Unable to get hash from row: {}", e), None))? {
            Value::Blob(b) => {
                trace!("Setting hash");
                node = node.with_hash(b)
            }
            Value::Null => trace!("Hash is Null"),
            v => {
                return Err(IndexError::Fatal(format!("node.hash is not blob type: {:?}", v), None))
            }
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

    use {Index, Node, NodeKind};
    use rusqlite::Connection;
    use super::*;
    use time::Timespec;

    fn index() -> SqlLightIndex {
        let _ = env_logger::init();
        let conn = Connection::open_in_memory().unwrap();
        SqlLightIndex::new(conn).unwrap()
    }

    #[test]
    fn insert_file() {
        let mut index = index();
        index.create_backup_set(0).expect("create_backup_set");

        let mtime = Timespec::new(10, 0);
        let mut n = Node::new_file("a", mtime, 1024, 500).with_backup_set(5);
        n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);

        index.insert(n).unwrap();
    }

    #[test]
    fn delete_file() {
        let mut index = index();
        index.create_backup_set(0).expect("create_backup_set");

        let mtime = Timespec::new(10, 0);
        let mut n = Node::new_file("a", mtime, 1024, 500).with_backup_set(5);
        n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
        let n = n.as_deleted();
        let mtime = n.mtime();

        index.insert(n.clone()).unwrap();
        index.close_backup_set();

        let mut latest = index.get("a".to_string(), None).expect("ok").expect("some");
        latest.set_mtime(mtime.clone());
        assert_eq!(n, latest);
    }

    #[test]
    fn update_node() {
        let mut index = index();
        index.create_backup_set(0).expect("create_backup_set");

        let n = Node::new_file("a", Timespec::new(10, 0), 1024, 500)
            .with_backup_set(5)
            .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);

        index.insert(n).unwrap();

        let n = Node::new_file("a", Timespec::new(11, 0), 1024, 500)
            .with_backup_set(6)
            .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
        index.insert(n).unwrap();
    }

    #[test]
    fn get_latest_file() {
        let mut index = index();

        let mtime = Timespec::new(10, 0);
        let mut n = Node::new_file("a", mtime, 1024, 500).with_backup_set(5);
        n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
                        21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);

        expect!(index.create_backup_set(0), "backup set");
        expect!(index.insert(n), "insert");
        expect!(index.close_backup_set(), "close backup set");

        let n = index.get("a".to_string(), None).unwrap();
        assert!(n.is_some());
        let n = n.unwrap();

        assert_eq!("a", n.path());
        assert_eq!(&Timespec::new(10, 0), n.mtime());
        assert_eq!(500, n.mode());
        assert_eq!(1024, n.size());
    }

    #[test]
    fn get_file_from() {
        let mut index = index();

        let bs_a = index.create_backup_set(600).expect("bs_a");
        {
            let mtime = Timespec::new(10, 0);
            let mut n = Node::new_file("a", mtime, 1024, 500).with_backup_set(bs_a);
            n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
            index.insert(n).expect("insert");
        }
        index.close_backup_set().expect("close backup_set");

        let bs_b = index.create_backup_set(1200).expect("bs_b");
        {
            let mtime = Timespec::new(11, 0);
            let mut n = Node::new_file("a", mtime, 1025, 500).with_backup_set(bs_b);
            n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
            index.insert(n).expect("insert");
        }
        index.close_backup_set().expect("close backup_set");

        {
            let n = index.get("a".to_string(), Some(Timespec::new(500, 0))).expect("get");
            assert!(n.is_none());
        }
        {
            let n = index.get("a".to_string(), None).expect("get");
            assert!(n.is_some());
            let n = n.expect("Some node");
            assert_eq!(1025, n.size());
        }
        {
            let n = index.get("a".to_string(), Some(Timespec::new(700, 0))).expect("get");
            assert!(n.is_some());
            let n = n.expect("Some node");
            assert_eq!(1024, n.size());
        }
        {
            let n = index.get("a".to_string(), Some(Timespec::new(1300, 0))).expect("get");
            assert!(n.is_some());
            let n = n.expect("Some node");
            assert_eq!(1025, n.size());
        }
    }

    #[test]
    fn list_from() {
        let mut index = index();

        let bs_a = index.create_backup_set(600).expect("bs_a");
        {
            let mtime = Timespec::new(10, 0);
            let mut n = Node::new_file("a", mtime, 1024, 500).with_backup_set(bs_a);
            n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
            index.insert(n).expect("insert");
        }
        index.close_backup_set().expect("close backup_set");

        let bs_b = index.create_backup_set(1200).expect("bs_b");
        {
            let mtime = Timespec::new(11, 0);
            let mut n = Node::new_file("b", mtime, 1025, 500).with_backup_set(bs_b);
            n.set_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
            index.insert(n).expect("insert");
        }
        index.close_backup_set().expect("close backup_set");

        {
            let list = index.list("".to_string(), Some(Timespec::new(500, 0))).expect("list");
            assert!(list.is_empty());
        }
        {
            let list = index.list("".to_string(), None).expect("list");
            assert!(!list.is_empty());
            assert!(list.get(0).expect("node").path() == "a");
            assert!(list.get(1).expect("node").path() == "b");
            assert_eq!(2, list.len());
        }
        {
            let list = index.list("".to_string(), Some(Timespec::new(700, 0))).expect("list");
            assert!(!list.is_empty());
            assert!(list.get(0).expect("node").path() == "a");
            assert_eq!(1, list.len());
        }
        {
            let list = index.list("".to_string(), Some(Timespec::new(1300, 0))).expect("list");
            assert!(!list.is_empty());
            assert!(list.get(0).expect("node").path() == "a");
            assert!(list.get(1).expect("node").path() == "b");
            assert_eq!(2, list.len());
        }
    }

    #[test]
    fn get_latest_dir() {
        let mut index = index();

        let mtime = Timespec::new(10, 0);
        let n = Node::new_dir("a", mtime, 500).with_backup_set(5);

        expect!(index.create_backup_set(0), "backup set");
        expect!(index.insert(n), "insert");
        expect!(index.close_backup_set(), "close backup set");

        let n = index.get("a".to_string(), None).unwrap();
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
        let mut index = index();
        expect!(index.create_backup_set(0), "backup set");

        let mtime = Timespec::new(10, 0);
        let dir = Node::new_dir("dir", mtime, 500).with_backup_set(5);
        expect!(index.insert(dir), "insert");

        let file_a = Node::new_file("dir/a", mtime, 3, 500)
            .with_backup_set(5)
            .with_hash(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                            20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31]);
        expect!(index.insert(file_a.clone()), "insert");

        expect!(index.close_backup_set(), "close backup set");

        let list = index.list("dir".to_string(), None).unwrap();
        let expected: Vec<Node> = vec![file_a];

        assert_eq!(expected, list);
    }

    #[test]
    fn list_dir_only() {
        let mut index = index();
        let mtime = Timespec::new(10, 0);
        let n = Node::new_dir("a", mtime, 500).with_backup_set(5);

        expect!(index.create_backup_set(0), "backup set");
        expect!(index.insert(n.clone()), "insert");
        expect!(index.close_backup_set(), "close backup set");

        let list = index.list("".to_string(), None).unwrap();

        let expected: Vec<Node> = vec![n];
        assert_eq!(expected, list);
    }

}
