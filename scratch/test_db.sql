DROP TABLE IF EXISTS path;
DROP TABLE IF EXISTS node;

CREATE TABLE IF NOT EXISTS path (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE
    );

CREATE TABLE IF NOT EXISTS node (
    id INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL,
    path_id INTEGER NOT NULL,
    kind CHAR(1) NOT NULL,
    mtime INTEGER NOT NULL,
    size BIGINT,
    mode INTEGER,
    deleted BOOLEAN NOT NULL,
    hash BLOB
    );

INSERT INTO path (id, path) VALUES (1, "");
INSERT INTO path (id, path) VALUES (2, "a");
INSERT INTO path (id, path) VALUES (3, "b");
INSERT INTO path (id, path) VALUES (4, "dir");
INSERT INTO path (id, path) VALUES (5, "dir/c");

INSERT INTO node
    (parent_id, path_id, kind, mtime, size, mode, deleted, hash)
    VALUES (1, 2, "F", 1, 1024, 490, 0, "bla");
INSERT INTO node
    (parent_id, path_id, kind, mtime, size, mode, deleted, hash)
    VALUES (1, 2, "F", 2, 1025, 490, 0, "blax");
INSERT INTO node
    (parent_id, path_id, kind, mtime, size, mode, deleted, hash)
    VALUES (1, 3, "F", 1, 1024, 490, 0, "bla");
INSERT INTO node
    (parent_id, path_id, kind, mtime, size, mode, deleted, hash)
    VALUES (1, 4, "D", 1, 0, 490, 0, "");
INSERT INTO node
    (parent_id, path_id, kind, mtime, size, mode, deleted, hash)
    VALUES (4, 5, "F", 1, 0, 490, 0, "");

# list path
SELECT path.path, node.kind, node.mtime, node.size, node.mode,
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
    ORDER BY path.path DESC