
from sqlite3 import connect as sqlite3_connect

CREATE_REHYDRATE = '''CREATE TABLE IF NOT EXISTS rehydrate (
    id INTEGER PRIMARY KEY
    , name TEXT NOT NULL
    , version TEXT NOT NULL
    , chunking TEXT NOT NULL
    , algorithm TEXT NOT NULL
    , data BLOB NOT NULL
    , UNIQUE (name, version)
);'''

SETUP_REHYDRATE = '''INSERT OR IGNORE INTO rehydrate (id, name, chunking, algorithm, data)
VALUES (0, 'nocompress-fixed', 'fixed size:0x2000', 'nocompress', X'')
    , (1, 'nocompress-crc32', 'crc32 cutoff:0x000a0000 initializer:0xfacade00 max:0x4000 min:0x0800', 'nocompress', X'')
'''

CREATE_CHUNKHASH = '''CREATE TABLE IF NOT EXISTS chunkhash (
    id INTEGER PRIMARY KEY
    , rehydrate INTEGER NOT NULL
        REFERENCES rehydrate (id)
        ON DELETE CASCADE
    , size INTEGER NOT NULL
    , data BLOB NOT NULL
    , UNIQUE (id, rehydrate, size)
    , UNIQUE (rehydrate, data)
);'''

CREATE_CHUNK = '''CREATE TABLE IF NOT EXISTS chunk (
    id INTEGER PRIMARY KEY
        REFERENCES chunkhash (id)
        ON DELETE CASCADE
    , data BLOB NOT NULL
);'''

CREATE_FILE = f'''CREATE TABLE IF NOT EXISTS file (
    id INTEGER PRIMARY KEY
    , hash BLOB NOT NULL
    , rehydrate INTEGER NOT NULL
        REFERENCES rehydrate (id)
        ON DELETE CASCADE
    , UNIQUE (hash, rehydrate)
);'''

CREATE_CONTENT = '''CREATE TABLE IF NOT EXISTS content (
    file INTEGER NOT NULL
        REFERENCES file (id)
        ON DELETE CASCADE
    , rehydrate INTEGER NOT NULL
        REFERENCES rehydrate (id)
        ON DELETE CASCADE
    , offset INTEGER NOT NULL
    , size INTEGER NOT NULL
    , chunk INTEGER NOT NULL
        REFERENCES chunk (id)
        ON DELETE RESTRICT
    , PRIMARY KEY (file, rehydrate, offset)
    , FOREIGN KEY (chunk, rehydrate, size)
        REFERENCES chunkhash (id, rehydrate, size)
        ON DELETE RESTRICT
) WITHOUT ROWID;'''

CREATE_ENTRY = '''CREATE TABLE IF NOT EXISTS entry (
    id INTEGER PRIMARY KEY
    , directory INTEGER NOT NULL
        REFERENCES file (id)
    , name BLOB NOT NULL
    , isdirectory BOOL NOT NULL
    , mode BLOB NOT NULL
    , size INTEGER NOT NULL
    , file INTEGER NOT NULL
        REFERENCES file (id)
    , UNIQUE (directory, name)
);'''

CREATE_ROOT = '''CREATE TABLE IF NOT EXISTS root (
    id INTEGER PRIMARY KEY
    , name TEXT NOT NULL
    , version TEXT NOT NULL
    , isdirectory BOOL NOT NULL
    , mode BLOB NOT NULL
    , size INTEGER NOT NULL
    , file INTEGER NOT NULL
        REFERENCES file (id)
    , UNIQUE (name, version)
);'''

def connect(dbpath):
    conn = sqlite3_connect(dbpath, uri=True)
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    for q in [
        CREATE_REHYDRATE
        , SETUP_REHYDRATE
        , CREATE_CHUNKHASH
        , CREATE_CHUNK
        , CREATE_FILE
        , CREATE_CONTENT
        , CREATE_ENTRY
        , CREATE_ROOT
        ]:
        print(q)
        conn.execute(q)
    return conn

