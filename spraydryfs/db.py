
'''
A module for setting up the database. Main reference for the schema.
'''

from sqlite3 import connect as sqlite3_connect

'''
The rehydrate table contains configurations for spraying, i.e. turning
ingested data into chunks suitable for storage, and drying, i.e. compressing
each individual chunk.
'''

CREATE_REHYDRATE = '''CREATE TABLE IF NOT EXISTS rehydrate (
    id INTEGER PRIMARY KEY
    , name TEXT NOT NULL
    , version TEXT NOT NULL
    , chunking TEXT NOT NULL
    , algorithm TEXT NOT NULL
    , data BLOB NOT NULL
    , UNIQUE (name, version)
);'''

SETUP_REHYDRATE = '''INSERT OR IGNORE INTO rehydrate (id, name, version, chunking, algorithm, data)
VALUES (0, 'nocompress-fixed', '0.1.0', 'fixed size:0x2000', 'nocompress', X'')
    , (1, 'nocompress-crc32', '0.1.0', 'crc32 cutoff:0x000a0000 initializer:0xfacade00 max:0x4000 min:0x0800', 'nocompress', X'')
'''

'''
Chunks are stored in the chunk table. To avoid SQLite problems around blob
storage, chunk metadata - the chunk's hash, its uncompressed size, the
appropriate rehydrate configuration - is stored in a separate chunkhash table.
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

'''
A file is essentially a supersized chunk that is not stored directly. Like a
chunk, it is identified by its hash; its content is specified by the content
table, mapping file offsets to chunk sizes and data. Not all files are
actually backed by data - directory hashes are computed on the fly, while the
directory's contents are currently only recorded in the entry table.

The database does not enforce the requirement that the chunks in a file leave
no gaps and do not overlap. A one-shot verification can be done via a
outer self-join of content using offset + size as the upper chunk limit.
'''

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

'''
Directory contents are stored in the entry table. The isdirectory flag is
necessary because the target file does not know how its contents should be
interpreted.
The database does not enforce acyclicality. Since acyclicality is equivalent
to the existence of a topological order, an additional entrytree (parent,
child) table would do the trick. The direction of the check - either id <
child_entry or child_entry < id - would constrain the ingestion implementation.
Additionally there's a tradeoff between relying on INTEGER PRIMARY KEY
monotonicity within one transaction in the absence of deletions and using
INTEGER PRIMARY KEY AUTOINCREMENT for properly guaranteed mononicity at the
cost of wasting ROWID values when rolling back on a failed ingestion.
Presumably the correct choice is to default to the safe behaviour and only
allow a switch to the less safe behaviour when performance becomes an issue
and correctness is guaranteed by other means, e.g. by nuking the database if
an ingestion failure occurs. The AUTOINCREMENT penalty could also be mitigated
by explicitly manipulating the sqlite_sequence table after a rollback.
Reference for all of this is https://sqlite.org/autoinc.html .
'''

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

'''
The structure described by the entry table is a directed graph, ideally
acyclic. The root table provides entry points to select a single tree from
this graph.
'''

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

def connect(dbpath, mmap=None):
    '''
    Sets up a writer connection to the database. dbpath must be in uri form.
    '''
    conn = sqlite3_connect(dbpath, uri=True)
    conn.isolation_level = None
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    if mmap is not None:
        conn.execute(f"PRAGMA mmap_size={mmap}")
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
        conn.execute(q)
    return conn

