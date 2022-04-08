
from dataclasses import dataclass, field
from hashlib import blake2b
from sqlite3 import connect

from pyzstd import EndlessZstdDecompressor, ZstdDict

@dataclass
class Entry():
    inode: int
    parent: int
    name: bytes
    isdir: bool
    mode: int
    size: int
    file: int
    def __post_init__(self):
        if isinstance(self.mode, bytes):
            self.mode = int.from_bytes(self.mode, 'little')
        return None

class Rehydrator():
    '''
    A class to transparently reassemble files from the chunks in the database.
    '''
    def __init__(self, dbpath, mmap=None):
        '''
        Setting up a readonly connection and the rehydrator function.
        '''
        self._db = dbpath
        self._reader = connect('file:'+dbpath+'?mode=ro', uri=True)
        if mmap is not None:
            self._reader.execute(f'PRAGMA mmap_size={mmap}')
        self._rehydrate = make_rehydrator(self._reader)
        self._reader.create_function('rehydrator', 3, self._rehydrate, deterministic=True)
        return None
    def __enter__(self):
        '''
        Nothing to do here.
        '''
        return None
    def __exit__(self, exc_type, exc, tb):
        '''
        Closing up.
        '''
        self.close()
        return None
    def close(self):
        '''
        Closing the database connection.
        '''
        self._reader.close()
        return None
    def root(self, name, version):
        '''
        Get the specified root, if possible.
        '''
        for (risdir, rmode, rsize, rfile) in self._reader.execute(
            'SELECT isdirectory, mode, size, file FROM root WHERE name = ? AND version = ?'
            , (name, version)
            ):
            return Entry(None, None, name, risdir, rmode, rsize, rfile)
        return None
    def attributes(self, entryid):
        '''
        Get a specific entry.
        '''
        q = '\n'.join([
            'SELECT id, directory, name, isdirectory, mode, size, file'
            , 'FROM entry'
            , 'WHERE id = ?'
            ])
        res = self._reader.execute(q, (entryid,)).fetchone()
        if res is None:
            return None
        return Entry(*res)
    def entry(self, dirid, name):
        '''
        Find an entry by directory and name.
        '''
        q = '\n'.join([
            'SELECT id, directory, name, isdirectory, mode, size, file'
            , 'FROM entry'
            , 'WHERE directory = ?'
            , '  AND name = ?'
            ])
        res = self._reader.execute(q, (dirid, name)).fetchone()
        if res is None:
            return None
        return Entry(*res)
    def listgen(self, dirid, offset=0):
        '''
        List the complete contents of the directory. The FUSE translation
        appears to only ever fetch one entry here...
        '''
        q = '\n'.join([
            'SELECT rownum, id, directory, name, isdirectory, mode, size, file'
            , 'FROM ('
            , '  SELECT ROW_NUMBER() OVER ( ORDER BY name ) AS rownum'
            , '    , id, directory, name, isdirectory, mode, size, file'
            , '  FROM entry'
            , '  WHERE directory = ?'
            , ')'
            , 'WHERE rownum > ?'
            ])
        for res in self._reader.execute(q, (dirid, offset)):
            yield res[0], Entry(*res[1:])
    def pread(self, fileid, offset, size):
        '''
        Read a part of the file.
        '''
        return b''.join(self.preadgen(fileid, offset, size))
    def preadgen(self, fileid, offset, size):
        '''
        Read a part of the file in blocks roughly corresponding to the chunks
        in the database.
        '''
        end = offset + size
        q = '\n'.join([
            'SELECT rehydrator(co.rehydrate, co.size, ch.data), co.offset, co.size'
            , 'FROM content AS co'
            , '  INNER JOIN chunk AS ch'
            , '    ON co.chunk = ch.id'
            , 'WHERE co.file = ?1'
            , '  AND ?2 < (co.offset + co.size)'
            , '  AND co.offset < (?2 + ?3)'
            , 'ORDER BY co.offset'
            ])
        for (chunk, cstart, csize) in self._reader.execute(
            q
            , (fileid, offset, size)
            ):
            if offset < cstart and cstart + csize < end:
                yield chunk
            else:
                yield chunk[max(offset - cstart, 0):min(end - cstart, csize)]
    def rehydrators(self):
        '''
        List all rehydration configurations from the database. Data is given
        as a hex-encoded blake2b hash for the sake of brevity.
        Intended for data display.
        '''
        q = 'SELECT name, version, chunking, algorithm, data FROM rehydrate'
        res = {}
        for name, version, sprayer, dryer, data in self._reader.execute(q):
            res.setdefault(name, {})[version] = {
                'sprayer': sprayer
                , 'dryer': dryer
                , 'data': '' if not data else 'blake2b-' + blake2b(data).hexdigest()
                }
        return res
    def roots(self):
        '''
        List all root configurations from the database. Hashes are given in
        hex, rehydrate configurations by name and version.
        Intended for data display.
        '''
        q = '\n'.join([
            'SELECT r.name, r.version, f.hash, h.name, h.version'
            , 'FROM root AS r'
            , '  INNER JOIN file AS f'
            , '    ON r.file = f.id'
            , '  INNER JOIN rehydrate AS h'
            , '    ON f.rehydrate = h.id'
            ])
        res = {}
        for name, version, hsh, r_name, r_version in self._reader.execute(q):
            splitres = hsh.split(b'-', maxsplit=1)
            if len(splitres) != 2:
                raise ValueError('Malformed root hash', name, version, hsh)
            hashtype, hashdata = splitres
            res.setdefault(name, {})[version] = {
                'hash': hashtype.decode('utf-8') + '-' + hashdata.hex()
                , 'rehydrate_name': r_name
                , 'rehydrate_version': r_version
                }
        return res


def make_rehydrator(conn):
    lookup = {
        rid: make_rehydrator_single(algorithm, data)
        for rid, algorithm, data in conn.execute(
            'SELECT id, algorithm, data FROM rehydrate'
            )
        }
    return lambda i, size, data: lookup[i](size, data)

def make_rehydrator_single(algorithm, data):
    parts = algorithm.split()
    algorithm = parts[0]
    params = {
        pname: int(pval, 16)
        for pname, pval in [
            p.split(':', maxsplit=1)
            for p in parts[1:]
            ]
        }
    if algorithm == 'nocompress':
        def rehydrator(chunksize, chunkdata):
            if not chunksize == len(chunkdata):
                raise RuntimeError('Bad chunk size', chunksize, chunkdata)
            return chunkdata
    elif algorithm == 'zstd':
        options = {
            k[7:]: v
            for k, v in params.items()
            if k[:7] == 'option.'
            }
        if not options:
            options = None
        decompressor = EndlessZstdDecompressor(
            zstd_dict=ZstdDict(data)
            , option=options
            )
        def rehydrator(chunksize, chunkdata):
            chunk = decompressor.decompress(chunkdata, max_length=chunksize)
            if not chunksize == len(chunk):
                raise RuntimeError('Bad chunk size', chunksize, chunk, chunkdata)
            return chunk
    else:
        raise ValueError('Unsupported algorithm for drying', algorithm)
    return rehydrator

