

from cachetools import cached
from mmap import mmap
from pyzstd import ZstdDecompressor, ZstdDict
from stat import S_ISDIR, S_ISREG
from zlib import crc32

from spraydryfs.db import connect


class SprayDryStore():
    def __init__(self, dbpath, mkhashobj, rehydratename):
        self._db = dbpath
        self._mkhashobj = mkhashobj
        self._hashname = mkhashobj().name.encode('utf-8')
        
        self._writer = connect(self._db)
        self._rehydrate, self._sprayer, self._dryer = make_mksprayer_dryer(
            self._writer
            , rehydratename
            )
    def savepoint(self, path):
        pathhsh = self._mkhashobj(bytes(path))
        savepointname = 'savepoint_' + pathhsh.hexdigest()
        self._writer.execute('SAVEPOINT ' + savepointname)
        return savepointname
    def rollback(self, savepoint):
        self._writer.execute(f'ROLLBACK TO ' + savepoint)
        self._writer.execute(f'RELEASE ' + savepoint)
        return None
    def release(self, savepoint):
        self._writer.execute(f'RELEASE ' + savepoint)
        return None
    def hash(self, indata):
        if isinstance(indata, bytes):
            return self._hashname + b'-' + self._mkhashobj(indata).digest()
        return self._hashname + b'-' + indata.digest()
    def tmpid(self, path):
        fakehsh = self._hashname + b'_' + self._mkhashobj(bytes(path)).digest()
        fileid = None
        res = self._writer.execute(
            'INSERT OR IGNORE INTO file (hash, rehydrate) VALUES (?,?) RETURNING id'
            , (fakehsh, self._rehydrate)
            ).fetchone()
        if res is None:
            raise ValueError('Could not insert preliminary file id:', path, fakehsh)
        return res[0]
    def store_chunk(self, fileid, chunk):
        chunkhsh = self.hash(chunk)
        res_insert = self._writer.execute(
            '\n'.join([
                'INSERT OR IGNORE INTO chunkhash (rehydrate, size, data)'
                , 'VALUES (?,?,?)'
                , 'RETURNING id'
                    ])
            , (self._rehydrate, len(chunk), chunkhsh)
            ).fetchone()
        if res_insert is None:
            res_existing = self._writer.execute(
                '\n'.join([
                    'SELECT id'
                    , 'FROM chunkhash'
                    , 'WHERE rehydrate = ? AND data = ?'
                    ])
                , (self._rehydrate, chunkhsh)
                ).fetchone()
            if res_existing is None:
                raise RuntimeError('Could neither insert chunk nor retrieve existing', chunkhsh)
            return res_existing[0]
        chunkid = res_insert[0]
        self._writer.execute(
            'INSERT INTO chunk (id, data) VALUES (?,?)'
            , (chunkid, self._dryer(chunk))
            )
        return chunkid
    def store_content(self, fileid, offset, chunkid):
        self._writer.execute(
            '\n'.join([
                'INSERT OR IGNORE INTO content (file, rehydrate, offset, size, chunk)'
                , 'SELECT ?, rehydrate, ?, size, id'
                , 'FROM chunkhash'
                , 'WHERE id = ?'
                ])
            , (fileid, offset, chunkid)
            )
        return None
    def dry_file(self, path):
        savepoint = self.savepoint(path)
        fileid = self.tmpid(path)
        filehashobj = self._mkhashobj()
        with open(path, 'rb+') as handle:
            with mmap(handle.fileno(), 0) as mm:
                for offset, chunk in self._sprayer(mm):
                    filehashobj.update(chunk)
                    chunkid = self.store_chunk(fileid, chunk)
                    self.store_content(fileid, offset, chunkid)
        filehash = self.hash(filehashobj)
        for (existingid,) in self._writer.execute(
            'SELECT id FROM file WHERE hash = ? AND rehydrate = ?'
            , (filehash, self._rehydrate)
            ):
            self.rollback(savepoint)
            return existingid, filehash
        self._writer.execute(
            'UPDATE file SET hash = ? WHERE id = ?'
            , (filehash, fileid)
            )
        self.release(savepoint)
        return fileid, filehash
    def dry_directory(self, path):
        savepoint = self.savepoint(path)
        fileid = self.tmpid(path)
        filehashobj = self._mkhashobj()
        for entry in sorted(path.iterdir()):
            entryid, entryhash, entrystat = self.dry(entry)
            entryname = bytes(entry.relative_to(path))
            entrymodebytes = make_modebytes(entrystat)
            entrysegment = b''.join([
                b'\x00'
                , entryhash
                , entrymodebytes 
                , entryname.hex().encode('utf-8')
                ])
            filehashobj.update(entrysegment)
            self._writer.execute(
                '\n'.join([
                    'INSERT OR IGNORE INTO entry ('
                    , 'directory, name, isdirectory, mode, size, file'
                    , ') VALUES (?,?,?,?,?,?)'
                    ])
                , (fileid, entryname, S_ISDIR(entrystat.st_mode), entrymodebytes, entrystat.st_size, entryid)
                )
        filehash = self.hash(filehashobj)
        for (existingid,) in self._writer.execute(
            'SELECT id FROM file WHERE hash = ? AND rehydrate = ?'
            , (filehash, self._rehydrate)
            ):
            self.rollback(savepoint)
            return existingid, filehash
        self._writer.execute(
            'UPDATE file SET hash = ? WHERE id = ?'
            , (filehash, fileid)
            )
        self.release(savepoint)
        return fileid, filehash
    def dry(self, path):
        stat = path.stat()
        mode = stat.st_mode
        if S_ISDIR(mode):
            fileid, filehash = self.dry_directory(path)
        elif S_ISREG(mode):
            fileid, filehash = self.dry_file(path)
        else:
            raise ValueError('Unsupported file type', path, stat)
        return fileid, filehash, stat
    def root(self, path, name, version):
        if isinstance(path, str):
            path = Path(path)
        realpath = path.resolve(strict=True)
        self._writer.execute('BEGIN')
        committed = False
        try:
            if self._writer.execute(
                'SELECT id FROM root WHERE name = ? AND version = ?'
                , (name, version)
                ).fetchone() is not None:
                raise ValueError('Root already exists', name, version)
            fileid, filehash, stat = self.dry(realpath)
            self._writer.execute(
                'INSERT INTO root (name, version, isdirectory, mode, size, file) VALUES (?,?,?,?,?,?)'
                , (name, version, S_ISDIR(stat.st_mode), make_modebytes(stat), stat.st_size, fileid)
                )
            self._writer.execute('COMMIT')
            committed = True
        finally:
            if not committed:
                self._writer.execute('ROLLBACK')
        return None

def make_modebytes(stat):
    return stat.st_mode.to_bytes(2, 'little')

def make_mksprayer_dryer(conn, name):
    for rehydrateID, chunker, algorithm, data in conn.execute(
        'SELECT id, chunking, algorithm, data FROM rehydrate WHERE name = ?'
        , (name,)
        ):
        sprayer = make_sprayer(chunker)
        algoname, algoparams = algosplit(algorithm)
        if algoname == 'nocompress':
            return rehydrateID, sprayer, lambda x: x
        if algoname == 'zstd':
            #This only supports levels for now
            compressdict = ZstdDict(data)
            level = algoparams.get('level')
            compressor = ZstdCompressor(
                level_or_option=(params if level is None else level)
                , zstd_dict=compressdict
                )
            return rehydrateID, sprayer, lambda x: compressor.compress(x, ZstdCompressor.FLUSH_FRAME)
        raise ValueError('Unsupported algorithm for drying:', algorithm)

def algosplit(instr):
    parts = instr.split()
    params = {
        k: int(v, 16) if v.startswith('0x') else v
        for k, v in [
            part.split(':')
            for part in parts[1:]
            ]
        }
    return parts[0], params
    

def make_sprayer(chunkstr):
    fields = chunkstr.split()
    algorithm = fields[0]
    params = {
        fieldname: int(fieldval, 16)
        for fieldname, fieldval in [
            field.split(':')
            for field in fields[1:]
            ]
        }
    if algorithm == 'fixed':
        return mk_spray_fixed(params['size'])
    if algorithm == 'crc32':
        return mk_spray_crc32(
            params['initializer']
            , params['cutoff']
            , params['min']
            , params['max']
            )
    raise ValueError('Unsupported spraying algorithm', algorithm)


def with_mmap(path, mksprayer):
    print(path)
    with open(path, 'rb+') as handle:
        with mmap(handle.fileno(), 0) as mm:
            for res in mksprayer(mm):
                yield res

def mk_spray_fixed(size):
    def spray_fixed_size(indata):
        for offset in range(0, len(indata), size):
            yield offset, indata[offset:offset+size]
    return spray_fixed_size

def mk_spray_crc32(initializer, cutoff, minimum, maximum):
    def chunker(indata):
        border = 0
        rolling = initializer
        for position, byte in enumerate(indata):
            rolling = crc32(byte, rolling)
            if rolling < cutoff:
                if position - border < minimum:
                    continue
                for interior_border in range(border, position, maximum):
                    next_border = min(position, interior_border + maximum)
                    yield interior_border, indata[interior_border:next_border]
                border = position
        last_chunk = indata[border:]
        if last_chunk:
            yield border, last_chunk
    return chunker
