
from errno import ENOENT, EACCES
from logging import getLogger, StreamHandler, Formatter
from os import getuid, getgid, O_RDWR, O_WRONLY
from pyfuse3 import (
    Operations
    , ROOT_INODE
    , EntryAttributes
    , FUSEError
    , readdir_reply
    , FileInfo
    , default_options
    , init as fuseinit
    , main as fusemain
    , close as fuseclose
    )
from pyfuse3_asyncio import enable as fuseenable

from spraydryfs.rehydrate import Rehydrator

MMAP_DEFAULT = 128 * 1024 * 1024

async def runSprayDryFS(dbpath, rootname, rootversion, mmap=MMAP_DEFAULT, mount=None, logger=None, loglevel='INFO'):
    async with SprayDryFS(dbpath, rootname, rootversion, mmap=mmap, mount=mount, logger=logger, loglevel=loglevel) as fs:
        await fs.run()
    return None

class SprayDryFS(Operations):
    def __init__(self, dbpath, rootname, rootversion, mmap=MMAP_DEFAULT, mount=None, logger=None, loglevel='INFO'):
        self._logger = logger if logger is not None else self._mklogger(loglevel)
        self._db = dbpath
        self._mount = None if mount is None else mount.resolve(strict=True)
        self._rehydrator = Rehydrator(dbpath, mmap=MMAP_DEFAULT)
        self._root = self._rehydrator.root(rootname, rootversion)
        if self._root is None:
            raise ValueError('No such root', rootname, rootversion)
        self._uid = getuid()
        self._gid = getgid()
        self._inode_offset = ROOT_INODE #Offset to ensure that reserved inodes are not used
    async def __aenter__(self):
        if self._mount is None:
            return self
        fuseenable()
        fuse_options = set(default_options)
        fuse_options.add('fsname=spraydryfs')
        self._logger.debug('FUSE options: %s', fuse_options)
        fuseinit(
            self
            , str(self._mount) #Ideally this would take a Path
            , fuse_options
            )
        return self
    async def __aexit__(self, exc_type, exc, tb):
        self.close()
        return None
    def close(self):
        self._logger.debug('Closing')
        if self._mount is not None:
            self._logger.info('Unmounting')
            fuseclose(unmount=True)
        self._rehydrator.close()
        self._logger.info('Closed')
        return None
    async def run(self):
        if self._mount is not None:
            self._logger.info('Running on %s', self._mount)
            return await fusemain()
        self._logger.warn('No mountpoint configured, nothing to do')
        return None
    def _mklogger(self, level):
        logger = getLogger('spraydryfs')
        logger.setLevel(level)
        handler = StreamHandler()
        handler.setLevel(level)
        formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
    def _mkattrs(self, inentry):
        entry = EntryAttributes()
        entry.st_mode = inentry.mode
        entry.st_size = inentry.size
        entry.st_atime_ns = 0
        entry.st_ctime_ns = 0
        entry.st_mtime_ns = 0
        entry.st_uid = self._uid
        entry.st_gid = self._gid
        entry.st_ino = ROOT_INODE if inentry.inode is None else inentry.inode + self._inode_offset
        return entry
    async def getattr(self, inode, ctx=None):
        self._logger.debug('GetAttr: Inode %s', inode)
        if inode == ROOT_INODE:
            return self._mkattrs(self._root)
        entry = self._rehydrator.attributes(inode - self._inode_offset)
        if entry is None:
            logger.warn('Inode %s not found', inode)
            raise FUSEError(ENOENT)
        res = self._mkattrs(entry)
        self._logger.debug('GetAttr: Inode %s , result %s', inode, res)
        return res
    async def lookup(self, parent_inode, name, ctx=None):
        self._logger.debug('Lookup at inode %s for name %s', parent_inode, name)
        if parent_inode == ROOT_INODE:
            entry = self._rehydrator.entry(self._root.file, name)
        else:
            entry = self._rehydrator.entry(parent_inode - self._inode_offset, name)
        if entry is None:
            raise FUSEError(ENOENT)
        return self._mkattrs(entry)
    async def opendir(self, inode, ctx):
        self._logger.debug('Opendir at inode %s', inode)
        if inode == ROOT_INODE:
            entry = self._root
        else:
            entry = self._rehydrator.attributes(inode - self._inode_offset)
        if entry is None or not entry.isdir:
            raise FUSEError(ENOENT)
        return inode
    async def readdir(self, fh, start_id, token):
        self._logger.debug('Reading directory at inode %s, offset %s', fh, start_id)
        dirid = self._root.inode if fh == ROOT_INODE else fh - self._inode_offset
        for entrynum, entry in self._rehydrator.listgen(fh, offset=start_id):
            attrs = self._mkattrs(entry)
            if not readdir_reply(token, entry.name, self._mkattrs(entry), entrynum):
                return
    async def open(self, inode, flags, ctx):
        self._logger.debug('Opening file at inode %s with flags %s', inode, flags)
        if flags & O_RDWR or flags & O_WRONLY:
            raise FUSEError(EACCES)
        fileid = self._root.inode if inode == ROOT_INODE else inode - self._inode_offset
        if self._rehydrator.attributes(inode - self._inode_offset) is None:
            raise FUSEError(ENOENT)
        return FileInfo(fh=inode, keep_cache=True)
    async def read(self, fh, off, size):
        self._logger.debug('Reading from inode %s , offset %s , size %s', fh, off, size)
        fileid = self._root.inode if fh == ROOT_INODE else fh - self._inode_offset
        return self._rehydrator.pread(fh, off, size)


