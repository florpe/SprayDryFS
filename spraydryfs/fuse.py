
'''
Translation from the content-oriented logic of spraydryfs.rehydrate to the
tree-oriented logic of the POSIX file system standard as demanded by libfuse.
'''

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

async def runSprayDryFS(
    dbpath
    , rootname
    , rootversion
    , mmap=None
    , mount=None
    , logger=None
    , loglevel='INFO'
    ):
    '''
    A wrapper around the SprayDryFS runner, to be ultimately used in
    await gather(*(runSprayDryFS(..) for ... in ...)) . Currently impossible
    due to a bug in pyfuse3.
    '''
    async with SprayDryFS(
        dbpath
        , rootname
        , rootversion
        , mmap=mmap
        , mount=mount
        , logger=logger
        , loglevel=loglevel
        ) as fs:
        await fs.run()
    return None

class SprayDryFS(Operations):
    '''
    Translation layer between spraydryfs.Rehydrator and pyfuse3.
    '''
    def __init__(
        self
        , dbpath
        , rootname
        , rootversion
        , mmap=None
        , mount=None
        , logger=None
        , loglevel='INFO'
        ):
        '''
        Setting up shop with a reader and a root.
        '''
        self._logger = logger if logger is not None else self._mklogger(loglevel)
        self._db = dbpath
        self._mount = None if mount is None else mount.resolve(strict=True)
        self._rehydrator = Rehydrator(dbpath, mmap=mmap)
        self._root = self._rehydrator.root(rootname, rootversion)
        if self._root is None:
            raise ValueError('No such root', rootname, rootversion)
        self._uid = getuid()
        self._gid = getgid()
        self._inode_offset = ROOT_INODE #Offset to ensure that reserved inodes are not used
    async def __aenter__(self):
        '''
        Set up FUSE if a mountpoint is given.
        '''
        if self._mount is None:
            return self
        fuseenable()
        fuse_options = set(default_options)
        fuse_options.add('fsname=spraydryfs')
        self._logger.debug('FUSE options: %s', fuse_options)
        #Ideally this would take a Path - libfuse is okay with that, but pyfuse3 is not
        fuseinit(
            self
            , str(self._mount)
            , fuse_options
            )
        return self
    async def __aexit__(self, exc_type, exc, tb):
        '''
        Cleanup.
        '''
        self.close()
        return None
    def close(self):
        '''
        Unmount the filesystem, close the FUSE and SQLite connections.
        '''
        self._logger.debug('Closing')
        if self._mount is not None:
            self._logger.info('Unmounting')
            fuseclose(unmount=True)
        self._rehydrator.close()
        self._logger.info('Closed')
        return None
    async def run(self):
        '''
        If a mountpoint is given, run FUSE.
        '''
        if self._mount is not None:
            self._logger.info('Running on %s', self._mount)
            return await fusemain()
        self._logger.warn('No mountpoint configured, nothing to do')
        return None
    def _mklogger(self, level):
        '''
        A tiny default logger.
        '''
        logger = getLogger('spraydryfs')
        logger.setLevel(level)
        handler = StreamHandler()
        handler.setLevel(level)
        formatter = Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger
    def _mkattrs(self, inentry):
        '''
        Translate spraydryfs.rehydrate.Entry to pyfuse3.EntryAttributes.
        '''
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
        '''
        Exactly what it says on the tin.
        '''
        self._logger.debug('GetAttr: Inode %s', inode)
        if inode == ROOT_INODE:
            return self._mkattrs(self._root)
        entry = self._rehydrator.attributes(inode - self._inode_offset)
        if entry is None:
            logger.warn('Inode %s not found', inode)
            raise FUSEError(ENOENT)
        res = self._mkattrs(entry)
        return res
    async def lookup(self, parent_inode, name, ctx=None):
        '''
        Exactly what it says on the tin.
        '''
        self._logger.debug('Lookup at inode %s for name %s', parent_inode, name)
        if parent_inode == ROOT_INODE:
            entry = self._rehydrator.entry(self._root.file, name)
        else:
            entry = self._rehydrator.entry(parent_inode - self._inode_offset, name)
        if entry is None:
            raise FUSEError(ENOENT)
        return self._mkattrs(entry)
    async def opendir(self, inode, ctx):
        '''
        Exactly what it says on the tin.
        '''
        self._logger.debug('Opendir at inode %s', inode)
        if inode == ROOT_INODE:
            entry = self._root
        else:
            entry = self._rehydrator.attributes(inode - self._inode_offset)
        if entry is None or not entry.isdir:
            raise FUSEError(ENOENT)
        return inode
    async def readdir(self, fh, start_id, token):
        '''
        Exactly what it says on the tin. Reading a normal file as a directory
        does not raise an error, but instead returns no entries.
        '''
        if fh == ROOT_INODE:
            entryid = None
            gen = self._rehydrator.listgen(self._root.file, offset=start_id)
        else:
            entryid = fh = self._offset
            gen = self._rehydrator.listgen(fh - self._inode_offset, offset=start_id)
        self._logger.debug(
            'Reading directory at inode %s, offset %s, entryid %s'
            , fh, start_id, entryid
            )
        for entrynum, entry in gen:
            attrs = self._mkattrs(entry)
            if not readdir_reply(token, entry.name, self._mkattrs(entry), entrynum):
                return
    async def open(self, inode, flags, ctx):
        '''
        Exactly what it says on the tin.
        '''
        self._logger.debug('Opening file at inode %s with flags %s', inode, flags)
        if flags & O_RDWR or flags & O_WRONLY:
            raise FUSEError(EACCES)
        await self.getattr(inode) #Check inode existence
        return FileInfo(fh=inode, keep_cache=True)
    async def read(self, fh, off, size):
        '''
        Exactly what it says on the tin.
        '''
        if fh == ROOT_INODE:
            fileid = self._root.file
            self._logger.debug(
                'Reading from root inode %s, offset %s, size %s, fileid %s'
                , fh, off, size, fileid
                )
            return self._rehydrator.pread(self._root.file, off, size)
        entryid = fh - self._inode_offset
        self._logger.debug(
            'Reading from inode %s, offset %s, size %s, entryid %s'
            , fh, off, size, entryid
            )
        return self._rehydrator.pread_entry(entryid, off, size)
