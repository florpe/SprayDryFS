
from argparse import ArgumentParser
from asyncio import run
from cProfile import run as prun
from hashlib import sha256, blake2b, new as newhash
from pathlib import Path

from spraydryfs.db import connect
from spraydryfs.spraydry import SprayDryStore, algosplit, make_rehydrate_entry
from spraydryfs.rehydrate import Rehydrator
from spraydryfs.fuse import SprayDryFS

DBFILE = 'file:./test.db'
TESTFILE  = '../sqlfs/traintest'

def parse_args():
    parser = ArgumentParser(
        prog='spraydryfs'
        , description='The Instant File System: Spray, dry, rehydrate!'
        )
    parser.add_argument(
        'dbfile'
        , help='SQLite database file backing the file system'
        )
    parser.add_argument(
        'root'
        , help='Root name and version'
        , type=root
        )
    parser.add_argument(
        'datasource'
        , help='Path or root to use as data source'
        , action='append'
        , type=path_or_root
        )
    parser.add_argument(
        '-L', '--log-level'
        , help='Logging level, defaults to INFO'
        , default='INFO'
        )
    parser.add_argument(
        '-X', '--hash-algorithm'
        , default=None
        , type=hash_algorithm
        , help='Hashing algorithm to use when training or ingesting'
        )
    parser.add_argument(
        '-S', '--sprayer-config'
        , default=None
        # , nargs='+'
        , type=algosplit
        )
    parser.add_argument(
        '-D', '--dryer-config'
        , default=None
        # , nargs='+'
        , type=algosplit
        )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        '-m', '--mount'
        , type=Path
        , help='Mount point for FUSE operation'
        )
    grp.add_argument(
        '-i', '--ingest'
        , help='Rehydration configuration name to be used for ingesting.'
        )
    grp.add_argument(
        '-t', '--train'
        , help='Rehydration configuration name to be used for training.'
        )
    return parser.parse_args()

def hash_algorithm(instr):
    hashobj = newhash(instr)
    def nh(*args):
        hshobj = hashobj.copy()
        for a in args:
            hshobj.update(a)
        return hshobj
    return nh

def path_or_root(instr):
    if not instr:
        raise ValueError
    if instr[0] == '/' or instr[:2] == './' or instr[:3] == '../':
        return Path(instr)
    return instr

def root(instr):
    splitres = instr.rsplit(':', maxsplit=1)
    if len(splitres) != 2:
        raise ValueError
    return splitres

async def main():
    args = parse_args()
    if args.ingest:
        if len(args.datasource) != 1:
            raise ValueError('Need exactly one data source')
        if args.hash_algorithm is None:
            raise ValueError('Need a hash algorithm for ingesting data')
        sds = SprayDryStore(
            args.dbfile
            , args.hash_algorithm
            , args.ingest
            , sprayconf=args.sprayer_config
            , dryconf=args.dryer_config
            )
        #TODO: Root permissions
        #TODO: Root as data source
        sds.root(args.root[0], args.root[1], args.datasource[0])
        return None
    if args.train:
        if not args.datasource:
            raise ValueError('Need at least one data source')
        make_rehydrate_entry(
            args.dbfile
            , args.train
            , args.sprayer_config
            , args.dryer_config
            , args.datasource
            )
        return None
    async with SprayDryFS(
        args.dbfile
        , args.root[0]
        , args.root[1]
        , mount=args.mount
        , loglevel=args.log_level
        ) as fs:
        await fs.run()

if __name__ == "__main__":
#    prun('run(main())', sort='tottime')
    run(main())
