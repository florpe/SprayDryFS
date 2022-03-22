
from argparse import ArgumentParser
from asyncio import run
from cProfile import run as prun
from hashlib import sha256, blake2b, new as newhash
from pathlib import Path

from spraydryfs.db import connect
from spraydryfs.spraydry import SprayDryStore, algosplit
from spraydryfs.rehydrate import Rehydrator
from spraydryfs.fuse import SprayDryFS

DBFILE = 'file:./test.db'
TESTFILE  = '../sqlfs/traintest'


async def main_():
    sds = SprayDryStore(DBFILE, blake2b, 'nocompress-crc32')
    try:
        sds.root(Path(TESTFILE), 'testroot', '0.0.1')
    except ValueError:
        pass
    rh = Rehydrator(DBFILE)
    root = rh.root('testroot', '0.0.1')
    entries = list(x[1] for x in rh.listgen(root.file))
    print(entries)
    for i in range(0,100000,3000):
        res = rh.pread(entries[0].file, i, 100)
        print(i, 'FOASDAS')
        print(res)
    return None

def parse_args():
    parser = ArgumentParser()
    parser.add_argument(
        'dbfile'
        )
    parser.add_argument(
        '-r', '--root'
        , required=True
        )
    parser.add_argument(
        '-v', '--root-version'
        , default='0.0.0'
        )
    parser.add_argument(
        '-L', '--log-level'
        , default='INFO'
        )
    parser.add_argument(
        '-A', '--hash-algorithm'
        , default=None
        , type=newhash
        )
    parser.add_argument(
        '-S', '--spray-algorithm'
        , default=None
        , nargs='+'
        , type=algosplit
        )
    parser.add_argument(
        '-D', '--drying-algorithm'
        , default=None
        , nargs='+'
        , type=algosplit
        )
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument(
        '-m', '--mount'
        )
    grp.add_argument(
        '-i', '--ingest'
        , action='append'
        )
    grp.add_argument(
        '-t', '--train'
        , nargs='+'
        )
    return parser.parse_args()


async def main():
    args = parse_args()
    if args.ingest:
        print(args)
        raise NotImplementedError
        return None
    if args.train:
        print(args)
        raise NotImplementedError
        return None
    async with SprayDryFS(args.dbfile, args.root, args.root_version, mount=args.mount, loglevel=args.log_level) as fs:
        await fs.run()

if __name__ == "__main__":
#    prun('run(main())', sort='tottime')
    run(main())
