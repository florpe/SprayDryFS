
from argparse import ArgumentParser, Action as ArgAction
from asyncio import gather, run
from cProfile import run as prun
from hashlib import sha256, blake2b, new as newhash
from json import dumps
from pathlib import Path

from spraydryfs.db import connect
from spraydryfs.spraydry import SprayDryStore, algosplit, make_rehydrate_entry
from spraydryfs.rehydrate import Rehydrator
from spraydryfs.fuse import SprayDryFS, runSprayDryFS

DBFILE = 'file:./test.db'
TESTFILE  = '../sqlfs/traintest'

class Mountpoint(ArgAction):
    def __call__(self, parser, namespace, values, *args, **kwargs):
        #TODO: Some more validation here:
        #   Path is writable
        #   Root name and version are good
        namespace.mount.append({
            'root_name': values[0]
            , 'root_version': values[1]
            , 'mount': Path(values[2]).resolve()
            })
        return None

class IngestSource(ArgAction):
    def __call__(self, parser, namespace, values, *args, **kwargs):
        #TODO: Some more validation here:
        #   Path is readable
        #   Root name and version are good
        try:
            hsh = hash_algorithm(values[2])
        except ValueError as e:
            raise ValueError from e
        namespace.ingest.append({
            'root_name': values[0]
            , 'root_version': values[1]
            , 'hash': hsh
            , 'rehydrate': values[3]
            , 'source': Path(values[4]).resolve()
            })
        return None

class TrainSource(ArgAction):
    def __call__(self, parser, namespace, values, *args, **kwargs):
        #TODO: Some more validation here:
        #   Path is readable
        if len(values) < 5:
            raise ValueError('Training needs at least one data source')
        namespace.train.append({
            'rehydrate_name': values[0]
            , 'rehydrate_version': values[1]
            , 'sprayer_config': algosplit(values[2])
            , 'dryer_config': algosplit(values[3])
            , 'source': list(sorted(
                Path(src).resolve()
                for src in values[4:]
                ))
            })
        return None

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
        '-L', '--log-level'
        , help='Logging level, defaults to INFO'
        , default='INFO'
        )
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument(
        '-m', '--mount'
        , nargs=3 #Validate: First is root name, second is root version, third is mount point
        , help='Root name, root version, and mount point for FUSE operation'
        , metavar=('ROOTNAME', 'ROOTVERSION', 'MOUNT')
        , action=Mountpoint
        , default=[]
        )
    grp.add_argument(
        '-i', '--ingest'
        , nargs=5 #Validate: First is root name, second is root version, third is hash name, fourth is datasource - one!
        , help='Rehydration configuration name to be used for ingesting.'
        , metavar=('ROOTNAME', 'ROOTVERSION', 'HASH', 'REHYDRATE', 'SOURCE')
        , action=IngestSource
        , default=[]
        )
    grp.add_argument(
        '-t', '--train'
        , nargs='*' #Validate: First is config name, second is config version, third is sprayer config, fourth is dryer config, rest is datasource
        , help='Rehydration configuration name to be used for training.'
        , metavar='REHYDRATENAME REHYDRATEVERSION SPRAYERCONFIG DRYERCONFIG SOURCE'
        , action=TrainSource
        , default=[]
        )
    return parser.parse_args()

async def main():
    args = parse_args()
    if args.mount:
        if len(args.mount) != 1:
            raise NotImplementedError('Multiple mount points are not yet supported')
        mnt = args.mount[0]
        async with SprayDryFS(
            args.dbfile
            , mnt['root_name']
            , mnt['root_version']
            , mount=mnt['mount']
            , loglevel=args.log_level
            ) as fs:
            await fs.run()
        return None
    for ngst in args.ingest:
        sds = SprayDryStore(
            args.dbfile
            , ngst['hash']
            , ngst['rehydrate']
            )
        #TODO: Explicit root permissions?
        sds.root(ngst['root_name'], ngst['root_version'], ngst['source'])
    for trn in args.train:
        make_rehydrate_entry(
            args.dbfile
            , trn['rehydrate_name']
            # , trn['rehydrate_version'] # Not yet in use!
            , trn['sprayer_config']
            , trn['dryer_config']
            , trn['source']
            )
    if not args.mount and not args.ingest and not args.train:
        rh = Rehydrator(args.dbfile, mmap=0)
        roots = rh.roots()
        rehydrators = rh.rehydrators()
        print(dumps({'root': roots, 'rehydrate': rehydrators}, indent=2))
    return None


def hash_algorithm(instr):
    hashobj = newhash(instr)
    def nh(*args):
        hshobj = hashobj.copy()
        for a in args:
            hshobj.update(a)
        return hshobj
    return nh

if __name__ == "__main__":
#    prun('run(main())', sort='tottime')
    run(main())
