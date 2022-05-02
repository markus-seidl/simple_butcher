import logging

import argparse
import datetime

from config import BackupConfig
from cmd_backup import Backup

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def do():
    parser = argparse.ArgumentParser(prog="simple_butcher")
    parser.add_argument("--database", help="Database directory", default="./db")
    subparsers = parser.add_subparsers(help="commands", dest="command")

    backup = subparsers.add_parser("backup")
    backup.add_argument("--backup_repository", help="Name of the backup repository", default="default")
    backup.add_argument("--ramdisk", help="Ramdisk for caching parts, should fit a single part")
    backup.add_argument("--compression", help="only zstd_pipe is supported", default="zstd_pipe")
    backup.add_argument("--source", help="Source directory", required=True)
    backup.add_argument("--password-file", help="Password in plain text as file", default="./password.key")
    backup.add_argument("--tape-length", help="Length of the tape in blocks", default=4781013, type=int)
    backup.add_argument("--tempdir", help="Store tar output", default="./temp")
    backup.add_argument("--tape", help="Tape device", default="/dev/nst0")
    backup.add_argument("--tape-dummy", help="Used for local debugging, if specified the tape isn't used.")
    backup.add_argument("--chunk-size", help="Backups are written in single chunks. Size in GB", default=10, type=int)

    args = parser.parse_args()
    # print(args)

    if args.command == 'backup':
        do_backup(args)
    else:
        print("Command unknown.")


def do_backup(args):
    config = BackupConfig(
        backup_repository=args.backup_repository,
        ramdisk=args.ramdisk,
        compression=args.compression,
        source=args.source,
        password_file=args.password_file,
        password="",
        tape_length=args.tape_length,
        tempdir=args.tempdir,
        tape=args.tape,
        tape_dummy=args.tape_dummy,
        chunk_size=args.chunk_size,
        backup_name=datetime.datetime.now().isoformat(timespec='seconds')
    )

    with open(config.password_file, 'r') as f:
        config.password = f.readline()

    print(config.__repr__().replace(config.password, "<password>"))
    Backup(config).do()


if __name__ == '__main__':
    do()
