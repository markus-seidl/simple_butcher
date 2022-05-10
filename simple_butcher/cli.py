import logging

import argparse
import datetime
import os

from config import BackupConfig
from cmd_backup import Backup
from cmd_list_backups import ListBackups

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO, datefmt='%I:%M:%S')


def do():
    parser = argparse.ArgumentParser(prog="simple_butcher")
    parser.add_argument("--database", help="Database directory", default="./db")
    subparsers = parser.add_subparsers(help="commands", dest="command")

    backup = subparsers.add_parser("backup")
    backup.add_argument("--backup-repository", help="Name of the backup repository", default="default")
    backup.add_argument("--ramdisk", help="Ramdisk for caching parts, should fit a single part")
    backup.add_argument("--compression", help="only zstd_pipe is supported", default="zstd_pipe_v2")
    backup.add_argument("--source", help="Source directory", required=True)
    backup.add_argument("--password-file", help="Password in plain text as file", default="./password.key")
    backup.add_argument("--tape-buffer", help="GBs left before changing to the next tape", default=10, type=int)
    backup.add_argument("--tempdir", help="Store tar output", default="./temp")
    backup.add_argument("--tape", help="Tape device", default="/dev/nst0")
    backup.add_argument("--tape-dummy", help="Used for local debugging, if specified the tape isn't used.")
    backup.add_argument("--chunk-size", help="Backups are written in single chunks. Size in GB", default=10, type=int)
    backup.add_argument("--incremental-time", help="If set only includes files modified in the past n days",
                        default=None, required=False, type=int)
    backup.add_argument("--exclude", help="tar exclude option", default=None, required=False, action='append', nargs='+')

    list_backups = subparsers.add_parser("list-backups")
    list_backups.add_argument("--backup_repository", help="Name of the backup repository", default="default")

    args = parser.parse_args()
    # print(args)

    if args.command == 'backup':
        do_backup(args)
    elif args.command == "list-backups":
        do_list_backup(args)
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
        tape_buffer=args.tape_buffer,
        tempdir=args.tempdir,
        tape=args.tape,
        tape_dummy=args.tape_dummy,
        chunk_size=args.chunk_size,
        backup_name=datetime.datetime.now().isoformat(timespec='seconds'),
        base_of_backup=None,
        incremental_time=args.incremental_time,
        excludes=args.exclude
    )

    with open(config.password_file, 'r') as f:
        config.password = f.readline().strip().strip(os.linesep)

    print(config.__repr__().replace(config.password, "<password>"))
    Backup(config).do()


def do_list_backup(args):
    config = BackupConfig(
        backup_repository=args.backup_repository,
        ramdisk=None,
        compression=None,
        source=None,
        password_file=None,
        password=None,
        tape_buffer=None,
        tempdir=None,
        tape=None,
        tape_dummy=None,
        chunk_size=None,
        backup_name=None,
        base_of_backup=None,
        incremental_time=None,
        excludes=None
    )

    ListBackups(config).do()


if __name__ == '__main__':
    do()
