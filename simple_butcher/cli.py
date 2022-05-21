import logging

import argparse
import datetime
import os

from config import BackupConfig, RestoreConfig, ListBackupConfig, ListFilesConfig
from cmd_backup import Backup
from cmd_restore import Restore
from cmd_list_backups import ListBackups
from cmd_list_files import ListFiles

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO, datefmt='%I:%M:%S')


def do():
    parser = argparse.ArgumentParser(prog="simple_butcher")
    parser.add_argument("--database", help="Database directory", default="./db")
    subparsers = parser.add_subparsers(help="commands", dest="command")

    backup = subparsers.add_parser("backup")
    backup.add_argument("--backup-repository", help="Name of the backup repository", default="default")
    backup.add_argument("--compression", help="only zstd_pipe is supported", default="zstd_pipe_v2")
    backup.add_argument("--source", help="Source directory", required=True)
    backup.add_argument("--password-file", help="Password in plain text as file", default="./password.age")
    backup.add_argument("--tape-buffer", help="GBs left before changing to the next tape", default=10, type=int)
    backup.add_argument("--tempdir", help="Store tar output", default="./temp")
    backup.add_argument("--tape", help="Tape device", default="/dev/nst0")
    backup.add_argument("--tape-dummy", help="Used for local debugging, if specified the tape isn't used.")
    backup.add_argument("--chunk-size", help="Backups are written in single chunks. Size in GB", default=10, type=int)
    backup.add_argument("--incremental-time", help="If set only includes files modified in the past n days",
                        default=None, required=False, type=int)
    backup.add_argument("--exclude", help="tar exclude option", default=None, required=False, action='append',
                        nargs='+')
    backup.add_argument("--description", help="Additional description for a backup", default="", type=str)

    list_backups = subparsers.add_parser("list-backups")
    list_backups.add_argument("--backup-repository", help="Name of the backup repository", default="default")

    list_files = subparsers.add_parser("list-files")
    list_files.add_argument("--backup-repository", help="Name of the backup repository", default="default")
    list_files.add_argument("--backup-name", help="Name of the backup to restore, or number", required=True)

    restore = subparsers.add_parser("restore")
    restore.add_argument("--backup-repository", help="Name of the backup repository", default="default")
    restore.add_argument("--backup-name", help="Name of the backup to restore, or number", required=True)
    restore.add_argument("--compression", help="only zstd_pipe is supported", default="zstd_pipe_v2")
    restore.add_argument("--dest", help="Dest directory", required=True)
    restore.add_argument("--password-file", help="Password in plain text as file", default="./password.age")
    restore.add_argument("--tempdir", help="Store tar output", default="./temp")
    restore.add_argument("--tape", help="Tape device", default="/dev/nst0")
    restore.add_argument("--tape-dummy", help="Used for local debugging, if specified the tape isn't used.")
    restore.add_argument("--exclude", help="tar exclude option", default=None, required=False, action='append',
                         nargs='+')

    args = parser.parse_args()

    if args.command == 'backup':
        do_backup(args)
    elif args.command == "list-backups":
        do_list_backup(args)
    elif args.command == 'list-files':
        do_list_files(args)
    elif args.command == 'restore':
        do_restore(args)
    else:
        print("Command unknown.")


def do_list_files(args):
    config = ListFilesConfig(
        backup_repository=args.backup_repository,
        backup_name=args.backup_name
    )

    ListFiles(config).do()


def do_restore(args):
    config = RestoreConfig(
        backup_repository=args.backup_repository,
        backup_name=args.backup_name,
        compression=args.compression,
        dest=args.dest,
        password_file=args.password_file,
        tempdir=args.tempdir,
        tape=args.tape,
        tape_dummy=args.tape_dummy,
        excludes=args.exclude
    )

    Restore(config).do()


def do_backup(args):
    config = BackupConfig(
        backup_repository=args.backup_repository,
        backup_name=datetime.datetime.now().isoformat(timespec='seconds'),
        description=args.description,
        compression=args.compression,
        source=args.source,
        password_file=args.password_file,
        tape_buffer=args.tape_buffer,
        tempdir=args.tempdir,
        tape=args.tape,
        tape_dummy=args.tape_dummy,
        chunk_size=args.chunk_size,
        incremental_time=args.incremental_time,
        excludes=args.exclude
    )

    with open(config.password_file, 'r') as f:
        config.password = f.readline().strip().strip(os.linesep)

    print(config.__repr__().replace(config.password, "<password>"))
    Backup(config).do()


def do_list_backup(args):
    config = ListBackupConfig(
        backup_repository=args.backup_repository,
    )

    ListBackups(config).do()


if __name__ == '__main__':
    do()
