import logging
import dataclasses
import json
import os
import shutil
import subprocess
from exe_paths import ZSTD

ZSTD_COMPRESSION = '{zstd} -5 -T0 {in_file}'
ZSTD_DECOMPRESSION = '{zstd} -d {in_file}'
DB_ROOT = "./db"
INCREMENTAL_INDEX_FILENAME = "incremental_index"
TAR_LOG_FILE = "tar.log"
TAR_INPUT_FILE_LIST = "tar_input_file_list"


@dataclasses.dataclass
class BackupRecord:
    tape_no: int
    volume_no: int
    archive_hash: str
    hash_type: str
    tar_line: str

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))


@dataclasses.dataclass
class BackupInfo:
    time_start: int
    time_end: int
    bytes_written: int
    tapes: int
    volumes: int
    base_backup: str
    incremental_time: int
    tape_start_index: int

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))

    @staticmethod
    def from_json(j):
        return BackupInfo(
            time_start=j['time_start'],
            time_end=j['time_end'],
            bytes_written=j['bytes_written'],
            tapes=j['tapes'],
            volumes=j['volumes'],
            base_backup=j['base_backup'] if 'base_backup' in j else None,
            tape_start_index=j['tape_start_index'] if 'tape_start_index' in j else None,
            incremental_time=j['incremental_time'] if 'incremental_time' in j else None
        )


class BackupDatabaseRepository:
    def __init__(self, database_dir, backup_repository):
        self.database_dir = database_dir
        self.backup_repository = backup_repository

    def backup_repository_dir(self) -> str:
        return self.database_dir + "/" + self.backup_repository

    def list_backups(self):
        backups = os.listdir(self.backup_repository_dir())
        backups.sort(reverse=True)

        filtered_backups = list()
        for dir in backups:
            if os.path.exists(self.backup_repository_dir() + "/" + dir + "/info.json"):
                filtered_backups.append(dir)

        return filtered_backups

    def read_backup_info(self, backup_dir: str) -> BackupInfo:
        info_file = self.backup_repository_dir() + "/" + backup_dir + "/info.json"
        with open(info_file, "r") as f:
            return BackupInfo.from_json(json.load(f))

    def copy_file(self, from_backup_name: str, to_backup_name: str, file_name: str):
        shutil.copy(
            self.backup_repository_dir() + f"/{from_backup_name}/{file_name}.zst",
            self.backup_repository_dir() + f"/{to_backup_name}/{file_name}.zst"
        )

        _decompress_file(self.backup_repository_dir() + f"/{to_backup_name}/{file_name}.zst")


class BackupDatabase:
    def __init__(self, database_dir, backup_repository, backup_name):
        self.database_dir = database_dir
        self.backup_repository = backup_repository
        self.backup_name = str(backup_name).replace(":", "")

    def open(self):
        db_file = self.database_file() + ".zst"
        if os.path.exists(db_file):
            self._decompress_file(db_file)

        # tar_file = self.tar_incremental_file() + ".zst"
        # if os.path.exists(tar_file):
        #     self._decompress_file(tar_file)

    def backup_db_dir(self) -> str:
        return self.database_dir + "/" + self.backup_repository + "/" + self.backup_name

    def database_file(self) -> str:
        return self.backup_db_dir() + "/contents.jsonl"

    def tar_incremental_file(self) -> str:
        return self.backup_db_dir() + f"/{INCREMENTAL_INDEX_FILENAME}"

    def tar_log_file(self) -> str:
        return self.backup_db_dir() + f"/{TAR_LOG_FILE}"

    def tar_input_file_list(self) -> str:
        return self.backup_db_dir() + f"/{TAR_INPUT_FILE_LIST}"

    def info_file(self) -> str:
        return self.backup_db_dir() + "/info.json"

    def start_backup(self):
        os.makedirs(self.backup_db_dir(), exist_ok=True)
        with open(self.database_file(), "a+") as f:
            pass  # just create file

    def store(self, records: [BackupRecord]):
        with open(self.database_file(), "a+") as f:
            for record in records:
                f.write(record.to_json())
                f.write(os.linesep)

    def close_and_compress(self, backup_info: BackupInfo):
        logging.info("Closing and compressing database...")
        with open(self.info_file(), "w+") as f:
            logging.info("Writing backup information file...")
            f.write(backup_info.to_json())

        to_compress = [
            self.database_file(),
            self.tar_incremental_file(),
            self.tar_log_file(),
            self.tar_input_file_list()
        ]

        for file in to_compress:
            if os.path.exists(file):
                logging.info(f"Compressing file {file}...")
                _compress_file(file)


def _compress_file(input_file: str):
    compression_cmd = ZSTD_COMPRESSION.format(
        zstd=ZSTD,
        in_file=input_file,
    )

    subprocess.check_call(compression_cmd, shell=True)

    if os.path.exists(input_file + ".zst"):
        os.remove(input_file)


def _decompress_file(input_file: str):
    decompression_cmd = ZSTD_DECOMPRESSION.format(
        zstd=ZSTD,
        in_file=input_file,
    )

    subprocess.check_call(decompression_cmd, shell=True)

    if os.path.exists(input_file) and os.path.exists(input_file[:-4]):
        os.remove(input_file)


if __name__ == '__main__':
    # print(BackupRecord(0, 1, "archive_sha256", "tar_line").to_json())

    print(BackupDatabaseRepository("../db/", "default").list_backups())
