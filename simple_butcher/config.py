from dataclasses import dataclass


@dataclass
class BackupConfig:
    backup_repository: str
    backup_name: str
    description: str
    compression: str
    source: str
    password_file: str
    tape_buffer: int
    tempdir: str
    tape: str
    tape_dummy: str
    chunk_size: int  # GB
    incremental_time: int
    excludes: [str]


@dataclass
class RestoreConfig:
    backup_repository: str
    backup_name: str
    compression: str
    dest: str
    password_file: str
    tempdir: str
    tape: str
    tape_dummy: str
    excludes: [str]


@dataclass
class ListBackupConfig:
    backup_repository: str


@dataclass
class ListFilesConfig:
    backup_repository: str
    backup_name: str
