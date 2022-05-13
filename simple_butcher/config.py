from dataclasses import dataclass


@dataclass
class BackupConfig:
    backup_repository: str
    compression: str
    source: str
    password_file: str
    tape_buffer: int
    tempdir: str
    tape: str
    tape_dummy: str
    chunk_size: int  # GB
    backup_name: str
    incremental_time: int
    excludes: [str]


@dataclass
class ListBackupConfig:
    backup_repository: str
