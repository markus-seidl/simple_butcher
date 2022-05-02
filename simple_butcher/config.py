from dataclasses import dataclass


@dataclass
class BackupConfig:
    backup_repository: str
    ramdisk: str
    compression: str
    source: str
    password_file: str
    password: str
    tape_length: int
    tempdir: str
    tape: str
    tape_dummy: str
    chunk_size: int  # GB
    backup_name: str

