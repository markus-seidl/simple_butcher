import json
import os
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class DestinationPath:
    path: str
    quota: Optional[float | int] = None  # 0..1 => percentage, >=1 => GB, None => use default


@dataclass
class DestinationConfig:
    default_quota: float | int = 0.95
    paths: List[DestinationPath] = field(default_factory=list)

    @staticmethod
    def from_dict(data: dict) -> "DestinationConfig":
        defaults = data.get("defaults", {}) or {}
        default_quota = defaults.get("quota", 0.95)

        paths: List[DestinationPath] = []
        for entry in data.get("paths", []) or []:
            quota = entry.get("quota")
            if quota == "" or quota is None:
                quota = default_quota
            paths.append(DestinationPath(path=entry["path"], quota=quota))

        return DestinationConfig(default_quota=default_quota, paths=paths)

    @staticmethod
    def load(file_path: str) -> Optional["DestinationConfig"]:
        if not file_path or not os.path.exists(file_path):
            return None

        with open(file_path, "r") as f:
            data = json.load(f)

        return DestinationConfig.from_dict(data)


@dataclass
class BackupDriveConfig:
    backup_repository: str
    backup_name: str
    description: str
    compression: str
    source: str
    password_file: str
    tempdir: str
    chunk_size: int  # GB
    incremental_time: int
    excludes: [str]
    destination_config_file: str
    destination_config: Optional[DestinationConfig]
    zstd_level: int = 5


@dataclass
class BackupTapeConfig:
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
    zstd_level: int = 5


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


@dataclass
class IdentifyConfig:
    tape: str
    tape_dummy: str
