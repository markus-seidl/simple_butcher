import dataclasses
import json
import os


@dataclasses.dataclass
class BackupRecord:
    tape_no: int
    volume_no: int
    archive_sha256: str
    tar_line: str

    def to_json(self):
        return json.dumps(dataclasses.asdict(self))


class Database:
    def __init__(self, database_dir, backup_repository, backup_name):
        self.database_dir = database_dir
        self.backup_repository = backup_repository
        self.backup_name = backup_name

    def backup_db_dir(self) -> str:
        return self.database_dir + "/" + self.backup_repository + "/" + self.backup_name

    def database_file(self) -> str:
        return self.backup_db_dir() + "/contents.jsonl"

    def start_backup(self):
        os.makedirs(self.backup_db_dir(), exist_ok=True)
        with open(self.database_file(), "a+") as f:
            pass

    def store(self, records: [BackupRecord]):
        with open(self.database_file(), "a+") as f:
            for record in records:
                f.write(record.to_json())
                f.write(os.linesep)


if __name__ == '__main__':
    print(BackupRecord(0, 1, "archive_sha256", "tar_line").to_json())
