import json

from common import file_size_format
from config import ListFilesConfig
from database import BackupRecord, BackupDatabase, BackupDatabaseRepository, DB_ROOT

from rich.console import Console
from rich.table import Table


class ListFiles:
    def __init__(self, config: ListFilesConfig):
        self.config = config
        self.repository = BackupDatabaseRepository(DB_ROOT, self.config.backup_repository)

    def do(self):
        backup_info, backup_database = self.repository.open_backup(self.config.backup_name)

        with open(backup_database.database_file(), "r") as f:
            line = f.readline()
            while line:
                record = BackupRecord.from_json(json.loads(line))

                print(record.tar_line)
                line = f.readline()

        backup_database.close()
