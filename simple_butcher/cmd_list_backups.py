from common import file_size_format
from config import ListBackupConfig
from database import BackupRecord, BackupDatabase, BackupDatabaseRepository, DB_ROOT

from rich.console import Console
from rich.table import Table


class ListBackups:
    def __init__(self, config: ListBackupConfig):
        self.config = config
        self.repository = BackupDatabaseRepository(DB_ROOT, self.config.backup_repository)

    def do(self):
        table = Table(title="Backups")
        table.add_column("No")
        table.add_column("Name", no_wrap=True)
        table.add_column("Duration")
        table.add_column("Size")
        table.add_column("T / V")
        table.add_column("Type", no_wrap=True)
        table.add_column("Description")

        no = 0
        all_backups = self.repository.list_backups()
        for backup_dir in all_backups:
            bi = self.repository.read_backup_info(backup_dir)

            hours = "%.02fh" % ((bi.time_end - bi.time_start) / 60.0 / 60.0)

            reference_backup = "Full" if bi.incremental_time is None else f"Inc - {bi.incremental_time} days"
            # if bi.base_backup is not None:
            #     reference_backup = f"{bi.base_backup} ({self._find_no(all_backups, bi.base_backup)})"

            table.add_row(
                f"{no}",
                backup_dir,
                hours,
                file_size_format(bi.bytes_written),
                f"{bi.tapes}/{bi.volumes}",
                reference_backup,
                bi.description
            )
            no += 1

        console = Console()
        console.print(table)

    def _find_no(self, all_backups, needle):
        if needle is None:
            return "-"

        for i in range(len(all_backups)):
            if needle == all_backups[i]:
                return i

        return -1
