from common import file_size_format
from config import ListBackupConfig
from tapeinfowrapper import TapeinfoWrapper
from database import BackupRecord, BackupDatabase, BackupDatabaseRepository, DB_ROOT
import logging


class IdentifyTape:
    def __init__(self, config: ListBackupConfig):
        self.config = config
        self.tapeinfo = TapeinfoWrapper(config)

    def do(self):
        volume_serial = self.tapeinfo.volume_serial()
        logging.info(f"Tape serial: {volume_serial}")
