import os
import logging
import shutil
import time

from config import RestoreConfig
from common import ArchiveVolumeNumber, compression_info, file_size_format, report_performance
from myzmq import SimpleMq
from tarwrapper import TarWrapper
from sha256wrapper import Sha256Wrapper
from decompression_zstdage_v2 import DecompressionZstdAgeV2
from tapeinfowrapper import TapeinfoWrapper
from database import BackupRecord, BackupDatabase, BackupDatabaseRepository, DB_ROOT, BackupInfo, \
    INCREMENTAL_INDEX_FILENAME
from mbufferwrapper import MBufferWrapper
from mtstwrapper import MTSTWrapper


class Restore:
    def __init__(self, config: RestoreConfig):
        self.config = config
        self.com = SimpleMq(config.tempdir + "/tar_archive_done")
        self.com.cleanup()
        self.tar_output_file = None
        self.tar = TarWrapper()
        self.sha256 = Sha256Wrapper()
        self.decompression_v2 = DecompressionZstdAgeV2()
        # self.tapeinfo = TapeinfoWrapper(config)
        # self.mbuffer = MBufferWrapper(config)
        self.mtst = MTSTWrapper(config.tape, config.tape_dummy)
        self.database = None

    def do(self):
        backup_repository = BackupDatabaseRepository(DB_ROOT, self.config.backup_repository)
        backup_info, database = backup_repository.read_backup_info(self.config.backup_name)

        # --> advise user to load correct tape
        # --> fast forward to correct position on tape

        max_volumes = self.count_volumes(backup_info, database)
        archive_volume_no = ArchiveVolumeNumber(0, 0, 0, 0)
        while archive_volume_no.volume_no <= max_volumes:
            output_file = self.decompression_v2.do(self.config, archive_volume_no)

        # Yes --> mbuffer --> age --> zstd -d --> file --> tar

        database.close()

    def load_database(self) -> (BackupInfo, BackupDatabase):
        backup_repository = BackupDatabaseRepository(DB_ROOT, self.config.backup_repository)
        backup_info = backup_repository.read_backup_info(self.config.backup_name)
        database = BackupDatabase(DB_ROOT, self.config.backup_repository, self.config.backup_name)
        database.open()

        return backup_info, database

    def count_volumes(self, backup_info: BackupInfo, database: BackupDatabase) -> int:
        return backup_info.volumes
