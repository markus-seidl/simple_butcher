import os
import logging
import shutil
import time
import typing

from config import BackupDriveConfig
from common import ArchiveVolumeNumber, get_safe_file_size
from myzmq import SimpleMq
from tarwrapper import TarWrapper
from sha256wrapper import Sha256Wrapper
from compression_zstdage_v2 import ZstdAgeV2
from database import BackupRecord, BackupDatabase, BackupDatabaseRepository, DB_ROOT, BackupInfo
from progressbar import ProgressDisplay, ByteTask


class BackupDrive:
    def __init__(self, config: BackupDriveConfig):
        self.config = config
        self.com = SimpleMq(config.tempdir + "/tar_archive_done")
        self.pd = ProgressDisplay()
        self.com.cleanup()
        self.tar_output_file = None
        self.tar = TarWrapper(self.pd)
        self.sha256 = Sha256Wrapper()
        self.compression_v2 = ZstdAgeV2(self.pd)
        self.database = None

    def do(self):
        self.database = BackupDatabase(DB_ROOT, self.config.backup_repository, self.config.backup_name)
        self.database.start_backup()
        backup_time_start = time.time()

        self.tar_output_file, tar_process, tar_thread = self.tar.main_backup_full(
            self.config, None, self.com.communication_file, self.database
        )

        self.pre_backup_hook()

        archive_volume_no = ArchiveVolumeNumber(tape_no=0, volume_no=0, block_position=0, bytes_written=0)

        while tar_thread.is_alive():
            if self.com.wait_for_signal():
                archive_volume_no, drive_changed = self.handle_archive(archive_volume_no)
                if drive_changed:
                    pass

        if os.path.exists(self.tar_output_file):
            archive_volume_no, drive_changed = self.handle_archive(archive_volume_no, last_archive=True)

        self.post_backup_hook()

        self.database.close_backup(BackupInfo(
            time_start=int(backup_time_start),
            time_end=int(time.time()),
            bytes_written=archive_volume_no.bytes_written,
            tapes=1,
            volumes=archive_volume_no.volume_no + 1,
            base_backup=None,
            incremental_time=self.config.incremental_time,
            tape_start_index=0,
            description=self.config.description,
            tape_serials=[]  # TODO
        ))
        logging.info("Backup process has finished.")

    def pre_backup_hook(self):
        pass

    def post_backup_hook(self):
        pass

    def compression_ratio(self):
        if self.compression_v2.all_bytes_read <= 1:
            return "-"
        return "%.2f" % (self.compression_v2.all_bytes_written / self.compression_v2.all_bytes_read)

    def handle_archive(
            self, archive_volume_no: ArchiveVolumeNumber, last_archive: bool = False
    ) -> typing.Tuple[ArchiveVolumeNumber, bool]:
        tar_archive_file = self.config.tempdir + "/files.tar.%09i" % archive_volume_no.volume_no
        shutil.move(self.tar_output_file, tar_archive_file)
        tar_archive_file_size = get_safe_file_size(tar_archive_file)

        if self.com and not last_archive:
            self.com.signal_tar_to_continue()

        tar_contents = self.tar.get_contents(archive_volume_no, tar_archive_file)

        if self.config.compression == "zstd_pipe_v2":
            self.compress_zstdage_v2(
                archive_volume_no, tar_archive_file, tar_archive_file_size, tar_contents
            )
        else:
            raise Exception("Unknown compression method: " + self.config.compression)

        return archive_volume_no

    def compress_zstdage_v2(
            self, archive_volume_no: ArchiveVolumeNumber, tar_archive_file: str,
            tar_archive_file_size: float, tar_contents
    ):
        final_archive_hash = self.compression_v2.do(
            config=self.config,
            archive_volume_no=archive_volume_no,
            input_file=tar_archive_file
        )

        archive_volume_no.bytes_written = self.compression_v2.all_bytes_written

        tar_contents = self.update_backup_records(tar_contents, final_archive_hash)
        self.database.store(tar_contents)

        archive_volume_no.incr_volume_no()

    def update_backup_records(
            self, backup_records: [BackupRecord], archive_hash: (str, str)
    ) -> [BackupRecord]:
        for record in backup_records:
            record.hash_type = archive_hash[0]
            record.archive_hash = archive_hash[1]
            record.tape_file_number = -1
            record.tape_volume_serial = None

        return backup_records
