import os
import logging
import shutil
import time
import typing
import getdevinfo.devinfo as devinfo

from config import BackupDriveConfig
from common import ArchiveVolumeNumber, get_safe_file_size
from myzmq import SimpleMq
from simple_butcher.config import DestinationPath
from tarwrapper import TarWrapper
from sha256wrapper import Sha256Wrapper
from compression_drive_zstdage import ZstdAgeDriveV2
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
        self.compression_v2 = ZstdAgeDriveV2(self.pd)
        self.database = None
        self.current_drive_serial = None

    def do(self):
        self.database = BackupDatabase(DB_ROOT, self.config.backup_repository, self.config.backup_name)
        self.database.start_backup()
        backup_time_start = time.time()

        self.tar_output_file, tar_process, tar_thread = self.tar.main_backup_full(
            self.config, None, self.com.communication_file, self.database
        )

        self.current_drive_serial = devinfo.get_serial_for_dir(self.config.destination_config.paths[0].path)

        self.pre_backup_hook()

        archive_volume_no = ArchiveVolumeNumber(tape_no=0, volume_no=0, block_position=0, bytes_written=0, current_path_idx=0)

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

        return archive_volume_no, False

    def compress_zstdage_v2(
            self, archive_volume_no: ArchiveVolumeNumber, tar_archive_file: str,
            tar_archive_file_size: float, tar_contents
    ):
        if not self.fits_on_drive_path(archive_volume_no, tar_archive_file_size):
            if len(self.config.destination_config.paths) == 1:  # drive mode
                self.handle_drive_change()
            else:  # directory mode
                paths = self.config.destination_config.paths
                idx = archive_volume_no.current_path_idx
                while idx < len(paths) and not self.has_quota_left(paths[idx], tar_archive_file_size):
                    idx += 1

                if idx >= len(paths):
                    raise OSError("All destination paths have reached their configured quota.")

                archive_volume_no.current_path_idx = idx
                logging.info(f"Switching to path {paths[idx]}.")

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
            record.drive_volume_serial = self.current_drive_serial

        return backup_records

    def fits_on_drive_path(self, archive_volume_no: ArchiveVolumeNumber, tar_archive_file_size: float | int) -> bool:
        dest_config = self.config.destination_config
        if dest_config is None or not dest_config.paths:
            raise ValueError("No destination paths configured!")

        paths = dest_config.paths

        if len(paths) == 1:
            # drive mode - need to change disk
            return self.has_quota_left(paths[0], tar_archive_file_size)
        else:  # directory mode
            idx = archive_volume_no.current_path_idx
            old_idx = idx
            while idx < len(paths) and not self.has_quota_left(paths[idx], tar_archive_file_size):
                idx += 1

            if idx >= len(paths):
                return False

            return idx == old_idx

    def has_quota_left(self, dest: DestinationPath, tar_archive_file_size: float | int) -> bool:
        os.makedirs(dest.path, exist_ok=True)

        quota = dest.quota
        if quota is None or quota <= 0:
            raise OSError(f"Quota must be a positive number and exist for {dest.path}")

        if quota <= 1:
            # Fraction of the filesystem that may be used.
            total, used, _ = shutil.disk_usage(dest.path)
            return ((used + tar_archive_file_size) / total) < quota

        # Absolute limit in GB on the bytes written into the destination directory.
        limit_bytes = quota * 1024 ** 3
        return self.get_directory_size(dest.path) + tar_archive_file_size < limit_bytes

    @staticmethod
    def get_directory_size(path: str) -> int:
        total = 0
        for root, _, files in os.walk(path):
            for name in files:
                file_path = os.path.join(root, name)
                if os.path.exists(file_path):
                    total += os.path.getsize(file_path)
        return total

    def handle_drive_change(self):
        drive_serial_before = self.current_drive_serial
        while True:
            logging.warning("Next archive will not fit on drive, please change it and press any key...")
            logging.warning(f"Remove tape {drive_serial_before}")
            input("Press enter key")

            drive_serial_after = devinfo.get_serial_for_dir(self.config.destination_config.paths[0].path)
            if drive_serial_after == drive_serial_before:
                logging.warning(f"Tape serial before {drive_serial_before} matches the current tape serial {drive_serial_after}.")
            else:
                self.current_drive_serial = devinfo.get_serial_for_dir(self.config.destination_config.paths[0].path)
                break
