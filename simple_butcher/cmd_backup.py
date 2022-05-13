import os
import logging
import shutil
import time

from config import BackupConfig
from common import ArchiveVolumeNumber, compression_info, file_size_format, report_performance
from myzmq import SimpleMq
from tarwrapper import TarWrapper
from sha256wrapper import Sha256Wrapper
from compression_zstdpipe import ZstdPipe
from compression_zstdage_v2 import ZstdAgeV2
from tapeinfowrapper import TapeinfoWrapper
from database import BackupRecord, BackupDatabase, BackupDatabaseRepository, DB_ROOT, BackupInfo, \
    INCREMENTAL_INDEX_FILENAME
from mbufferwrapper import MBufferWrapper
from mtstwrapper import MTSTWrapper


class Backup:
    def __init__(self, config: BackupConfig):
        self.config = config
        self.com = SimpleMq(config.tempdir + "/tar_archive_done")
        self.com.cleanup()
        self.tar_output_file = None
        self.tar = TarWrapper()
        self.sha256 = Sha256Wrapper()
        self.compression = ZstdPipe()
        self.compression_v2 = ZstdAgeV2()
        self.tapeinfo = TapeinfoWrapper(config)
        self.mbuffer = MBufferWrapper(config)
        self.mtst = MTSTWrapper(config.tape, config.tape_dummy)
        self.database = None

    def do(self):
        self.database = BackupDatabase(DB_ROOT, self.config.backup_repository, self.config.backup_name)
        self.database.start_backup()
        backup_time_start = time.time()

        tape_start_index, _, _ = self.mtst.current_position()
        base_backup_name = None  # self.prepare_incremental_file(self.database)

        self.tar_output_file, tar_process, tar_thread = self.tar.main_backup_full(
            self.config, None, self.com.communication_file, self.database
        )

        initial_tape_size = self.tapeinfo.size_statistics()

        self.pre_backup_hook()

        chunk_start_time = time.time()
        archive_volume_no = ArchiveVolumeNumber(tape_no=0, volume_no=0, block_position=0, bytes_written=0)

        while tar_thread.is_alive():
            if self.com.wait_for_signal():
                logging.info(f"Tar chunk finished for {report_performance(chunk_start_time, self.tar_output_file)}")
                chunk_start_time = time.time()

                archive_volume_no, tape_changed = self.handle_archive(archive_volume_no)
                if tape_changed:
                    logging.info(f"################ Tape Changed to {archive_volume_no.tape_no} ################")

                initial_tape_size = self.tapeinfo.size_statistics()
                logging.info(
                    f"Tape status {file_size_format(initial_tape_size.written_bytes)} written, "
                    f"{file_size_format(initial_tape_size.remaining_bytes)} remaining"
                )

        if os.path.exists(self.tar_output_file):  # backup also last output file
            archive_volume_no, tape_changed = self.handle_archive(archive_volume_no, last_archive=True)

        self.post_backup_hook()

        self.database.close_and_compress(BackupInfo(
            time_start=int(backup_time_start),
            time_end=int(time.time()),
            bytes_written=archive_volume_no.bytes_written,
            tapes=archive_volume_no.tape_no + 1,  # no vs count!
            volumes=archive_volume_no.volume_no + 1,  # no vs count!
            base_backup=None,
            incremental_time=self.config.incremental_time,
            tape_start_index=tape_start_index
        ))
        logging.info("Backup process has finished.")

    def pre_backup_hook(self):
        pass

    def post_backup_hook(self):
        pass

    def handle_archive(
            self, archive_volume_no: ArchiveVolumeNumber, last_archive: bool = False
    ) -> (ArchiveVolumeNumber, bool):
        """
        :param archive_volume_no:
        :return: (ArchiveVolumeNumber, Tape position / backuped size)
        """
        logging.info("Processing next chunk...")

        # Move output to new file so the tar process can be executed while compression/enc/tape writing is done
        tar_archive_file = self.config.tempdir + "/files.tar.%09i" % archive_volume_no.volume_no
        shutil.move(self.tar_output_file, tar_archive_file)
        tar_archive_file_size = os.path.getsize(tar_archive_file)

        # Unleash the TAR process to prepare the next file
        if self.com and not last_archive:  # can't signal on last archive, as there is no one listening
            self.com.signal_tar_to_continue()

        tar_contents = self.tar.get_contents(archive_volume_no, tar_archive_file)

        # Determine if next tape is necessary. Since we don't know the compression ratio yet, we assume the worst.
        tape_change = False
        if not self.fit_on_tape(tar_archive_file_size):
            logging.warning("Next archive will not fit on tape, please change it and press any key...")
            input("Press any key")

            archive_volume_no.incr_tape_no()
            tape_change = True

        # Compression / Encryption / Writing to Tape
        if self.config.compression == "zstd_pipe_v2":
            self.compress_zstdage_v2(
                archive_volume_no, tar_archive_file, tar_archive_file_size, tar_contents
            )
        elif self.config.compression == "zstd_pipe":
            self.compress_zstd_pipe(
                archive_volume_no, tar_archive_file, tar_archive_file_size, tar_contents
            )

        logging.debug("Written to tape: " + str(archive_volume_no.bytes_written))

        return archive_volume_no, tape_change

    def compress_zstdage_v2(
            self, archive_volume_no: ArchiveVolumeNumber, tar_archive_file: str,
            tar_archive_file_size: float, tar_contents
    ):

        final_archive_hash = self.compression_v2.do(
            config=self.config,
            archive_volume_no=archive_volume_no,
            input_file=tar_archive_file
        )

        if self.config.tape_dummy:
            archive_volume_no.bytes_written += int(tar_archive_file_size) # fake for no-tape
        else:
            archive_volume_no.bytes_written = self.compression_v2.all_bytes_written

        ratio = "%.2f" % (self.compression_v2.all_bytes_written / self.compression_v2.all_bytes_read)
        logging.info(
            f"Statistics "
            f"{file_size_format(self.compression_v2.all_bytes_read)} read, "
            f"{file_size_format(self.compression_v2.all_bytes_written)} written = {ratio} "
        )

        tape_file_number, _, _ = self.mtst.current_position()
        tar_contents = self.update_backup_records(tar_contents, final_archive_hash, tape_file_number - 1)
        self.database.store(tar_contents)

        archive_volume_no.incr_volume_no()

    def compress_zstd_pipe(
            self, archive_volume_no: ArchiveVolumeNumber, tar_archive_file: str,
            tar_archive_file_size: float, tar_contents
    ):
        compression_timer_start = time.time()
        final_archive = self.compression.do(
            config=self.config,
            archive_volume_no=archive_volume_no,
            input_file=tar_archive_file
        )
        final_archive_size = os.path.getsize(final_archive)
        self.sha256.start_calc_sum(final_archive)
        compression_time = time.time() - compression_timer_start
        logging.debug(
            "Compression took: %3.1fs %s %s/s" % (
                compression_time, compression_info(tar_archive_file_size, final_archive_size),
                file_size_format(final_archive_size / compression_time)
            )
        )
        # Determine if next tape is necessary
        if self.config.tape_dummy:
            archive_volume_no.bytes_written += int(final_archive_size)  # fake for no-tape
        else:
            archive_volume_no.bytes_written = self.tapeinfo.size_statistics().written_bytes

        self.mbuffer.write(final_archive)

        final_archive_sha = self.sha256.wait_for_sha_sum()
        tar_contents = self.update_backup_records(tar_contents, final_archive_sha)
        self.database.store(tar_contents)

        archive_volume_no.incr_volume_no()

    def update_backup_records(
            self, backup_records: [BackupRecord], archive_hash: (str, str), tape_file_number: int = -1
    ) -> [BackupRecord]:
        for record in backup_records:
            record.hash_type = archive_hash[0]
            record.archive_hash = archive_hash[1]
            record.tape_file_number = tape_file_number

        return backup_records

    def estimate_block_length(self, file_size) -> int:
        block_size_bytes = 524272  # rough estimate
        return file_size / block_size_bytes

    def fit_on_tape(self, file_size_bytes):
        if self.config.tape_dummy is not None:
            return True

        written_bytes = self.tapeinfo.size_statistics().written_bytes
        remaining_bytes = self.tapeinfo.size_statistics().remaining_bytes

        # on my LTO-6 tapes I can only write until 115GB are remaining, maybe do some --tapeaware on mbuffer?
        buffer_bytes = self.config.tape_buffer * 1000 * 1000 * 1000
        return file_size_bytes + buffer_bytes < remaining_bytes

    # def prepare_incremental_file(self, current_backup: BackupDatabase) -> str:
    #     """
    #     Copies the incremental file from the base_backup_of to the current database. If it doesn't exist, TAR won't do
    #     an incremental backup
    #     """
    #     if self.config.base_of_backup is None:
    #         # logging.info("No base to incremental from.")
    #         return None
    #
    #     repository = BackupDatabaseRepository(DB_ROOT, self.config.backup_repository)
    #     past_backups = repository.list_backups()
    #     if len(past_backups) == 0:
    #         logging.info("No previous backup, starting fresh.")
    #         return None
    #
    #     if self.config.base_of_backup >= len(past_backups) or self.config.base_of_backup < 0:
    #         logging.info(f"Index out of range base_of_backup: {self.config.base_of_backup}. Starting fresh.")
    #         return None
    #
    #     logging.info(f"Basing backup of {past_backups[self.config.base_of_backup]}...")
    #     repository.copy_file(
    #         past_backups[self.config.base_of_backup], current_backup.backup_name, INCREMENTAL_INDEX_FILENAME
    #     )
    #
    #     return past_backups[self.config.base_of_backup]

# def update_tqdm_n_desc(bar, n, desc):
#     bar.set_description(desc)
#     bar.update(n)
