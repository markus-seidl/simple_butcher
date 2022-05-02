import os
import logging
import shutil
import time

from config import BackupConfig
from common import ArchiveVolumeNumber, compression_info, file_size_format
from myzmq import MyZmq
from tarwrapper import TarWrapper
from sha256wrapper import Sha256Wrapper
from compression import ZstdPipe
from tapeinfowrapper import TapeinfoWrapper
from database import BackupRecord, Database
from mbufferwrapper import MBufferWrapper
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


class Backup:
    def __init__(self, config: BackupConfig):
        self.config = config
        self.com = MyZmq()
        self.tar_output_file = None
        self.tar = TarWrapper()
        self.sha256 = Sha256Wrapper()
        self.compression = ZstdPipe()
        self.tapeinfo = TapeinfoWrapper(config)
        self.mbuffer = MBufferWrapper(config)
        self.database = None

    def do(self):
        with logging_redirect_tqdm():
            self.database = Database("./db", self.config.backup_repository, self.config.backup_name)
            self.database.start_backup()

            self.tar_output_file, tar_process, tar_thread = self.tar.main_backup_full(self.config)

            archive_volume_no = ArchiveVolumeNumber(tape_no=0, volume_no=0, block_position=0, bytes_written=0)
            pbar = self.tqdm_create_main_bar()
            while tar_thread.is_alive():
                if self.com.wait_for_signal():
                    archive_volume_no, tape_changed = self.handle_archive(archive_volume_no)
                    if tape_changed:
                        pbar.close()
                        pbar = self.tqdm_create_main_bar()

                    pbar.n = archive_volume_no.block_position

            if os.path.exists(self.tar_output_file):
                # backup also last output file
                archive_volume_no = self.handle_archive(archive_volume_no)

            logging.info("Backup process has finished.")

    def tqdm_create_main_bar(self):
        return tqdm(
            total=self.config.tape_length,
            unit="blocks",
            desc="Current tape",
            dynamic_ncols=True
        )

    def handle_archive(self, archive_volume_no: ArchiveVolumeNumber) -> (ArchiveVolumeNumber, bool):
        """
        :param archive_volume_no:
        :return: (ArchiveVolumeNumber, Tape position / backuped size)
        """
        logging.debug("Chunk is ready...")
        handle_bar = tqdm(
            total=4,
            unit="steps",
            desc="Handling archive",
            dynamic_ncols=True,
            position=1
        )

        with handle_bar:
            self.update_tqdm(handle_bar, 1, "List contents")

            # Move output to new file so the tar process can be executed while compression/enc/tape writing is done
            tar_archive_file = self.config.tempdir + "/files.tar.%09i" % archive_volume_no.volume_no
            shutil.move(self.tar_output_file, tar_archive_file)
            tar_archive_file_size = os.path.getsize(tar_archive_file)

            # Unleash the TAR process to prepare the next file
            if self.com:
                self.com.socket.send(b"CONTINUE")

            tar_contents = self.tar.get_contents(archive_volume_no, tar_archive_file)

            # Compression / Encryption
            compression_timer_start = time.time()

            self.update_tqdm(handle_bar, 1, "Compress")
            final_archive = self.compression.do(
                config=self.config,
                archive_volume_no=archive_volume_no,
                input_file=tar_archive_file
            )

            self.update_tqdm(handle_bar, 1, "SHA256")
            final_archive_size = os.path.getsize(final_archive)
            final_archive_sha = self.sha256.calc_sum(final_archive)
            tar_contents = self.update_backup_records(tar_contents, final_archive_sha)
            self.database.store(tar_contents)

            compression_time = time.time() - compression_timer_start
            logging.debug(
                "Compression took: %3.1fs %s %s/s" % (
                    compression_time, compression_info(tar_archive_file_size, final_archive_size),
                    file_size_format(final_archive_size / compression_time)
                )
            )

            # Determine if next tape is necessary
            if self.config.tape_dummy:
                archive_volume_no.block_position = int(self.estimate_block_length(final_archive_size))
            else:
                archive_volume_no.block_position += self.tapeinfo.blockposition()  # fake for debugging
            archive_volume_no.bytes_written += final_archive_size
            logging.debug("Block position (before writing): " + str(archive_volume_no.block_position))

            tape_change = False
            if not self.fit_on_tape(final_archive_size):
                logging.warning("Next archive will not fit on tape, please change it and press any key...")
                input("Press any key")
                tape_change = True

            self.update_tqdm(handle_bar, 1, "Write to tape")
            self.mbuffer.write(final_archive)
            archive_volume_no.incr_volume_no()

            return archive_volume_no, tape_change

    def update_backup_records(self, backup_records: [BackupRecord], archive_sha256) -> [BackupRecord]:
        for record in backup_records:
            record.archive_sha256 = archive_sha256

        return backup_records

    def estimate_block_length(self, file_size) -> int:
        block_size_bytes = 524272  # rough estimate
        return file_size / block_size_bytes

    def fit_on_tape(self, file_size):
        position = self.tapeinfo.blockposition()
        return position + self.estimate_block_length(file_size) + 10 < self.config.tape_length

    def update_tqdm(self, bar, n, desc):
        bar.set_description(desc)
        bar.update(n)
