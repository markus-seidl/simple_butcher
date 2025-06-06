import os
import logging
import shutil
import subprocess
import time
import json

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
from progressbar import ProgressDisplay, ByteTask
from exe_paths import ZSTD, AGE, TEE, MBUFFER, SHA256SUM, MD5SUM


class Test:
    """
    Copy of cmd_restore (remember to update here as well!). Simple test mechanism which uses zstd -t to test
    the archives, as tar doesn't have a checksum to my knowledge
    """

    def __init__(self, config: RestoreConfig):
        self.config = config
        self.com = SimpleMq(config.tempdir + "/tar_archive_done")
        self.com.cleanup()
        self.tar_output_file = None
        self.pd = ProgressDisplay()
        # self.tar = TarWrapper(self.pd)
        self.sha256 = Sha256Wrapper()
        self.decompression_v2 = DecompressionZstdAgeV2(test_mode=True)
        # self.tapeinfo = TapeinfoWrapper(config)
        # self.mbuffer = MBufferWrapper(config)
        self.mtst = MTSTWrapper(config.tape, config.tape_dummy)
        self.database = None

    def do(self):
        backup_info, database = self.load_database()

        # --> advise user to load correct tape
        # --> fast forward to correct position on tape
        # self.mtst.move_to_file(backup_info.first_volume_no)

        tape_vol_map, max_volumes = self.count_volumes(backup_info, database)
        logging.info(f"Testing {max_volumes} volumes from {len(tape_vol_map.keys())} tapes.")
        archive_volume_no = ArchiveVolumeNumber(0, 0, 0, 0)
        # tar_thread = None
        # tar_input_file = self.config.tempdir + "/tar_file"
        while archive_volume_no.volume_no <= max_volumes:
            output_file = self.decompression_v2.do(self.config, archive_volume_no)
            archive_volume_no.incr_volume_no()
            logging.info(f"Archive {archive_volume_no.volume_no} loaded from Tape.")

            # output_file is zstd
            zstd_process = subprocess.Popen([ZSTD, "-t", output_file])
            output_stdout, output_stderr = zstd_process.communicate()

            if zstd_process.returncode != 0:
                raise OSError(output_stderr)

            os.remove(output_file)

            # shutil.move(output_file, tar_input_file)

            # if tar_thread is None:
            #     tar_thread = self.tar.restore_full(self.config, self.com.communication_file, tar_input_file)
            # else:
            #     logging.info("Signaling tar to continue...")
            #     self.com.signal_tar_to_continue()

            # logging.info("Waiting for tar to finish...")
            # while tar_thread.is_alive():
            #     if self.com.wait_for_signal():
            #         break

            if self.should_change_tape(archive_volume_no, tape_vol_map):
                input("Change tape!")
                archive_volume_no.incr_tape_no()

        logging.info("Restoration done.")
        database.close()

    def should_change_tape(self, archive_volume_no: ArchiveVolumeNumber, tape_vol_map: dict) -> bool:
        vol_no = archive_volume_no.volume_no
        for tn in tape_vol_map:
            vols = tape_vol_map[tn]
            if vol_no in vols and archive_volume_no.tape_no != tn:
                return True
        return False

    def load_database(self) -> (BackupInfo, BackupDatabase):
        backup_repository = BackupDatabaseRepository(DB_ROOT, self.config.backup_repository)
        return backup_repository.open_backup(self.config.backup_name)

    def count_volumes(self, backup_info: BackupInfo, database: BackupDatabase) -> (dict, int):
        volume_map = dict()
        max_volume_no = 0
        with open(database.database_file(), "r") as f:
            line = f.readline()
            while line:
                record = BackupRecord.from_json(json.loads(line))
                tn, vn = record.tape_no, record.volume_no

                if tn not in volume_map:
                    volume_map[tn] = list()

                volume_map[tn].append(vn)
                if vn > max_volume_no:
                    max_volume_no = vn

                line = f.readline()

        return volume_map, max_volume_no
