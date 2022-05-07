import subprocess
import logging
import threading
import os
import time

from tqdm import tqdm
import re

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber, report_performance
from database import BackupRecord
from exe_paths import MBUFFER

WRITE_TO_TAPE_OPTS = "{cmd} -i {in_file} -P 90 -l {logfile} -q -o {tape}  -s {blocksize}"


class MBufferWrapper(Wrapper):
    def __init__(self, config: BackupConfig):
        super().__init__()
        self.config = config

    def write(self, archive_file):
        if self.config.tape_dummy is not None:
            return

        mbuffer_log = self.config.tempdir + "/mbuffer.log"
        cmd = WRITE_TO_TAPE_OPTS.format(
            cmd=MBUFFER,
            in_file=archive_file,
            tape=self.config.tape,
            blocksize="512K",
            logfile=mbuffer_log
        )

        mbuffer_process = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1,
            universal_newlines=True
        )

        logging.info(f"Start writing file {archive_file} to tape {self.config.tape}")
        start_time = time.time()
        # with create_tqdm(total=100, unit="%", leave=False) as pbar:
        last_percentage = -1
        while True:
            try:
                with open(mbuffer_log, "r") as f:
                    temp = f.readlines()
                    if len(temp) > 0:
                        last_line = temp[-1]
                        if last_line and "%" in last_line:
                            s = re.search("(\\d+)% done", last_line, re.IGNORECASE)
                            if s:
                                cur_percentage = int(s.group(1))
                                if last_percentage != cur_percentage:
                                    logging.info(f"Writing to tape... {cur_percentage}%")
                                    last_percentage = cur_percentage
                                # pbar.update(int(s.group(1)) - pbar.n)
            except:
                pass

            time.sleep(2)

            if mbuffer_process.poll() is not None:
                break

        logging.info(f"Finished writing to tape for {report_performance(start_time, archive_file)}")

        s_out, s_err = mbuffer_process.communicate()
        if mbuffer_process.returncode != 0:
            raise OSError(s_err)

        os.remove(archive_file)


if __name__ == '__main__':
    config = BackupConfig(
        backup_repository=None,
        ramdisk=None,
        compression=None,
        source=None,
        password_file=None,
        password=None,
        tape_length=None,
        tempdir=None,
        tape="/dev/nst0",
        tape_dummy=None,
        chunk_size=None,
        backup_name=None,
    )
    MBufferWrapper(config).write("/mnt/scratch/tar_output")
