import subprocess
import logging
import threading
import os
import time

from tqdm import tqdm
import re

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber
from database import BackupRecord
from exe_paths import MBUFFER

WRITE_TO_TAPE_OPTS = "{cmd} -i {in_file} -P 90 -l ./mbuffer.log -q -o {tape}  -s {blocksize}"


class MBufferWrapper(Wrapper):
    def __init__(self, config: BackupConfig):
        super().__init__()
        self.config = config

    def write(self, archive_file):
        if self.config.tape_dummy is not None:
            return

        cmd = WRITE_TO_TAPE_OPTS.format(
            cmd=MBUFFER,
            in_file=archive_file,
            tape=self.config.tape,
            blocksize="512K"
        )

        mbuffer_process = subprocess.Popen(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, bufsize=1,
            universal_newlines=True
        )
        with tqdm(total=100, unit="%") as pbar:
            while True:
                with open("./mbuffer.log", "r") as f:
                    temp = f.readlines()
                    if len(temp) > 0:
                        last_line = temp[-1]
                        if last_line and "%" in last_line:
                            s = re.search("(\\d+)% done", last_line, re.IGNORECASE)
                            if s:
                                pbar.update(int(s.group(1)) - pbar.n)
                    time.sleep(0.1)

                if mbuffer_process.poll():
                    break

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
