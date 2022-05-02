import subprocess
import logging
import threading
import os
from tqdm import tqdm
import re

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber
from database import BackupRecord
from exe_paths import MBUFFER

WRITE_TO_TAPE_OPTS = "{cmd} -i {in_file} -P 90 -l ./mbuffer.log -o {tape}  -s {blocksize}"


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

        mbuffer_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        with tqdm(total=100) as pbar:
            while True:
                realtime_output = mbuffer_process.stdout.readline()
                realtime_output = realtime_output.decode("UTF-8")
                if "%" in realtime_output:
                    s = re.search("(\d+)% done", realtime_output, re.IGNORECASE)
                    if s:
                        pbar.n = int(s.group(1))

                if mbuffer_process.poll():
                    break

        s_out, s_err = mbuffer_process.communicate()
        if process.returncode != 0:
            raise OSError(s_err)

        os.remove(archive_file)
