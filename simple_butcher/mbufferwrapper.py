import subprocess
import logging
import threading
import os

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

        subprocess.check_call(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        os.remove(archive_file)
