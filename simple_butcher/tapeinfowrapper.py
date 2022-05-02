import subprocess
import logging
import threading
import os

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber
from database import BackupRecord
from exe_paths import TAPEINFO

TAPE_CMD = '{cmd} -f {tape}'


class TapeinfoWrapper(Wrapper):
    def __init__(self, config: BackupConfig):
        super().__init__()
        self.config = config

    def blockposition(self) -> int:
        if self.config.tape_dummy:
            return -1

        cmd = TAPE_CMD.format(
            cmd=TAPEINFO,
            tape=self.config.tape
        )
        tape_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_out, s_err = tape_process.communicate()

        lines = s_out.decode("UTF-8").split(os.linesep)
        for line in lines:
            if "Block Position:" in line:
                return int(str(line).replace("Block Position:", ""))

        return -1


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
    print(TapeinfoWrapper(config).blockposition())
