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
LOG_CMD = '{cmd} -a {tape}'


class SizeInfo:
    def __init__(self, remaining_bytes=-1, written_bytes=-1, maximum_bytes=-1):
        self.remaining_bytes = remaining_bytes
        self.written_bytes = written_bytes
        self.maximum_bytes = maximum_bytes


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

    def size_statistics(self) -> SizeInfo:
        if self.config.tape_dummy:
            return SizeInfo()

        remaining_capa_line = "Main partition remaining capacity (in MiB): "
        maximum_size_line = "Main partition maximum capacity (in MiB): "

        log_process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_out, s_err = log_process.communicate()

        remaining_bytes = -1
        maximum_bytes = -1
        lines = s_out.decode("UTF-8").split(os.linesep)
        for line in lines:
            if remaining_capa_line in line:
                remaining_bytes = int(line.replace(remaining_capa_line, "")) * 1000 * 1000
            if maximum_size_line in line:
                maximum_bytes = int(line.replace(maximum_size_line, "")) * 1000 * 1000

        ret = SizeInfo(remaining_bytes, maximum_bytes - remaining_bytes, maximum_bytes)
        return ret


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
