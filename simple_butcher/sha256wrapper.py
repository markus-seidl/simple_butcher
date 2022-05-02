import subprocess
import logging
import threading
import os

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber
from database import BackupRecord
from exe_paths import SHA256SUM

SHA_256_CMD = '{cmd} -b {file} '


class Sha256Wrapper(Wrapper):
    def __init__(self):
        super().__init__()

    def calc_sum(self, file) -> str:
        sha_cmd = SHA_256_CMD.format(
            cmd=SHA256SUM,
            file=file
        )
        logging.debug(f"sha256sum cmd: {sha_cmd}")
        list_process = subprocess.Popen(sha_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_out, s_err = list_process.communicate()

        lines = s_out.decode("UTF-8").split(os.linesep)

        return lines[0].split(" ")[0]
