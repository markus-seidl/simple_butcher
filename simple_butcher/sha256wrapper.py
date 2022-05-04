import subprocess
import logging
import threading
import os
import time

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber
from database import BackupRecord
from exe_paths import SHA256SUM

SHA_256_CMD = '{cmd} -b {file} '


class Sha256Wrapper(Wrapper):
    def __init__(self):
        super().__init__()
        self.sha_sum = None

    def start_calc_sum(self, file):
        self.sha_sum = None

        sha_cmd = SHA_256_CMD.format(
            cmd=SHA256SUM,
            file=file
        )
        logging.debug(f"sha256sum cmd: {sha_cmd}")
        sha_process = subprocess.Popen(sha_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        sha_thread = threading.Thread(
            target=self._wait_for_process, args=(sha_process,)
        )
        sha_thread.start()

    def wait_for_sha_sum(self):
        while self.sha_sum is None:
            time.sleep(0.1)
        return self.sha_sum

    def _wait_for_process(self, sha_process):
        s_out, s_err = sha_process.communicate()
        lines = s_out.decode("UTF-8").split(os.linesep)

        self.sha_sum = lines[0].split(" ")[0]
