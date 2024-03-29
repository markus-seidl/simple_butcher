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
from exe_paths import MT_ST

CMD = "{exe} -f {tape} {cmd}"


class MTSTWrapper(Wrapper):
    def __init__(self, tape: str, tape_dummy: str):
        super().__init__()
        self._tape = tape
        self._tape_dummy = tape_dummy

    def current_position(self) -> (int, int, int):
        if self._tape_dummy is not None:
            return -1, -1, -1

        output = self._exec("status")
        output_lines = output.decode("UTF-8").split(os.linesep)

        for line in output_lines:
            # File number=1, block number=0, partition=0
            s = re.search(
                "File number=(\\d+), block number=(\\d+), partition=(\\d+)", line, re.IGNORECASE
            )
            if s:
                return int(s.group(1)), int(s.group(2)), int(s.group(3))

        return -1, -1, -1

    def move_to_file(self, file_no: int):
        if self._tape_dummy is not None:
            return

        logging.info("Rewinding tape ...")
        self._exec(f"rewind")

        logging.info(f"Moving head to the beginning of {file_no} ...")
        self._exec(f"fsf {file_no}")

    def _exec(self, mtst_cmd) -> str:
        cmd = CMD.format(
            exe=MT_ST,
            tape=self._tape,
            cmd=mtst_cmd
        )

        return subprocess.check_output(cmd, shell=True)


if __name__ == '__main__':
    print(MTSTWrapper("/dev/nst0", None).current_position())
