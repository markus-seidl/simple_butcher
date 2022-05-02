import subprocess
import logging
import threading
import os
from tqdm import tqdm

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber
from database import BackupRecord
from exe_paths import TAR

COMPRESS_TAR_BACKUP_FULL_CMD = \
    '{cmd} cvM -L{chunk_size}G ' \
    '--new-volume-script="python simple_butcher/archive_finalizer.py" ' \
    '--label="{backup_name}" ' \
    ' -f {output_file} ' \
    ' {source} '

LIST_CMD = \
    '{cmd} tvf {tar_file}'


class TarWrapper(Wrapper):
    def __init__(self):
        super().__init__()

    def main_backup_full(self, config: BackupConfig, backup_bar: tqdm) -> (str, subprocess.Popen, threading.Thread):
        tar_output_file = config.tempdir + "/tar_output"

        tar_cmd = COMPRESS_TAR_BACKUP_FULL_CMD.format(
            cmd=TAR,
            chunk_size=config.chunk_size,
            backup_name=config.backup_name,
            output_file=tar_output_file,
            source=config.source
        )
        logging.debug(f"tar full backup cmd: {tar_cmd}")

        tar_process = subprocess.Popen(tar_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        tar_thread = threading.Thread(target=self._wait_for_process_finish_full_backup, args=(tar_process, backup_bar,))
        tar_thread.start()

        return tar_output_file, tar_process, tar_thread

    def _wait_for_process_finish_full_backup(self, process: subprocess.Popen, backup_bar: tqdm):
        while True:
            realtime_output = process.stdout.readline()
            backup_bar.set_postfix(current_file=realtime_output)

            if process.poll():
                break

        s_out, s_err = process.communicate()
        if process.returncode != 0:
            raise OSError(s_err)

    def get_contents(self, archive_volume_no: ArchiveVolumeNumber, tar_file: str) -> [BackupRecord]:
        """
        Gets the contents of the tar archive
        """
        tar_cmd = LIST_CMD.format(
            cmd=TAR,
            tar_file=tar_file
        )
        logging.debug(f"tar list cmd: {tar_cmd}")

        list_process = subprocess.Popen(tar_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_out, s_err = list_process.communicate()

        lines = s_out.decode("UTF-8").split(os.linesep)

        ret = []
        for line in lines:
            if line == '':
                continue

            ret.append(BackupRecord(
                tape_no=archive_volume_no.tape_no,
                volume_no=archive_volume_no.volume_no,
                tar_line=line,
                archive_sha256=None
            ))

        return ret
