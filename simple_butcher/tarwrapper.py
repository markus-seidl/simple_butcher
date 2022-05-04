import subprocess
import logging
import threading
import os
import time

from tqdm import tqdm

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber
from database import BackupRecord
from exe_paths import TAR

COMPRESS_TAR_BACKUP_FULL_CMD = \
    '{cmd} cvM -L{chunk_size}G ' \
    '--new-volume-script="python simple_butcher/archive_finalizer.py \"{communication_file}\"" ' \
    '--label="{backup_name}" ' \
    ' -f {output_file} ' \
    ' {source} > {file_list} '

LIST_CMD = \
    '{cmd} tvf {tar_file}'


class TarWrapper(Wrapper):
    def __init__(self):
        super().__init__()

    def main_backup_full(
            self, config: BackupConfig, backup_bar: tqdm, communication_file: str
    ) -> (str, subprocess.Popen, threading.Thread):
        tar_output_file = config.tempdir + "/tar_output"

        file_list = config.tempdir + "/tar_file_list"
        tar_cmd = COMPRESS_TAR_BACKUP_FULL_CMD.format(
            cmd=TAR,
            chunk_size=config.chunk_size,
            backup_name=config.backup_name,
            output_file=tar_output_file,
            communication_file=communication_file,
            source=config.source,
            file_list=file_list
        )

        logging.debug(f"tar full backup cmd: {tar_cmd}")

        tar_process = subprocess.Popen(tar_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        tar_thread = threading.Thread(
            target=self._wait_for_process_finish_full_backup, args=(tar_process, backup_bar, file_list)
        )
        tar_thread.start()

        return tar_output_file, tar_process, tar_thread

    def _wait_for_process_finish_full_backup(self, process: subprocess.Popen, backup_bar: tqdm, file_list: str):
        self._update_tar_progressbar(backup_bar, process, file_list)

        _, s_err = process.communicate()
        if process.returncode != 0:
            raise OSError(s_err)

    def _update_tar_progressbar(self, backup_bar, process, file_list):
        while True:
            time.sleep(1)
            count_files = buf_count_newlines_gen(file_list)
            backup_bar.update(count_files - backup_bar.n)

            if process.poll() is not None:
                break

        backup_bar.close()

    def update_tar_progressbar_old(self, backup_bar, process):
        # this either slows down tar or stalls it to a halt.
        while True:
            time.sleep(0.05)
            realtime_output = process.stdout.readline()
            realtime_output = realtime_output.decode("UTF-8").strip()
            if len(realtime_output) > 0:
                backup_bar.set_postfix(current_file=self._cut_filename(realtime_output))
                backup_bar.update(1)

            if process.poll() is not None:
                break

    def _cut_filename(self, file_name: str, prefix_length: int = 30, postfix_length: int = 10) -> str:
        """
        Reduces the length of the filename to the specified size
        :param file_name:
        :return:
        """

        if len(file_name) > prefix_length + postfix_length:
            return file_name[0:prefix_length] + "..." + file_name[-(prefix_length + postfix_length + 1):]

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


def buf_count_newlines_gen(fname):
    """
    Fastest method by https://stackoverflow.com/a/68385697
    :param fname:
    :return:
    """

    def _make_gen(reader):
        b = reader(2 ** 16)
        while b:
            yield b
            b = reader(2 ** 16)

    with open(fname, "rb") as f:
        count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
    return count
