import subprocess
import logging
import threading
import os
import time

from tqdm import tqdm

from base_wrapper import Wrapper
from config import BackupConfig, RestoreConfig
from common import ArchiveVolumeNumber, file_size_format, get_safe_file_size
from database import BackupRecord, BackupDatabase
from exe_paths import TAR, FIND
from progressbar import ProgressDisplay, ByteTask

COMPRESS_TAR_BACKUP_FULL_CMD = \
    '{cmd} cvM {excludes} -L{chunk_size}G ' \
    '--new-volume-script="python simple_butcher/archive_finalizer.py \"{communication_file}\"" ' \
    '--label="{backup_name}" ' \
    ' {tar_incremental_stuff} ' \
    ' {incremental_stuff} ' \
    ' -f {output_file} ' \
    ' {source} > {tar_log_file} 2>&1 '

INCREMENTAL_STUFF = '--files-from={input_file_list}'
FIND_CMD = '{cmd} {source} -type f -mtime -{days} > {input_file_list} '

LIST_CMD = '{cmd} tvf {tar_file}'


class TarWrapper(Wrapper):
    def __init__(self, pd: ProgressDisplay):
        super().__init__()
        self.pd = pd

    def main_backup_full(
            self, config: BackupConfig, backup_bar, communication_file: str, database: BackupDatabase
    ) -> (str, subprocess.Popen, threading.Thread):
        tar_output_file = config.tempdir + "/tar_output"

        if os.path.exists(tar_output_file):
            os.remove(tar_output_file)

        tar_incremental_stuff = ""  # f" --listed-incremental={database.tar_incremental_file()} "
        incremental_stuff = ""
        source = config.source
        if config.incremental_time is not None:
            logging.info(f"Incremental backup.")
            logging.info(f"Creating list of files that have changed in the last {config.incremental_time} days")
            input_file_list = self._build_incremental_file_list(config, database)

            found_files = buf_count_newlines_gen(input_file_list)
            logging.info(f"Found {found_files} changed files.")

            source = ""
            incremental_stuff = INCREMENTAL_STUFF.format(input_file_list=input_file_list)

        excludes = ""
        if config.excludes:
            for exclude in config.excludes:
                excludes += f' --exclude "{exclude[0]}"'

        tar_log_file = database.tar_log_file()
        tar_cmd = COMPRESS_TAR_BACKUP_FULL_CMD.format(
            cmd=TAR,
            excludes=excludes,
            chunk_size=config.chunk_size,
            backup_name=config.backup_name,
            output_file=tar_output_file,
            communication_file=communication_file,
            tar_incremental_stuff=tar_incremental_stuff,
            source=source,
            tar_log_file=tar_log_file,
            incremental_stuff=incremental_stuff
        )

        logging.info(f"Tar full backup cmd: {tar_cmd}")

        tar_process = subprocess.Popen(tar_cmd, shell=True)  # , stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        tar_thread = threading.Thread(
            target=self._wait_for_process_finish_full_backup,
            args=(tar_process, backup_bar, tar_log_file, tar_output_file, config.chunk_size)
        )
        tar_thread.start()

        return tar_output_file, tar_process, tar_thread

    def restore_full(self, config: RestoreConfig, communication_file: str, tar_input_file: str):
        # tar_cmd = [TAR, "xvM", "-f", tar_input_file, "--directory", config.dest]
        # tar_cmd.append("--new-volume-script")
        # tar_cmd.append(f'')

        tar_cmd = f"{TAR} xvM -f {tar_input_file} --directory {config.dest} " \
                  f'--new-volume-script="python simple_butcher/archive_finalizer.py \"{communication_file}\""'

        tar_process = subprocess.Popen(tar_cmd, shell=True)
        tar_thread = threading.Thread(
            target=self._wait_for_process_finish_restore,
            args=(tar_process,)
        )
        tar_thread.start()

        return tar_thread

    def _wait_for_process_finish_restore(self, process: subprocess.Popen):
        _, s_err = process.communicate()
        if process.returncode != 0:
            raise OSError(s_err)

    def _build_incremental_file_list(self, config: BackupConfig, database: BackupDatabase) -> str:
        find_cmd = FIND_CMD.format(
            cmd=FIND,
            source=config.source,
            days=config.incremental_time,
            input_file_list=database.tar_input_file_list()
        )

        logging.debug(f"find cmd: {find_cmd}")

        subprocess.check_call(find_cmd, shell=True)

        return database.tar_input_file_list()

    def _wait_for_process_finish_full_backup(
            self, process: subprocess.Popen, backup_bar, tar_log_file: str, output_file: str,
            chunk_size: int
    ):
        self._update_tar_progressbar(backup_bar, process, tar_log_file, output_file, chunk_size)

        _, s_err = process.communicate()
        if process.returncode != 0:
            raise OSError("Error might be an artefact if files changed while reading, check tar.log: " + s_err)

    def _update_tar_progressbar(self, backup_bar, process, tar_log_file, output_file, chunk_size: int):
        last_size = -1
        chunk_size_bytes = chunk_size * 1024 * 1024 * 1024
        with self.pd.create_byte_bar("tar", chunk_size_bytes) as p:
            while True:
                if os.path.exists(output_file):
                    cur_size = get_safe_file_size(output_file)
                    if last_size > cur_size:
                        self.pd.progress.reset(p.task_id, total=chunk_size_bytes)
                    p.update(completed=cur_size)
                    last_size = cur_size
                time.sleep(0.1)

                if process.poll() is not None:
                    break

    # def update_tar_progressbar_old(self, backup_bar, process):
    #     # this either slows down tar or stalls it to a halt.
    #     while True:
    #         time.sleep(0.05)
    #         realtime_output = process.stdout.readline()
    #         realtime_output = realtime_output.decode("UTF-8").strip()
    #         if len(realtime_output) > 0:
    #             backup_bar.set_postfix(current_file=self._cut_filename(realtime_output))
    #             backup_bar.update(1)
    #
    #         if process.poll() is not None:
    #             break

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
                archive_hash=None,
                hash_type=None
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
