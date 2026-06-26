import logging
import os
import shutil
import subprocess
import time
import re

from base_wrapper import Wrapper
from common import ArchiveVolumeNumber, report_performance, report_performance_bytes

from config import BackupDriveConfig, DestinationPath
from common import ArchiveVolumeNumber, file_size_format, get_safe_file_size
from database import BackupRecord
from exe_paths import ZSTD, AGE, TEE, MBUFFER, SHA512SUM, MD5SUM
from base_compression import Compression
from progressbar import ProgressDisplay, ByteTask


class ZstdAgeDriveV2(Compression):
    """
    This class compresses, encrypts and writes to disk with zstd and age.
    Additionally, md5 is also computed.
    """

    def __init__(self, pd: ProgressDisplay):
        super().__init__()
        self.all_bytes_read = 0
        self.all_bytes_written = 0
        self.pd = pd

    def determine_output_file(self, config: BackupDriveConfig, archive_volume_no: ArchiveVolumeNumber) -> str:
        file_name = "/%09i.tar.zst.age" % archive_volume_no.volume_no

        paths = config.destination_config.paths
        idx = archive_volume_no.current_path_idx

        return paths[idx].path + file_name

    def do(self, config: BackupDriveConfig, archive_volume_no: ArchiveVolumeNumber, input_file: str) -> (str, str):
        output_file = self.determine_output_file(config, archive_volume_no)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        original_size = get_safe_file_size(input_file)
        self.all_bytes_read += original_size

        if os.path.exists(output_file):
            os.remove(output_file)

        mbuffer_log = config.tempdir + "/mbuffer.log"
        # ---
        zstd_process = subprocess.Popen(
            [ZSTD, f"-{config.zstd_level}", "-T0", input_file, "--stdout"], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
        age_process = subprocess.Popen(
            [AGE, "-e", "-i", config.password_file, "-o", output_file], stdin=zstd_process.stdout, stdout=subprocess.PIPE
        )

        start_piping = time.time()
        output_stdout, output_stderr = age_process.communicate()

        if age_process.returncode != 0:
            raise OSError(output_stderr)

        bytes_written = get_safe_file_size(output_file)

        logging.info("C/E/xxx done with " + report_performance_bytes(start_piping, bytes_written))
        self.all_bytes_written += bytes_written

        os.remove(input_file)

        hash_process = subprocess.Popen(
            [MD5SUM, output_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        hash_stdout, hash_stderr = hash_process.communicate()
        
        if hash_process.returncode != 0:
            logging.warning(f"MD5 hash calculation failed: {hash_stderr}")
            return "None", "-"
        
        hash_out = hash_stdout.decode('utf-8').strip().split()[0]
        
        return "md5sum", hash_out

    def parse_mbuffer_progress_log(self, mbuffer_log: str) -> (int, int):
        # mbuffer: in @  164 MiB/s, out @  164 MiB/s, 3102 MiB total, buffer  99% full
        # summary: 5119 MiByte in 37.0sec - average of  138 MiB/s
        try:
            with open(mbuffer_log, "r") as f:
                lines = f.readlines()

            if len(lines) <= 0:
                return -1, -1

            for line in reversed(lines):
                if "buffer" in line:
                    s = re.search(
                        ", +([\\d.]+) +MiB total, buffer +([\\d.]+)% full", line, re.IGNORECASE
                    )
                    if s:
                        bytes_written = int(float(s.group(1)) * 1024 * 1024)
                        buffer_percent = int(s.group(2))
                        return bytes_written, buffer_percent
                    s = re.search(
                        ", +([\\d.]+) +GiB total, buffer +([\\d.]+)% full", line, re.IGNORECASE
                    )
                    if s:
                        bytes_written = int(float(s.group(1)) * 1024 * 1024 * 1024)
                        buffer_percent = int(s.group(2))
                        return bytes_written, buffer_percent
        except:
            pass

        return -1, -1

    def get_file_size(self, file) -> (int, int):
        """
        Mimics the output of parse_mbuffer_progress_log
        """
        return get_safe_file_size(file), -1

    def parse_mbuffer_md5(self, mbuffer_log: str) -> (str):
        # MD5 hash: 289067bcd5472f102e946f8b71c7729b
        try:
            with open(mbuffer_log, "r") as f:
                lines = f.readlines()

                for line in lines:
                    if line.startswith("MD5 hash:"):
                        return line.replace("MD5 hash:", "").strip()
        except:
            pass

        return None

    # def parse_mbuffer_summary_log(self, mbuffer_log: str) -> (int, int):
    #     # mbuffer: in @  164 MiB/s, out @  164 MiB/s, 3102 MiB total, buffer  99% full
    #     # summary: 5119 MiByte in 37.0sec - average of  138 MiB/s
    #     try:
    #         with open(mbuffer_log, "r") as f:
    #             temp = f.readlines()
    #
    #         if len(temp) <= 0:
    #             return -1, -1
    #
    #         last_line = temp[-1]
    #
    #         if "summary" in last_line:
    #             s = re.search(
    #                 "summary: +(\\d+) +MiByte in", last_line, re.IGNORECASE
    #             )
    #             if s:
    #                 bytes_written = int(s.group(1)) * 1000 * 1000
    #                 return bytes_written, -1
    #     except:
    #         pass
    #
    #     return -1, -1

    def overall_compression_ratio(self) -> float:
        return self.all_bytes_read / float(self.all_bytes_written)

