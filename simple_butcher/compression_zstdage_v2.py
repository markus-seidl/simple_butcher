import logging
import os
import subprocess
import time
import re

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber, report_performance, report_performance_bytes

from config import BackupConfig
from common import ArchiveVolumeNumber, file_size_format
from database import BackupRecord
from exe_paths import ZSTD, AGE, TEE, MBUFFER, SHA256SUM, MD5SUM
from compression import Compression


class ZstdAgeV2(Compression):
    """
    This class compresses, encrypts and writes to tape with zstd, age and mbuffer in a single pipe command.
    Additionally, md5 is also computed.
    """

    def __init__(self):
        super().__init__()
        self.all_bytes_read = 0
        self.all_bytes_written = 0

    def do(self, config: BackupConfig, archive_volume_no: ArchiveVolumeNumber, input_file: str) -> (str, str):
        output_file = config.tempdir + "/%09i.tar.zst.age" % archive_volume_no.volume_no

        original_size = os.path.getsize(input_file)
        self.all_bytes_read += original_size

        if os.path.exists(output_file):
            os.remove(output_file)

        mbuffer_log = config.tempdir + "/mbuffer.log"
        # ---
        zstd_process = subprocess.Popen(
            [ZSTD, "-3", "-T0", input_file, "--stdout"], stdout=subprocess.PIPE
        )
        age_process = subprocess.Popen(
            [AGE, "-e", "-i", config.password_file], stdin=zstd_process.stdout, stdout=subprocess.PIPE
        )

        mbuffer_process = subprocess.Popen(
            [MBUFFER, "-P", "90", "-l", mbuffer_log, "-q", "-m", "5G", "-o", config.tape, "-s", "512k", "--md5"],
            stdin=age_process.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        start_piping = time.time()
        last_report_time = start_piping

        while True:
            bytes_written, _ = self.parse_mbuffer_progress_log(mbuffer_log)

            if time.time() - last_report_time >= 1 and bytes_written > 0:
                last_report_time = time.time()
                logging.info(f"C/E/xxx written {file_size_format(bytes_written)}")

            time.sleep(0.1)

            if mbuffer_process.poll() is not None:
                break

        mbuffer_stdout, mbuffer_stderr = mbuffer_process.communicate()

        if mbuffer_process.returncode != 0:
            raise OSError(mbuffer_stderr)

        logging.info("C/E/xxx done with " + report_performance_bytes(start_piping, bytes_written))
        self.all_bytes_written += bytes_written

        os.remove(input_file)

        hash_out = self.parse_mbuffer_md5(mbuffer_log)

        return "md5sum", hash_out.replace(" *-", "")

    def parse_mbuffer_progress_log(self, mbuffer_log: str) -> (int, int):
        # mbuffer: in @  164 MiB/s, out @  164 MiB/s, 3102 MiB total, buffer  99% full
        # summary: 5119 MiByte in 37.0sec - average of  138 MiB/s
        try:
            with open(mbuffer_log, "r") as f:
                temp = f.readlines()

            if len(temp) <= 0:
                return -1, -1

            last_line = temp[-1]

            if "buffer" in last_line:
                s = re.search(
                    ", +(\\d+) +MiB total, buffer +(\\d+)% full", last_line, re.IGNORECASE
                )
                if s:
                    bytes_written = int(s.group(1)) * 1000 * 1000
                    buffer_percent = int(s.group(2))
                    # done_percent = int(s.group(3))

                    return bytes_written, buffer_percent
        except:
            pass

        return -1, -1

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

    def parse_mbuffer_summary_log(self, mbuffer_log: str) -> int:
        # mbuffer: in @  164 MiB/s, out @  164 MiB/s, 3102 MiB total, buffer  99% full
        # summary: 5119 MiByte in 37.0sec - average of  138 MiB/s
        try:
            with open(mbuffer_log, "r") as f:
                temp = f.readlines()

            if len(temp) <= 0:
                return -1

            last_line = temp[-1]

            if "summary" in last_line:
                s = re.search(
                    "summary: +(\\d+) +MiByte in", last_line, re.IGNORECASE
                )
                if s:
                    bytes_written = int(s.group(1)) * 1000 * 1000
                    return bytes_written
        except:
            pass

        return -1

    def overall_compression_ratio(self) -> float:
        return self.all_bytes_read / float(self.all_bytes_written)


if __name__ == '__main__':
    ZstdAgeV2().parse_mbuffer_progress_log("/mnt/scratch/mbuffer.log")
