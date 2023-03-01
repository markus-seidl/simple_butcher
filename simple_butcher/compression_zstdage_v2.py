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
from progressbar import ProgressDisplay, ByteTask


class ZstdAgeV2(Compression):
    """
    This class compresses, encrypts and writes to tape with zstd, age and mbuffer.
    Additionally, md5 is also computed.
    """

    def __init__(self, pd: ProgressDisplay):
        super().__init__()
        self.all_bytes_read = 0
        self.all_bytes_written = 0
        self.pd = pd

    def do(self, config: BackupConfig, archive_volume_no: ArchiveVolumeNumber, input_file: str) -> (str, str):
        output_file = config.tempdir + "/%09i.tar.zst.age" % archive_volume_no.volume_no

        original_size = os.path.getsize(input_file)
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
            [AGE, "-e", "-i", config.password_file], stdin=zstd_process.stdout, stdout=subprocess.PIPE
        )

        if config.tape_dummy is not None:
            # output_process = subprocess.Popen(
            #     f" > {output_file}", shell=True, stdin=age_process.stdout, stdout=subprocess.PIPE,
            #     stderr=subprocess.PIPE
            # )
            # the above method doesn't work on newer macos/python, the method below seems to be slower.
            output_process = subprocess.Popen(
                ["/bin/dd", "bs=512K", f"of={output_file}"], stdin=age_process.stdout, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
        else:
            output_process = subprocess.Popen(
                [
                    MBUFFER, "-P", "90", "-l", mbuffer_log, "-q", "-m", "5G", "-o", config.tape, "-s",
                    "512k", "--md5", "--tapeaware"
                ],
                stdin=age_process.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

        start_piping = time.time()

        with self.pd.create_byte_bar(
                "C/E", total_bytes=original_size, postfix=f"archive_no={archive_volume_no.volume_no}"
        ) as p:
            while True:
                if config.tape_dummy is not None:
                    bytes_written, _ = self.get_file_size(output_file)
                else:
                    bytes_written, _ = self.parse_mbuffer_progress_log(mbuffer_log)

                p.update(completed=bytes_written)
                time.sleep(0.1)

                if output_process.poll() is not None:
                    break

        output_stdout, output_stderr = output_process.communicate()

        if output_process.returncode != 0:
            raise OSError(output_stderr)

        bytes_written, _ = self.parse_mbuffer_progress_log(mbuffer_log)
        # logging.info("C/E/xxx done with " + report_performance_bytes(start_piping, bytes_written))
        self.all_bytes_written += bytes_written

        os.remove(input_file)

        hash_out = self.parse_mbuffer_md5(mbuffer_log)
        if hash_out is None:
            return "None", "-"

        return "md5sum", hash_out.replace(" *-", "")

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
        if os.path.exists(file):
            return os.path.getsize(file), -1
        else:
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


if __name__ == '__main__':
    print(ZstdAgeV2(None).parse_mbuffer_summary_log("../mbuffer.log"))
    # config = BackupConfig(
    #     backup_repository="",
    #     backup_name="",
    #     description="",
    #     compression="",
    #     source="",
    #     password_file="../password.age",
    #     tape_buffer=0,
    #     tempdir="../temp/",
    #     tape="",
    #     tape_dummy="../temp/blah",
    #     chunk_size=0,
    #     incremental_time=0,
    #     excludes=None
    # )
    # # config: BackupConfig, archive_volume_no: ArchiveVolumeNumber, input_file: str
    # ZstdAgeV2().do(
    #     config,
    #     archive_volume_no=ArchiveVolumeNumber(
    #         tape_no=0, volume_no=0, block_position=0, bytes_written=0
    #     ),
    #     input_file="../temp_src/blah1"
    # )
