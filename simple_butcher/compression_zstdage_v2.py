import logging
import os
import subprocess
import time
import re

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber, report_performance

from config import BackupConfig
from common import ArchiveVolumeNumber, file_size_format
from database import BackupRecord
from exe_paths import ZSTD, AGE, TEE, MBUFFER, SHA256SUM
from compression import Compression

# PIPE_CMD_TAPE = '{zstd_exe} -3 -T0 {in_file} --stdout | ' \
#                 '{age_exe} -e -i {age_key} | ' \
#                 '{tee_exe} ' \
#                 ' >( {mbuffer_exe} -P 90 -l {mbuffer_logfile} -q -o {tape} -s {tape_blocksize} )' \
#                 ' >( {sha_exe} -b > {hash_output} ) ' \
#                 ' > /dev/null '

# PIPE_CMD_DUMMY = '{zstd_exe} -3 -T0 {in_file} --stdout | ' \
#                  '{age_exe} -e -i {age_key} | ' \
#                  '{tee_exe} ' \
#                  ' >( {sha_exe} -b > {hash_output} ) ' \
#                  ' > {output_file} '

# zstd -3 -T0 "$1" --stdout | age -e -i "$2" | tee  >( md5sum -b > "$3" ) > "$4"
PIPE_CMD_DUMMY = './simple_butcher/zstdage_v2_dummy_helper.sh "{in_file}" "{age_key}" "{hash_output}" "{output_file}"'

# zstd -4 -T0 "$1" --stdout | age -e -i "$2" | tee >( mbuffer -P 90 -l "$3" -q -o "$4" -m 5G -s "512k" ) >( md5sum -b > "$5" ) > /dev/null
PIPE_CMD_TAPE = './simple_butcher/zstdage_v2_tape_helper.sh "{in_file}" "{age_key}" "{mbuffer_logfile}" "{tape}" "{hash_output}"'


class ZstdAgeV2(Compression):
    """
    This class compresses, encrypts and writes to tape with zstd, age and mbuffer in a single pipe command.
    Additionally, sha256 is also computed.
    """

    def __init__(self):
        super().__init__()
        self.all_bytes_read = 0
        self.all_bytes_written = 0

    def do(self, config: BackupConfig, archive_volume_no: ArchiveVolumeNumber, input_file: str) -> (str, str):
        output_file = config.ramdisk + "/%09i.tar.zst.age" % archive_volume_no.volume_no

        original_size = os.path.getsize(input_file)
        self.all_bytes_read += original_size

        if os.path.exists(output_file):
            os.remove(output_file)

        all_cmd = PIPE_CMD_TAPE
        if config.tape_dummy is not None:
            all_cmd = PIPE_CMD_DUMMY

        # Has problems with pipes...
        mbuffer_log = config.tempdir + "/mbuffer.log"
        hash_output = config.tempdir + "/hash.log"
        all_cmd = all_cmd.format(
            # zstd_exe=ZSTD,
            # age_exe=AGE,
            # tee_exe=TEE,
            # mbuffer_exe=MBUFFER,
            # sha_exe=SHA256SUM,
            # blocksize="512K",
            in_file=input_file,
            age_key=config.password_file,
            mbuffer_logfile=mbuffer_log,
            tape=config.tape,
            hash_output=hash_output,
            output_file=output_file
        )

        start_time = time.time()
        logging.debug(f"all cmd: {all_cmd}")
        all_process = subprocess.Popen(
            all_cmd, shell=True
        )

        while True:
            if os.path.exists(output_file):  # DUMMY Mode - no tape
                logging.info(f"C/E/SHA ... {file_size_format(os.path.getsize(output_file))}")

            if os.path.exists(mbuffer_log):  # tape mode
                bytes_written, buffer_percent = self.parse_mbuffer_progress_log(mbuffer_log)
                done_percent = "%03.0f" % ((bytes_written / float(original_size)) * 100.0)
                logging.info(f"C/E/M/SHA ... {file_size_format(bytes_written)} - {done_percent} done")

            time.sleep(1)

            if all_process.poll() is not None:
                break

        s_out, s_err = all_process.communicate()

        if all_process.returncode != 0:
            raise OSError(s_err.decode("UTF-8"))

        if os.path.exists(mbuffer_log):  # tape mode
            elapsed_time = time.time() - start_time
            elapsed_time_str = "%.0f" % elapsed_time
            total_bytes_written = self.parse_mbuffer_summary_log(mbuffer_log)
            self.all_bytes_written += total_bytes_written
            logging.info(
                f"C/E/SHA done for {file_size_format(total_bytes_written)} in {elapsed_time_str} "
                f" with {file_size_format(total_bytes_written / elapsed_time)}/s"
            )

            compression_ratio_str = f"%.2f" % (total_bytes_written / original_size * 100)
            logging.info(f"Compression ratio: {compression_ratio_str}%")

        else:  # dummy mode
            logging.info(f"C/E/SHA done for {report_performance(start_time, output_file)}")
            output_size = os.path.getsize(output_file)
            self.all_bytes_written += output_size

            compression_ratio_str = f"%.2f" % (output_size / original_size * 100)
            logging.info(f"Compression ratio: {compression_ratio_str}%")

        os.remove(input_file)

        with open(hash_output, "r") as f:
            backup_hash = f.readlines()[0]
            return "md5sum", backup_hash

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
