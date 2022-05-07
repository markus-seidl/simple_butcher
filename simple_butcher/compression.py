import logging
import os
import subprocess
import time

from base_wrapper import Wrapper
from config import BackupConfig
from common import ArchiveVolumeNumber, report_performance

from config import BackupConfig
from common import ArchiveVolumeNumber, file_size_format
from database import BackupRecord
from exe_paths import ZSTD, SEVEN_Z
# from tqdm import tqdm

# COMPRESS_ZSTD_OPTS_PIPE = ' -3 -T0 -v %s --stdout '
# CRYPT_OPTS_PIPE = ' --batch --yes --passphrase "%s" --symmetric --cipher-algo AES256 --compress-algo none -o %s '
# CRYPT_OPTS_PIPE = ' a -si -mx=0 -p"%s" %s '  # 7z in store only mode, use only encryption module
# CRYPT_OPTS = ' aes-256-cbc -iter 100000 -pass pass:"%s" -in %s -out %s '
# CRYPT_OPTS = ' --batch --yes --passphrase "%s" --symmetric --cipher-algo AES256 --compress-algo none -o %s %s '
# CRYPT_OPTS = ' a -mx=0 -p"%s" %s %s '

COMPRESSION_PIPE_CMD = '{zstd} -3 -T0 -v {in_file} --stdout | {seven_z} a -si -mx=0 -p"{password}" {out_file}'


class ZstdPipe(Wrapper):
    def __init__(self):
        super().__init__()

    def do(self, config: BackupConfig, archive_volume_no: ArchiveVolumeNumber, input_file: str):
        output_file = config.ramdisk + "/%09i.tar.zst.7z" % archive_volume_no.volume_no

        original_size = os.path.getsize(input_file)

        # with create_tqdm(
        #     total=original_size,  # we assume that it can only reach to the original size which is mostly true
        #     unit_divisor=1024,
        #     unit_scale=True,
        #     unit="bytes",
        #     desc="Compressing / Encrypting"
        # ) as pbar:
        if os.path.exists(output_file):
            # 7z will add to the output file if it already exists, make sure that isn't the case
            os.remove(output_file)

        compression_cmd = COMPRESSION_PIPE_CMD.format(
            zstd=ZSTD,
            seven_z=SEVEN_Z,
            in_file=input_file,
            password=config.password,
            out_file=output_file
        )

        start_time = time.time()
        logging.debug(f"compression cmd: {compression_cmd.replace(config.password, '<password>')}")
        compression_process = subprocess.Popen(
            compression_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        while True:
            if os.path.exists(output_file):
                logging.info(f"Compressing + Encrypting... {file_size_format(os.path.getsize(output_file))}")
                # pbar.update(os.path.getsize(output_file) - pbar.n)
                time.sleep(2)

            if compression_process.poll() is not None:
                break

        s_out, s_err = compression_process.communicate()

        if compression_process.returncode != 0:
            raise OSError(s_err.decode("UTF-8"))

        logging.info(f"Compressing + Encrypting done for {report_performance(start_time, output_file)}")

        output_size = os.path.getsize(output_file)
        compression_ratio_str = f"%.2f" % (output_size / original_size * 100)
        logging.info(f"Compression ratio: {compression_ratio_str}%")

        os.remove(input_file)

        return output_file
