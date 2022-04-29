import os
import sys
import threading
import time
import shutil
import datetime

import zmq
import subprocess

TEMP_DIR = "./temp"
TAPE = "/dev/nst0"
DATABASE_FILE = "backup_db.txt"
VOLUME_NAME = datetime.datetime.now().isoformat()
BLOCKSIZE = "512K"

OPENSSL = "/usr/bin/openssl"
OPENSSL_OPTS = ' aes-256-cbc -iter 100000 -pass pass:"%s" -in %s -out %s '

if sys.platform == "linux":
    TAR = "/usr/bin/tar"
    SEVEN_Z = "/usr/bin/7z"
    MBUFFER = "/usr/bin/mbuffer"
    TAPEINFO = "/usr/sbin/tapeinfo -f " + TAPE
    ZSTD = "/usr/bin/zstd"
elif sys.platform == "darwin":
    TAR = "/usr/local/bin/gtar"
    SEVEN_Z = "/usr/local/bin/7z"
    ZSTD = "/usr/local/bin/zstd"
    MBUFFER = None
    TAPEINFO = None

COMPRESS_SEVEN_Z_OPTS = ' a -p"%s" '
COMPRESS_ZSTD_OPTS = ' -4 -T0 -v %s -o %s '
COMPRESS_TAR_BACKUP_FULL_OPTS = f'cvM -L10G --new-volume-script="python archive_finalizer.py" --label="{VOLUME_NAME}" '
COMPRESS_WRITE_TO_TAPE_OPTS = " -i %s -P 90 -l ./mbuffer.log -o " + TAPE + "  -s " + BLOCKSIZE


#  -g, --listed-incremental=FILE


class BackupConfig:
    def __init__(self, password: str, src_dir: str):
        self.password = password
        self.src_dir = src_dir
        self.compression_type = "zstd"


class MyZmq:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://127.0.0.1:5555")

    def __del__(self):
        self.socket.close()
        self.context.term()


def setup_mq_server():
    return MyZmq()


def wait_for_process_finish(process: subprocess.Popen):
    process.communicate()
    process.wait()


def block_position():
    if not TAPEINFO:
        return -1

    tape_process = subprocess.Popen(
        TAPEINFO, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    s_out, s_err = tape_process.communicate()

    lines = s_out.decode("UTF-8").split(os.linesep)
    for line in lines:
        if "Block Position:" in line:
            return int(str(line).replace("Block Position:", ""))

    return -1


def do_message(bc: BackupConfig, com: MyZmq, msg, tar_output_file: str, archive_volume: (int, int)) -> (int, int):
    print("\tArchive is ready...")

    im_file = TEMP_DIR + "/files.tar.%09i" % archive_volume[1]

    shutil.move(tar_output_file, im_file)

    if com:
        com.socket.send(b"CONTINUE")

    update_database(archive_volume, im_file)

    # Compression Command
    compression_timer_start = time.time()
    output_file = compress_archive(bc, im_file, archive_volume)
    print(
        "\tCompression took: %3.1fs size diff %f" % (
            time.time() - compression_timer_start, os.path.getsize(im_file) - os.path.getsize(output_file))
    )

    os.remove(im_file)

    # Determine if next tape is necessary
    print("Block position (before writing): " + str(block_position()))

    # Put on tape
    if MBUFFER:
        print("Write to Tape")
        subprocess.check_call(
            MBUFFER + COMPRESS_WRITE_TO_TAPE_OPTS % output_file, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        os.remove(output_file)

    # Determine if next tape is necessary
    print("Block position (before writing): " + str(block_position()))

    if False:
        return archive_volume[0] + 1, archive_volume[1] + 1
    else:
        return archive_volume[0], archive_volume[1] + 1


def compress_archive(bc, im_file, archive_volume):
    if bc.compression_type == "7z":
        output_file = TEMP_DIR + "/%09i.7zenc" % archive_volume[1]

        compression_cmd = SEVEN_Z + (COMPRESS_SEVEN_Z_OPTS % bc.password) + output_file + " " + im_file
        compression_process = subprocess.Popen(
            compression_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        s_out, s_err = compression_process.communicate()
        if compression_process.returncode != 0:
            raise OSError(s_err.decode("UTF-8"))

        return output_file
    elif bc.compression_type == "zstd":
        output_file = TEMP_DIR + "/%09i.tar.zstd" % archive_volume[1]

        compression_cmd = ZSTD + (COMPRESS_ZSTD_OPTS % (im_file, output_file))
        compression_process = subprocess.Popen(
            compression_cmd, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        s_out, s_err = compression_process.communicate()
        if compression_process.returncode != 0:
            raise OSError(s_err.decode("UTF-8"))

        im_file2 = output_file
        output_file = TEMP_DIR + "/%09i.tar.zstd.enc" % archive_volume[1]

        encryption_cmd = OPENSSL + (OPENSSL_OPTS % (bc.password, im_file2, output_file))
        encryption_process = subprocess.Popen(
            encryption_cmd, shell=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        s_out, s_err = encryption_process.communicate()
        if encryption_process.returncode != 0:
            raise OSError(s_err.decode("UTF-8"))

        return output_file

    raise ValueError("Unknown compression_type")


def update_database(archive_volume, im_file):
    """Writes contents of im_file to the backup_db.txt using archive_volume information"""
    with open(DATABASE_FILE, "a+") as f:
        list_process = subprocess.Popen(
            TAR + " tvf " + im_file, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        # Ignore any errors, as this archive is mostly not complete. But that's expected.
        s_out, s_err = list_process.communicate()

        lines = s_out.decode("UTF-8").split(os.linesep)
        for line in lines:
            if line == '':
                continue
            f.write("%09i\t%09i\t%s" % (archive_volume[0], archive_volume[1], line))
            f.write(os.linesep)


def backup(bc: BackupConfig):
    com = setup_mq_server()

    with open(DATABASE_FILE, "a+") as f:
        f.write(os.linesep)
        f.write("Backup of " + VOLUME_NAME)
        f.write(os.linesep)
        f.write(os.linesep)

    tar_output_file = TEMP_DIR + "/tar_output"
    tar_cmd = TAR + " " + COMPRESS_TAR_BACKUP_FULL_OPTS + " -f " + tar_output_file + " " + bc.src_dir
    tar_process = subprocess.Popen(tar_cmd, shell=True)

    tar_thread = threading.Thread(target=wait_for_process_finish, args=(tar_process,))
    tar_thread.start()

    archive_volume = (0, 0)  # (tape, volume)
    while tar_thread.is_alive():
        try:
            msg = com.socket.recv(flags=zmq.DONTWAIT)

            archive_volume = do_message(bc, com, msg, tar_output_file, archive_volume)
        except zmq.error.Again:
            # No msg available
            time.sleep(1)  # Save CPU as we don't have anything to do then wait

    if os.path.exists(tar_output_file):
        # backup also last output file
        do_message(bc, None, None, tar_output_file, archive_volume)

    print("Tar process has ended.")


if __name__ == '__main__':
    cmd = sys.argv[1]

    if "backup" == cmd:
        bc = BackupConfig(sys.argv[2], sys.argv[3])
        backup(bc)
    elif "block_position" == cmd:
        print(block_position())
