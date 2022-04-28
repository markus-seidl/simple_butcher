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

TAR = "/usr/local/bin/gtar"
SEVEN_Z = "/usr/local/bin/7z"

TAR = "/usr/bin/tar"
SEVEN_Z = "/usr/bin/7z"
MBUFFER = "/usr/bin/mbuffer"

COMPRESS_SEVEN_Z_OPTS = ' a -m0=brotli -mmt=9 -p"%s" '
COMPRESS_TAR_BACKUP_FULL_OPTS = f'cvM -L10M --new-volume-script="python archive_finalizer.py" --label="{VOLUME_NAME}" '
COMPRESS_WRITE_TO_TAPE_OPTS = MBUFFER + " -i %s -P 90 -l ./mbuffer.log -o " + TAPE + "  -s " + BLOCKSIZE

TAPEINFO = "/usr/sbin/tapeinfo -f " + TAPE


#  -g, --listed-incremental=FILE

class BackupConfig:
    def __init__(self, password: str, src_dir: str):
        self.password = password
        self.src_dir = src_dir


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
    tape_process = subprocess.Popen(
        TAPEINFO, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    s_out, s_err = tape_process.communicate()

    lines = s_out.decode("UTF-8").split(os.linesep)
    for line in lines:
        if "Block Position:" in line:
            return int(str(line).replace("Block Position:", ""))

    return 0


def do_message(bc: BackupConfig, com: MyZmq, msg, tar_output_file: str, archive_volume: (int, int)) -> (int, int):
    print("\tArchive is ready...")

    im_file = TEMP_DIR + "/files.tar.%09i" % archive_volume[1]

    shutil.move(tar_output_file, im_file)

    com.socket.send(b"CONTINUE")

    # Compression Command
    compression_timer_start = time.time()
    output_file = TEMP_DIR + "/%09i.7zenc" % archive_volume[1]
    compression_cmd = SEVEN_Z + (COMPRESS_SEVEN_Z_OPTS % bc.password) + output_file + " " + im_file
    compression_process = subprocess.Popen(compression_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Fill database
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

    s_out, s_err = compression_process.communicate()
    if compression_process.returncode != 0:
        raise OSError(s_err.decode("UTF-8"))

    print(
        "\tCompression took: %3.1fs size diff %f" % (
            time.time() - compression_timer_start, os.path.getsize(im_file) - os.path.getsize(output_file))
    )

    os.remove(im_file)

    # Determine if next tape is necessary
    print("Block position (before writing): " + str(block_position()))

    # Put on tape
    print("Write to Tape")
    subprocess.check_call(
        COMPRESS_WRITE_TO_TAPE_OPTS % output_file, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )

    # Determine if next tape is necessary
    print("Block position (before writing): " + str(block_position()))

    if False:
        return archive_volume[0] + 1, archive_volume[1] + 1
    else:
        return archive_volume[0], archive_volume[1] + 1


def backup(bc: BackupConfig):
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

    com = setup_mq_server()

    archive_volume = (0, 0)  # (tape, volume)
    while tar_thread.is_alive():
        try:
            msg = com.socket.recv(flags=zmq.DONTWAIT)

            archive_volume = do_message(bc, com, msg, tar_output_file, archive_volume)
        except zmq.error.Again:
            # No msg available
            time.sleep(1)  # Save CPU as we don't have anything to do then wait

    print("Tar process has ended.")


if __name__ == '__main__':
    cmd = sys.argv[1]

    if "backup" == cmd:
        bc = BackupConfig(sys.argv[2], sys.argv[3])
        backup(bc)
    elif "block_position" == cmd:
        print(block_position())
