import os
import sys
import threading
import time
import shutil

import zmq
import subprocess

TEMP_DIR = "./temp2"
SOURCE_DIR = "./temp"
DEST_DIR = "./dest"
DATABASE_FILE = "backup.txt"

TAR = "/usr/local/bin/gtar"
SEVEN_Z = "/usr/local/bin/7z"
SEVEN_Z_OPTS = ' a -m0=brotli -mmt=9 -p"dasisteintest" '
TAR_BACKUP_FULL_OPTS = 'cvM -L10M --new-volume-script="python archive_finalizer.py" --label="into-the-unknown" '


#  -g, --listed-incremental=FILE

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


def do_message(com, msg, tar_output_file, archive_volume):
    print("\tArchive is ready...")

    im_file = TEMP_DIR + "/files.tar.%09i" % archive_volume[1]

    shutil.move(tar_output_file, im_file)

    com.socket.send(b"CONTINUE")

    # Compression Command
    compression_timer_start = time.time()
    output_file = DEST_DIR + "/%09i.7zenc" % archive_volume[1]
    compression_cmd = SEVEN_Z + SEVEN_Z_OPTS + output_file + " " + im_file
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

    compression_process.wait()
    print("\tCompression took: %3.1fs" % (time.time() - compression_timer_start))

    os.remove(im_file)

    # Determine if next tape is necessary

    # Put on tape
    print("Write to Tape")

    # Determine if next tape is necessary
    if False:
        return archive_volume[0] + 1, archive_volume[1] + 1
    else:
        return archive_volume[0], archive_volume[1] + 1


def backup():
    tar_output_file = TEMP_DIR + "/tar_output"
    tar_cmd = TAR + " " + TAR_BACKUP_FULL_OPTS + " -f " + tar_output_file + " " + SOURCE_DIR
    tar_process = subprocess.Popen(tar_cmd, shell=True)

    tar_thread = threading.Thread(target=wait_for_process_finish, args=(tar_process,))
    tar_thread.start()

    com = setup_mq_server()

    archive_volume = (0, 0)  # (tape, volume)
    while tar_thread.is_alive():
        try:
            msg = com.socket.recv(flags=zmq.DONTWAIT)

            archive_volume = do_message(com, msg, tar_output_file, archive_volume)
        except zmq.error.Again:
            # No msg available
            time.sleep(1)  # Save CPU as we don't have anything to do then wait

    print("Tar process has ended.")


if __name__ == '__main__':
    backup()
