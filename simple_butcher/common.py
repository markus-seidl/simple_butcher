import time
import os
from dataclasses import dataclass


def file_size_format(size):
    size = size / 1024.0 / 1024.0  # mb
    if size < 1024:
        return "%03.2f MB" % size
    size = size / 1024.0
    return "%03.2f GB" % size


def compression_info(im_file_size, output_file_size):
    o_size = file_size_format(im_file_size)
    c_size = file_size_format(output_file_size)
    percent = "%2.0f%%" % ((output_file_size / im_file_size) * 100)
    return f"{o_size} -> {c_size} = {percent}"


def report_performance(start_time: float, file: str):
    file_size = os.path.getsize(file)
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time_str = "%.0f" % elapsed_time
    performance = file_size / elapsed_time
    return f"{file_size_format(file_size)} in {elapsed_time_str}s = {file_size_format(performance)}/s"


def report_performance_bytes(start_time: float, bytes: int) -> str:
    file_size = bytes
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time_str = "%.0f" % elapsed_time
    performance = file_size / elapsed_time
    return f"{file_size_format(file_size)} in {elapsed_time_str}s = {file_size_format(performance)}/s"


@dataclass
class ArchiveVolumeNumber:
    tape_no: int
    volume_no: int
    block_position: int
    bytes_written: int

    def incr_tape_no(self):
        self.tape_no += 1

    def incr_volume_no(self):
        self.volume_no += 1
