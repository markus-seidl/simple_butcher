import time
import os
from dataclasses import dataclass
from datetime import timedelta


def file_size_format(size):
    size = size / 1024.0 / 1024.0  # mb
    if size < 1024:
        return "%03.2f MB" % size
    size = size / 1024.0
    return "%03.2f GB" % size


def tape_performance(tape_start_time, tape_size) -> str:
    remaining_seconds = tape_size.remaining_bytes / (tape_size.written_bytes / (time.time() - tape_start_time))
    if remaining_seconds < 60 * 60 * 24:  # 1d
        remaining = timedelta(seconds=remaining_seconds)
        return "remaining=" + str(remaining)
    else:
        return "remaining>1d"


def compression_info(im_file_size, output_file_size):
    o_size = file_size_format(im_file_size)
    c_size = file_size_format(output_file_size)
    percent = "%2.0f%%" % ((output_file_size / im_file_size) * 100)
    return f"{o_size} -> {c_size} = {percent}"


def report_performance(start_time: float, file: str):
    file_size = get_safe_file_size(file)
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


def get_safe_file_size(filename: str) -> int:
    if os.path.exists(filename):
        return os.path.getsize(filename)
    return 0


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
