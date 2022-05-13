import time
import os
from dataclasses import dataclass


# from tqdm import tqdm
#
# def create_tqdm(iterable=None, desc=None, total=None, leave=True, file=None,
#                 mininterval=0.1, maxinterval=10.0, miniters=None,
#                 ascii=None, disable=False, unit='it', unit_scale=False,
#                 dynamic_ncols=False, smoothing=0.3, bar_format=None, initial=0,
#                 position=None, postfix=None, unit_divisor=1000, write_bytes=None,
#                 lock_args=None, nrows=None, colour=None, delay=0, gui=False,
#                 **kwargs):
#     return tqdm(ascii=True, ncols=100,
#                 iterable=iterable, desc=desc, total=total, leave=leave, file=file,
#                 mininterval=mininterval, maxinterval=maxinterval, miniters=miniters,
#                 disable=disable, unit=unit, unit_scale=unit_scale,
#                 dynamic_ncols=dynamic_ncols, smoothing=smoothing, bar_format=bar_format, initial=initial,
#                 position=position, postfix=postfix, unit_divisor=unit_divisor, write_bytes=write_bytes,
#                 lock_args=lock_args, nrows=nrows, colour=colour, delay=delay, gui=gui,
#                 **kwargs)


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
