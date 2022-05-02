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
