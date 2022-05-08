from config import BackupConfig
from common import ArchiveVolumeNumber


class Compression:
    def __init__(self):
        super().__init__()

    def do(self, config: BackupConfig, archive_volume_no: ArchiveVolumeNumber, input_file: str):
        return None

    def overall_compression_ratio(self) -> float:
        return 1.0
