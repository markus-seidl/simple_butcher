import sys

if sys.platform == "linux":
    TAR = "/usr/bin/tar"
    SEVEN_Z = "/usr/bin/7z"
    MBUFFER = "/usr/bin/mbuffer"
    TAPEINFO = "/usr/sbin/tapeinfo -f {tape}"
    ZSTD = "/usr/bin/zstd"
    CRYPT_CMD = SEVEN_Z
    SHA256SUM = "/usr/bin/sha256sum"
elif sys.platform == "darwin":
    TAR = "/usr/local/bin/gtar"
    SEVEN_Z = "/usr/local/bin/7z"
    ZSTD = "/usr/local/bin/zstd"
    CRYPT_CMD = SEVEN_Z
    MBUFFER = None
    TAPEINFO = None
    SHA256SUM = "/usr/local/bin/sha256sum"
