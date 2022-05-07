import sys

if sys.platform == "linux":
    TAR = "/usr/bin/tar"
    SEVEN_Z = "/usr/bin/7z"
    MBUFFER = "/usr/bin/mbuffer"
    TAPEINFO = "/usr/sbin/tapeinfo"
    ZSTD = "/usr/bin/zstd"
    CRYPT_CMD = SEVEN_Z
    SHA256SUM = "/usr/bin/sha256sum"
    SG_LOGS = "/usr/bin/sg_logs"
    FIND = "/usr/bin/find"
    MT_ST = "/usr/bin/mt-st"
elif sys.platform == "darwin":
    TAR = "/usr/local/bin/gtar"
    SEVEN_Z = "/usr/local/bin/7z"
    ZSTD = "/usr/local/bin/zstd"
    CRYPT_CMD = SEVEN_Z
    MBUFFER = None
    TAPEINFO = None
    SG_LOGS = None
    SHA256SUM = "/usr/local/bin/sha256sum"
    FIND = "/usr/local/bin/gfind"
    MT_ST = None
