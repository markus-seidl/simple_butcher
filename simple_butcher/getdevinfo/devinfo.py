import os
import subprocess
import sys
import typing


def _parse_mounts() -> typing.List:
    try:
        cmd = subprocess.run(["mount"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
        stdout = cmd.stdout
    except (OSError, subprocess.CalledProcessError) as err:
        raise IOError("Exception: " + str(err) + " while running mount")

    ret = []
    for line in stdout.decode("utf-8").split("\n"):
        if " on " in line:
            parts = line.split(" on ")
            device = parts[0]
            mountpoint = parts[1].split(" ")[0]
            ret.append((device, mountpoint))

    return ret


def get_serial_for_dir(directory: str) -> str:
    directory = os.path.abspath(directory)

    if sys.platform == "darwin":
        mp = _parse_mounts()

        found_mp = None
        for device, mountpoint in mp:
            if directory.startswith(mountpoint):
                found_mp = device

        return darwin_find_serial(found_mp)

    if sys.platform == "linux":
        return linux_get_uuid_for_dir(directory)
    else:
        raise RuntimeError("Unsupported platform: " + sys.platform)


def darwin_find_serial(disk) -> str:
    import plistlib
    # Run diskutil info to get disk info.
    try:
        cmd = subprocess.run(["diskutil", "info", "-plist", disk], stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, check=True)
        stdout = cmd.stdout
        plist = plistlib.loads(stdout)
    except (OSError, subprocess.CalledProcessError) as err:
        raise IOError("Exception: " + str(err) + " while running diskutil info")

    return plist['VolumeUUID'] if 'VolumeUUID' in plist else None


def linux_get_uuid_for_dir(directory: str) -> str:
    # The partition UUID is unique enough, so we can re-identify the partition when restoring the backup (or give the users hints, where
    # to find the backup and on which disk)
    result = subprocess.run(
        ["findmnt", "-n", "-o", "UUID", "--target", directory],
        check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


if __name__ == "__main__":
    print(get_serial_for_dir(sys.argv[1]))
