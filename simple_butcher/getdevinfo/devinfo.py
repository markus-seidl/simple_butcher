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
    if sys.platform == "darwin":
        mp = _parse_mounts()

        found_mp = None
        for device, mountpoint in mp:
            if directory.startswith(mountpoint):
                found_mp = device

        return darwin_find_serial(found_mp)

    # elif sys.platform == "linux":
    #     pass
    #  TODO
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
