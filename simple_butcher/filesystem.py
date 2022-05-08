import os


class Filesystem:
    """
    unused
    """
    def __init__(self):
        pass


def walk(level: int, max_level: int, directory: str) -> int:
    ret = 0
    if max_level != -1 and level > max_level:
        return ret

    try:
        for name in os.listdir(directory):
            joined = os.path.join(directory, name)
            if os.path.isdir(joined):
                ret += walk(level + 1, max_level, joined)
            else:
                ret += 1
    except PermissionError:
        pass
    return ret


if __name__ == '__main__':
    print(walk(0, 5, "/Users/seidlm/"))
