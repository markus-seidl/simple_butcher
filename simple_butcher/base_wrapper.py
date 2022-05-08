import subprocess


class Wrapper:
    def __init__(self):
        pass

    @staticmethod
    def wait_for_process_finish(process: subprocess.Popen):
        s_out, s_err = process.communicate()

        if process.returncode != 0:
            raise OSError(s_err)
