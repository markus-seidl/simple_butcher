import os
import time

import zmq


class MyZmq:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind("tcp://127.0.0.1:5555")

    def wait_for_signal_blocking(self):
        self.socket.recv()

    def wait_for_signal(self) -> bool:
        try:
            msg = self.socket.recv(flags=zmq.DONTWAIT)

            if msg is not None:
                return True
        except zmq.error.Again:
            time.sleep(0.1)  # Save CPU as we don't have anything to do then wait
            return False

    def __del__(self):
        self.socket.close()
        self.context.term()


class SimpleMq:
    def __init__(self, communication_file: str):
        self.communication_file = communication_file

    def cleanup(self):
        if os.path.exists(self.communication_file):
            os.remove(self.communication_file)

    def wait_for_signal(self) -> bool:
        if os.path.exists(self.communication_file):
            return True

        time.sleep(0.1)
        return False

    def signal_tar_to_continue(self):
        os.remove(self.communication_file)
