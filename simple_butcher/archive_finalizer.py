import time
import os
import json
import sys
from pathlib import Path


communication_file = sys.argv[1]

print(f"Writing to comminication file {communication_file}")
Path(communication_file).touch(exist_ok=False)

while os.path.exists(communication_file):
    time.sleep(0.5)

print(f"Communication file gone, continuing...")

if False:
    import zmq

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://127.0.0.1:5555")

    start_time = time.time()
    try:
        socket.send(b"PART_DONE")
    except zmq.error.ZMQError:
        pass

    while True:
        try:
            msg = socket.recv(flags=zmq.DONTWAIT)
            if msg is None:
                time.sleep(0.1)  # nothing yet
            else:
                break

            if time.time() - start_time > 5:
                socket.send(b"PART_DONE")
                start_time = time.time()
        except zmq.error.ZMQError:
            time.sleep(0.1)  # Save CPU as we don't have anything to do then wait

    socket.close()
    context.term()
