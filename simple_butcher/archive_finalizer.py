import zmq
import time

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
