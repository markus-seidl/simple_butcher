import zmq

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("tcp://127.0.0.1:5555")

socket.send(b"PART_DONE")
socket.recv()

socket.close()
context.term()
