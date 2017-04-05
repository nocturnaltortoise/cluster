import socket
from functools import reduce
from exceptions import NoMapDefinedException, NoReduceDefinedException


class Worker:
    def __init__(self, map_func=None, reduce_func=None):
        self.worker_data = []
        self.hostname = socket.gethostname()
        self.map_func = map_func
        self.reduce_func = reduce_func

    def __str__(self):
        return "Host: {0}, Data size: {1}".format(self.hostname,
                                                  len(self.worker_data))

    def apply_map(self):
        if self.map_func is None:
            raise NoMapDefinedException("No mapping function set on worker.")
        else:
            return map(self.map_func, self.worker_data)

    def apply_reduce(self):
        if self.reduce_func is None:
            raise NoReduceDefinedException("No reduce function set on worker.")
        else:
            return reduce(self.reduce_func, self.worker_data)
