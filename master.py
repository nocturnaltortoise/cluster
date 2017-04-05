from functools import reduce
from worker import Worker


class Master:
    def __init__(self, num_workers):
        self.workers = [Worker() for i in range(num_workers)]
        self.input_data = [x for x in range(1000)]

    def split_between_workers(self):
        data_size = len(self.input_data)
        num_workers = len(self.workers)

        data_chunks = Master.chunk_data(self.input_data, num_workers, [])

        for i, worker in enumerate(self.workers):
            worker.worker_data = data_chunks[i]

    @staticmethod
    def chunk_data(data_to_chunk, num_workers, data_chunks):
        data_size = len(data_to_chunk)
        remainder = data_size % num_workers

        if data_size <= num_workers:
            chunk_size = data_size
            data_chunks[0] += data_to_chunk
            return data_chunks
        else:
            chunk_size = int(data_size / num_workers)

        data_size = chunk_size * num_workers

        for i in range(0, data_size, chunk_size):
            chunk = data_to_chunk[i:i+chunk_size]
            data_chunks.append(chunk)

        if remainder == 0:
            return data_chunks
        else:
            remaining_data = data_to_chunk[chunk_size * num_workers:]
            return Master.chunk_data(remaining_data, num_workers, data_chunks)

    def map_reduce(self):
        reduced_data = []
        for worker in self.workers:
            worker.map_func = lambda x: x * 5
            worker.reduce_func = lambda x,y: x if x > y else y

            worker.worker_data = worker.apply_map()
            worker.worker_data = [worker.apply_reduce()]
            reduced_data.append(worker.worker_data)

        if len(reduced_data) > 1:
            available_worker = self.workers[0]
            available_worker.worker_data = reduced_data
            available_worker.worker_data = available_worker.apply_reduce()
            # this isn't necessary but leaves the worker in a "finished" state
            return available_worker.worker_data
        else:
            return reduced_data


master = Master(7)
master.split_between_workers()
print(master.map_reduce())
for worker in master.workers:
    print(worker)
