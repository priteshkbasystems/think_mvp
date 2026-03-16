from concurrent.futures import ThreadPoolExecutor


class ParallelExecutor:

    def __init__(self, workers=4):

        self.executor = ThreadPoolExecutor(max_workers=workers)

    def run(self, func, items):

        futures = []

        for item in items:

            futures.append(self.executor.submit(func, item))

        results = []

        for f in futures:
            results.append(f.result())

        return results