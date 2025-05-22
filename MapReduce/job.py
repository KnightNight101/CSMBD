# mapreduce/job.py

from mapreduce.mapper_passenger import PassengerFlightMapper
from mapreduce.reducer_passenger import PassengerFlightReducer
from threading import Thread, Lock
from collections import defaultdict
from typing import List, Dict, Any, Optional


class MapReduceJob:
    """
    Orchestrates a full MapReduce pipeline:
    - Splits input into chunks
    - Runs Mappers in threads
    - Shuffles outputs (group-by-key)
    - Runs Reducers in threads
    - Returns sorted result
    """

    def __init__(self, data: List[str], num_threads: int = 4):
        self.data = data
        self.num_threads = num_threads
        self.mapper_outputs = []
        self.reducer_outputs = []
        self.lock = Lock()

    def split_data(self) -> List[List[str]]:
        """Split input data into equal chunks for threading."""
        chunk_size = max(1, len(self.data) // self.num_threads)
        return [self.data[i:i + chunk_size] for i in range(0, len(self.data), chunk_size)]

    def run_mappers(self) -> None:
        """Run all Mapper threads."""
        chunks = self.split_data()
        threads = []

        for chunk in chunks:
            mapper = PassengerFlightMapper(chunk, lock=self.lock)

            def run_mapper(m=mapper):
                m.map()
                with self.lock:
                    self.mapper_outputs.extend(m.get_output())

            thread = Thread(target=run_mapper)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def shuffle(self) -> Dict[Any, List[Any]]:
        """Group Mapper outputs by key."""
        grouped = defaultdict(list)
        for key, value in self.mapper_outputs:
            grouped[key].append(value)
        return grouped

    def run_reducers(self, grouped_data: Dict[Any, List[Any]]) -> None:
        """Run all Reducer threads."""
        threads = []

        for key, values in grouped_data.items():
            reducer = PassengerFlightReducer(key, values, lock=self.lock)

            def run_reducer(r=reducer):
                r.reduce()
                with self.lock:
                    self.reducer_outputs.append(r.get_result())

            thread = Thread(target=run_reducer)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def run(self, top_n: Optional[int] = None) -> List:
        """
        Execute the full pipeline and return sorted results.
        :param top_n: Optional. Limit the number of returned top records.
        """
        self.run_mappers()
        grouped = self.shuffle()
        self.run_reducers(grouped)

        results = sorted(self.reducer_outputs, key=lambda x: x[1], reverse=True)
        return results[:top_n] if top_n else results
