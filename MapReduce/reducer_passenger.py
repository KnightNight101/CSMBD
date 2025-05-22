# mapreduce/reducer_passenger.py

from mapreduce.base import BaseReducer
from typing import Any, List

class PassengerFlightReducer(BaseReducer):
    """
    Reducer that aggregates the total number of flights per passenger.
    It expects a list of integers (e.g., [1, 1, 1]) and emits (PassengerID, total_count).
    """

    def reduce(self) -> None:
        try:
            self.result = sum(int(v) for v in self.values)
        except (TypeError, ValueError) as e:
            print(f"[Reducer Error] Invalid values for key={self.key}: {e}")
            self.result = 0  # fallback value for corrupted value list
