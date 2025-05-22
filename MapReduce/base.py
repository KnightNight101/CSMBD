# Creating Two Abstract Base Classes

# Base Mapper: defines mapping interface
# Base Reducer: defines reducing interface

# base.py

from abc import ABC, abstractmethod
from threading import Lock
from typing import Any, List, Tuple, Optional

class BaseMapper(ABC):
    """
    Abstract base class for all mappers.
    All mapper subclasses must implement the map() method.
    """

    def __init__(self, chunk: List[str], lock: Optional[Lock] = None):
        self.chunk = chunk                  # A list of lines (rows of CSV)
        self.output = []                    # Stores emitted (key, value) pairs
        self.lock = lock                    # Optional threading lock (if needed)

    @abstractmethod
    def map(self) -> None:
        """
        Processes the assigned chunk and emits intermediate key-value pairs.
        Must be implemented by any subclass.
        """
        pass

    def emit(self, key: Any, value: Any) -> None:
        """
        Safely appends a key-value pair to the output list.
        This method can be overridden if threading safety is required.
        """
        if self.lock:
            with self.lock:
                self.output.append((key, value))
        else:
            self.output.append((key, value))

    def get_output(self) -> List[Tuple[Any, Any]]:
        return self.output