# Creating Two Abstract Base Classes

# Base Mapper: defines mapping interface
# Base Reducer: defines reducing interface

# base.py

from abc import ABC, abstractmethod
from threading import Lock
from typing import Any, List, Tuple, Optional
