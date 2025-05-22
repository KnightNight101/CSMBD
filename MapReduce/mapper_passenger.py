# mapreduce/mapper_passenger.py

from mapreduce.base import BaseMapper
from typing import List
import csv
from io import StringIO

class PassengerFlightMapper(BaseMapper):
    """
    Concrete Mapper to emit (PassengerID, 1) from each valid CSV row.
    Inherits from BaseMapper.
    """

    def map(self) -> None:
        for line in self.chunk:
            line = line.strip()
            if not line:
                continue  # skip blank lines

            try:
                reader = csv.reader(StringIO(line))
                row = next(reader)

                # Expected format: PassengerID, DateTime, Origin, Dest, FlightNo
                if len(row) < 1:
                    continue

                passenger_id = row[0].strip()

                if passenger_id:
                    self.emit(passenger_id, 1)

            except Exception as e:
                print(f"[Mapper Error] {e} in line: {line}")
