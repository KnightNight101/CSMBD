# Sanity test and establishing the main python file
print("hello world")

# main.py

from mapreduce.job import MapReduceJob
import os

def load_csv_lines(file_path: str, skip_header: bool = True) -> list:
    """
    Loads CSV lines from the provided file path.
    Optionally skips the header.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    if skip_header:
        return lines[1:]
    return lines

def main():
    # Use the correct dataset
    data_path = os.path.join("Data", "AComp_Passenger_data_no_error_DateTime.csv")

    print(f"\nLoading data from: {data_path}")
    lines = load_csv_lines(data_path)

    print(f"Total records (excluding header): {len(lines)}")

    # Run the MapReduce job
    job = MapReduceJob(data=lines, num_threads=4)
    results = job.run(top_n=10)

    print("\nTop Passengers by Flight Count:")
    for rank, (passenger, count) in enumerate(results, start=1):
        print(f"{rank:>2}. {passenger}: {count} flights")

        # Write output to a text file
    output_path = "passenger_flight_counts.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("Top Passengers by Flight Count:\n")
        for rank, (passenger, count) in enumerate(results, start=1):
            f.write(f"{rank:>2}. {passenger}: {count} flights\n")

    print(f"\nOutput written to: {output_path}")


if __name__ == "__main__":
    main()

