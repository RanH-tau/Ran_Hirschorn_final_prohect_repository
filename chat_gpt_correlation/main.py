import os
import pandas as pd
from link_correlation import process_raw_data_parallel
import time
import multiprocessing


def process_folder(example_metadata_path, metadata_folder, raw_data_path, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    consolidated_output_path = os.path.join(output_folder, "consolidated_metadata.csv")

    # Delete the output file if it exists to start fresh
    if os.path.exists(consolidated_output_path):
        os.remove(consolidated_output_path)
        print(f"Deleted existing output file: {consolidated_output_path}")

    try:
        print(f"\n{'=' * 80}")
        print(f"Processing raw data file: {raw_data_path}")
        print(f"{'=' * 80}")

        # Get the number of CPU cores and use that for max_workers
        num_cores = multiprocessing.cpu_count()
        # Use slightly fewer cores than available to prevent system overload
        max_workers = max(1, num_cores - 1)

        final_data = process_raw_data_parallel(
            raw_data_path,
            metadata_folder,
            example_metadata_path,
            consolidated_output_path,
            max_workers=max_workers
        )

        # Print summary
        print(f"\n{'=' * 80}")
        print("Processing Summary")
        print(f"{'=' * 80}")
        print(f"Total rows processed: {len(final_data)}")
        print(f"Output file: {consolidated_output_path}")

        return True

    except Exception as e:
        print(f"\n Error processing file: {str(e)}")
        return False

def main():
    # Configuration
    example_metadata_path = r"D:\final_project\cml_metadata.csv"  # Path to the example metadata file
    metadata_folder = r"D:\final_project\metadatas_with_dates"  # Folder containing all metadata files
    raw_data_path = r"D:\final_project\raw_datas\filtered_raw_data.xlsx"  # Path to the raw data file
    output_folder = r"D:\final_project\output_from_code"  # Folder to save the processed files

    # Process the data
    start_time = time.time()
    success = process_folder(
        example_metadata_path,
        metadata_folder,
        raw_data_path,
        output_folder
    )
    end_time = time.time()

    # Print final status
    print(f"\n{'=' * 80}")
    print("Final Status")
    print(f"{'=' * 80}")
    print(f"Processing {'completed successfully' if success else 'failed'}")
    print(f"Total processing time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()