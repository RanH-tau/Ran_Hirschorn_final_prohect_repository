import pandas as pd
import xarray as xr
import numpy as np
from datetime import datetime
import os
import time
import tempfile


def memory_efficient_csv_to_netcdf(csv_file, output_file, chunk_size=10000):
    """
    A memory-efficient version that processes the CSV file in chunks
    """
    print(f"\nProcessing file: {os.path.basename(csv_file)}")
    print("First pass: collecting unique values...")
    times_set = set()
    links_set = set()

    # First pass to get unique values - using tab delimiter
    for chunk in pd.read_csv(csv_file, chunksize=chunk_size, sep='\t'):
        # Convert to datetime using your specific format
        chunk['DATETIME_ID'] = pd.to_datetime(chunk['DATETIME_ID'], format='%d/%m/%Y %I:%M:%S %p')
        times_set.update(chunk['DATETIME_ID'])
        links_set.update(chunk['KEY10NEW'])

    times = sorted(times_set)
    links = sorted(links_set)

    print(f"Found {len(times)} unique timestamps and {len(links)} unique links")

    # Create the dataset with the complete structure
    ds = xr.Dataset(
        {
            'RxLevel': (['time', 'link'], np.full((len(times), len(links)), np.nan)),
            'TxLevel': (['time', 'link'], np.full((len(times), len(links)), np.nan))
        },
        coords={
            'time': times,
            'link': links
        }
    )

    # Add metadata
    ds.attrs['description'] = 'Radio link measurements'
    ds.attrs['created'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ds.attrs['source_file'] = os.path.basename(csv_file)
    ds['RxLevel'].attrs['units'] = 'dBm'
    ds['RxLevel'].attrs['long_name'] = 'Received Signal Level'
    ds['TxLevel'].attrs['units'] = 'dBm'
    ds['TxLevel'].attrs['long_name'] = 'Transmitted Signal Level'

    # Create mapping dictionaries for faster lookups
    times_dict = {time: idx for idx, time in enumerate(times)}
    link_to_idx = {link: idx for idx, link in enumerate(links)}

    print("Processing chunks...")
    chunk_count = 0
    total_rows = 0

    # Process the data in chunks
    for chunk in pd.read_csv(csv_file, chunksize=chunk_size, sep='\t'):
        chunk_count += 1
        total_rows += len(chunk)
        print(f"Processing chunk {chunk_count} (Total rows processed: {total_rows})...")

        chunk['DATETIME_ID'] = pd.to_datetime(chunk['DATETIME_ID'], format='%d/%m/%Y %I:%M:%S %p')

        # Update the data arrays directly
        for _, row in chunk.iterrows():
            time_idx = times_dict[row['DATETIME_ID']]
            link_idx = link_to_idx[row['KEY10NEW']]
            ds['RxLevel'][time_idx, link_idx] = row['RxLevel']
            ds['TxLevel'][time_idx, link_idx] = row['TxLevel']

    print(f"Saving final NetCDF file to {output_file}...")
    # Create the output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Save the final dataset
    ds.to_netcdf(output_file)
    ds.close()
    print(f"Completed processing {os.path.basename(csv_file)}!")


def process_directory(input_directory, output_directory):
    """
    Process all CSV files in the input directory and save NetCDF files to the output directory
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Get list of all CSV files in the input directory
    csv_files = [f for f in os.listdir(input_directory) if f.lower().endswith('.csv')]

    if not csv_files:
        print(f"No CSV files found in {input_directory}")
        return

    print(f"Found {len(csv_files)} CSV files to process")

    # Process each CSV file
    for idx, csv_file in enumerate(csv_files, 1):
        try:
            start_time = time.time()

            input_path = os.path.join(input_directory, csv_file)
            output_filename = os.path.splitext(csv_file)[0] + '.nc'
            output_path = os.path.join(output_directory, output_filename)

            print(f"\nProcessing file {idx}/{len(csv_files)}: {csv_file}")
            memory_efficient_csv_to_netcdf(input_path, output_path)

            end_time = time.time()
            processing_time = end_time - start_time
            print(f"Processing time: {processing_time:.2f} seconds")

        except Exception as e:
            print(f"Error processing {csv_file}: {str(e)}")
            continue


if __name__ == "__main__":
    # Define your input and output directories
    input_directory = r"D:\final_project\raw_datas"
    output_directory = r"D:\final_project\net_cdf_files"

    total_start_time = time.time()

    process_directory(input_directory, output_directory)

    total_end_time = time.time()
    total_processing_time = total_end_time - total_start_time
    print(f"\nTotal processing time: {total_processing_time:.2f} seconds")