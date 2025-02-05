import xarray as xr
import numpy as np
import pandas as pd


def clean_netcdf(input_path: str, output_path: str):
    """
    Remove duplicate links from NetCDF file and save a cleaned version.

    Args:
        input_path (str): Path to input NetCDF file
        output_path (str): Path where cleaned NetCDF will be saved
    """
    # Load the dataset
    print("Loading NetCDF file...")
    ds = xr.open_dataset(input_path)

    # Get unique link IDs
    unique_links = list(set(ds.link.values))
    print(f"Found {len(unique_links)} unique links out of {len(ds.link)} total")

    # Initialize arrays for cleaned data
    time_len = len(ds.time)
    new_rx = np.full((time_len, len(unique_links)), np.nan)
    new_tx = np.full((time_len, len(unique_links)), np.nan)

    # Process each unique link
    for idx, link_id in enumerate(unique_links):
        print(f"Processing link {link_id}...")

        # Get all occurrences of this link
        link_data_rx = ds.RxLevel.sel(link=link_id)
        link_data_tx = ds.TxLevel.sel(link=link_id)

        if len(link_data_rx.shape) > 1:  # If there are duplicates
            # Combine data from duplicates, taking non-NaN values
            rx_combined = link_data_rx.values
            tx_combined = link_data_tx.values

            # For each timestep, take the first non-NaN value
            for t in range(time_len):
                rx_vals = rx_combined[t]
                tx_vals = tx_combined[t]

                new_rx[t, idx] = np.nanmean(rx_vals)  # Use mean of non-NaN values
                new_tx[t, idx] = np.nanmean(tx_vals)  # Use mean of non-NaN values
        else:
            # If no duplicates, just copy the data
            new_rx[:, idx] = link_data_rx.values
            new_tx[:, idx] = link_data_tx.values

    # Create new dataset with cleaned data
    new_ds = xr.Dataset(
        {
            'RxLevel': (['time', 'link'], new_rx),
            'TxLevel': (['time', 'link'], new_tx),
        },
        coords={
            'time': ds.time,
            'link': unique_links
        }
    )

    # Copy attributes if they exist
    if hasattr(ds, 'attrs'):
        new_ds.attrs = ds.attrs
    if hasattr(ds.RxLevel, 'attrs'):
        new_ds.RxLevel.attrs = ds.RxLevel.attrs
    if hasattr(ds.TxLevel, 'attrs'):
        new_ds.TxLevel.attrs = ds.TxLevel.attrs

    # Save cleaned dataset
    print(f"Saving cleaned dataset to {output_path}...")
    new_ds.to_netcdf(output_path)

    # Print summary
    print("\nCleaning Summary:")
    print(f"Original number of links: {len(ds.link)}")
    print(f"Number of links after cleaning: {len(unique_links)}")
    print(f"Number of duplicates removed: {len(ds.link) - len(unique_links)}")

    # Verify data completeness
    rx_missing = np.isnan(new_rx).sum()
    tx_missing = np.isnan(new_tx).sum()
    total_points = new_rx.size

    print(f"\nData Completeness:")
    print(f"RxLevel missing values: {rx_missing} ({rx_missing / total_points * 100:.2f}%)")
    print(f"TxLevel missing values: {tx_missing} ({tx_missing / total_points * 100:.2f}%)")

    return new_ds


if __name__ == "__main__":
    input_file = r"D:\final_project\analysis_files\filtered_netcdf.nc"  # Your input file
    output_file = r"D:\final_project\analysis_files\filtered_netcdf_cleaned.nc"  # The cleaned output file

    cleaned_ds = clean_netcdf(input_file, output_file)