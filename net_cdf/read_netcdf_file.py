from netCDF4 import Dataset, num2date
import pandas as pd
import numpy as np


def print_link_data(nc_file_path, num_times=10, num_links=10):
    try:
        nc_file = Dataset(nc_file_path, 'r')

        time_data = nc_file.variables['time'][:num_times]
        rx_data = nc_file.variables['RxLevel'][:num_times, :num_links]
        tx_data = nc_file.variables['TxLevel'][:num_times, :num_links]

        # Create a list to store formatted data
        rows = []
        for t in range(num_times):
            for l in range(num_links):
                rows.append({
                    'Time': time_data[t],
                    'Link': l,
                    'RxLevel': rx_data[t, l],
                    'TxLevel': tx_data[t, l]
                })

        df = pd.DataFrame(rows)

        print("\nFirst few rows of data:")
        print(df)

        nc_file.close()

    except Exception as e:
        print(f"Error processing NetCDF file: {str(e)}")


def print_link_names(nc_file_path):
    try:
        nc_file = Dataset(nc_file_path, 'r')

        # Get the links variable
        links = nc_file.variables['link'][:]

        # Print information about the links
        print(f"\nTotal number of links: {len(links)}")
        print("\nFirst 10 link names:")
        for i, link in enumerate(links[:]):
            # If link is a bytes object, decode it
            if isinstance(link, bytes):
                link = link.decode('utf-8')
            print(f"Link {i}: {link}")

        nc_file.close()

    except Exception as e:
        print(f"Error processing NetCDF file: {str(e)}")


from netCDF4 import Dataset
import numpy as np
from datetime import datetime, timedelta


def get_netcdf_time_range(file_path):
    try:
        # Open the NetCDF file
        nc = Dataset(file_path, 'r')

        # Get the time variable (usually named 'time')
        time_var = nc.variables['time']

        # Get time units and calendar if available
        time_units = time_var.units if hasattr(time_var, 'units') else None
        calendar = time_var.calendar if hasattr(time_var, 'calendar') else 'standard'

        # Get first and last time values
        start_time = time_var[0]
        end_time = time_var[-1]

        # Convert to datetime objects if units are available
        if time_units:
            start_datetime = num2date(start_time, time_units, calendar)
            end_datetime = num2date(end_time, time_units, calendar)
            print(f"Start time: {start_datetime}")
            print(f"End time: {end_datetime}")
        else:
            print(f"Raw start time value: {start_time}")
            print(f"Raw end time value: {end_time}")
            print("Note: Time units not available in the file")

        # Print additional time information
        print(f"\nTotal number of time steps: {len(time_var)}")

        if len(time_var) > 1:
            time_step = time_var[1] - time_var[0]
            print(f"Time step size (in original units): {time_step}")

        nc.close()

    except Exception as e:
        print(f"Error reading NetCDF file: {str(e)}")



#get_netcdf_time_range(r"D:\final_project\analysis_files\filtered_netcdf.nc")


print_link_names(r"D:\final_project\analysis_files\filtered_netcdf.nc")
# Usage
#print_link_data(r"D:\final_project\analysis_files\filtered_netcdf.nc")
