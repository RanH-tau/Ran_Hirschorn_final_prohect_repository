import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


class LinkDataset:
    def __init__(self, netcdf_file, metadata_file):
        """
        Initialize the LinkDataset with NetCDF and metadata files
        """
        self.data = xr.open_dataset(netcdf_file)
        self.metadata = pd.read_csv(metadata_file)

        # Debug information
        print("NetCDF dimensions:", self.data.dims)
        print("NetCDF variables:", list(self.data.variables))
        print("\nFirst few rows of metadata:")
        print(self.metadata.head())

        # Convert link IDs to integers in both sources
        self.netcdf_links = self.data.link.values.astype(int)
        self.metadata_links = self.metadata['Link'].values.astype(int)

        print("\nFirst few NetCDF link IDs (converted to int):", self.netcdf_links[:5])
        print("First few metadata link IDs:", self.metadata_links[:5])

        self.links = self._match_data_with_metadata()

    def _match_data_with_metadata(self):
        """Match time series data with link characteristics"""
        links = {}

        # Find common links between NetCDF and metadata
        common_links = np.intersect1d(self.netcdf_links, self.metadata_links)
        print(f"\nFound {len(common_links)} common links between NetCDF and metadata")

        for link_id in common_links:
            try:
                # Convert link_id to int for metadata lookup
                metadata_matches = self.metadata[self.metadata['Link'] == int(link_id)]
                if len(metadata_matches) == 0:
                    print(f"No metadata found for link {link_id}")
                    continue

                metadata_row = metadata_matches.iloc[0]

                # Convert link_id to str for NetCDF lookup
                links[int(link_id)] = {
                    'coords': (
                        metadata_row['NearLongitude_DecDeg'],
                        metadata_row['NearLatitude_DecDeg'],
                        metadata_row['FarLongitude_DecDeg'],
                        metadata_row['FarLatitude_DecDeg']
                    ),
                    'freq': metadata_row['Frequency_GHz'],
                    'length': metadata_row['Length_km'],
                    'polarization': metadata_row['Polarization'],
                    'rx': self.data['RxLevel'].sel(link=str(link_id)),  # Convert back to str for NetCDF
                    'tx': self.data['TxLevel'].sel(link=str(link_id))
                }
            except Exception as e:
                print(f"Error processing link {link_id}: {str(e)}")
                continue

        print(f"\nSuccessfully processed {len(links)} links")
        return links

    def plot_links(self, scale=True, scale_factor=1):
        """Plot all links on a map"""
        if not self.links:
            print("No links to plot!")
            return

        plt.figure(figsize=(12, 8))
        for link_id, link_data in self.links.items():
            start_x, start_y, end_x, end_y = link_data['coords']
            plt.plot([start_x, end_x], [start_y, end_y], 'b-', alpha=0.5)

        if scale:
            plt.axis('equal')
        plt.grid(True)
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.title(f'Link Network Layout ({len(self.links)} links)')

    def get_link(self, link_id):
        """Get data for a specific link"""
        return self.links.get(int(link_id))

    def plot_link_data(self, link_id):
        """Plot Rx/Tx data for a specific link"""
        link = self.get_link(link_id)
        if link is None:
            print(f"Link {link_id} not found")
            return

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

        # Plot Rx
        ax1.plot(link['rx'].time, link['rx'].values)
        ax1.set_title(f'Received Power (Rx) - Link {link_id}')
        ax1.grid(True)
        ax1.set_ylabel('RxLevel')

        # Plot Tx
        ax2.plot(link['tx'].time, link['tx'].values)
        ax2.set_title(f'Transmitted Power (Tx) - Link {link_id}')
        ax2.grid(True)
        ax2.set_ylabel('TxLevel')
        ax2.set_xlabel('Time')

        plt.tight_layout()
        return fig

    def plot_first_n_links(self, num_links=10):
        """Create separate plots for the first n links"""
        if not self.links:
            print("No links to plot!")
            return

        # Get the first num_links links
        link_ids = list(self.links.keys())[:num_links]

        for link_id in link_ids:
            # Create a new figure for each link
            link = self.get_link(link_id)
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

            # Plot Rx
            ax1.plot(link['rx'].time, link['rx'].values)
            ax1.set_title(f'Received Power (Rx) - Link {link_id}')
            ax1.grid(True)
            ax1.set_ylabel('RxLevel')
            ax1.tick_params(axis='x', rotation=45)

            # Plot Tx
            ax2.plot(link['tx'].time, link['tx'].values)
            ax2.set_title(f'Transmitted Power (Tx) - Link {link_id}')
            ax2.grid(True)
            ax2.set_ylabel('TxLevel')
            ax2.set_xlabel('Time')
            ax2.tick_params(axis='x', rotation=45)

            plt.tight_layout()
            plt.show()  # Show each plot separately

    def calculate_attenuation(self, link_id):
        """Calculate attenuation for a specific link"""
        link = self.links.get(int(link_id))
        if link is None:
            return None

        # Calculate attenuation (A = Tx - Rx)
        attenuation = link['tx'].values - link['rx'].values
        time = link['rx'].time

        # Calculate rolling maximum and minimum
        window = 60  # Adjust window size as needed
        A_max = pd.Series(attenuation).rolling(window=window, center=True).max()
        A_min = pd.Series(attenuation).rolling(window=window, center=True).min()

        return time, A_max, A_min

    def plot_attenuation(self, link_id):
        """Plot attenuation analysis for a specific link"""
        result = self.calculate_attenuation(link_id)
        if result is None:
            print(f"Link {link_id} not found")
            return

        time, A_max, A_min = result

        plt.figure(figsize=(12, 6))
        plt.plot(time, A_max, label='A$_n^{max}$', color='blue')
        plt.plot(time, A_min, label='A$_n^{min}$', color='orange')
        plt.grid(True)
        plt.ylabel('A[dB]')
        plt.title(f'Attenuation Analysis - Link {link_id}')
        plt.legend()
        plt.tight_layout()




dataset = LinkDataset(r"D:\final_project\analysis_files\filtered_netcdf.nc", r"D:\final_project\analysis_files\final_metadata_with_normal_coordinates.csv")
# Plot network layout
#dataset.plot_links()
#plt.show()
dataset.plot_first_n_links(num_links=1)
plt.show()
first_link_id = 679

dataset.plot_attenuation(first_link_id)
plt.show()
# Print first available link ID for testing
#if dataset.links:
#    first_link_id = next(iter(dataset.links.keys()))
#    print(f"\nPlotting data for first available link: {first_link_id}")
#    dataset.plot_link_data(first_link_id)
#    plt.show()
#else:
#    print("No valid links found!")