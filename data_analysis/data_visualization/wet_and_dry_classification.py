import numpy as np
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple, Optional
import torch


class StatisticalWetDryClassifier:
    def __init__(self, threshold: float = 0.1, window_size: int = 8):
        """
        Initialize the wet-dry classifier.

        Args:
            threshold (float): Threshold for wet classification
            window_size (int): Size of the rolling window for standard deviation calculation
        """
        self.threshold = threshold
        self.window_size = window_size

    def calculate_attenuation(self, rx_level: np.ndarray, tx_level: np.ndarray) -> np.ndarray:
        """
        Calculate attenuation from received and transmitted power levels.

        Args:
            rx_level (np.ndarray): Received power levels
            tx_level (np.ndarray): Transmitted power levels

        Returns:
            np.ndarray: Calculated attenuation
        """
        return tx_level - rx_level

    def rolling_std(self, data: np.ndarray) -> np.ndarray:
        """
        Calculate rolling standard deviation.

        Args:
            data (np.ndarray): Input attenuation data

        Returns:
            np.ndarray: Rolling standard deviation
        """
        # Convert to tensor and ensure correct dimensions
        data_tensor = torch.tensor(data, dtype=torch.float32)
        if len(data_tensor.shape) == 1:
            data_tensor = data_tensor.unsqueeze(0)

        padding = self.window_size // 2
        padded_data = torch.nn.functional.pad(data_tensor, (padding, padding), mode='replicate')

        # Calculate rolling standard deviation
        std_vector = torch.zeros_like(data_tensor)
        for i in range(data_tensor.shape[1]):
            window = padded_data[:, i:i + self.window_size]
            std_vector[:, i] = torch.std(window, dim=1)

        return std_vector.numpy()

    def classify(self, attenuation: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Perform wet-dry classification.

        Args:
            attenuation (np.ndarray): Attenuation data

        Returns:
            Tuple[np.ndarray, np.ndarray]: Classification results and standard deviation vector
        """
        std_vector = self.rolling_std(attenuation)
        classification = (std_vector > self.threshold).astype(int)
        return classification, std_vector


def process_cml_data(netcdf_path: str, metadata_path: str, link_id: Optional[str] = None):
    """
    Process CML data and perform wet-dry classification.

    Args:
        netcdf_path (str): Path to NetCDF file
        metadata_path (str): Path to metadata CSV file
        link_id (str, optional): Specific link ID to process. If None, processes first link.
    """
    # Load data
    ds = xr.open_dataset(netcdf_path)
    metadata = pd.read_csv(metadata_path)

    # Print available link IDs for debugging
    available_links = ds.link.values
    print("Available link IDs in NetCDF file:", available_links)

    # Convert metadata link IDs to strings if necessary
    metadata['Link'] = metadata['Link'].astype(str)

    # Select link to process
    if link_id is None:
        link_id = available_links[0]
    elif link_id not in available_links:
        print(f"Link ID '{link_id}' not found. Using first available link: {available_links[0]}")
        link_id = available_links[0]

    # Get link data
    rx_data = ds.RxLevel.sel(link=link_id).values
    tx_data = ds.TxLevel.sel(link=link_id).values

    # Initialize classifier
    classifier = StatisticalWetDryClassifier()

    # Calculate attenuation and perform classification
    attenuation = classifier.calculate_attenuation(rx_data, tx_data)
    classification, std_vector = classifier.classify(attenuation)

    # Plot results
    fig, ax = plt.subplots(3, 1, figsize=(12, 8))

    # Plot classification results
    ax[0].plot(classification[0])
    ax[0].set_xlabel('Time Index')
    ax[0].set_ylabel('Wet/Dry Classification')
    ax[0].grid(True)
    ax[0].set_title(f'Wet-Dry Classification for Link {link_id}')

    # Plot standard deviation
    ax[1].plot(std_vector[0])
    ax[1].set_xlabel('Time Index')
    ax[1].set_ylabel('Rolling Standard Deviation')
    ax[1].grid(True)

    # Plot attenuation
    ax[2].plot(attenuation, label='Attenuation')
    ax[2].set_xlabel('Time Index')
    ax[2].set_ylabel('Attenuation (dB)')
    ax[2].grid(True)

    # Add wet period highlighting
    wet_periods = classification[0] == 1
    ax[2].fill_between(range(len(attenuation)),
                       ax[2].get_ylim()[0],
                       ax[2].get_ylim()[1],
                       where=wet_periods,
                       alpha=0.3,
                       color='blue',
                       label='Wet Period')
    ax[2].legend()

    plt.tight_layout()
    plt.show()

    return classification, std_vector, attenuation
# Usage example:
classification, std_vector, attenuation = process_cml_data(
     r"D:\final_project\analysis_files\filtered_netcdf_cleaned.nc",
     r"D:\final_project\analysis_files\final_metadata_with_normal_coordinates.csv",
     link_id='8394'  # Optional: specify link ID
 )