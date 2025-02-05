import numpy as np
import xarray as xr
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple, Dict


class RainfallEstimator:
    def __init__(self):
        """Initialize the rainfall estimator with default parameters."""
        self.power_law_params = {
            # Default α and β parameters for different frequency ranges
            # Based on common literature values
            'alpha': {
                'low': 0.0163,  # For f < 10 GHz
                'medium': 0.0335,  # For 10 GHz ≤ f < 20 GHz
                'high': 0.0691  # For f ≥ 20 GHz
            },
            'beta': {
                'low': 0.851,  # For f < 10 GHz
                'medium': 1.021,  # For 10 GHz ≤ f < 20 GHz
                'high': 1.223  # For f ≥ 20 GHz
            }
        }

    def get_power_law_params(self, frequency: float, polarization: str) -> Tuple[float, float]:
        """
        Get α and β parameters based on frequency and polarization.

        Args:
            frequency (float): Link frequency in GHz
            polarization (str): Link polarization ('V' or 'H')

        Returns:
            Tuple[float, float]: α and β parameters
        """
        # Select base parameters based on frequency
        if frequency < 10:
            alpha = self.power_law_params['alpha']['low']
            beta = self.power_law_params['beta']['low']
        elif frequency < 20:
            alpha = self.power_law_params['alpha']['medium']
            beta = self.power_law_params['beta']['medium']
        else:
            alpha = self.power_law_params['alpha']['high']
            beta = self.power_law_params['beta']['high']

        # Adjust for polarization (horizontal polarization typically has higher attenuation)
        if polarization.upper() == 'H':
            alpha *= 1.2

        return alpha, beta

    def calculate_rainfall(self, attenuation: np.ndarray, wet_periods: np.ndarray,
                           link_length: float, frequency: float, polarization: str) -> np.ndarray:
        """
        Calculate rainfall intensity using the power-law model.

        Args:
            attenuation (np.ndarray): Measured attenuation values
            wet_periods (np.ndarray): Binary wet-dry classification (1 for wet, 0 for dry)
            link_length (float): Length of the link in km
            frequency (float): Link frequency in GHz
            polarization (str): Link polarization

        Returns:
            np.ndarray: Estimated rainfall intensity in mm/hr
        """
        alpha, beta = self.get_power_law_params(frequency, polarization)

        # Initialize rainfall array
        rainfall = np.zeros_like(attenuation)

        # Calculate rainfall only for wet periods
        # Using the power-law model: R = (A/(α*L))^(1/β)
        wet_mask = wet_periods == 1
        normalized_attenuation = attenuation[wet_mask] / link_length

        rainfall[wet_mask] = np.power(normalized_attenuation / alpha, 1 / beta)

        # Remove negative values and unrealistic high values
        rainfall[rainfall < 0] = 0
        rainfall[rainfall > 200] = 200  # Cap at 200 mm/hr as sanity check

        return rainfall


def process_and_plot_rainfall(netcdf_path: str, metadata_path: str, link_id):
    """
    Process CML data and estimate rainfall using the power-law model.

    Args:
        netcdf_path (str): Path to NetCDF file
        metadata_path (str): Path to metadata CSV file
        link_id (str, optional): Specific link ID to process
    """
    # Load data
    ds = xr.open_dataset(netcdf_path)
    metadata = pd.read_csv(metadata_path)

    # Get available links
    available_links = ds.link.values
    if link_id is None:
        link_id = available_links[0]
    elif link_id not in available_links:
        print(f"Link ID '{link_id}' not found. Using first available link: {available_links[0]}")
        link_id = available_links[0]

    # Get link metadata
    metadata['Link'] = metadata['Link'].astype(str)
    link_meta = metadata[metadata['Link'] == link_id].iloc[0]

    # Get link data
    rx_data = ds.RxLevel.sel(link=link_id).values
    tx_data = ds.TxLevel.sel(link=link_id).values

    # Calculate attenuation
    attenuation = tx_data - rx_data

    # Perform wet-dry classification
    from wet_and_dry_classification import StatisticalWetDryClassifier
    classifier = StatisticalWetDryClassifier()
    wet_dry_classification, _ = classifier.classify(attenuation)

    # Initialize rainfall estimator
    estimator = RainfallEstimator()

    # Ensure proper dimensions and handle NaN values
    attenuation = np.nan_to_num(attenuation, nan=0.0)
    wet_dry_classification = wet_dry_classification.squeeze()

    # Calculate rainfall
    rainfall = estimator.calculate_rainfall(
        attenuation=attenuation,
        wet_periods=wet_dry_classification,
        link_length=float(link_meta['Length_km']),
        frequency=float(link_meta['Frequency_GHz']),
        polarization=str(link_meta['Polarization'])
    )

    # Calculate cumulative rainfall
    cumulative_rainfall = np.cumsum(rainfall)

    # Plotting
    plt.figure(figsize=(12, 6))
    plt.plot(cumulative_rainfall, label='Estimated Rainfall')
    plt.xlabel('Time index')
    plt.ylabel('Accumulate Rain [mm]')
    plt.grid(True)
    plt.legend()
    plt.title(f'Cumulative Rainfall Estimation - Link {link_id}')
    plt.tight_layout()
    plt.show()

    return cumulative_rainfall, rainfall

cumulative_rainfall, rainfall = process_and_plot_rainfall(
     r"D:\final_project\analysis_files\filtered_netcdf.nc",
     r"D:\final_project\analysis_files\final_metadata_with_normal_coordinates.csv",
     link_id="8394"
 )