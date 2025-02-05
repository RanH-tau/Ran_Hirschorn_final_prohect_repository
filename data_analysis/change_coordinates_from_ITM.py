import pandas as pd
import math

import pandas as pd
import math


def itm2wgs84(X, Y):
    # ITM Grid to WGS84 conversion constants
    a = 6378137.0  # Semi-major axis
    e2 = 0.00669437999013  # Square of eccentricity
    lambda0 = 0.6145667421719  # Central meridian in radians (35.2045169444444 degrees)
    phi0 = 0.55386447682762  # Latitude of origin in radians (31.7343936111111 degrees)
    k0 = 1.0000067  # Scale factor
    FE = 219529.584  # False Easting
    FN = 626907.39  # False Northing

    # Remove false easting and northing
    x = (X - FE) / k0
    y = (Y - FN) / k0

    # Initial values
    phi = phi0 + (y / 6366197.724)
    nu = a / (math.sqrt(1 - (e2 * math.pow(math.sin(phi), 2))))

    # First iteration
    phi = phi0 + (y / 6366197.724) + \
          (math.pow(x / nu, 2) * math.tan(phi0)) / 2

    # Calculate longitude
    lambda_ = lambda0 + (x / (nu * math.cos(phi0)))

    # Convert to degrees
    lat = phi * 180 / math.pi
    lon = lambda_ * 180 / math.pi

    return lat, lon


def process_csv(input_file, output_file):
    # Read the CSV file
    df = pd.read_csv(input_file)

    # Convert Near coordinates
    near_converted_coords = [itm2wgs84(row['NearLongitude_DecDeg'], row['NearLatitude_DecDeg'])
                             for _, row in df.iterrows()]

    # Convert Far coordinates
    far_converted_coords = [itm2wgs84(row['FarLongitude_DecDeg'], row['FarLatitude_DecDeg'])
                            for _, row in df.iterrows()]

    # Update Near coordinate columns
    df['NearLatitude_DecDeg'] = [coord[0] for coord in near_converted_coords]
    df['NearLongitude_DecDeg'] = [coord[1] for coord in near_converted_coords]

    # Update Far coordinate columns
    df['FarLatitude_DecDeg'] = [coord[0] for coord in far_converted_coords]
    df['FarLongitude_DecDeg'] = [coord[1] for coord in far_converted_coords]

    # Save to new CSV file
    df.to_csv(output_file, index=False)
    print(f"Conversion completed. Output saved to {output_file}")

if __name__ == "__main__":
    input_csv = r"D:\final_project\output_from_code\consolidated_metadata_updated.csv"
    output_converted =r"D:\final_project\analysis_files\final_metadata_with_normal_coordinates.csv"
    process_csv(input_csv, output_converted)

