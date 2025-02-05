import pandas as pd
import folium
from folium import plugins


def create_israel_map(csv_file_path):
    """
    Create an interactive map of Israel with network links from CSV data.
    Handles NaN values in both Near and Far coordinates.

    Parameters:
    csv_file_path (str): Path to the CSV file containing link data

    Returns:
    folium.Map: Interactive map with the links visualized
    """
    # Read the CSV file
    try:
        df = pd.read_csv(csv_file_path)
        required_columns = ['Link', 'NearLatitude_DecDeg', 'NearLongitude_DecDeg',
                            'FarLatitude_DecDeg', 'FarLongitude_DecDeg']

        # Verify all columns are present
        if not all(col in df.columns for col in required_columns):
            raise ValueError("CSV file missing required columns")

    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

    # Filter out rows with NaN in Near coordinates
    valid_df = df[~(pd.isna(df['NearLatitude_DecDeg']) | pd.isna(df['NearLongitude_DecDeg']))].copy()

    if valid_df.empty:
        print("No valid coordinates found in the data")
        return None

    # Calculate center of Israel for initial map view using valid coordinates
    center_lat = valid_df['NearLatitude_DecDeg'].mean()
    center_lon = valid_df['NearLongitude_DecDeg'].mean()

    # Create a map centered on Israel
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=8,
        tiles='CartoDB positron'
    )

    # Add a legend
    legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; 
                    right: 50px; 
                    z-index: 1000;
                    background-color: white;
                    padding: 10px;
                    border-radius: 5px;
                    border: 2px solid grey;">
            <p><i class="fa fa-arrow-right" style="color:blue"></i> Complete Link</p>
            <p><i class="fa fa-circle" style="color:red"></i> Point Only</p>
        </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))

    # Count valid and invalid entries
    total_rows = len(df)
    skipped_rows = total_rows - len(valid_df)
    complete_links = 0
    point_only = 0

    # Process each row in the dataframe
    for _, row in valid_df.iterrows():
        # Check if Far coordinates are valid
        has_valid_far = not (pd.isna(row['FarLatitude_DecDeg']) or pd.isna(row['FarLongitude_DecDeg']))

        if has_valid_far:
            # Create coordinates for the complete link
            coordinates = [
                [row['NearLatitude_DecDeg'], row['NearLongitude_DecDeg']],
                [row['FarLatitude_DecDeg'], row['FarLongitude_DecDeg']]
            ]

            # Add an arrow line for the complete link
            plugins.AntPath(
                locations=coordinates,
                popup=f"{row['Link']}",
                tooltip=row['Link'],
                weight=2,
                color='blue'
            ).add_to(m)

            # Add markers for start and end points
            folium.CircleMarker(
                location=[row['NearLatitude_DecDeg'], row['NearLongitude_DecDeg']],
                radius=3,
                color='blue',
                fill=True,
                popup=f"Start: {row['Link']}"
            ).add_to(m)

            folium.CircleMarker(
                location=[row['FarLatitude_DecDeg'], row['FarLongitude_DecDeg']],
                radius=3,
                color='blue',
                fill=True,
                popup=f"End: {row['Link']}"
            ).add_to(m)

            complete_links += 1

        else:
            # Add a single marker for links with NaN Far coordinates
            folium.CircleMarker(
                location=[row['NearLatitude_DecDeg'], row['NearLongitude_DecDeg']],
                radius=5,
                color='red',
                fill=True,
                popup=f"{row['Link']} (Point Only)",
                tooltip=row['Link']
            ).add_to(m)

            point_only += 1

    # Add a title and statistics to the map
    title_html = f'''
        <div style="position: fixed; 
                    top: 10px; 
                    left: 50%; 
                    transform: translateX(-50%);
                    z-index: 1000;
                    background-color: white;
                    padding: 10px;
                    border-radius: 5px;
                    border: 2px solid grey;">
            <h3 style="text-align: center; margin: 0;">Israel Network Links Map</h3>
            <p style="text-align: center; margin: 5px 0;">Blue: Complete Links ({complete_links}) | Red: Points Only ({point_only})</p>
            <p style="text-align: center; margin: 0; font-size: 0.8em;">Skipped {skipped_rows} rows with invalid Near coordinates</p>
        </div>
    '''
    m.get_root().html.add_child(folium.Element(title_html))

    return m


def main():
    # Specify your CSV file path
    csv_file_path = r"D:\final_project\analysis_files\final_metadata_with_normal_coordinates.csv"

    # Create the map
    map_obj = create_israel_map(csv_file_path)

    if map_obj:
        # Save the map to an HTML file
        output_file = 'israel_network_map.html'
        map_obj.save(output_file)
        print(f"Map has been created and saved as {output_file}")
    else:
        print("Failed to create map")


if __name__ == "__main__":
    main()