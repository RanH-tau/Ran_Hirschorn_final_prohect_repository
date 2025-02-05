import pandas as pd
import os
import concurrent.futures
import multiprocessing
from read_file import read_file
from chatgpt_correlation import calculate_correlation_batch
from column_mapping import get_ai_column_mapping, apply_mapping
import traceback


def find_best_metadata_match(raw_data_row, metadata_files_folder, correlation_threshold=0.85, target_correlation=0.9,
                             batch_size=3):
    """
    Find the best matching metadata row for a given raw data row.
    First finds best match based on data fields, then looks for latest version of that link.
    """
    best_correlation = 0
    best_link_number = None
    best_metadata = None
    best_metadata_file = None
    best_explanation = None
    best_matching_points = None

    # First pass: Find best match based on data fields
    for metadata_file in os.listdir(metadata_files_folder):
        metadata_path = os.path.join(metadata_files_folder, metadata_file)
        metadata = read_file(metadata_path)

        # Process metadata in batches
        for i in range(0, len(metadata), batch_size):
            metadata_batch = metadata.iloc[i:i + batch_size]

            # Get correlations for this batch
            batch_results = calculate_correlation_batch(raw_data_row, metadata_batch)

            # Process results
            for result in batch_results:
                correlation = result['correlation']

                if correlation > correlation_threshold and correlation > best_correlation:
                    best_correlation = correlation
                    best_metadata = metadata.iloc[i + result['metadata_index']]
                    best_metadata_file = metadata_file
                    best_explanation = result.get('explanation', '')
                    best_matching_points = result.get('matching_points', [])
                    # Get link number from the metadata row instead of the correlation result
                    best_link_number = best_metadata.get('Link') if 'Link' in best_metadata else None

                # Early stopping if we found a very good match
                if correlation > target_correlation:
                    break

            if best_correlation > target_correlation:
                break

        if best_correlation > target_correlation:
            break

    # If we found a good match and it has a link number, search for the latest version
    if best_link_number is not None:
        latest_metadata = None
        latest_file = None
        latest_explanation = None
        latest_matching_points = None

        # Second pass: Look for latest version of the best link
        for metadata_file in os.listdir(metadata_files_folder):
            metadata_path = os.path.join(metadata_files_folder, metadata_file)
            metadata = read_file(metadata_path)

            # Find rows with matching link number
            matching_rows = metadata[metadata['Link'] == best_link_number]

            if not matching_rows.empty:
                # Process these rows to find the latest version
                for _, row in matching_rows.iterrows():
                    metadata_batch = pd.DataFrame([row])
                    batch_results = calculate_correlation_batch(raw_data_row, metadata_batch)

                    for result in batch_results:
                        # Check if this version is better than what we have
                        if result['correlation'] >= best_correlation:
                            latest_metadata = row
                            latest_file = metadata_file
                            latest_explanation = result.get('explanation', '')
                            latest_matching_points = result.get('matching_points', [])

        # Return the latest version if found
        if latest_metadata is not None:
            return latest_metadata, best_correlation, latest_file, latest_explanation, latest_matching_points

    return best_metadata, best_correlation, best_metadata_file, best_explanation, best_matching_points


def process_raw_data_parallel(raw_data_path, metadata_folder, example_metadata_path, output_path, max_workers=4):
    """
    Process raw data in parallel, with metadata batching
    """
    raw_data = read_file(raw_data_path)
    example_metadata = read_file(example_metadata_path)

    # Store all processed rows in a list first
    processed_rows = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for idx, raw_row in raw_data.iterrows():
            future = executor.submit(find_best_metadata_match, raw_row, metadata_folder)
            # Store the raw_row with the future
            futures.append((future, raw_row))

        completed = 0
        for future, raw_row in futures:
            try:
                metadata_row, correlation, metadata_file, explanation, matching_points = future.result()
                if metadata_row is not None:
                    # Create new row dictionary
                    new_row = {}

                    # First map fields from raw data
                    # Link number from raw data (primary source)
                    if 'LINKNUMBER' in raw_row:
                        try:
                            new_row['Link'] = pd.to_numeric(raw_row['LINKNUMBER'], errors='coerce')
                        except Exception as e:
                            print(f"Error mapping link number from raw data: {str(e)}")
                            # If raw data mapping fails, try metadata mapping as fallback
                            link_columns = ['Link', 'link_number', 'LinkNumber', 'מספר עורק']
                            for link_col in link_columns:
                                if link_col in metadata_row and not pd.isna(metadata_row[link_col]):
                                    try:
                                        new_row['Link'] = pd.to_numeric(metadata_row[link_col], errors='coerce')
                                        break
                                    except Exception as e:
                                        print(f"Error mapping link number from metadata: {str(e)}")

                    # Coordinates from raw data
                    if 'ITMX' in raw_row and 'ITMY' in raw_row:
                        try:
                            new_row['NearLongitude_DecDeg'] = pd.to_numeric(raw_row['ITMX'], errors='coerce')
                            new_row['NearLatitude_DecDeg'] = pd.to_numeric(raw_row['ITMY'], errors='coerce')
                        except Exception as e:
                            print(f"Error mapping coordinates: {str(e)}")

                    # RxLevel and TxLevel from raw data
                    if 'RxLevel' in raw_row:
                        try:
                            new_row['RxLevel'] = pd.to_numeric(raw_row['RxLevel'], errors='coerce')
                        except Exception as e:
                            print(f"Error mapping RxLevel: {str(e)}")

                    if 'TxLevel' in raw_row:
                        try:
                            new_row['TxLevel'] = pd.to_numeric(raw_row['TxLevel'], errors='coerce')
                        except Exception as e:
                            print(f"Error mapping TxLevel: {str(e)}")

                    # Get AI-generated mapping for other columns
                    column_mapping = get_ai_column_mapping(metadata_row.index, example_metadata.columns)

                    # Apply the mapping for other columns
                    for source_col, target_col in column_mapping.items():
                        if source_col in metadata_row:
                            try:
                                value = metadata_row[source_col]
                                if pd.isna(value):
                                    continue

                                # Convert value based on target column dtype
                                if pd.api.types.is_numeric_dtype(example_metadata[target_col].dtype):
                                    try:
                                        value = pd.to_numeric(value, errors='coerce')
                                    except:
                                        continue
                                new_row[target_col] = value
                            except Exception as e:
                                print(f"Error mapping column {source_col} to {target_col}: {str(e)}")
                                continue

                    # Add tracking and explanation columns
                    new_row['correlation'] = correlation
                    new_row['metadata_source'] = metadata_file
                    new_row['match_explanation'] = explanation
                    new_row['matching_points'] = '; '.join(matching_points) if matching_points else ''

                    # Add the processed row to our list
                    processed_rows.append(new_row)

                completed += 1
                print(f"Processed {completed}/{len(raw_data)} rows")

                # Save progress every 2 successful correlations
                if completed % 2 == 0:
                    # Create temporary DataFrame from processed rows
                    temp_df = pd.DataFrame(processed_rows)
                    # Ensure all required columns exist with correct types
                    for col in example_metadata.columns:
                        if col not in temp_df:
                            temp_df[col] = pd.Series(dtype=example_metadata[col].dtype)
                    if 'RxLevel' not in temp_df:
                        temp_df['RxLevel'] = pd.Series(dtype='float64')
                    if 'TxLevel' not in temp_df:
                        temp_df['TxLevel'] = pd.Series(dtype='float64')

                    print("Columns in temp_df:", temp_df.columns.tolist())
                    print("Sample row:", temp_df.iloc[-1])
                    temp_df.to_csv(f"{output_path}.partial", index=False, encoding='utf-8-sig')

            except Exception as e:
                print(f"Error processing row: {str(e)}")
                print(f"Full error: {traceback.format_exc()}")

    # Create final DataFrame from all processed rows
    final_data = pd.DataFrame(processed_rows)

    # Ensure all required columns exist with correct types
    for col in example_metadata.columns:
        if col not in final_data:
            final_data[col] = pd.Series(dtype=example_metadata[col].dtype)
    if 'RxLevel' not in final_data:
        final_data['RxLevel'] = pd.Series(dtype='float64')
    if 'TxLevel' not in final_data:
        final_data['TxLevel'] = pd.Series(dtype='float64')

    # Save final results
    final_data.to_csv(output_path, index=False, encoding='utf-8-sig')
    return final_data