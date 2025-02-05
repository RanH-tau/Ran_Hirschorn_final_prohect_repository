import pandas as pd
from chatgpt_correlation import get_column_mapping


def get_ai_column_mapping(source_columns, target_columns):
    """
    Get column mapping using AI assistance.
    Returns a dictionary where:
    - Keys are the source metadata column names
    - Values are the target example metadata column names
    """
    try:
        mappings, explanations = get_column_mapping(source_columns, target_columns)

        # Verify all target columns are covered
        mapped_target_cols = set(mappings.values())
        missing_cols = set(target_columns) - mapped_target_cols - {'correlation', 'metadata_source'}

        if missing_cols:
            print(f"Warning: Missing mappings for columns: {missing_cols}")

        return mappings
    except Exception as e:
        print(f"Error in get_ai_column_mapping: {str(e)}")
        # Return empty mapping instead of failing
        return {}
def print_available_columns(example_columns, source_columns):
    """
    Print available columns from both example metadata and source metadata.
    """
    print("\nAvailable columns for mapping:")
    print("\nExample metadata columns:")
    for i, col in enumerate(example_columns, 1):
        print(f"{i}. {col}")

    print("\nSource metadata columns:")
    for i, col in enumerate(source_columns, 1):
        print(f"{i}. {col}")

def verify_mapping(mapping_dict, example_columns, source_columns):
    """
    Verify that the mapping is valid by checking if all referenced columns exist.
    """
    errors = []
    warnings = []

    # Check source columns
    for source_col in mapping_dict.keys():
        if source_col not in source_columns:
            errors.append(f"Source column '{source_col}' not found in source metadata")

    # Check target columns
    mapped_target_cols = set(mapping_dict.values())
    for target_col in example_columns:
        if target_col not in mapped_target_cols and target_col not in ['correlation', 'metadata_source']:
            warnings.append(f"Target column '{target_col}' has no mapping")

    is_valid = len(errors) == 0
    return is_valid, errors, warnings

def apply_mapping(mapping_dict, source_row, target_df, row_index=0):
    """
    Apply the column mapping to copy data from source row to target dataframe.
    """
    mapped_values = {}
    for source_col, target_col in mapping_dict.items():
        if source_col in source_row.index and target_col in target_df.columns:
            target_df.at[row_index, target_col] = source_row[source_col]
            mapped_values[target_col] = source_row[source_col]

    return mapped_values
