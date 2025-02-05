import pandas as pd
import json
import os
from chardet import detect

def read_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    if not os.access(file_path, os.R_OK):
        raise PermissionError(f"Permission denied: {file_path}")

    file_extension = os.path.splitext(file_path)[1].lower()

    try:
        if file_extension == '.csv':
            # Detect file encoding
            with open(file_path, 'rb') as file:
                raw_data = file.read()
                encoding = detect(raw_data)['encoding']

            # Try to read the file with different delimiters
            for delimiter in [',', '\t']:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter)
                    if len(df.columns) > 1:
                        return df
                except:
                    continue

            raise ValueError(f"Could not determine the correct delimiter for file: {file_path}")
        elif file_extension in ['.xls', '.xlsx']:
            return pd.read_excel(file_path)
        elif file_extension in ['.txt', '.json']:
            with open(file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)

            if isinstance(json_data, list):
                return pd.DataFrame(json_data)
            elif isinstance(json_data, dict):
                return pd.DataFrame([json_data])
            else:
                raise ValueError(f"Unsupported JSON structure in file: {file_path}")
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")
    except (pd.errors.EmptyDataError, json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"Error reading file {file_path}: {str(e)}")
