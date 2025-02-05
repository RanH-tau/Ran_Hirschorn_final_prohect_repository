# Ran_Hirschorn_final_prohect_repository
# Statistical Analysis and Data Organization of a Research Group

## Project Overview
This project implements a system for analyzing and organizing cellular network data for environmental sensing applications. It processes data from multiple cellular operators, correlates metadata with raw data, and generates unified datasets for weather monitoring and analysis.

## Features
- Intelligent data correlation using GPT API
- Memory-efficient NetCDF file generation
- Parallel processing of large datasets
- Automated metadata mapping
- Multi-format file support (CSV, Excel, JSON)

## Project Structure
```
project/
├── src/
│   ├── main.py                 # Main execution script
│   ├── link_correlation.py     # Core correlation logic
│   ├── chatgpt_correlation.py  # GPT API integration
│   ├── column_mapping.py       # Column mapping utilities
│   ├── read_file.py           # File reading utilities
│   └── netcdf_processor.py    # NetCDF file generation
├── config/
│   └── .env                   # Environment configuration
├── data/
│   ├── raw_data/             # Raw input data
│   ├── metadata/             # Metadata files
│   └── output/               # Processed outputs
└── docs/                     # Detailed documentation
```

## Requirements
- Python 3.8+
- OpenAI API key
- Required Python packages:
  - pandas
  - numpy
  - xarray
  - langchain
  - openai
  - chardet
  - matplotlib
  - typing
  - folium
  - netCDF4

## Installation
1. Clone the repository
2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure environment variables:
   - Create `.env` file with:
     ```
     OPENAI_API_KEY=your_api_key
     OPENAI_MODEL_NAME=your_model_name
     ```

## Usage
1. Prepare your data:
   - Place raw data files in `data/raw_data/`
   - Place metadata files in `data/metadata/`
   - Ensure example metadata file is available

2. Run the main script:
   ```bash
   python src/main.py
   ```

3. Check outputs in `data/output/`:
   - Consolidated metadata files
   - NetCDF files
   - Analysis results

## Module Descriptions

### main.py
- Entry point for the application
- Handles configuration and orchestration
- Manages parallel processing

### link_correlation.py
- Implements core correlation logic
- Matches raw data with metadata
- Handles batch processing

### chatgpt_correlation.py
- Integrates with GPT API
- Provides intelligent matching
- Handles response processing

### column_mapping.py
- Manages column name mapping
- Provides verification utilities
- Handles data transformation

### read_file.py
- Handles multiple file formats
- Provides encoding detection
- Implements error handling

### code_for_netcdf_file.py
- Converts CSV data to NetCDF format
- Implements memory-efficient chunked processing
- Features:
  * Automatic dimension detection
  * Memory-optimized data processing
  * Batch processing capabilities
  * Progress tracking and timing
  * Error handling and recovery
- Input: CSV files with columns:
  * DATETIME_ID (format: '%d/%m/%Y %I:%M:%S %p')
  * KEY10NEW (link identifier)
  * RxLevel (Received Signal Level in dBm)
  * TxLevel (Transmitted Signal Level in dBm)
- Output: NetCDF files with:
  * Two dimensions: time and link
  * Two variables: RxLevel and TxLevel
  * Comprehensive metadata and attributes

## Contributing
1. Fork the repository
2. Create a new branch
3. Make your changes
4. Submit a pull request

## License
[Your License Here]

## Contact
[Your Contact Information]
