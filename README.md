# Weather Monitoring and Analysis System

## Project Overview
This project implements a comprehensive system for analyzing and organizing cellular network data for environmental sensing applications. It processes data from multiple cellular operators, correlates metadata with raw data, and generates unified datasets for weather monitoring and analysis.

## Project Structure
```
project/
├── chatgpt_correlation/
│   ├── main.py                  # Main execution script
│   ├── link_correlation.py      # Core correlation logic
│   ├── chatgpt_correlation.py   # GPT API integration
│   ├── column_mapping.py        # Column mapping utilities
│   └── read_file.py            # File reading utilities
│
├── netcdf/
│   ├── create_netcdf_file.py    # NetCDF file generation
│   ├── read_netcdf_file.py      # NetCDF file reading
│   └── remove_duplicates.py     # Data cleaning utilities
│
├── data_analysis/
│   ├── create_a_map.py          # Mapping functionality
│   ├── change_coordinates_from_ITM.py  # Coordinate conversion
│   ├── israel_network_map.html  # Network visualization
│   │
│   └── data_visualization/
│       ├── load_data_and_visualize.py  # Data visualization
│       ├── rain_estimator.py           # Rainfall estimation
│       └── wet_and_dry_classification.py  # Weather classification
│
└── requirements.txt             # Project dependencies
```

## Features
- Intelligent metadata correlation using GPT API
- Memory-efficient NetCDF file handling
- Automated data processing and cleaning
- Interactive network mapping
- Advanced weather analysis and visualization
- Coordinate system conversion
- Rainfall estimation and classification

## Components

### ChatGPT Correlation
Located in `chatgpt_correlation/`:
- Intelligent matching of metadata with raw data
- GPT-powered column mapping
- Multi-format file support
- Error handling and validation

### NetCDF Processing
Located in `netcdf/`:
- Memory-efficient file creation
- Data reading and validation
- Duplicate removal and data cleaning
- Time series management

### Data Analysis
Located in `data_analysis/`:
- Interactive map creation
- Coordinate system conversion (ITM to standard)
- Network visualization
- Weather pattern analysis
- Rainfall estimation
- Wet/dry period classification

## Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/weather-monitoring-project.git
cd weather-monitoring-project
```

2. Install required packages:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
# Create .env file with:
OPENAI_API_KEY=your_api_key
OPENAI_MODEL_NAME=your_model_name
```

## Usage

### Data Processing Pipeline
1. Prepare your data files
2. Run the correlation process:
```bash
cd chatgpt_correlation
python main.py
```

### NetCDF Processing
```bash
cd netcdf
python create_netcdf_file.py
```

### Data Analysis and Visualization
```bash
cd data_analysis
python create_a_map.py  # For network mapping
cd data_visualization
python load_data_and_visualize.py  # For data visualization
```

## File Descriptions

### ChatGPT Correlation
- `main.py`: Entry point and pipeline orchestration
- `link_correlation.py`: Core correlation algorithms
- `chatgpt_correlation.py`: GPT API integration
- `column_mapping.py`: Intelligent column mapping
- `read_file.py`: Multi-format file handling

### NetCDF Processing
- `create_netcdf_file.py`: Generates NetCDF files
- `read_netcdf_file.py`: Reads and processes NetCDF data
- `remove_duplicates.py`: Cleans and deduplicates data

### Data Analysis
- `create_a_map.py`: Generates interactive maps
- `change_coordinates_from_ITM.py`: Coordinate conversion
- `israel_network_map.html`: Network visualization output
- Data Visualization:
  * `load_data_and_visualize.py`: Data visualization tools
  * `rain_estimator.py`: Rainfall analysis
  * `wet_and_dry_classification.py`: Weather classification

## Requirements
See `requirements.txt` for detailed package dependencies.

## Contributing
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a new Pull Request

## License
[Your License Here]

## Contact
[Your Contact Information]
