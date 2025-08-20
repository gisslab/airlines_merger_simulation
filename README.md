# airlines_merger_simulation

This project demonstrates how to estimate market demand and simulate mergers, designed as a teaching tool for master's students in the University of Wisconsin–Madison Economics program.

## OVERVIEW:
This project estimates demand and simulates the merger process between airlines.

## PROJECT STRUCTURE:
- src/: Code that downloads, cleans, and estimates demand. 
- data/: Input data files (airline data, routes, financial data)
    - raw/: Raw data from sources
    - processed/: Processed data from functions
- doc/: Useful documentation and screenshots for running example and outputs.  
- secured_keys/: This folder must contain a JSON file (api_keys.json) used to access FRED and Census if necessary. 

## PROGRESS AND RUNNING ORDER:
1. Run data downloads .py files, see instructions (Run: download_db1b.py)
2. Preprocess downloaded data: processing_data.do
3. Demand estimation: demand_estimation.do
4. Merger simulation → TODO

## REQUIREMENTS:
- Python 3.x
- Required packages listed in requirements.txt
- Stata 17

For detailed information about specific files, please refer to inline documentation.
