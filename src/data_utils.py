"""
data_utils.py
------------------

This module provides utility functions for data handling in the airline merger simulation project.

It includes functions to fetch and process economic data, such as the Consumer Price Index (CPI),
from external APIs like FRED (Federal Reserve Economic Data). It also includes functions to read and extract specific columns
from ZIP archives.

Author: Giselle Labrador-Badia
Date: 2025-08-16
"""

import os
import pandas as pd
import requests
import io
import sys
import zipfile
from typing import Iterable, List, Optional, Union

import json
from pathlib import Path


def load_api_keys(path: Union[str, None] = None) -> dict:
    """
    Load API keys from a JSON file and return them as a dictionary.
    
    Example usage:
        keys = load_api_keys()
        print(keys["census_key"])

    Parameters
    ----------
    path : str, optional
        Path to the JSON file. Defaults to '../secured_keys/api_keys.json'.
    
    Returns
    -------
    dict
        Dictionary containing 'census_key' and 'fred_key'.
    """
    if path is None:
        path = "../secured_keys/api_keys.json"
    
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"API keys file not found: {path}")
    
    with open(path, "r") as f:
        keys = json.load(f)
    
    return keys


def extract_columns_from_zip(zip_path: str, required_columns: List[str]) -> pd.DataFrame:
    """Extract and return only the specified columns from a DB1B market ZIP or other archive.

    This function reads the file directly from the ZIP archive into a pandas DataFrame
    and returns only the columns listed in ``required_columns``.  Column
    names are treated case‐insensitively.

    Parameters
    ----------
    zip_path : str
        Path to the downloaded ZIP archive.
    required_columns : list of str
        Columns to extract from the file.  The function will raise a
        ``KeyError`` if any of these columns are not found.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing only the requested columns.
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Assume the first non‑directory member is the data file
        data_members = [m for m in zf.namelist() if not m.endswith("/")]
        if not data_members:
            raise RuntimeError(f"No files found inside {zip_path}")
        data_file = data_members[0]
        with zf.open(data_file) as fh:
            # Try comma delimiter first; if that fails, fall back to pipe
            try:
                df = pd.read_csv(fh, low_memory=False)
            except Exception:
                fh.seek(0)
                df = pd.read_csv(fh, delimiter="|", low_memory=False)

    # Normalize column names to upper case for matching
    col_map = {c.upper(): c for c in df.columns}
    missing = [col for col in required_columns if col.upper() not in col_map]
    if missing:
        raise KeyError(f"Missing columns in {zip_path}: {', '.join(missing)}")
    # Select and return in original casing
    selected = {col: df[col_map[col.upper()]] for col in required_columns}
    return pd.DataFrame(selected)


def get_cpi_index(base_year: int = 2008, 
                output_path: Union[str, None] = None,
                api_key: Union[str, None] = None) -> pd.Series:
    """
    Fetch monthly CPIAUCSL data from FRED, compute annual averages, and index to base_year=100.
    
    Example usage:
        cpi_index = get_cpi_index(base_year=2008, api_key='YOUR_FRED_API_KEY')
        print(cpi_index.head())

    Parameters
    ----------
    base_year : int, optional
        The year whose mean CPI will serve as the base (default is 2008).
    api_key : str, optional
        Your FRED API key; if not provided, looks for FRED_API_KEY in environment.
    
    Returns
    -------
    pandas.Series
        A series of annual CPI indexes (base_year=100), indexed by year.
    """
    api_key = api_key or os.getenv('FRED_API_KEY')
    if not api_key:
        raise ValueError("You must supply a FRED API key (see https://fred.stlouisfed.org).")

    # Call the FRED API for CPIAUCSL observations
    url = 'https://api.stlouisfed.org/fred/series/observations'
    params = {
        'series_id': 'CPIAUCSL',
        'api_key': api_key,
        'file_type': 'json',
        'observation_start': '2000-01-01',  # start date can be adjusted
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()['observations']

    # Convert to DataFrame and parse types
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df['value'] = pd.to_numeric(df['value'])
    df['year'] = df['date'].dt.year

    # Compute annual average CPI and index it
    annual_mean = df.groupby('year')['value'].mean()
    base_value = annual_mean.loc[base_year]
    cpi_index = (annual_mean / base_value) * 100

    # save the CPI index to a CSV file in output_path
    if output_path:
        cpi_index.to_csv(output_path, header=True)
    print(f"\033[32m - CPI data successfully fetched and indexed to base year {base_year}.\033[0m")
    return cpi_index


