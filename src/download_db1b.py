"""
download_db1b.py
------------------

This script automates the retrieval of quarterly market level data from the
U.S. Department of Transportation's Origin and Destination Survey (DB1B).

The DB1B survey contains three relational tables (Coupon, Ticket and
Market) and is distributed as quarterly ZIP archives.  Each archive
contains a comma separated (or pipe separated) text file with many
variables.  Typically, the Coupon and Ticket tables t are combined to
recover origin-destination pairs, but the Market table already includes
market‐level information such as the number of passengers, fares and
distances.  This script focuses on the Market table.

The goal of this script is to download all DB1B Market files for a
specified range of years and quarters, extract a subset of variables and
save them to disk, same with ancillary data.  

The default variable list for the DB1B matches the requirements
specified in the user's project: 

            - List of required columns: `req_cols' 

The script uses the public PREZIP directory on the TranStats web site to
download each quarterly file. For example, the
market file for the first quarter of 2019 is called
``Origin_and_Destination_Survey_DB1BMarket_2019_1.zip`` and is located at

    https://transtats.bts.gov/PREZIP/Origin_and_Destination_Survey_DB1BMarket_2019_1.zip

If the PREZIP URL fails (because BTS occasionally serves content from
``www.transtats.bts.gov``), the script attempts the secondary URL
``https://www.transtats.bts.gov/PREZIP/<file>``.

Ancillary tables such as airport populations, vacation schedules, carrier
lookup codes and CPI data must be supplied in the
project's data directory; placeholders and stub functions are provided
below.

Usage:

    python download_db1b.py \
        --start-year 2005 \
        --end-year 2019 \
        --quarters 1 2 3 4 \
        --out-dir ./db1b_market \
        --data-dir ./data \
        --raw-dir ./data/raw \
        --proc-dir ./data/processed \
        --skip-market False \
        --out-proc-dir ./processed_db1b_market

    - skip-market is a flag to skip the market data processing if already have the files downloaded.
        It jumps to loading the ancillary data.

The script will create the output directory if it does not already
exist and will write one CSV file per quarter.

Limitations:

  * Requires external network access to BTS TranStats servers.
  * DB1B files are large (tens of MB per quarter) and require sufficient storage.
  * BTS may change file naming conventions or locations occasionally.

Author: Giselle Labrador-Badia
Date: 2025-08-16
"""

import argparse
import io
import os
import sys
import zipfile
from typing import Iterable, List, Optional

import pandas as pd  # type: ignore
import requests

# from project
from data_utils import extract_columns_from_zip, get_cpi_index, load_api_keys, _load_table_any
 

req_cols = [
    "OriginCityMarketID",
    "DestCityMarketID",
    "OriginAirportID",
    "DestAirportID",
    "Passengers",
    "MktFare",
    "MktDistance",
    "NonStopMiles",
    "MktCoupons",
    "TkCarrier",
    "TkCarrierChange",
    "OpCarrierChange",   # optional
    "RPCarrier",         # optional
    "Year",
    "Quarter",
]


def build_db1b_url(year: int, quarter: int) -> List[str]:
    """Construct possible URLs for a DB1B market file.

    The TranStats web site hosts DB1B files under both ``transtats.bts.gov``
    and ``www.transtats.bts.gov``.  This helper returns a list of URLs to
    try sequentially.

    Parameters
    ----------
    year : int
        Four‑digit year (e.g. 2019).
    quarter : int
        Quarter number (1-4).

    Returns
    -------
    list of str
        Candidate URLs pointing to the ZIP archive.
    """
    filename = f"Origin_and_Destination_Survey_DB1BMarket_{year}_{quarter}.zip"
    base_urls = [
        "https://transtats.bts.gov/PREZIP",
        "https://www.transtats.bts.gov/PREZIP",
    ]
    return [f"{base}/{filename}" for base in base_urls]


def download_file(urls: Iterable[str], dest: str, session: Optional[requests.Session] = None) -> None:
    """Download the first successfully retrieved URL to ``dest``.

    If multiple URLs are provided, they are tried in order until a
    successful response (HTTP status 200) is obtained.  The file is
    streamed to disk in chunks to avoid loading the entire content into
    memory.  If no URL can be downloaded, a ``RuntimeError`` is raised.

    Parameters
    ----------
    urls : iterable of str
        Candidate URLs pointing to the file.
    dest : str
        Path on disk where the downloaded file should be written.
    session : requests.Session, optional
        Optional session object for connection pooling and custom
        configuration.  If ``None``, a new session is created.
    """

    # before attempting to download see if file is already present in dest
    if os.path.exists(dest):
        print(f"\033[33mSkipping download; {dest} already exists.\033[0m")
        return
    
    sess = session or requests.Session()
     # Disable SSL verification on the session (use with caution!)
    sess.verify = False
    for url in urls:
        try:
            with sess.get(url, stream=True, timeout=60) as resp:
                if resp.status_code == 200:
                    # Create directory if needed
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    with open(dest, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=8192):
                            if chunk:  # filter out keep‑alive chunks
                                fh.write(chunk)
                    print(f" - Downloaded {url} -> {dest}")
                    return
                else:
                    print(f"URL {url} returned status {resp.status_code}; trying next.")
        except requests.RequestException as e:
            print(f"Error downloading {url}: {e}")
            continue
    raise RuntimeError(f"All download attempts failed for {urls}")



def process_quarter(year: int, quarter: int, required_columns: List[str], out_dir: str,
                    out_proc_dir: str = "../data/processed/db1b_market",
                    session: Optional[requests.Session] = None) -> str:
    """Download and process a single DB1B market quarter.

    Parameters
    ----------
    year : int
        Year of the quarter.
    quarter : int
        Quarter (1-4).
    required_columns : list of str
        Columns to extract.
    out_dir : str
        Directory in which to save the processed CSV file.
    session : requests.Session, optional
        Optional session to reuse for multiple requests.

    Returns
    -------
    str
        Path to the processed CSV file.
    """
    urls = build_db1b_url(year, quarter)
    zip_path = os.path.join(out_dir, f"db1b_market_{year}q{quarter}.zip")
    csv_path = os.path.join(out_proc_dir, f"db1b_market_{year}q{quarter}.csv")
    # Skip download if zip already exists
    if not os.path.exists(zip_path):
        download_file(urls, zip_path, session=session)
    else:
        print(f"\033[33mSkipping download; {zip_path} already exists.\033[0m")
    # Extract the requested columns and save as CSV
    df = extract_columns_from_zip(zip_path, required_columns)
    df.to_csv(csv_path, index=False)
    print(f"\033[32mWrote filtered data to {csv_path}\033[0m")
    return csv_path


def load_ancillary_data(data_dir: str) -> dict:
    """Load additional datasets required for the project.

    This function loads the auxiliary datasets referenced in the project
    description.  You should place these files in the ``data_dir`` and
    adjust the file names below as necessary.  The returned dictionary
    contains pandas objects keyed by a descriptive name.

    Expected files:

      * ``L_CITY_MARKET_ID.csv`` - city market lookup table.
      * ``populations.csv`` - airport, year and population.
      * ``vacations.dta``, ``lookup_and_hub_r.dta``, ``slot_controlled.dta`` -
        Stata datasets with additional variables.
      * ``CPIAUCSL.dta`` - monthly CPI from FRED; will be collapsed to annual
        means and indexed to 2008 in the returned dictionary.

    Parameters
    ----------
    data_dir : str
        Directory containing the ancillary files.

    Returns
    -------
    dict
        Dictionary of pandas DataFrames or Series.
    """
    result: dict = {}

    # * Load all datasets with multiple format support
    datasets = [
        ("city_market_lookup", ["L_CITY_MARKET_ID.csv"]),
        ("population", ["populations.csv"]),
        ("vacations", ["vacations.R"]),
        ("lookup_and_hub_r", ["lookup_and_hub_r.R"]),
        ("slot_controlled", ["slot_controlled.R"]),
        ("t_master_cord", ["T_MASTER_CORD.csv"])
    ]
    
    for key, filenames in datasets:
        loaded = False
        for fname in filenames:
            print(f"* Trying to load {key} from {fname}...")
            path = os.path.join(data_dir, fname)
            if os.path.exists(path):
                try:

                    df = _load_table_any(path)
                    if not isinstance(df, pd.DataFrame):
                        raise ValueError("Loaded object is not a DataFrame")
                    result[key] = df

                    if not result[key].empty:
                        print(f"\033[32m - Loaded {path}\033[0m")
                        print("     - Columns:", ", ".join(result[key].columns))
                        print(f"     - Shape: {result[key].shape}")
                    else:
                        print(f"\033[33m - Loaded {path} (empty dataset)\033[0m")
                    loaded = True
                    break
                except Exception as e:
                    print(f"\033[31mError loading {path}: {e}\033[0m")
                    continue
        
        if not loaded:
            print(f"\033[33m Warning: No readable file found for {key} dataset; will be empty.\033[0m")
            result[key] = pd.DataFrame()

        # * save in csv format for easier future loading in processed dir (instead of raw)
        if not result[key].empty:
            csv_path = os.path.join(data_dir.replace("raw", "processed"), f"{key}.csv")
            result[key].to_csv(csv_path, index=False)
            print(f"\033[35m - Saved {key} to {csv_path}\033[0m")

    # * Load CPI data and compute annual index relative to 2008

    cpi_path = os.path.join(data_dir, "cpi_index.csv")

    # if it dos't exist download it from FRED using the get_cpi_index function
    if not os.path.exists(cpi_path):
        print(f"\033[33m CPI data not found at {cpi_path}; downloading from FRED...\033[0m")
        try:
            cpi = get_cpi_index(base_year=2008, api_key=load_api_keys()["fred_key"], 
                                output_path=cpi_path)
            print(f"\033[32m Downloaded CPI data to {cpi_path}\033[0m")
        except Exception as e:
            print(f"\033[31m Failed to download CPI data: {e}\033[0m")
            result["cpi_index"] = pd.Series(dtype=float)
            return result
    if os.path.exists(cpi_path):
        cpi = pd.read_csv(cpi_path)
        # Ensure the dataset has 'year' and 'value' columns
        if "year" in cpi.columns and "value" in cpi.columns:
            result["cpi_index"] = pd.Series(cpi.set_index("year")["value"], name="cpi_index")
            print(f"\033[32m - Loaded CPI index (base 2008) from {cpi_path}\033[0m")
        else:
            print(f"\033[33m Warning: Unexpected columns in {cpi_path}; skipping CPI computation.\033[0m")
            result["cpi_index"] = pd.Series(dtype=float)
    else:
        print(f"\033[33m Warning: {cpi_path} not found; CPI index will be empty.\033[0m")
        result["cpi_index"] = pd.Series(dtype=float)
    return result


def main(args: Optional[List[str]] = None) -> None:
    """Entry point for the command line interface."""
    parser = argparse.ArgumentParser(
        description="Download and process DB1B market data for specified years and quarters.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=2005,
        help="First year to download (inclusive)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=2019,
        help="Last year to download (inclusive)",
    )
    parser.add_argument(
        "--quarters",
        type=int,
        nargs="+",
        default=[1, 2, 3, 4],
        choices=[1, 2, 3, 4],
        help="Quarter numbers to download",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default="../data/raw/db1b_market",
        help="Directory to store downloaded files from TranStats",
    )
    parser.add_argument(
    "--out-proc-dir",
    type=str,
    default="../data/processed/db1b_market",
    help="Directory to store processed files from TranStats",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="../data/raw/other",
        help="Directory containing ancillary datasets (lookup tables, etc.)",
    )
    parser.add_argument(
        "--raw-dir",
        type=str,
        default="../data/raw",
        help="Directory containing raw datasets"
    )
    parser.add_argument(
        "--proc-dir",
        type=str,
        default="../data/procesessed",
        help="Directory containing raw datasets"
    )
    parser.add_argument(
        "--skip-market",
        type=str,
        default="False",
        help="Skip market data processing",
    )
    parsed = parser.parse_args(args)
    years = range(parsed.start_year, parsed.end_year + 1)

    print("\033[1;34m \n-------------------  Starting DB1B market data download and processing... ----------------- \033[0m")

    # Create the output directory
    os.makedirs(parsed.out_dir, exist_ok=True)
    os.makedirs(parsed.out_proc_dir, exist_ok=True)

    if parsed.skip_market.lower() != "true":
        # Reuse a session for all downloads to improve performance
        session = requests.Session()
        for year in years:
            for quarter in parsed.quarters:
                try:
                    print(f"\033[1;34m *********** Processing {year} Q{quarter}... ***********\033[0m")
                    process_quarter(year, quarter, req_cols, parsed.out_dir, parsed.out_proc_dir, session=session)
                except Exception as ex:
                    print(f"\033[31mFailed to process {year} Q{quarter}: {ex}\033[0m")
    else:
        print("\033[33mSkipping market data processing as per --skip-market flag.\033[0m")

    # * Load ancillary datasets 
    ancillary = load_ancillary_data(parsed.data_dir)

    # At this point I could merge the DB1B market files with the ancillary -> do this on Stata

    print("\033[1;34mProcessing complete. -------------------------------------------------------------------------- \033[0m")


if __name__ == "__main__":
    main()