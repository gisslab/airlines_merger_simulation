"""
download_db1b.py
------------------

This script automates the retrieval of quarterly market level data from the
U.S. Department of Transportation’s Origin and Destination Survey (DB1B).

The DB1B survey contains three relational tables (Coupon, Ticket and
Market) and is distributed as quarterly ZIP archives.  Each archive
contains a comma separated (or pipe separated) text file with many
variables.  Typically, the Coupon and Ticket tables t are combined to
recover origin-destination pairs, but the Market table already includes
market‐level information such as the number of passengers, fares and
distances.  This script focuses on the Market table.

The goal of this script is to download all DB1B Market files for a
specified range of years and quarters, extract a subset of variables and
save them to disk.  The default variable list matches the requirements
specified in the user’s project:

    - ORIGIN_CITY_MARKET_ID
    - DEST_CITY_MARKET_ID
    - ORIGIN_AIRPORT_ID
    - DEST_AIRPORT_ID
    - PASSENGERS
    - MARKET_FARE
    - MARKET_DISTANCE
    - NONSTOP_MILES
    - COUPONS
    - TK_CARRIER
    - YEAR
    - QUARTER

The script uses the public PREZIP directory on the TranStats web site to
download each quarterly file.  File names follow a predictable pattern
that depends on the table type, year and quarter.  For example, the
market file for the first quarter of 2019 is called
``Origin_and_Destination_Survey_DB1BMarket_2019_1.zip`` and is located at

    https://transtats.bts.gov/PREZIP/Origin_and_Destination_Survey_DB1BMarket_2019_1.zip

If the PREZIP URL fails (because BTS occasionally serves content from
``www.transtats.bts.gov``), the script attempts the secondary URL
``https://www.transtats.bts.gov/PREZIP/<file>``.  Users behind strict
firewalls or proxies may need to configure the `requests` session
accordingly (for example by setting the ``HTTP_PROXY`` or ``HTTPS_PROXY``
environment variables).

The script also demonstrates how to join the DB1B market data with
ancillary tables such as airport populations, vacation schedules, carrier
lookup codes and CPI data.  Those datasets must be supplied in the
project’s data directory; placeholders and stub functions are provided
below.

Usage:

    python download_db1b.py \
        --start-year 2005 \
        --end-year 2019 \
        --quarters 1 2 3 4 \
        --out-dir ./db1b_market \
        --data-dir ./data

The script will create the output directory if it does not already
exist and will write one CSV file per quarter.

Limitations:

  * This script requires external network access to the BTS TranStats
    servers.  If your environment blocks connections or requires SSL
    certificates, you may need to run the script from a different
    location.
  * The DB1B files are large (tens of megabytes per quarter) and
    downloading many years of data can take considerable time and disk
    space.  Ensure you have sufficient storage available.
  * BTS occasionally changes the naming convention or location of the
    PREZIP files.  If downloads fail, check the TranStats website for
    updated file names.

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


req_cols = [
    "ORIGIN_CITY_MARKET_ID",
    "DEST_CITY_MARKET_ID",
    "ORIGIN_AIRPORT_ID",
    "DEST_AIRPORT_ID",
    "PASSENGERS",
    "MARKET_FARE",
    "MARKET_DISTANCE",
    "NONSTOP_MILES",
    "COUPONS",
    "TK_CARRIER",
    "YEAR",
    "QUARTER",
]

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


def extract_columns_from_zip(zip_path: str, required_columns: List[str]) -> pd.DataFrame:
    """Extract and return only the specified columns from a DB1B market ZIP.

    The DB1B market ZIP file contains a single delimited text file
    (usually with a ``.csv`` or ``.dat`` extension).  This function
    reads the file directly from the ZIP archive into a pandas DataFrame
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
    # Load city market lookup
    city_file = os.path.join(data_dir, "L_CITY_MARKET_ID.csv")
    if os.path.exists(city_file):
        result["city_market_lookup"] = pd.read_csv(city_file)
        print(f"Loaded {city_file}")
    else:
        print(f"\033[33mWarning: {city_file} not found; city market lookup will be empty.\033[0m")
        result["city_market_lookup"] = pd.DataFrame()
    # Load population data
    pop_file = os.path.join(data_dir, "populations.csv")
    if os.path.exists(pop_file):
        result["population"] = pd.read_csv(pop_file)
        print(f"Loaded {pop_file}")
    else:
        print(f"\033[33mWarning: {pop_file} not found; population data will be empty.\033[0m")
        result["population"] = pd.DataFrame()
    # Load Stata datasets using pandas.read_stata
    stata_files = [
        ("vacations", "vacations.dta"),
        ("lookup_and_hub_r", "lookup_and_hub_r.dta"),
        ("slot_controlled", "slot_controlled.dta"),
    ]
    for key, fname in stata_files:
        path = os.path.join(data_dir, fname)
        if os.path.exists(path):
            result[key] = pd.read_stata(path)
            print(f"Loaded {path}")
        else:
            print(f"\033[33mWarning: {path} not found; {key} dataset will be empty.\033[0m")
            result[key] = pd.DataFrame()
    # Load CPI data and compute annual index relative to 2008
    cpi_path = os.path.join(data_dir, "CPIAUCSL.dta")
    if os.path.exists(cpi_path):
        cpi = pd.read_stata(cpi_path)
        # Ensure the dataset has 'year' and 'value' columns
        # If the dataset uses a different structure, adjust accordingly.
        if "DATE" in cpi.columns and "VALUE" in cpi.columns:
            cpi["YEAR"] = pd.to_datetime(cpi["DATE"]).dt.year
            annual_mean = cpi.groupby("YEAR")["VALUE"].mean()
            base = annual_mean.loc[2008]
            index = (annual_mean / base) * 100
            result["cpi_index"] = index
            print(f"Computed CPI index relative to 2008 from {cpi_path}")
        else:
            print(f"\033[33mWarning: Unexpected columns in {cpi_path}; skipping CPI computation.\033[0m")
            result["cpi_index"] = pd.Series(dtype=float)
    else:
        print(f"\033[33mWarning: {cpi_path} not found; CPI index will be empty.\033[0m")
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
        help="Directory to store downloaded files",
    )
    parser.add_argument(
    "--out-proc-dir",
    type=str,
    default="../data/processed/db1b_market",
    help="Directory to store processed files",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="../data/raw/",
        help="Directory containing ancillary datasets (lookup tables, etc.)",
    )
    parsed = parser.parse_args(args)
    years = range(parsed.start_year, parsed.end_year + 1)

    # Create the output directory
    os.makedirs(parsed.out_dir, exist_ok=True)
    os.makedirs(parsed.out_proc_dir, exist_ok=True)

    # Reuse a session for all downloads to improve performance
    session = requests.Session()
    for year in years:
        for quarter in parsed.quarters:
            try:
                print(f"\033[1;34m *********** Processing {year} Q{quarter}... ***********\033[0m")
                process_quarter(year, quarter, req_cols, parsed.out_dir, parsed.out_proc_dir, session=session)
            except Exception as ex:
                print(f"\033[31mFailed to process {year} Q{quarter}: {ex}\033[0m")
    # Load ancillary datasets (optional)
    ancillary = load_ancillary_data(parsed.data_dir)
    # At this point you could merge the DB1B market files with the ancillary
    # datasets using pandas.  For example:
    #
    # for csv_path in sorted(glob.glob(os.path.join(parsed.out_dir, 'db1b_market_*.csv'))):
    #     df = pd.read_csv(csv_path)
    #     df = df.merge(ancillary['city_market_lookup'], left_on='ORIGIN_CITY_MARKET_ID', right_on='CITY_MARKET_ID', how='left')
    #     # Additional merges and transformations here
    #     df.to_csv(csv_path.replace('.csv', '_enriched.csv'), index=False)
    #
    print("Processing complete.")


if __name__ == "__main__":
    main()