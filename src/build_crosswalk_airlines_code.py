"""
author: Giselle Labrador-Badia
This script downloads and processes a crosswalk of airline codes to airline names

Run as:
    python src/build_crosswalk_airlines_code.py
"""



import pandas as pd
import requests
from io import StringIO
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# DOT source for airline codes and names (BTS)
DOT_URL = "https://www.transtats.bts.gov/Download_Lookup.asp?Lookup=L_UNIQUE_CARRIERS"

# Output path
out_path = "../data/processed/other/crosswalk_airlines_code.csv"



# Download the CSV from DOT using a session with SSL verification disabled
print("Downloading airline codes from DOT...")
session = requests.Session()
session.verify = False
resp = session.get(DOT_URL)
resp.raise_for_status()

# The file is tab-delimited, with header
csv_data = resp.content.decode("utf-8")
df = pd.read_csv(StringIO(csv_data), sep="\t")

# Clean up columns
# Typical columns: Code, Description, ...
df = df.rename(columns={"Code": "carrier_code", "Description": "carrier_name"})
df = df[["carrier_code", "carrier_name"]]

# Remove blank codes
df = df[df["carrier_code"].notnull() & (df["carrier_code"].str.strip() != "")]

# Save to the correct location (ensure directory exists)
os.makedirs(os.path.dirname(out_path), exist_ok=True)
df.to_csv(out_path, index=False)
print(f"Crosswalk saved to {out_path}")
