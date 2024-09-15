import datetime
import re
import dask.dataframe as dd
from dask.dataframe import DataFrame
import pandas as pd

from .constants.datasets import PAC_DATASET, SCAC_DATASET, LOBBY_DATASET

def prepare_amout(value):
    if isinstance(value, str) and re.search("[+-]?([0-9]*[.])?[0-9]+.",value):
        return float(value.strip().replace(',', ''))
    if isinstance(value, int):
        return float(value)
    return float("0")

def parse_year(value):
    if isinstance(value, str):
        return value.split("-")[0]
    
    if isinstance(value, datetime.date):
        return value.year
    
    return value

def apply_col_manipulation(row, col):
    if row["_merge"] == "both":
        return row[col]
    
    return None

def prepare_pac_ds():
    pac_df: DataFrame = dd.from_map(pd.read_excel, [PAC_DATASET])
    pac_df['Amount'] = pac_df['Amount'].apply(prepare_amout, meta=('Amount', 'float64'))
    pac_df['Year'] = pac_df['Year'].apply(parse_year, meta=('Year', 'int64')) # Remap Year column
    pac_df = pac_df.groupby(['Year','Organization'], group_keys=True)["Amount"].agg({
        'Amount': 'sum',
    }).reset_index()
    pac_df = pac_df.rename(columns={"Amount": "PAC AMOUNT", "Year": "YEAR", "Organization": "ORGANIZATION"})

    return pac_df

def prepare_lobbying_ds():
    lob_df: DataFrame = dd.from_map(pd.read_excel, [LOBBY_DATASET])
    lob_df['AMOUNT'] = lob_df['AMOUNT'].apply(prepare_amout, meta=('AMOUNT', 'float64'))
    lob_df['YEAR'] = lob_df['YEAR'].apply(parse_year, meta=('YEAR', 'int64')) # Remap Year column
    lob_df = lob_df.groupby(['YEAR','ORGANIZATION'], group_keys=True)["AMOUNT"].agg({
        'AMOUNT': 'sum',
    }).reset_index()
    lob_df = lob_df.rename(columns={"AMOUNT": "LOBBY AMOUNT"})

    return lob_df

def prepare_scac_ds():
    scac_df: DataFrame = dd.from_map(pd.read_excel, [SCAC_DATASET])
    # Remap Staus Value
    scac_df["STATUS"] = scac_df["STATUS"].replace(to_replace={
        "CASE ONGOING": "0",
        "CASE DISMISSED": "-1",
        "CASE SETTLED": "1"
    })
    scac_df['YEAR'] = scac_df['YEAR'].apply(parse_year, meta=('Year', 'int64')) # Remap Year column

    # Group all the data and return the DF
    scac_df = scac_df.groupby(['ORGANIZATION', 'YEAR', 'STATUS']).mean(numeric_only=True).reset_index()

    return scac_df