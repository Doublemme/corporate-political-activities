import datetime
import re
import dask.dataframe as dd
from dask.dataframe import DataFrame
import pandas as pd

from .constants.datasets import PAC_DATASET, SCAC_DATASET, LOBBY_DATASET, META_TRAITS, CHAIRMEN_TRANCE, CHAIRMEN_VIDEOMETRICS

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

def get_traits_base_cols(cell):
    if isinstance(cell, str):
        [scale, chairman,question] = cell.split("_")
        return [chairman,int(question),scale]
    return cell

def get_avg_with_traits(row):
    traits = get_traits_base_cols(row["RecipientFirstName"])
    return pd.Series([*traits, row["AVERAGE"]], index=["id", "Numero domanda", "SCALE", "AVERAGE"])

def reverse_hexaco_avg(row):
    if isinstance(row["NOTE"], str) and row["NOTE"] == "REVERSE":
        row["AVERAGE"] = 8.00 - float(row["AVERAGE"])
        return row

    return row    

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

def prepare_chairmen_df():
    charimen_df: DataFrame = dd.from_map(pd.read_excel, [CHAIRMEN_VIDEOMETRICS])
    traits_df: DataFrame = dd.from_map(pd.read_excel, [CHAIRMEN_VIDEOMETRICS], args=[3])
    traits_scales_df: DataFrame = dd.from_map(pd.read_excel, [CHAIRMEN_VIDEOMETRICS], args=["Scales"])

    # Create in chairmen_df the columns required for the analysis
    # based on the traits_scales_df SCALE and SUBSCALE values
    grouped_traits_scales_df = traits_scales_df.groupby(["SCALE", "SUBSCALE"]).mean(numeric_only=True).reset_index()
    traits_cols:list[str] = (grouped_traits_scales_df["SCALE"].astype(str) + "_" + grouped_traits_scales_df["SUBSCALE"].astype(str)).compute().tolist()
    print(traits_cols)

    # Manipulate traits_df to get a view datframe to help with all the operations
    view_df: DataFrame = dd.from_pandas(pd.DataFrame())
    # Add average value sum for each question for each chairman and Calculate the REVERSE HEXACO
    view_df: DataFrame = dd.from_pandas(pd.DataFrame())
    result = traits_df.apply(get_avg_with_traits, axis=1, meta={"id": str, "Numero domanda": int, "SCALE": str, "AVERAGE": float})
    view_df = view_df.assign(id=result["id"], Numero_domanda=result["Numero domanda"], SCALE=result["SCALE"], AVERAGE=result["AVERAGE"])
    view_df = view_df.rename(columns={"Numero_domanda": "Numero domanda"})
    filtered_traits_df:DataFrame = traits_scales_df[["Numero domanda", "SUBSCALE", "NOTE"]]
    view_df = view_df.merge(filtered_traits_df, on="Numero domanda", how="left")
    view_df = view_df.apply(reverse_hexaco_avg, axis=1, meta={"id": str, "Numero domanda": int, "SCALE": str, "AVERAGE": float, "SUBSCALE": str, "NOTE": object})
    view_df.to_csv("out/view.csv",  mode="w", single_file=True, index=False)
    view_df = view_df.compute()
    view_df = view_df.drop(columns=["NOTE"], axis=1)
    # TODO: Combine everything with the chairmen_df
    # TODO: Get the sum of all subscale average grouped by scale
    sums_df: DataFrame = dd.from_pandas(pd.DataFrame())
    summed_avg_df = view_df.groupby(["id", "Numero domanda", "SCALE", "SUBSCALE"]).sum().reset_index()

    summed_avg_df['column_name'] = summed_avg_df['SCALE'] + '_' + summed_avg_df['SUBSCALE']

    # Pivot the dataframe to get the desired structure: one row per 'id' with summed values for each scale_subscale combination
    pivot_df = summed_avg_df.pivot_table(
        index='id', 
        columns='column_name', 
        values='AVERAGE', 
        aggfunc='sum', 
        fill_value=0
    )

    # Reset the index to make 'id' a column (optional)
    pivot_df.reset_index(inplace=True)

    # Display the output
    print(pivot_df)
    pivot_df.to_csv("out/chairmen_traits_dataset.csv", mode="w", index=False)