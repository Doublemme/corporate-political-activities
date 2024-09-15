import dask.dataframe as dd
from dask.dataframe import DataFrame
import pandas as pd

from utils.datasets_utils import prepare_pac_ds, prepare_scac_ds, prepare_lobbying_ds, apply_col_manipulation
from utils.constants.datasets import MAIN_DATASET 

YEARS = [2001,2002,2023]

def main() -> DataFrame:
    main_df: DataFrame = dd.from_map(pd.read_excel, [MAIN_DATASET])
    #TODO: Add the missing years in the main dataframe to have a more complete dataset (so like 20 rows for each organization)
    gen_pac_df: DataFrame = prepare_pac_ds()
    gen_scac_df: DataFrame = prepare_scac_ds()
    gen_lobbying_df: DataFrame = prepare_lobbying_ds()
    merged_df:DataFrame = DataFrame.merge(main_df, gen_pac_df, on=['YEAR','ORGANIZATION'], how="left")
    merged_df = merged_df.merge(gen_lobbying_df, on=['YEAR','ORGANIZATION'], how="left")
    merged_df = merged_df.merge(gen_scac_df, on=['YEAR','ORGANIZATION'], how="left", indicator=True)

    # PAC merging
    main_df["AMOUNT PAC"] = merged_df["PAC AMOUNT"]
    # Lobbying merging
    main_df["AMOUNT LOBBYING"] = merged_df["LOBBY AMOUNT"]
    # Total Amount CPA
    main_df["TOTAL AMOUNT CPA"] = merged_df["PAC AMOUNT"].fillna(0) + merged_df["LOBBY AMOUNT"].fillna(0)
    # SCAC merging
    main_df["COURT PROCEEDINGS"] = merged_df['_merge'].apply(lambda x: 1 if x == 'both' else 0, meta=("_merge", "int64"))
    main_df["C.P. STATUS"] = merged_df.apply(apply_col_manipulation, axis=1, meta=(None, object), col="STATUS")
    main_df["SETTLEMENT GROSS AMOUNT"] = merged_df.apply(apply_col_manipulation, axis=1, meta=(None, object), col="SETLEMENT AMOUNT")
    
    return main_df

if __name__ == "__main__":
    combined_dataframe = main()

    combined_dataframe.to_csv("out/dataset.csv", mode="w", single_file=True, index=False)