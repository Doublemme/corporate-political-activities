from dask.dataframe import DataFrame
import dask.dataframe as dd
import pandas as pd
import pingouin as pg

from utils.constants.datasets import CHAIRMEN_VIDEOMETRICS

def main():
    traits_df: DataFrame = dd.from_map(pd.read_excel, [CHAIRMEN_VIDEOMETRICS], args=[3])
    # Prepare dataframe before calculating icc
    traits_df = traits_df.drop(columns=["RecipientFirstName", "Unnamed: 5", "AVERAGE"])
    # traits_df = traits_df.dropna()
    traits_df = traits_df.compute()
    traits_df = traits_df.melt(var_name='Rater', value_name='Rating', ignore_index=False).reset_index()

    # Calculate the ICC
    icc_results = pg.intraclass_corr(data=traits_df, targets='index', raters='Rater', ratings='Rating', nan_policy="omit")
    icc_results.to_csv("out/icc_result.csv", columns=icc_results.columns, mode="w", index=False)

if __name__ == "__main__": 
    main()