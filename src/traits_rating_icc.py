from dask.dataframe import DataFrame
import dask.dataframe as dd
import pandas as pd
import pingouin as pg

from utils.constants.datasets import CHAIRMEN_VIDEOMETRICS

RATERS_GRP = [["Antonio", "Roberta", "Alessia"], ["Antonio", "Alessia","Marjorie"], ["Antonio", "Roberta", "Marjorie"], ["Roberta", "Alessia","Marjorie"]]

def main():
    traits_df: DataFrame = dd.from_map(pd.read_excel, [CHAIRMEN_VIDEOMETRICS], args=[3])
    for raters in RATERS_GRP:
        # Prepare dataframe before calculating icc
        filtered_df = traits_df[raters]
        # traits_df = traits_df.dropna()
        filtered_df = filtered_df.compute()
        filtered_df = filtered_df.melt(var_name='Rater', value_name='Rating', ignore_index=False).reset_index()

        # Calculate the ICC
        icc_results = pg.intraclass_corr(data=filtered_df, targets='index', raters='Rater', ratings='Rating', nan_policy="omit")
        icc_results.to_csv("out/ICC_%s.csv" % "-".join([rater[:3] for rater in raters]), columns=icc_results.columns, mode="w", index=False)

if __name__ == "__main__": 
    main()