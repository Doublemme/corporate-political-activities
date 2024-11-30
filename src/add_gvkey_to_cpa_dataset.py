import pandas as pd

GVKEY_DATASET = "./data/dataset/Originale.xlsx"
CPA_DATASET= "./data/dataset/firms_CPA and Misconduct_dataset.csv"


def main():
    gvkey_df = pd.read_excel(GVKEY_DATASET, dtype='object')
    cpa_df = pd.read_csv(CPA_DATASET)
    cpa_df = cpa_df.merge(gvkey_df[["Unnamed: 1","Unnamed: 2"]], how="left", left_on="ORGANIZATION", right_on="Unnamed: 2")
    cpa_df = cpa_df.drop(columns=["Unnamed: 2"])
    cpa_df = cpa_df.rename(columns={"Unnamed: 1": "GVKEY"})
    cpa_df = cpa_df.reindex(['GVKEY', 'ORGANIZATION', 'YEAR', ' CHIEF EXECUTIVE OFFICER', 'CEO DUALITY','CHAIRMAN', 'AMOUNT PAC', 'AMOUNT LOBBYING', 'TOTAL AMOUNT CPA','COURT PROCEEDINGS', 'C.P. STATUS', 'SETTLEMENT GROSS AMOUNT'], axis=1)
    cpa_df["GVKEY"] = cpa_df['GVKEY'].apply(lambda x: str(x))
    print(gvkey_df)
    cpa_df.to_csv('./out/firms_CPA and Misconduct_dataset_with_gvkey.csv')

if __name__ == "__main__":
    main()