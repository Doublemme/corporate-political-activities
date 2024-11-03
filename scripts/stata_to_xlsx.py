import os
import pandas as pd

META_TRAITS_STATA = "./data/dataset/ceo_chairmen_traits/Meta_ABCDE.dta"

def main():
    stata_df = pd.read_stata(META_TRAITS_STATA)
    
    path, file = os.path.split(META_TRAITS_STATA)
    file_name = file.split('.')[0]
    
    stata_df.to_excel(os.path.join(path, "%s.xlsx" %(file_name)), columns=stata_df.columns)


if __name__ == "__main__":
    main()
