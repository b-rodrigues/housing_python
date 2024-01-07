import polars as pl
import pandas as pd
from skimpy import clean_columns

sheets = pd.ExcelFile("vente-maison-2010-2021.xlsx").sheet_names

def read_clean(excel_file, sheet):
    housing_raw = (
        pd.read_excel(
            io = excel_file,
            sheet_name = sheet,
            skiprows = 10,
            header = 1
        )
        .dropna(
            axis = "columns",
            how = "all"
        )
        .pipe(
            clean_columns
        )
        .assign(
            year = sheet
        )
        )
    return housing_raw

read_clean("vente-maison-2010-2021.xlsx", "2010")

# need to apply this below now
        .rename(
            columns={
                "locality": "commune",
                "n_offers": "nombre_doffres",
                "average_price_nominal_euros": "prix_moyen_annonce_en_courant",
                "average_price_m2_nominal_euros": "prix_moyen_annonce_au_m_en_courant"
            }
        )
     )
