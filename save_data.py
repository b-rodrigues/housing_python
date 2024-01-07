import polars as pl
import pandas as pd
from skimpy import clean_columns
from functools import reduce

sheets = pd.ExcelFile("vente-maison-2010-2021.xlsx").sheet_names

# we remove 2021 for now, because data starts at row 8 and not row 10
sheets.remove("2021")

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
        .dropna(
            axis = "rows",
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

def read_clean_excel(sheet):
    return read_clean("vente-maison-2010-2021.xlsx", sheet)

datasets_raw = pd.concat(list(map(read_clean_excel, sheets)), ignore_index = True)

# need to apply this below now
dataset = (
    datasets_raw
    .assign(
        commune = datasets_raw.commune.str.strip()
    )
    .rename(
        columns={
          "locality": "commune",
          "n_offers": "nombre_doffres",
          "average_price_nominal_euros": "prix_moyen_annonce_en_courant",
          "average_price_m2_nominal_euros": "prix_moyen_annonce_au_m_en_courant"
        }
    )
)
