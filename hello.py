import polars as pl
import pandas as pd

sheets = pd.ExcelFile("vente-maison-2010-2021.xlsx").sheet_names

housing_raw = (
    pd.read_excel(
        io = "vente-maison-2010-2021.xlsx",
        sheet_name = "2010",
        skiprows = 10,
        header = 1
    )
    .dropna(
        axis = "columns",
        how = "all")
    )
