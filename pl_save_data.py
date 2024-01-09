import polars as pl
import polars.selectors as cs
#from skimpy import clean_columns
import re

datasets = pl.read_excel(
    source = "vente-maison-2010-2021.xlsx",
    sheet_id = 0,
    read_csv_options = {
        # Polars skip empty rows that that come before any data by default, which is quite helpful
        # with Pandas, 10 rows should get skipped for sheets 2010 to 2020, but only 8 for sheet 2021
        # but in the case of Polars, because empty rows get skipped automatically, 6 more rows
        # must get skipped. Check out the Excel file to see what I mean.
        "skip_rows": 6,
        "has_header": True#,
        # new_columns would be the preferred approach, but for some reason when using it on this Excel File,
        # two more empty columns appear. So I could call them a and b and then remove them
        # this is what the commented line below does. However, I decided to apply a function
        # that cleans the column names myself. It’s more complicated, but also more elegant as it would
        # work for any number of columns and in any order
        # "new_columns": ["a", "b","locality", "n_offers", "average_price_nominal_euros", "average_price_m2_nominal_euros"]
    }
)

# Need to use this instead of the above to add year column
def wrap_read_excel(excel_file, sheet):
    pl.read_excel(
        source = excel_file,
        sheet_name = name,
        read_csv_options = {
        "skip_rows": 6,
        "has_header": True
        }
    ).with_columns(pl.lit(sheet).alias("year"))


# This function will be used belowe to clean the column names
def clean_names(string):
    # inspired by https://nadeauinnovations.com/post/2020/11/python-tricks-replace-all-non-alphanumeric-characters-in-a-string/
    clean_string = [s for s in string if s.isalnum() or s.isspace()]
    out = "".join(clean_string).lower()
    out = re.sub(r"\s+", "_", out)
    out = out.encode("ascii", "ignore").decode("utf-8")
    return out

# This row-binds all the datasets (first converting the dict to a list), and
# then renames the columns using the above defined function
# Not as nice as skimpy.clean_columns, put works on Polars DataFrames
raw_data = pl.concat(datasets.values()).select(pl.all().name.map(clean_names))

raw_data = (
    raw_data
    .rename(
      {
        "commune": "locality",
        "nombre_doffres": "n_offers",
        "prix_moyen_annonc_en_courant": "average_price_nominal_euros",
        "prix_moyen_annonc_au_m_en_courant": "average_price_m2_nominal_euros"
      }
    )
    .with_columns(
      pl.col("locality").str.replace_all("Luxembourg.*", "Luxembourg")
    )
    .with_columns(
      pl.col("locality").str.replace_all("P.*tange", "Pétange")
    )
    .with_columns(
      pl.col("locality").str.strip_chars()
    )
)

# Always look at your data
(
raw_data
    .filter(pl.col("average_price_nominal_euros").is_null())
)


# Remove empty locality
raw_data = (
raw_data
    .filter(~pl.col("locality").is_null())
)

# Only keep communes in the data

commune_level_data = (
raw_data
    .filter(~pl.col("locality").str.contains("nationale|offre|Source"))
)

country_level = (
raw_data
    .filter(pl.col("locality").str.contains("nationale"))
    .select(~cs.contains("n_offers"))
)

offers_country = (
raw_data
    .filter(pl.col("locality").str.contains("Total d.offres"))
    .select(["year", "n_offers"])
)

country_level_data = (
    country_level.join(offers_country, on = "year")
    .with_columns(pl.lit("Grand-Duchy of Luxembourg").alias("locality"))
)



#from skimpy import clean_columns

#sheets = pd.ExcelFile("vente-maison-2010-2021.xlsx").sheet_names

# we remove 2021 for now, because data starts at row 8 and not row 10
#sheets.remove("2021")

#dataset = pl.read_excel(
#    source = "vente-maison-2010-2021.xlsx",
#    sheet_name = "2015",
#    read_csv_options = {"skip_rows": 10, "has_header": True}
#)


