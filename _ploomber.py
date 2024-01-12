from polars import read_csv, col
from ploomber.micro import dag_from_functions
from ploomber.io import unserializer_pickle
from functions import get_laspeyeres, filtered_data, make_plot

def read_target(target):
    return unserializer_pickle(dag[target].product)

def read_commune_level_data():
    commune_level_data = read_csv("commune_level_data.csv")
    return commune_level_data

def read_country_level_data():
    country_level_data = read_csv("country_level_data.csv")
    return country_level_data

def laspeyeres_commune_data(read_commune_level_data):
    commune_data = get_laspeyeres(read_commune_level_data)
    return commune_data

def laspeyeres_country_data(read_country_level_data):
    country_data = get_laspeyeres(read_country_level_data)
    return country_data

def lux_filtered_data(laspeyeres_commune_data, laspeyeres_country_data):
    out = filtered_data(laspeyeres_commune_data,
                        laspeyeres_country_data,
                        "Luxembourg")
    return out

def luxembourg_plot(lux_filtered_data):
    luxembourg_plot = make_plot(lux_filtered_data)
    luxembourg_plot.save(filename = "lux_plot.pdf",
                         path = "ploomber_plots/")

    return "Plot save to disk"

dag = dag_from_functions(
    [
        read_commune_level_data,
        read_country_level_data,
        laspeyeres_commune_data,
        laspeyeres_country_data,
        lux_filtered_data,
        luxembourg_plot
    ],
    output="cache"
)

if __name__ == "__main__":
    dag.build()
