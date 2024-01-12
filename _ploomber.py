from polars import read_csv, col
from ploomber.micro import dag_from_functions
from ploomber.io import unserializer_pickle
from functions import get_laspeyeres, get_filtered_data, make_plot

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

def lux_filtered_data(laspeyeres_country_data, laspeyeres_commune_data):
    out = get_filtered_data(laspeyeres_country_data,
                            laspeyeres_commune_data,
                            "Luxembourg")
    return out

def esch_filtered_data(laspeyeres_country_data, laspeyeres_commune_data):
    out = get_filtered_data(laspeyeres_country_data,
                            laspeyeres_commune_data,
                            "Esch-sur-Alzette")
    return out

def mamer_filtered_data(laspeyeres_country_data, laspeyeres_commune_data):
    out = get_filtered_data(laspeyeres_country_data,
                            laspeyeres_commune_data,
                            "Mamer")
    return out

def schengen_filtered_data(laspeyeres_country_data, laspeyeres_commune_data):
    out = get_filtered_data(laspeyeres_country_data,
                            laspeyeres_commune_data,
                            "Schengen")
    return out

def luxembourg_plot(lux_filtered_data):
    luxembourg_plot = make_plot(lux_filtered_data)
    luxembourg_plot.save(filename = "lux_plot.pdf",
                         path = "ploomber_plots/")

    return 1

def esch_plot(esch_filtered_data):
    esch_plot = make_plot(esch_filtered_data)
    esch_plot.save(filename = "esch_plot.pdf",
                         path = "ploomber_plots/")

    return 1

def mamer_plot(mamer_filtered_data):
    mamer_plot = make_plot(mamer_filtered_data)
    mamer_plot.save(filename = "mamer_plot.pdf",
                         path = "ploomber_plots/")

    return 1

def schengen_plot(schengen_filtered_data):
    schengen_plot = make_plot(schengen_filtered_data)
    schengen_plot.save(filename = "schengen_plot.pdf",
                         path = "ploomber_plots/")

    return 1

dag = dag_from_functions(
    [
        read_commune_level_data,
        read_country_level_data,
        laspeyeres_commune_data,
        laspeyeres_country_data,
        lux_filtered_data,
        luxembourg_plot,
        esch_filtered_data,
        esch_plot,
        mamer_filtered_data,
        mamer_plot,
        schengen_filtered_data,
        schengen_plot,
    ],
    output="cache"
)

if __name__ == "__main__":
    dag.build()
