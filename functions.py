from polars import when, col, concat
from polars.selectors import starts_with
from plotnine import ggplot, geom_line, aes, ggsave

def get_laspeyeres(x_level_data):
    out = (
        x_level_data
        .with_columns(
            when(col("year") == 2010)
            .then(col("average_price_nominal_euros")).over("locality").alias("p0"),
            when(col("year") == 2010)
            .then(col("average_price_m2_nominal_euros")).over("locality").alias("p0_m2")
        )
        .with_columns(
            starts_with("p0").forward_fill().over("locality")
        )
        .with_columns(
            pl = col("average_price_nominal_euros")/col("p0")*100,
            pl_m2 = col("average_price_m2_nominal_euros")/col("p0_m2")*100
        )
        .select(
            ["year", "locality", 'n_offers',
             'average_price_nominal_euros', 'average_price_m2_nominal_euros',
             'p0', 'p0_m2', 'pl', 'pl_m2']
        )
    )
    return out

def get_filtered_data(country_level_data, commune_level_data, commune):
  filtered_data = (
      commune_level_data
      .filter(col("locality").eq(commune))
  )

  data_to_plot = concat(
      [
          country_level_data,
          filtered_data
      ]
  )

  return data_to_plot


def make_plot(data_to_plot):

  out = (
      ggplot(data_to_plot) +
      geom_line(aes(y = "pl_m2",
                    x = "year",
                    group = "locality",
                    colour = "locality"))
  )

  return out
