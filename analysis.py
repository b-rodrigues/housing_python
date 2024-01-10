import polars as pl

commune_level_data = pl.read_csv("commune_level_data.csv")
country_level_data = pl.read_csv("country_level_data.csv")

(
commune_level_data
    .group_by(pl.col("locality"))
    .with_columns(
        pl.when(pl.col("year") == "2010")
        .then(pl.col("average_price_nominal_euros"))
        .otherwise(float("NaN"))
        .alias("p0")
    )
)

#commune_level_data <- commune_level_data %>%
#  group_by(locality) %>%
#  mutate(p0 = ifelse(year == "2010", average_price_nominal_euros, NA)) %>%
#  fill(p0, .direction = "down") %>%
#  mutate(p0_m2 = ifelse(year == "2010", average_price_m2_nominal_euros, NA)) %>%
#  fill(p0_m2, .direction = "down") %>%
#  ungroup() %>%
#  mutate(pl = average_price_nominal_euros/p0*100,
#         pl_m2 = average_price_m2_nominal_euros/p0_m2*100)
