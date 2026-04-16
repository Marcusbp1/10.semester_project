

library(tseries)
library(urca)
library(dplyr)
library(lubridate)
library(zoo)


df1 <- read.csv("Speciale/data/processed/60min_DK1_el_prod.csv") |> 
    rename(OnShore_Actual  = OnshoreWindPower, OffShore_Actual  = OffshoreWindPower, Solar_Actual = SolarPower)
df2 <- read.csv("Speciale/data/processed/60min_DK1_price_imbalance_new.csv") |> mutate(Minutes60UTC = as_datetime(Minutes60UTC))
df3 <- read.csv("Speciale/data/processed/60min_DK1_price_imbalance_old.csv") |> mutate(Minutes60UTC = as_datetime(Minutes60UTC))
df4 <- read.csv("Speciale/data/processed/60min_DK1_price_spot_old.csv") |> mutate(Minutes60UTC = as_datetime(Minutes60UTC))
df5 <- read.csv("Speciale/data/processed/60min_DK1_prod_forecast.csv") |> 
    rename(OnShore_Forecast  = OnshoreWindPower, OffShore_Forecast  = OffshoreWindPower, Solar_Forecast = SolarPower)

## Prices ##

price_df <- merge(df3, df4, by="Minutes60UTC", all=TRUE) |> 
    arrange(Minutes60UTC) |> 
    select(Minutes60UTC, ImbalancePriceEUR, SpotPriceEUR) |>
    mutate(Minutes60UTC = as_datetime(Minutes60UTC)) |>
    filter(Minutes60UTC >= "2009-01-02T00:00:00", Minutes60UTC <= "2025-03-04T11:00:00")
#View(price_df)

# Verify they are gone
is.na(price_df) |> sum() # Check for missing values again

price_df <- bind_rows(price_df, df2) |>
  arrange(Minutes60UTC) |>
  # Just in case there's an overlap on the transition day, keep only unique timestamps
  distinct(Minutes60UTC, .keep_all = TRUE) |>
  select(Minutes60UTC, ImbalancePriceEUR, SpotPriceEUR)

# 3. Verify the timeline
print(range(price_df$Minutes60UTC))
#View(price_df)


## Production ##
production_df <- merge(df1, df5, by="Minutes60UTC", all.x=TRUE) |> 
    arrange(Minutes60UTC) |> filter(Minutes60UTC >= "2019-10-10T11:00:00") |> 
    select(Minutes60UTC, OnShore_Forecast, OffShore_Forecast, OnShore_Actual, OffShore_Actual,
    Solar_Actual, Solar_Forecast) |>
    mutate(Minutes60UTC = as_datetime(Minutes60UTC))


price_df |> filter(Minutes60UTC >= "2023-01-01T00:00:00")
production_df |> filter(Minutes60UTC >= "2023-01-01T00:00:00")

#View(production_df)

is.na(price_df) |> sum() 
is.na(production_df) |> sum() # Check for missing values



# This will show you exactly which rows have the NAs
price_df[rowSums(is.na(price_df)) > 0, ]
# Perform linear interpolation on the Price columns:
#A total of 7 missing values were identified, all corresponding to the October Daylight Savings Time transition. 
#These were handled using linear interpolation to ensure a continuous time series for the ARMA modeling."
price_df$SpotPriceEUR <- na.approx(price_df$SpotPriceEUR, na.rm = FALSE)
price_df$ImbalancePriceEUR <- na.approx(price_df$ImbalancePriceEUR, na.rm = FALSE)


production_df <- production_df |>
  mutate(
    hour = hour(Minutes60UTC),
    # 1. Handle Solar: If it's night, NAs become 0. If it's day, we interpolate.
    Solar_Actual = ifelse(hour <= 5 | hour >= 20, coalesce(Solar_Actual, 0), Solar_Actual),
    Solar_Actual = na.approx(Solar_Actual, na.rm = FALSE),
    
    Solar_Forecast = ifelse(hour <= 5 | hour >= 20, coalesce(Solar_Forecast, 0), Solar_Forecast),
    Solar_Forecast = na.approx(Solar_Forecast, na.rm = FALSE),
)



# The missing values in the forecast columns are likely due to gaps in the forecast data. 
#To fill these, we can calculate a 'Typical' value for each hour of the week 
#(168 combinations of day and hour) based on the available data. 
#This way, we can impute missing forecasts with the average forecast for that specific time of the week.

# Create helper columns for "Time of Week"
production_df <- production_df |>
  mutate(
    h = hour(Minutes60UTC),
    dw = wday(Minutes60UTC)
  )

# Calculate the 'Typical' value and merge it back
# Fill missing values with the TypicalValue
# Clean up helper columns
production_df <- production_df |>
    group_by(dw, h) |>
    mutate(
        TypicalValueOnshore = mean(OnShore_Forecast, na.rm = TRUE),
        TypicalValueOffshore = mean(OffShore_Forecast, na.rm = TRUE),
    
        # coalesce fills NA in the first arg with the value from the second
        OnShore_Forecast = coalesce(OnShore_Forecast, TypicalValueOnshore),
        OffShore_Forecast = coalesce(OffShore_Forecast, TypicalValueOffshore)
    ) |>
    ungroup() |>
    # Remove the helper columns
    select(-h, -dw, -TypicalValueOnshore, -TypicalValueOffshore)

is.na(production_df) |> sum() # Check for missing values
is.na(price_df) |> sum() 

production_df <- production_df |> mutate(
    Forecast_Production = Solar_Forecast + OffShore_Forecast + OnShore_Forecast,
    Actual_Production = Solar_Actual + OffShore_Actual + OnShore_Actual) |>
    select(Minutes60UTC, Forecast_Production, Actual_Production)

#View(production_df)
#View(price_df)


   
# Write the final dataframes to CSV for use in later modeling.
write.csv(price_df, "Speciale/data/processed/S_imb_S_spot.csv", row.names = TRUE)
write.csv(production_df, "Speciale/data/processed/Q_forecast_Q_Actual.csv", row.names = TRUE)




#### Checking the series are in fact not stationary (Prices)-----

# Ensure the directory exists so the code doesn't crash
if (!dir.exists("Speciale/plots/02_I(1)_stationarity_check/")) {
  dir.create("Speciale/plots/02_I(1)_stationarity_check/", recursive = TRUE)
}

# Plot each time series
for (i in colnames(price_df)[-1]) {
    png(filename = paste0("Speciale/plots/02_I(1)_stationarity_check/", i, "_plot.png"), width = 800, height = 600)
    
    plot(price_df$Minutes60UTC, price_df[[i]], 
         type = "l", col = "blue", 
         xlab = "Time", ylab = "Price (EUR)", 
         main = paste("Actual", i))
    
    dev.off()
}

for (i in colnames(price_df)[-1]) {
    png(filename = paste0("Speciale/plots/02_I(1)_stationarity_check/", "ACF&PACF_", i, ".png"), width = 800, height = 800)

    par(mfrow = c(2, 1))
    acf(price_df[[i]], main = paste("ACF of", i))
    pacf(price_df[[i]], main = paste("PACF of", i))

    dev.off()
}

for (i in colnames(price_df)[-1]) {
    adf_test <- adf.test(price_df[[i]], alternative = "stationary")
    print(paste("ADF Test for", i, ": p-value =", adf_test$p.value, ", test statistic =", adf_test$statistic))
}

for (i in colnames(price_df)[-1]) {
    kpss_test <- ur.kpss(price_df[[i]], type = "tau")
    print(paste("KPSS Test for", i, ": Test Statistic =", kpss_test@teststat))
}

"
All four series show strong autocorrelation at lag 1, 
and a slow decay in the ACF, which is a strong indication of non-stationarity.

The PACF plots show a significant spike at lag 1 and then quickly drop off, 
which is also consistent with non-stationarity.

The ADF test results will likely show high p-values, 
failing to reject the null hypothesis of a unit root, 
confirming that the series are non-stationary in levels.

The KPSS test statistics are very high, indicating non stationarity.
"


#### Checking for stationarity after differencing (Prices) -----

differenced_price_df <- price_df |>
    arrange(Minutes60UTC) |>
    mutate(
        Diff_Imbalance = c(NA, diff(ImbalancePriceEUR)),
        Diff_spot = c(NA, diff(SpotPriceEUR))
    ) |>
    select(Minutes60UTC, starts_with("Diff")) |>
    filter(!is.na(Diff_spot)) # Remove the first row with

View(differenced_price_df)

for (i in colnames(differenced_price_df)[-1]) {
    png(filename = paste0("Speciale/plots/02_I(1)_stationarity_check/", "I(1)_", i, "_plot.png"), width = 800, height = 600)

    plot(differenced_price_df$Minutes60UTC, differenced_price_df[[i]], 
         type = "l", col = "blue", 
         xlab = "Time", ylab = "Price (EUR)", 
         main = paste("Actual", i))
    
    dev.off()
}

for (i in colnames(differenced_price_df)[-1]) {
    png(filename = paste0("Speciale/plots/02_I(1)_stationarity_check/", "I(1)_ACF&PACF_", i, ".png"), width = 800, height = 800)

    par(mfrow = c(2, 1))
    acf(differenced_price_df[[i]], main = paste("ACF of", i))
    pacf(differenced_price_df[[i]], main = paste("PACF of", i))

    dev.off()
}

for (i in colnames(differenced_price_df)[-1]) {
    adf_test <- adf.test(differenced_price_df[[i]], alternative = "stationary")
    print(paste("ADF Test for", i, ": p-value =", adf_test$p.value, ", test statistic =", adf_test$statistic))
}

for (i in colnames(differenced_price_df)[-1]) {
    kpss_test <- ur.kpss(differenced_price_df[[i]], type = "tau")
    print(paste("KPSS Test for", i, ": Test Statistic =", kpss_test@teststat))
}

" 
After differencing, the ACF plots show a much more rapid decay, 
and the PACF plots show significant spikes at lag 1 and then quickly drop off, 
which is consistent with stationarity.

Furthermore, the KPSS test statistics are now much lower, indicating stationarity,
and the ADF test statistics together with the p-values, indicate strong stationarity, 
as we can reject the null hypothesis of a unit root.

In conclusion, the series is I(1)-stationary
"


#### Checking the series are in fact not stationary (Production) ----

# Plot each time series
for (i in colnames(production_df)[-1]) {
    png(filename = paste0("Speciale/plots/02_I(1)_stationarity_check/", i, "_plot.png"), width = 800, height = 600)
    
    plot(production_df$Minutes60UTC, production_df[[i]], 
         type = "l", col = "blue", 
         xlab = "Time", ylab = "Production (MW)", 
         main = paste("Actual", i))
    
    dev.off()
}

for (i in colnames(production_df)[-1]) {
    png(filename = paste0("Speciale/plots/02_I(1)_stationarity_check/", "ACF&PACF_", i, ".png"), width = 800, height = 800)

    par(mfrow = c(2, 1))
    acf(production_df[[i]], main = paste("ACF of", i))
    pacf(production_df[[i]], main = paste("PACF of", i))

    dev.off()
}

for (i in colnames(production_df)[-1]) {
    adf_test <- adf.test(production_df[[i]], alternative = "stationary")
    print(paste("ADF Test for", i, ": p-value =", adf_test$p.value, ", test statistic =", adf_test$statistic))
}

for (i in colnames(production_df)[-1]) {
    kpss_test <- ur.kpss(production_df[[i]], type = "tau")
    print(paste("KPSS Test for", i, ": Test Statistic =", kpss_test@teststat))
}

"
All four series show strong autocorrelation at lag 1, 
and a slow decay in the ACF, which is a strong indication of non-stationarity.

The PACF plots show a significant spike at lag 1 and then quickly drop off, 
which is also consistent with non-stationarity.

The ADF test results will likely show high p-values, 
failing to reject the null hypothesis of a unit root, 
confirming that the series are non-stationary in levels.

The KPSS test statistics are very high, indicating non stationarity.
"


#### Checking for stationarity after differencing (Production) -----

differenced_df <- production_df |>
    arrange(Minutes60UTC) |>
    mutate(
        Diff_Actual_Production = c(NA, diff(Actual_Production)),
        Diff_Forecast_Production = c(NA, diff(Forecast_Production))
    ) |>
    select(Minutes60UTC, starts_with("Diff")) |>
    filter(!is.na(Diff_Actual_Production)) # Remove the first row with

View(differenced_df)

for (i in colnames(differenced_df)[-1]) {
    png(filename = paste0("Speciale/plots/02_I(1)_stationarity_check/", "I(1)_", i, "_plot.png"), width = 800, height = 600)
    
    plot(differenced_df$Minutes60UTC, differenced_df[[i]], 
         type = "l", col = "blue", 
         xlab = "Time", ylab = "Production (MW)", 
         main = paste("Actual", i))
    
    dev.off()
}

for (i in colnames(differenced_df)[-1]) {
    png(filename = paste0("Speciale/plots/02_I(1)_stationarity_check/", "I(1)_ACF&PACF_", i, ".png"), width = 800, height = 800)

    par(mfrow = c(2, 1))
    acf(differenced_df[[i]], main = paste("ACF of", i))
    pacf(differenced_df[[i]], main = paste("PACF of", i))

    dev.off()
}

for (i in colnames(differenced_df)[-1]) {
    adf_test <- adf.test(differenced_df[[i]], alternative = "stationary")
    print(paste("ADF Test for", i, ": p-value =", adf_test$p.value, ", test statistic =", adf_test$statistic))
}

for (i in colnames(differenced_df)[-1]) {
    kpss_test <- ur.kpss(differenced_df[[i]], type = "tau")
    print(paste("KPSS Test for", i, ": Test Statistic =", kpss_test@teststat))
}

" 
After differencing, the ACF plots show a much more rapid decay, 
and the PACF plots show significant spikes at lag 1 and then quickly drop off, 
which is consistent with stationarity.

Furthermore, the KPSS test statistics are now much lower, indicating stationarity,
and the ADF test statistics together with the p-values, indicate strong stationarity, 
as we can reject the null hypothesis of a unit root.

In conclusion, the series is I(1)-stationary
"


#### checking for cointegration -----

# Since for the power market a 100 MW deficit is simply a 100 MW deficit, we can add the onshore and offshore togehter

#production_df_co <- production_df |>
    # mutate(
    #     Production_Actual = OnShore_Actual + OffShore_Actual + Solar_Actual,
    #     Production_Forecast = OnShore_Forecast + OffShore_Forecast + Solar_Forecast
    # ) |>
    # select(Production_Actual, Production_Forecast)
#warningCondition()
# Before checking for cointegration we make sure that it is the same time for which we check in borg production and price.
# Align the Dataframes by Timestamp
# This ensures we only analyze the period where BOTH datasets exist
combined_df <- inner_join(price_df, production_df, by = "Minutes60UTC")

# Price Cointegration (u_t) on Aligned Data
price_mat_co <- combined_df |> select(SpotPriceEUR, ImbalancePriceEUR)
j_price <- ca.jo(price_mat_co, type = "trace", ecdet = "const", K = 2)
j_price@V
u_t <- as.numeric(combined_df$SpotPriceEUR
    - abs(j_price@V[2,1]) * (combined_df$ImbalancePriceEUR) - j_price@V[3,1])#

# Production Cointegration (v_t) on Aligned Data
prod_mat_co <- combined_df |> select(Forecast_Production, Actual_Production)
j_prod <- ca.jo(prod_mat_co, type = "trace", ecdet = "none", K = 2)
j_prod@V
v_t <- as.numeric(combined_df$Forecast_Production
    - abs(j_prod@V[2,1]) * (combined_df$Actual_Production)) #

# Combine into a regression dataframe
reg_df <- data.frame(u_t = u_t, v_t = v_t)


max(prod_mat_co$Actual_Production)
min(prod_mat_co$Actual_Production)
max(prod_mat_co$Forecast_Production)
min(prod_mat_co$Forecast_Production)



# png(filename = paste0("Speciale/plots/02_stationarity_cointegration_check/", "Production_error_vs_imbalance_price.png"), width = 800, height = 600)
# plot(combined_df$Production_Forecast - combined_df$Production_Actual, combined_df$ImbalancePriceEUR - combined_df$SpotPriceEUR, 
#      xlab = "Production Forecast Error (MW)", 
#      ylab = "Price Spread (EUR)", 
#      main = "Price Spread vs Production Forecast Error", 
#      pch = 20, col = rgb(0, 0, 1, 0.2)) # Using transparency for density
# dev.off()

