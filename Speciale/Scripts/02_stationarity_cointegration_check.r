

library(tseries)
library(urca)
library(dplyr)
library(lubridate)
library(zoo)


df1 <- read.csv("Speciale/data/processed/60min_DK1_el_prod.csv") |> 
    rename(OnShore_Actual  = OnshoreWindPower, OffShore_Actual  = OffshoreWindPower) 
df2 <- read.csv("Speciale/data/processed/60min_DK1_price_imbalance_new.csv") |> mutate(Minutes60UTC = as_datetime(Minutes60UTC))
df3 <- read.csv("Speciale/data/processed/60min_DK1_price_imbalance_old.csv") |> mutate(Minutes60UTC = as_datetime(Minutes60UTC))
df4 <- read.csv("Speciale/data/processed/60min_DK1_price_spot_old.csv") |> mutate(Minutes60UTC = as_datetime(Minutes60UTC))
df5 <- read.csv("Speciale/data/processed/60min_DK1_prod_forecast.csv") |> 
    rename(OnShore_Forecast  = OnshoreWindPower, OffShore_Forecast  = OffshoreWindPower)

## Prices ##

price_df <- merge(df3, df4, by="Minutes60UTC", all=TRUE) |> 
    arrange(Minutes60UTC) |> 
    select(Minutes60UTC, ImbalancePriceEUR, SpotPriceEUR) |>
    mutate(Minutes60UTC = as_datetime(Minutes60UTC)) |>
    filter(Minutes60UTC >= "2009-01-02T00:00:00", Minutes60UTC <= "2025-03-04T11:00:00")
#View(price_df)
is.na(price_df) |> sum() # Check for missing values
# This will show you exactly which rows have the NAs
price_df[rowSums(is.na(price_df)) > 0, ]
# Perform linear interpolation on the Price columns:
#A total of 7 missing values were identified, all corresponding to the October Daylight Savings Time transition. 
#These were handled using linear interpolation to ensure a continuous time series for the ARMA modeling."
price_df$SpotPriceEUR <- na.approx(price_df$SpotPriceEUR, na.rm = FALSE)
price_df$ImbalancePriceEUR <- na.approx(price_df$ImbalancePriceEUR, na.rm = FALSE)
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
    select(Minutes60UTC, OnShore_Forecast, OffShore_Forecast, OnShore_Actual, OffShore_Actual) |>
    mutate(Minutes60UTC = as_datetime(Minutes60UTC))

#View(production_df)

is.na(production_df) |> sum() # Check for missing values

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


# Write the final dataframes to CSV for use in later modeling.
write.csv(price_df, "Speciale/data/processed/S_imb_S_spot.csv", row.names = TRUE)
write.csv(production_df, "Speciale/data/processed/Q_forecast_Q_Actual.csv", row.names = TRUE)




#### Checking the series are in fact not stationary (Prices)-----

# Ensure the directory exists so the code doesn't crash
if (!dir.exists("Speciale/plots/00_I(1)_stationarity_check/")) {
  dir.create("Speciale/plots/00_I(1)_stationarity_check/", recursive = TRUE)
}

# Plot each time series
for (i in colnames(price_df)[-1]) {
    png(filename = paste0("Speciale/plots/00_I(1)_stationarity_check/", i, "_plot.png"), width = 800, height = 600)
    
    plot(price_df$Minutes60UTC, price_df[[i]], 
         type = "l", col = "blue", 
         xlab = "Time", ylab = "Price (EUR)", 
         main = paste("Actual", i))
    
    dev.off()
}

for (i in colnames(price_df)[-1]) {
    png(filename = paste0("Speciale/plots/00_I(1)_stationarity_check/", "ACF&PACF_", i, ".png"), width = 800, height = 800)

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
    png(filename = paste0("Speciale/plots/00_I(1)_stationarity_check/", "I(1)_", i, "_plot.png"), width = 800, height = 600)

    plot(differenced_price_df$Minutes60UTC, differenced_price_df[[i]], 
         type = "l", col = "blue", 
         xlab = "Time", ylab = "Price (EUR)", 
         main = paste("Actual", i))
    
    dev.off()
}

for (i in colnames(differenced_price_df)[-1]) {
    png(filename = paste0("Speciale/plots/00_I(1)_stationarity_check/", "I(1)_ACF&PACF_", i, ".png"), width = 800, height = 800)

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
    png(filename = paste0("Speciale/plots/00_I(1)_stationarity_check/", i, "_plot.png"), width = 800, height = 600)
    
    plot(production_df$Minutes60UTC, production_df[[i]], 
         type = "l", col = "blue", 
         xlab = "Time", ylab = "Production (MW)", 
         main = paste("Actual", i))
    
    dev.off()
}

for (i in colnames(production_df)[-1]) {
    png(filename = paste0("Speciale/plots/00_I(1)_stationarity_check/", "ACF&PACF_", i, ".png"), width = 800, height = 800)

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
        Diff_OnShore_Actual = c(NA, diff(OnShore_Actual)),
        Diff_OffShore_Actual = c(NA, diff(OffShore_Actual)),
        Diff_OnShore_Forecast = c(NA, diff(OnShore_Forecast)),
        Diff_OffShore_Forecast = c(NA, diff(OffShore_Forecast))
    ) |>
    select(Minutes60UTC, starts_with("Diff")) |>
    filter(!is.na(Diff_OnShore_Actual)) # Remove the first row with

View(differenced_df)

for (i in colnames(differenced_df)[-1]) {
    png(filename = paste0("Speciale/plots/00_I(1)_stationarity_check/", "I(1)_", i, "_plot.png"), width = 800, height = 600)
    
    plot(differenced_df$Minutes60UTC, differenced_df[[i]], 
         type = "l", col = "blue", 
         xlab = "Time", ylab = "Production (MW)", 
         main = paste("Actual", i))
    
    dev.off()
}

for (i in colnames(differenced_df)[-1]) {
    png(filename = paste0("Speciale/plots/00_I(1)_stationarity_check/", "I(1)_ACF&PACF_", i, ".png"), width = 800, height = 800)

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

production_df_co <- production_df |>
    mutate(
        Wind_total_Actual = OnShore_Actual + OffShore_Actual,
        Wind_total_Forecast = OnShore_Forecast + OffShore_Forecast
    ) |>
    select(Wind_total_Actual, Wind_total_Forecast)

# Before checking for cointegration we make sure that it is the same time for which we check in borg production and price.
# Align the Dataframes by Timestamp 
# This ensures we only analyze the period where BOTH datasets exist
combined_df <- inner_join(price_df, production_df, by = "Minutes60UTC") |>
    mutate(
        Wind_total_Actual = OnShore_Actual + OffShore_Actual,
        Wind_total_Forecast = OnShore_Forecast + OffShore_Forecast
    )

# Price Cointegration (u_t) on Aligned Data
price_mat_co <- combined_df |> select(ImbalancePriceEUR, SpotPriceEUR)
j_price <- ca.jo(price_mat_co, type = "trace", ecdet = "const", K = 2)

# Extract u_t correctly using matrix multiplication
Z0_price <- cbind(j_price@Z0, 1) 
u_t <- as.numeric(Z0_price %*% j_price@V[, 1])

# Production Cointegration (v_t) on Aligned Data 
prod_mat_co <- combined_df |> select(Wind_total_Actual, Wind_total_Forecast)
j_prod <- ca.jo(prod_mat_co, type = "trace", ecdet = "const", K = 2)

# Extract v_t correctly
Z0_prod <- cbind(j_prod@Z0, 1)
v_t <- as.numeric(Z0_prod %*% j_prod@V[, 1])

# Combine into a regression dataframe
reg_df <- data.frame(u_t = u_t, v_t = v_t)
