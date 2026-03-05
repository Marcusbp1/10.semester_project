
# Load necessary packages
using Pkg
Pkg.activate(".") # Activate the project environment

using Dates
using DataFrames
using Statistics
using CSV

# Load custom functions
include("../src/functions.jl")
   

#### DK1 ####

#### ------------------------------- ####
#### DK1_electricity_production_5min.csv ####
#### ------------------------------- ####
raw_electricity_production_5min = CSV.read("Speciale/data/raw/DK1_electricity_production_5min.csv", DataFrame)

# Ensure DateTime type
raw_electricity_production_5min.Minutes5UTC = DateTime.(raw_electricity_production_5min.Minutes5UTC)

describe(raw_electricity_production_5min)
# All data for BornholmSE4 is missing, so we can drop that PriceArea
filter!(row -> row.PriceArea != "BornholmSE4", raw_electricity_production_5min)

# Trim to match your Forecast dataset (The Nov 24th cutoff)
#df_actuals_trimmed = filter(row -> row.Minutes5UTC >= DateTime(2025, 11, 24, 14, 0), raw_electricity_production_5min)

# Aggregate to 60-minutes
raw_electricity_production_5min.Minutes60UTC = floor.(raw_electricity_production_5min.Minutes5UTC, Minute(60))

df_actuals_60min = combine(groupby(raw_electricity_production_5min, [:Minutes60UTC, :PriceArea]), 
    :ProductionLt100MW      => mean => :ProductionLt100MW,
    :ProductionGe100MW      => mean => :ProductionGe100MW,
    :OffshoreWindPower      => mean => :OffshoreWindPower,
    :OnshoreWindPower       => mean => :OnshoreWindPower,
    :SolarPower             => mean => :SolarPower,
    :ExchangeGreatBelt      => mean => :ExchangeGreatBelt,
    :ExchangeGermany        => mean => :ExchangeGermany,
    :ExchangeNetherlands    => mean => :ExchangeNetherlands,
    :ExchangeGreatBritain   => mean => :ExchangeGreatBritain,
    :ExchangeNorway         => mean => :ExchangeNorway,
    :ExchangeSweden         => mean => :ExchangeSweden
)


sort!(df_actuals_60min, :Minutes60UTC)

# Save the cleaned and aggregated data
final_name = "60min_DK1_el_prod"
mkpath("Speciale/data/processed")
file_path = "Speciale/data/processed/$(final_name).csv"

CSV.write(file_path, df_actuals_60min)


#### ------------------------------- ####
####      DK1_price_imbalance_new.csv     ####
#### ------------------------------- ####
raw_price_imbalance_new = CSV.read("Speciale/data/raw/DK1_price_imbalance_new.csv", DataFrame)

# Rename so it is the same as the other datasets, and we can merge on it later. 
rename!(raw_price_imbalance_new, :TimeUTC => :Minutes60UTC)
rename!(raw_price_imbalance_new, :TimeDK => :Minutes60DK)
# Ensure DateTime type
raw_price_imbalance_new.Minutes60UTC = DateTime.(raw_price_imbalance_new.Minutes60UTC)

describe(raw_price_imbalance_new)

# Keep everything EXCEPT the the latest data (4 rows)
raw_price_imbalance_new = raw_price_imbalance_new[4:end, :]


# Check for missing BalancingDemand and DominatingDirection values
missing_data_1 = filter(row -> ismissing(row.BalancingDemand), raw_price_imbalance_new)
missing_data_2 = filter(row -> ismissing(row.DominatingDirection), raw_price_imbalance_new)
missing_data_2.Minutes60UTC == missing_data_1.Minutes60UTC  


"""
The missing values appear random thoughout the dataset, and BalancingDemand and DominatingDirection are 
missing for the same timestamps.
"""
#for now drop the two columns
select!(raw_price_imbalance_new, Not([:BalancingDemand, :DominatingDirection]))


# Round both time columns to the start of the hour
raw_price_imbalance_new.Minutes60UTC = floor.(raw_price_imbalance_new.Minutes60UTC, Minute(60))
raw_price_imbalance_new.Minutes60DK = floor.(raw_price_imbalance_new.Minutes60DK, Minute(60))


# Group by the hourly timestamps and PriceArea
df_raw_price_imbalance_new = combine(groupby(raw_price_imbalance_new, [:Minutes60UTC, :PriceArea, :Minutes60DK]), 
    # Core Prices (Averaged)
    :ImbalancePriceEUR   => mean => :ImbalancePriceEUR,
    :ImbalancePriceDKK   => mean => :ImbalancePriceDKK,
    :SpotPriceEUR        => mean => :SpotPriceEUR,
    
    # Balancing Demand & Volumes (Averaged)
    #:BalancingDemand     => mean => :BalancingDemand,
    :aFRRUpMW            => mean => :aFRRUpMW,
    :aFRRDownMW          => mean => :aFRRDownMW,
    
    # Financial VWA & Marginal Prices (Averaged)
    :aFRRVWAUpEUR        => mean => :aFRRVWAUpEUR,
    :aFRRVWADownEUR      => mean => :aFRRVWADownEUR,
    :aFRRVWAUpDKK        => mean => :aFRRVWAUpDKK,
    :aFRRVWADownDKK      => mean => :aFRRVWADownDKK,
    :mFRRMarginalPriceUpEUR   => mean => :mFRRMarginalPriceUpEUR,
    :mFRRMarginalPriceDownEUR => mean => :mFRRMarginalPriceDownEUR,
    :mFRRMarginalPriceUpDKK   => mean => :mFRRMarginalPriceUpDKK,
    :mFRRMarginalPriceDownDKK => mean => :mFRRMarginalPriceDownDKK,
)


sort!(df_raw_price_imbalance_new, :Minutes60UTC)

# Save the cleaned and aggregated data
final_name = "60min_DK1_price_imbalance_new"
mkpath("Speciale/data/processed")
file_path = "Speciale/data/processed/$(final_name).csv"
CSV.write(file_path, df_raw_price_imbalance_new)


#### ------------------------------- ####
####      DK1_price_imbalance_old.csv    ####
#### ------------------------------- ####
raw_imbalance_price_old = CSV.read("Speciale/data/raw/DK1_price_imbalance_old.csv", DataFrame)
#check for missing values
describe(raw_imbalance_price_old)

rename!(raw_imbalance_price_old, :HourUTC => :Minutes60UTC)
rename!(raw_imbalance_price_old, :HourDK => :Minutes60DK)


raw_imbalance_price_old.Minutes60UTC = DateTime.(raw_imbalance_price_old.Minutes60UTC)

# Create a Year column for grouping
raw_imbalance_price_old.Year = year.(raw_imbalance_price_old.Minutes60UTC)

# Count missing values per year
missing_by_year = combine(groupby(raw_imbalance_price_old, :Year), 
    :ImbalancePriceEUR => (x -> sum(ismissing.(x))) => :Missing_Prices,
    nrow => :Total_Rows)

# Calculate percentage
missing_by_year.Pct_Missing = (missing_by_year.Missing_Prices ./ missing_by_year.Total_Rows) .* 100

sort(missing_by_year, :Year)

# Keep only the years where the data actually exists
raw_imbalance_price_old = filter(row -> row.Year >= 2009, raw_imbalance_price_old)
describe(raw_imbalance_price_old)
# There are still missing values, but not for ImbalancePriceEUR and ImbalancePriceDKK.

raw_imbalance_price_old = select(raw_imbalance_price_old, Not(:Year))

# Save the cleaned and aggregated data
final_name = "60min_DK1_price_imbalance_old"
mkpath("Speciale/data/processed")
file_path = "Speciale/data/processed/$(final_name).csv"

CSV.write(file_path, raw_imbalance_price_old)

#### ------------------------------- ####
####      price_spot_old.csv         ####
#### ------------------------------- ####
raw_price_spot_old = CSV.read("Speciale/data/raw/DK1_price_spot_old.csv", DataFrame)

#check for missing values
describe(raw_price_spot_old)
# There are no missing values
rename!(raw_price_spot_old, :HourUTC => :Minutes60UTC)
rename!(raw_price_spot_old, :HourDK => :Minutes60DK)
# Ensure DateTime type
raw_price_spot_old.Minutes60UTC = DateTime.(raw_price_spot_old.Minutes60UTC)

# Save the cleaned and aggregated data
final_name = "60min_DK1_price_spot_old"
mkpath("Speciale/data/processed")
file_path = "Speciale/data/processed/$(final_name).csv"
CSV.write(file_path, raw_price_spot_old)


#### ------------------------------- ####
####     prod_forecast_5min.csv      ####
#### ------------------------------- ####
raw_prod_forecast_5min = CSV.read("Speciale/data/raw/DK1_prod_forecast_5min.csv", DataFrame)

## Pivot so 'Offshore Wind', 'Onshore Wind', etc., become columns, thus eliminating multiple 
## rows per timestamp and area. This will make it easier to merge with price data later.
raw_prod_forecast_5min = unstack(raw_prod_forecast_5min, 
    [:Minutes5UTC, :Minutes5DK, :PriceArea], 
    :ForecastType, 
    :ForecastCurrent)

# Checking for missing values after unstacking
describe(raw_prod_forecast_5min)

raw_prod_forecast_5min.Minutes5UTC = DateTime.(raw_prod_forecast_5min.Minutes5UTC)
# Calculate how many rows SHOULD be there
expected_rows = (Dates.value(maximum(raw_prod_forecast_5min.Minutes5UTC) - minimum(raw_prod_forecast_5min.Minutes5UTC)) / (1000 * 60 * 5)) + 1
actual_rows = nrow(raw_prod_forecast_5min)

println("Expected: $expected_rows, Actual: $actual_rows")

# Check if the rows are missing at regular 5-minute intervals or at start or end of the dataset
# Sort the data first to ensure we are checking chronologically
sort!(raw_prod_forecast_5min, :Minutes5UTC)
# Find the time difference between each row
diffs = diff(raw_prod_forecast_5min.Minutes5UTC)
# Create a DataFrame of the gaps
gaps = DataFrame(
    TimeBeforeGap = raw_prod_forecast_5min.Minutes5UTC[1:end-1][diffs .!= Minute(5)],
    TimeAfterGap = raw_prod_forecast_5min.Minutes5UTC[2:end][diffs .!= Minute(5)],
    GapSize = diffs[diffs .!= Minute(5)]
)
display(gaps)
# Gaps is the first 2 days, thus removing it makes the most sense.

# Filter to keep only the data AFTER the big gap
#raw_prod_forecast_5min = filter(row -> row.Minutes5UTC >= DateTime(2025, 11, 24, 14, 0), raw_prod_forecast_5min)

# Re-check the row count for this new period
expected_new = (Dates.value(maximum(raw_prod_forecast_5min.Minutes5UTC) - minimum(raw_prod_forecast_5min.Minutes5UTC)) / (60000 * 5)) + 1
actual_new = nrow(raw_prod_forecast_5min)

println("New Period - Expected: $expected_new, Actual: $actual_new")
# This looks good, we have the expected number of rows for the new period.


# Create the 60-minute bucket column
# This turns 14:00, 14:05, and 14:10 into 14:00
raw_prod_forecast_5min.Minutes60UTC = floor.(raw_prod_forecast_5min.Minutes5UTC, Minute(60))

# Group by the 60-minute bucket and PriceArea, then take the mean
df_60min = combine(groupby(raw_prod_forecast_5min, [:Minutes60UTC, :PriceArea]), 
    :"Offshore Wind" => mean => :OffshoreWindPower,
    :"Onshore Wind"  => mean => :OnshoreWindPower,
    :"Solar"         => mean => :SolarPower
)

#Minutes5UTC,Minutes5DK,PriceArea,ForecastType,ForecastDayAhead,Forecast5Hour,Forecast1Hour,ForecastCurrent,TimestampUTC,TimestampDK

# Final Sort
sort!(df_60min, :Minutes60UTC)

# Final verification
println("Total 60-min intervals: ", nrow(df_60min))


# Save the cleaned and aggregated data
final_name = "60min_DK1_prod_forecast"
mkpath("Speciale/data/processed")
file_path = "Speciale/data/processed/$(final_name).csv"

CSV.write(file_path, df_60min)




#####################################################################################################

#####################################################################################################








data_frame = outerjoin(
    CSV.read("Speciale/data/processed/60min_DK1_el_prod.csv", DataFrame),
    CSV.read("Speciale/data/processed/60min_DK1_price_imbalance_new.csv", DataFrame),
    CSV.read("Speciale/data/processed/60min_DK1_price_imbalance_old.csv", DataFrame),
    CSV.read("Speciale/data/processed/60min_DK1_price_spot_old.csv", DataFrame),
    CSV.read("Speciale/data/processed/60min_DK1_prod_forecast.csv", DataFrame),
    
    on = :Minutes60UTC, 
    makeunique=true
)

sort!(data_frame, :Minutes60UTC)

select!(data_frame, Not([:Minutes60DK_1, :Minutes60DK_2, 
    :PriceArea_1, :PriceArea_2, :PriceArea_3, :PriceArea_4])) 

# 2014-12-31T23:00:00.0, so we can trim to that later if needed.

# Drop redundant columns from the merge
vscodedisplay(data_frame)#; data_frame = 0

describe(data_frame)
