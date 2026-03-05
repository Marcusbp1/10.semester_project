
using Pkg
Pkg.activate(".") # Activate the project environment

include("../src/functions.jl")

# List over datasets to download: (EDS_ID, custom_name)
data_list = [
    ("ElectricityProdex5MinRealtime",   "electricity_production_5min"), #Electricity Production and Exchange 5 min Realtime
    ("Forecasts_5Min",                  "prod_forecast_5min"),          #Forecast Wind and Solar Power, 5 min
    ("ImbalancePrice",                  "price_imbalance_new"),          #Imbalance Price
    ("RegulatingBalancePowerdata",      "price_imbalance_old"),         #Regulating and Balance Power, Overall Data (Discontinued)
    ("Elspotprices",                    "price_spot_old")               #Elspot Prices (Discontinued)
]



#RegulatingBalancePowerdata
#ImbalancePrice
#Forecasts_5Min
#ElectricityProdex5MinRealtime


println("Starting download process...")
for area in ["DK1", "DK2"]
    for (eds_id, my_name) in data_list
        download_eds_data(eds_id; limit=0, area=area, custom_name="$(area)_$my_name")
    end
end
println("Done")