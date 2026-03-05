

using HTTP, JSON3, DataFrames, CSV

"""
download_eds_data(dataset_id::String; limit::Int=10000, area::String="DK1", custom_name::String="")

Henter data fra EDS, renser 'nothing' værdier til 'missing', og gemmer med et pænt navn.
"""
function download_eds_data(dataset_id::String; limit::Int=10000, area::String="DK1", custom_name::String="")
    base_url = "https://api.energidataservice.dk/dataset/$(dataset_id)"

    # We use 'timezone=dk' to get data in Danish time right away
    params = Dict(
        "limit" => limit,
        "filter" => "{\"PriceArea\":[\"$area\"]}",
        "timezone" => "dk"
    )
    
    println("Henter: $dataset_id ...")
    
    try
        response = HTTP.get(base_url, query = params)
        json_data = JSON3.read(response.body)
        df = DataFrame(json_data[:records])
        
        if isempty(df)
            println("Ingen data fundet for $dataset_id")
            return nothing
        end

        # --- FIX: Convert 'nothing' to 'missing' ---
        # Check all cells. If the value is nothing, change to missing.
        df = mapcols(col -> replace(col, nothing => missing), df)
        
        # Save with custom name if provided
        final_name = isempty(custom_name) ? dataset_id : custom_name
        mkpath("Speciale/data/raw")
        file_path = "Speciale/data/raw/$(final_name).csv"
        
        CSV.write(file_path, df)
        println("Gemt: $file_path ($(nrow(df)) rækker)")
        
    catch e
        println("Fejl ved download af $dataset_id: $e")
    end
end

