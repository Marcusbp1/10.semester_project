# 10. Semester Project: Electricity Market Analysis

This repository contains the scripts and processed data for my 10th-semester project.

## Project Structure
- **Scripts/**: Julia (.jl) and R (.r) files for data cleaning and econometric modeling.
- **Speciale/data/processed/**: Intermediate datasets used for final models.
- **Project.toml / Manifest.toml**: Julia environment files for reproducibility.

## Data Note
The **Raw Data** files (approx. 800MB) are excluded from this repository due to GitHub's file size limits. 

**To run the full pipeline:**
1. Download the raw CSVs from this link: [INSERT YOUR ONEDRIVE/DROPBOX LINK HERE]
2. Place the CSVs in the `Speciale/data/Raw/` directory.
3. Run `00_download_data.jl` (if applicable) or proceed to `01_data_cleaning.jl`.

## How to Run
To set up the Julia environment, run:
```julia
using Pkg
Pkg.activate(".")
Pkg.instantiate()