import pandas as pd
import numpy as np

# --- Load CSV files ---
rain_2000_2021 = pd.read_csv("malaysia_rainfall_2000_2021.csv")
rain_2014_2020 = pd.read_csv("malaysia_daily_rainfall_2014_2020.csv")
avg_rainfall = pd.read_csv("malaysia_avg_annual_rainfall_final_normalized.csv")

# --- Clean column names ---
rain_2000_2021.columns = [c.strip() for c in rain_2000_2021.columns]
rain_2014_2020.columns = [c.strip() for c in rain_2014_2020.columns]
avg_rainfall.columns = [c.strip() for c in avg_rainfall.columns]

# --- Step 1: Aggregate daily rainfall into annual totals ---
rain_2014_2020["Rainfall (mm)"] = (
    rain_2014_2020["Rainfall (mm)"]
    .astype(str)
    .str.replace("[^0-9.-]", "", regex=True)
)
rain_2014_2020["Rainfall (mm)"] = pd.to_numeric(rain_2014_2020["Rainfall (mm)"], errors="coerce")

rain_2014_2020 = (
    rain_2014_2020.groupby(["State", "Year"], as_index=False)["Rainfall (mm)"]
    .sum()
    .rename(columns={"Rainfall (mm)": "Total_Rainfall_mm"})
)

# --- Step 2: Standardize 2000–2021 dataset ---
if "Total Rainfall in millimetres" in rain_2000_2021.columns:
    rain_2000_2021.rename(
        columns={"Total Rainfall in millimetres": "Total_Rainfall_mm"}, inplace=True
    )

rain_2000_2021["Total_Rainfall_mm"] = (
    rain_2000_2021["Total_Rainfall_mm"]
    .astype(str)
    .str.replace("[^0-9.-]", "", regex=True)
)
rain_2000_2021["Total_Rainfall_mm"] = pd.to_numeric(rain_2000_2021["Total_Rainfall_mm"], errors="coerce")

# Normalize Negeri Sembilan spelling
rain_2000_2021["State"] = rain_2000_2021["State"].replace(
    {"NSembilan": "Negeri Sembilan"}
)

# --- Step 3: Handle Kuala Lumpur & Putrajaya missing data ---
def fill_kl_putrajaya(df):
    selangor_rows = df[df["State"] == "Selangor"]
    kl_rows = selangor_rows.copy()
    kl_rows["State"] = "Wilayah Persekutuan Kuala Lumpur"
    pj_rows = selangor_rows.copy()
    pj_rows["State"] = "Wilayah Persekutuan Putrajaya"
    return pd.concat([df, kl_rows, pj_rows], ignore_index=True)

rain_2000_2021 = fill_kl_putrajaya(rain_2000_2021)

# --- Step 4: Merge datasets ---
combined = pd.concat([rain_2000_2021, rain_2014_2020], ignore_index=True)

# --- Step 5: Fill missing rainfall data with state averages ---
state_avg = (
    combined.groupby("State", dropna=False)["Total_Rainfall_mm"]
    .mean(numeric_only=True)
    .rename("State_Avg_Rainfall_mm")
    .reset_index()
)
combined = combined.merge(state_avg, on="State", how="left")

combined["Total_Rainfall_mm"] = combined["Total_Rainfall_mm"].fillna(
    combined["State_Avg_Rainfall_mm"]
)
combined.drop(columns=["State_Avg_Rainfall_mm"], inplace=True)

# --- Step 6: Merge normalized rainfall density ---
if "Average_Annual_Rainfall_per_sqkm" in avg_rainfall.columns:
    combined = combined.merge(
        avg_rainfall[["State", "Average_Annual_Rainfall_per_sqkm"]],
        on="State",
        how="left"
    )

# --- Step 7: Sort and save ---
combined = combined.sort_values(["State", "Year"])
combined.to_csv("malaysia_rainfall_merged_gapfilled.csv", index=False)

print("✅ Clean merge completed.")
print(f"Total records: {len(combined)}")
print(f"States found: {combined['State'].nunique()}")
print(combined.head(10))
# print(combined)
