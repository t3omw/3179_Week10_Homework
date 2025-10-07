import pandas as pd
import numpy as np

# --- Load CSV files ---
rain_2000_2021 = pd.read_csv("malaysia_rainfall_2000_2021.csv")
rain_2014_2020 = pd.read_csv("malaysia_daily_rainfall_2014_2020.csv")
avg_rainfall = pd.read_csv("malaysia_avg_annual_rainfall_final_normalized.csv")

# --- Clean column names ---
for df in [rain_2000_2021, rain_2014_2020, avg_rainfall]:
    df.columns = [c.strip() for c in df.columns]

# --- Step 1: Aggregate daily rainfall into annual totals ---
rain_2014_2020["Rainfall (mm)"] = (
    rain_2014_2020["Rainfall (mm)"].astype(str).str.replace("[^0-9.-]", "", regex=True)
)
rain_2014_2020["Rainfall (mm)"] = pd.to_numeric(
    rain_2014_2020["Rainfall (mm)"], errors="coerce"
)
rain_2014_2020 = (
    rain_2014_2020.groupby(["State", "Year"], as_index=False)["Rainfall (mm)"]
    .sum()
    .rename(columns={"Rainfall (mm)": "Total_Rainfall_mm"})
)

# --- Step 2: Clean 2000–2021 dataset ---
if "Total Rainfall in millimetres" in rain_2000_2021.columns:
    rain_2000_2021.rename(
        columns={"Total Rainfall in millimetres": "Total_Rainfall_mm"}, inplace=True
    )

rain_2000_2021["Total_Rainfall_mm"] = (
    rain_2000_2021["Total_Rainfall_mm"]
    .astype(str)
    .str.replace("[^0-9.-]", "", regex=True)
)
rain_2000_2021["Total_Rainfall_mm"] = pd.to_numeric(
    rain_2000_2021["Total_Rainfall_mm"], errors="coerce"
)

rain_2000_2021["State"] = rain_2000_2021["State"].replace({"NSembilan": "Negeri Sembilan"})

# --- Step 3: Fill KL & Putrajaya from Selangor ---
def fill_kl_putrajaya(df):
    selangor = df[df["State"] == "Selangor"]
    kl = selangor.copy(); kl["State"] = "Wilayah Persekutuan Kuala Lumpur"
    pj = selangor.copy(); pj["State"] = "Wilayah Persekutuan Putrajaya"
    return pd.concat([df, kl, pj], ignore_index=True)

rain_2000_2021 = fill_kl_putrajaya(rain_2000_2021)

# --- Step 4: Merge datasets ---
combined = pd.concat([rain_2000_2021, rain_2014_2020], ignore_index=True)

# --- Step 5: Remove duplicates ---
combined = (
    combined.groupby(["State", "Year"], as_index=False)["Total_Rainfall_mm"]
    .mean(numeric_only=True)
)

# --- Step 6: Interpolate only missing years, not full reindex ---
def interpolate_existing(g):
    g = g.sort_values("Year").copy()
    g["Total_Rainfall_mm"] = g["Total_Rainfall_mm"].interpolate(
        method="linear", limit_direction="both"
    )
    return g

combined = combined.groupby("State", group_keys=False).apply(interpolate_existing)

# --- Step 7: Merge rainfall density info ---
if "Average_Annual_Rainfall_per_sqkm" in avg_rainfall.columns:
    combined = combined.merge(
        avg_rainfall[["State", "Average_Annual_Rainfall_per_sqkm"]],
        on="State",
        how="left"
    )

# --- Step 8: Save final dataset ---
combined = combined.sort_values(["State", "Year"])
combined.to_csv("malaysia_rainfall_gapfilled_clean.csv", index=False)

print("✅ Merge complete. No excessive year expansion.")
print(f"✅ Rows: {len(combined)} | States: {combined['State'].nunique()}")
