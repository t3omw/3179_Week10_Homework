# compile.py
import pandas as pd
import numpy as np

# --- 0. Settings ---
DAILY_CSV = "malaysia_daily_rainfall_2014_2020.csv"
YEARLY_SRC_CSV = "malaysia_rainfall_2000_2021.csv"
OUTPUT_COMPILED = "malaysia_rainfall_compiled_full_2014_2020_scaled.csv"
OUTPUT_AVG = "malaysia_rainfall_state_averages_2014_2020_scaled.csv"

# --- 1. Load daily file ---
try:
    df_daily_raw = pd.read_csv(DAILY_CSV)
except FileNotFoundError:
    raise SystemExit(f"Error: {DAILY_CSV} not found.")

# Normalize State text
df_daily_raw['State'] = (
    df_daily_raw['State'].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
)
# Map common variants
df_daily_raw['State'] = df_daily_raw['State'].replace({
    'NSembilan': 'Negeri Sembilan',
    'N Sembilan': 'Negeri Sembilan',
    'Negri Sembilan': 'Negeri Sembilan',
    'Penang': 'Pulau Pinang',
    'Selangor-Wilayah': 'Selangor'
})

# Duplicate selangor daily data for KL and Putrajaya
selangor_daily = df_daily_raw[df_daily_raw['State'] == 'Selangor'].copy()
if not selangor_daily.empty:
    kl_daily = selangor_daily.copy(); kl_daily['State'] = 'Kuala Lumpur'
    pj_daily = selangor_daily.copy(); pj_daily['State'] = 'Putrajaya'
    df_daily_all = pd.concat([df_daily_raw, kl_daily, pj_daily], ignore_index=True)
else:
    df_daily_all = df_daily_raw.copy()

# Compute yearly totals (sum of daily rainfall rows per state-year)
daily_sum = (
    df_daily_all.groupby(['State', 'Year'], as_index=False)['Rainfall (mm)']
    .sum()
    .rename(columns={'Rainfall (mm)': 'Daily Rainfall (mm)'})
)

# --- 2. Load yearly source file and aggregate for 2014-2020 ---
try:
    df_yr_raw = pd.read_csv(YEARLY_SRC_CSV)
except FileNotFoundError:
    raise SystemExit(f"Error: {YEARLY_SRC_CSV} not found.")

# Clean headers & state names
df_yr_raw.columns = df_yr_raw.columns.str.strip()
df_yr_raw['State'] = (
    df_yr_raw['State'].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
)

# Keep 2014-2020 only
df_yr = df_yr_raw[(df_yr_raw['Year'] >= 2014) & (df_yr_raw['Year'] <= 2020)].copy()

# Ensure numeric
df_yr['Total Rainfall in millimetres'] = pd.to_numeric(
    df_yr['Total Rainfall in millimetres'], errors='coerce'
)

# Map station-level Kuala Lumpur entries to Selangor for aggregation (if present)
state_map = {
    'Wilayah Persekutuan Labuan': 'Labuan',
    'NSembilan': 'Negeri Sembilan',
    'Negri Sembilan': 'Negeri Sembilan',
    'N Sembilan': 'Negeri Sembilan',
    'Kuala Lumpur': 'Selangor'
}
df_yr['State_Aggregated'] = df_yr['State'].replace(state_map)

# If there's a station column mention KLIA/Subang map to Selangor
station_col = next((c for c in df_yr.columns if 'station' in c.lower()), None)
if station_col:
    df_yr.loc[df_yr[station_col].str.contains('Kuala Lumpur International Airport', na=False, case=False),
              'State_Aggregated'] = 'Selangor'
    df_yr.loc[df_yr[station_col].str.contains('Subang', na=False, case=False),
              'State_Aggregated'] = 'Selangor'

# Aggregate yearly rainfall from source
yearly_src = (
    df_yr.groupby(['State_Aggregated', 'Year'], as_index=False)['Total Rainfall in millimetres']
    .sum()
    .rename(columns={'State_Aggregated': 'State', 'Total Rainfall in millimetres': 'Yearly Rainfall (mm)'})
)

# --- 3. Merge daily sums and yearly source into one table ---
df = pd.merge(
    daily_sum, yearly_src,
    on=['State', 'Year'],
    how='outer',
    sort=True
)

# Standardize numeric types
df['Daily Rainfall (mm)'] = pd.to_numeric(df['Daily Rainfall (mm)'], errors='coerce')
df['Yearly Rainfall (mm)'] = pd.to_numeric(df['Yearly Rainfall (mm)'], errors='coerce')

# --- 4. Compute per-row ratio where both sources exist ---
mask_both = df['Daily Rainfall (mm)'].notna() & df['Yearly Rainfall (mm)'].notna() & (df['Daily Rainfall (mm)'] > 0)
df.loc[mask_both, 'ratio'] = df.loc[mask_both, 'Yearly Rainfall (mm)'] / df.loc[mask_both, 'Daily Rainfall (mm)']

# Remove extreme ratios before computing medians (keep plausible ratios)
# reasonable range chosen conservatively; adjust if you prefer
valid_ratio_mask = df['ratio'].between(0.2, 6.0)
df.loc[~valid_ratio_mask, 'ratio'] = np.nan

# Compute median ratio per Year (fallback to global median if necessary)
median_ratio_by_year = df.groupby('Year')['ratio'].median().to_dict()
global_median_ratio = df['ratio'].median(skipna=True)
# Fill any year medians that are NaN with global median
for y, v in list(median_ratio_by_year.items()):
    if pd.isna(v):
        median_ratio_by_year[y] = global_median_ratio

# --- 5. Fill missing Yearly Rainfall (mm) using median ratio for that Year ---
def estimate_yearly(row):
    if pd.notna(row['Yearly Rainfall (mm)']):
        return row['Yearly Rainfall (mm)']
    # if we have daily total and a median ratio for that year
    if pd.notna(row['Daily Rainfall (mm)']) and row['Year'] in median_ratio_by_year and pd.notna(median_ratio_by_year[row['Year']]):
        return round(row['Daily Rainfall (mm)'] * median_ratio_by_year[row['Year']], 2)
    return np.nan

df['Yearly Est (mm)'] = df.apply(estimate_yearly, axis=1)
# Prefer real yearly values; if missing, take estimated
df['Yearly Rainfall (mm)'] = df['Yearly Rainfall (mm)'].fillna(df['Yearly Est (mm)'])

# --- 6. For states with no daily data (Sabah/Sarawak/Labuan) but have yearly, compute daily estimate as yearly/365 ---
for region in ['Sabah', 'Sarawak', 'Labuan']:
    mask = (df['State'] == region) & df['Daily Rainfall (mm)'].isna() & df['Yearly Rainfall (mm)'].notna()
    if mask.any():
        df.loc[mask, 'Daily Rainfall (mm)'] = (df.loc[mask, 'Yearly Rainfall (mm)'] / 365.0).round(2)

# --- 7. Ensure Kuala Lumpur & Putrajaya use Selangor's values (overwrite) ---
sel_vals = df[df['State'] == 'Selangor'][['Year', 'Daily Rainfall (mm)', 'Yearly Rainfall (mm)']].set_index('Year')
for target in ['Kuala Lumpur', 'Putrajaya']:
    for year, row in sel_vals.iterrows():
        mask = (df['State'] == target) & (df['Year'] == year)
        if mask.any():
            df.loc[mask, ['Daily Rainfall (mm)', 'Yearly Rainfall (mm)']] = [row['Daily Rainfall (mm)'], row['Yearly Rainfall (mm)']]
        else:
            # If there's no row for KL/PJ for that year, append one
            df = pd.concat([df, pd.DataFrame([{
                'State': target, 'Year': year,
                'Daily Rainfall (mm)': row['Daily Rainfall (mm)'],
                'Yearly Rainfall (mm)': row['Yearly Rainfall (mm)']
            }])], ignore_index=True)

# --- 8. Final tidy up ---
# Drop helper column
if 'Yearly Est (mm)' in df.columns: df.drop(columns=['Yearly Est (mm)'], inplace=True)
# Re-round and sort
df['Daily Rainfall (mm)'] = pd.to_numeric(df['Daily Rainfall (mm)'], errors='coerce').round(2)
df['Yearly Rainfall (mm)'] = pd.to_numeric(df['Yearly Rainfall (mm)'], errors='coerce').round(2)
df.sort_values(['State', 'Year'], inplace=True)
df = df[['State', 'Year', 'Daily Rainfall (mm)', 'Yearly Rainfall (mm)']].reset_index(drop=True)

# --- 9. Quick validation / diagnostics ---
# show median ratio used per year (helpful to understand scaling)
print("\nMedian scale factor (Yearly / Daily) used per Year:")
for y in sorted(median_ratio_by_year.keys()):
    print(f"  {y}: {median_ratio_by_year[y]:.3f}")

# If Negeri Sembilan had missing or unrealistic yearly before, show corrected values
print("\nNegeri Sembilan rows (post-scaling):")
print(df[df['State']=='Negeri Sembilan'].to_string(index=False))

# Flag any extremely large yearly totals > 20000 mm (you said ignore Sarawak but we still warn)
extreme = df[df['Yearly Rainfall (mm)'] > 20000]
if not extreme.empty:
    print("\n⚠️ Extremely large yearly totals remain (review):")
    print(extreme.to_string(index=False))

# --- 10. Save compiled CSV and state averages for 2014-2020 ---
df.to_csv(OUTPUT_COMPILED, index=False)
print(f"\n✅ Compiled file saved to: {OUTPUT_COMPILED}")

# Compute state-level averages across 2014-2020
state_avg = (
    df.groupby('State', as_index=False)[['Daily Rainfall (mm)', 'Yearly Rainfall (mm)']]
    .mean()
    .round(2)
)
state_avg.to_csv(OUTPUT_AVG, index=False)
print(f"✅ State averages (2014-2020) saved to: {OUTPUT_AVG}")

print("\nSample of state averages:")
print(state_avg.head(15).to_string(index=False))
