import pandas as pd
import io

# Read the CSV data into pandas DataFrames
df_area = pd.read_csv("malaysia_state_area.csv")
df_rainfall = pd.read_csv("malaysia_rainfall_compiled_full_2014_2020_scaled.csv")

# Merge the two dataframes on the 'State' column
df_merged = pd.merge(df_rainfall, df_area, on='State', how='left')

# Calculate 'Yearly Rainfall (mm/km^2)'
# Divide 'Yearly Rainfall (mm)' by 'Area_sqkm'
df_merged['Yearly Rainfall (mm/km^2)'] = df_merged['Yearly Rainfall (mm)'] / df_merged['Area_sqkm']

# Display the resulting DataFrame, focusing on relevant columns
# print(df_merged[['State', 'Year', 'Yearly Rainfall (mm)', 'Area_sqkm', 'Yearly Rainfall (mm/km^2)']])

output_df = df_merged[['State', 'Year', 'Yearly Rainfall (mm)', 'Area_sqkm', 'Yearly Rainfall (mm/km^2)']]

# Define the output CSV file name
output_csv_filename = 'malaysia_rainfall_per_sqkm.csv'

# Save the DataFrame to a CSV file
output_df.to_csv(output_csv_filename, index=False)

print(f"Results saved to '{output_csv_filename}'")

df_rainfall_per_sqkm = pd.read_csv("malaysia_rainfall_per_sqkm.csv")

# Check for missing values in each column
missing_data = df_rainfall_per_sqkm.isnull().sum()

print("Missing data in each column:")
print(missing_data)

# You can also check the total number of missing values
total_missing = missing_data.sum()
print(f"\nTotal missing values in the dataset: {total_missing}")

if total_missing == 0:
    print("\nNo missing data found in the CSV file.")
else:
    print("\nMissing data found in the CSV file. Please review the output above.")