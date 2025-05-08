import pandas as pd
import unidecode
import re

# Load CSV
df = pd.read_csv("../../original_dataset/olist_geolocation_dataset.csv")

# --- Column names ------------------
zip_code_column = "geolocation_zip_code_prefix"
city_column = "geolocation_city"
state_column = "geolocation_state"
latitude_column = "geolocation_lat"
longitude_column = "geolocation_lng"

distinct_zip_codes = df[zip_code_column].nunique()
print(f"Number of distinct ZIP codes in the original dataset: {distinct_zip_codes}")

# Step 1: Sanitize columns
def clean_city(city):
    city = str(city).strip()
    city = unidecode.unidecode(city)
    city = city.lower()
    city = re.sub(r'[^a-zA-Z\s]', '', city)
    city = re.sub(r'\s+', ' ', city)
    city = city.title()
    return city

def clean_state(state):
    state = str(state).strip()
    state = state.upper()
    state = re.sub(r'[^A-Z\s]', '', state)
    return state

def clean_zip(zip_code):
    zip_code = str(zip_code).strip()
    zip_code = re.sub(r'[^0-9]', '', zip_code)
    return zip_code.zfill(5)

def cast_to_double(column):
    return column.astype("float64", errors="ignore")

df = (
    df.dropna(subset=[
        zip_code_column,
        city_column,
        state_column,
        latitude_column,
        longitude_column
    ])
)

df[city_column] = df[city_column].apply(clean_city)
df[state_column] = df[state_column].apply(clean_state)
df[zip_code_column] = df[zip_code_column].apply(clean_zip)
df[latitude_column] = cast_to_double(df[latitude_column])
df[longitude_column] = cast_to_double(df[longitude_column])

# group by zip/city/state and average lat/lng
city_state_counts = (
    df.groupby([zip_code_column, city_column, state_column])
    .agg(
        **{
            latitude_column: (latitude_column, "mean"),
            longitude_column: (longitude_column, "mean"),
            "freq": (city_column, "count")
        }
    )
    .reset_index()
)

# select the most frequent city / state for each zip code
most_frequent = city_state_counts.sort_values("freq", ascending=False).drop_duplicates(subset=[zip_code_column])

final_columns = [zip_code_column, city_column, state_column, latitude_column, longitude_column]
df_cleaned = most_frequent[final_columns].sort_values(zip_code_column).reset_index(drop=True)

df_cleaned.to_csv("cleaned_geolocation_dataset.csv", index=False)

print(f"✅ Final cleaned geolocation rows: {df_cleaned.shape[0]}")

# Check if any ZIP still maps to multiple city/state combos
dup_check = df_cleaned.groupby(zip_code_column).size()
problematic_zips = dup_check[dup_check > 1]
print(f"⚠️ ZIPs with multiple mappings (should be empty):\n{problematic_zips}")