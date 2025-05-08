import pandas as pd
import unidecode
import re

# Load CSV
df = pd.read_csv("../../original_dataset/olist_customers_dataset.csv")

customer_id_column = "customer_id"
customer_unique_id_column = "customer_unique_id"
zip_code_column = "customer_zip_code_prefix"
city_column = "customer_city"
state_column = "customer_state"

def clean_id(cust_id):
    cust_id = str(cust_id).strip().lower()
    return cust_id

def clean_city(city):
    if pd.isna(city):
        return None
    city = str(city).strip()
    city = unidecode.unidecode(city)
    city = city.lower()
    city = re.sub(r'[^a-zA-Z\s]', '', city)
    city = re.sub(r'\s+', ' ', city)
    city = city.title()
    return city

def clean_state(state):
    if pd.isna(state):
        return None
    state = str(state).strip()
    state = state.upper()
    state = re.sub(r'[^A-Z\s]', '', state)
    return state

def clean_zip(zip_code):
    zip_code = str(zip_code).strip()
    zip_code = re.sub(r'[^0-9]', '', zip_code)
    return zip_code.zfill(5)

# print(f"Number of rows before cleaning: {df.shape[0]}")

df = df.dropna(subset=[customer_id_column, customer_unique_id_column, zip_code_column])

df[customer_id_column] = df[customer_id_column].apply(clean_id)
df[customer_unique_id_column] = df[customer_unique_id_column].apply(clean_id)
df[city_column] = df[city_column].apply(clean_city)
df[state_column] = df[state_column].apply(clean_state)
df[zip_code_column] = df[zip_code_column].apply(clean_zip)

df = df.drop_duplicates()

df.to_csv("cleaned_customers_dataset.csv", index=False)

# print(f"Number of rows after cleaning: {df.shape[0]}")