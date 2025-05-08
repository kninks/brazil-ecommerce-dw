import pandas as pd

# Load CSV
df = pd.read_csv("../../original_dataset/olist_order_items_dataset.csv")

order_id_column = "order_id"
order_item_id_column = "order_item_id"
product_id_column = "product_id"
seller_id_column = "seller_id"
shipping_limit_date_column = "shipping_limit_date"
price_column = "price"
freight_value_column = "freight_value"

def strip_lower(raw):
    return str(raw).strip().lower()

def clean_time(raw_time):
    if pd.isnull(raw_time):
        return pd.NaT
    try:
        return pd.to_datetime(raw_time, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    except ValueError:
        return pd.NaT

def cast_to_float(column):
    return column.astype("float64", errors="ignore")

df = (
    df.dropna(subset=[
        order_id_column,
        order_item_id_column,
        product_id_column,
        seller_id_column,
        shipping_limit_date_column,
        price_column,
        freight_value_column,
    ])
)

df[order_id_column] = df[order_id_column].apply(strip_lower)
df[order_item_id_column] = df[order_item_id_column].apply(strip_lower)
df[product_id_column] = df[product_id_column].apply(strip_lower)
df[seller_id_column] = df[seller_id_column].apply(strip_lower)
df[shipping_limit_date_column] = df[shipping_limit_date_column].apply(clean_time)
df[price_column] = cast_to_float(df[price_column])
df[freight_value_column] = cast_to_float(df[freight_value_column])

# Remove negative values
df = df[
    (df[price_column] > 0) &
    (df[freight_value_column] >= 0)
]

df = df.drop_duplicates()

df.to_csv("cleaned_order_items_dataset.csv", index=False)
