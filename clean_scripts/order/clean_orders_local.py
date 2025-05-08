import pandas as pd

# Load CSV
df = pd.read_csv("../../original_dataset/olist_orders_dataset.csv")

order_id_column = "order_id"
customer_id_column = "customer_id"
order_status_column = "order_status"
order_purchase_timestamp_column = "order_purchase_timestamp"
order_approved_at_column = "order_approved_at"
order_delivered_carrier_date_column = "order_delivered_carrier_date"
order_delivered_customer_date_column = "order_delivered_customer_date"
order_estimated_delivery_date_column = "order_estimated_delivery_date"
order_status_enum = [
    "delivered",
    "invoiced",
    "shipped",
    "processing",
    "unavailable",
    "canceled",
    "created",
    "approved"
]

def strip_lower(raw):
    return str(raw).strip().lower()

def clean_time(raw_time):
    if pd.isnull(raw_time):
        return pd.NaT
    try:
        return pd.to_datetime(raw_time, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    except ValueError:
        return pd.NaT

df = (
    df.dropna(subset=[
        order_id_column,
        customer_id_column,
        order_status_column,
        order_purchase_timestamp_column,
        order_approved_at_column,
        order_delivered_carrier_date_column,
        order_delivered_customer_date_column,
        order_estimated_delivery_date_column
    ])
)

df[order_id_column] = df[order_id_column].apply(strip_lower)
df[customer_id_column] = df[customer_id_column].apply(strip_lower)
df[order_status_column] = df[order_status_column].apply(strip_lower)
df[order_purchase_timestamp_column] = df[order_purchase_timestamp_column].apply(clean_time)
df[order_approved_at_column] = df[order_approved_at_column].apply(clean_time)
df[order_delivered_carrier_date_column] = df[order_delivered_carrier_date_column].apply(clean_time)
df[order_delivered_customer_date_column] = df[order_delivered_customer_date_column].apply(clean_time)
df[order_estimated_delivery_date_column] = df[order_estimated_delivery_date_column].apply(clean_time)

now = pd.Timestamp.now()
df = df[
    df[order_status_column].isin(order_status_enum) &
    (df[order_purchase_timestamp_column] <= now) &
    (df[order_approved_at_column] <= now) &
    (df[order_delivered_carrier_date_column] <= now) &
    (df[order_delivered_customer_date_column] <= now) &
    (df[order_purchase_timestamp_column] <= df[order_approved_at_column]) &
    (df[order_approved_at_column] <= df[order_delivered_carrier_date_column]) &
    (df[order_delivered_carrier_date_column] <= df[order_delivered_customer_date_column]) &
    (df[order_estimated_delivery_date_column] >= df[order_purchase_timestamp_column])
]
df = df.drop_duplicates()

df.to_csv("cleaned_orders_dataset.csv", index=False)
