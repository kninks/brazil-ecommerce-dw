import pandas as pd

# Load CSV
df = pd.read_csv("../../original_dataset/olist_order_payments_dataset.csv")

order_id_column = "order_id"
payment_sequential_column = "payment_sequential"
payment_type_column = "payment_type"
payment_installments_column = "payment_installments"
payment_value_column = "payment_value"
payment_type_enum = [
    "credit_card",
    "boleto",
    "voucher",
    "debit_card",
    "not_defined"
]

def strip_lower(raw):
    return str(raw).strip().lower()

def cast_to_int(column):
    return column.astype("int64", errors="ignore")

def cast_to_float(column):
    return column.astype("float64", errors="ignore")

df = (
    df.dropna(subset=[
        order_id_column,
        payment_sequential_column,
        payment_type_column,
        payment_installments_column,
        payment_value_column
    ])
)

df[order_id_column] = df[order_id_column].apply(strip_lower)
df[payment_sequential_column] = cast_to_int(df[payment_sequential_column])
df[payment_type_column] = df[payment_type_column].apply(strip_lower)
df[payment_installments_column] = cast_to_int(df[payment_installments_column])
df[payment_value_column] = cast_to_float(df[payment_value_column])

unexpected_types = df[~df[payment_type_column].isin(payment_type_enum)][payment_type_column].unique()
if len(unexpected_types) > 0:
    print("⚠️ Unexpected payment types found:", unexpected_types)
else:
    print("✅ All payment types are valid.")

# Remove negative values
df = df[
    (df[payment_type_column].isin(payment_type_enum)) &
    (df[payment_sequential_column] > 0) &
    (df[payment_type_column].isin(payment_type_enum)) &
    (df[payment_installments_column] >= 0) &
    (df[payment_value_column] >= 0)
]

df = df.drop_duplicates()

df.to_csv("cleaned_order_payments_dataset.csv", index=False)
