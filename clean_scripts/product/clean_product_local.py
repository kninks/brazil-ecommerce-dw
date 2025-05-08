import pandas as pd
import unidecode
import re

# Load CSV
df = pd.read_csv("../../original_dataset/olist_products_dataset.csv")
df_c = pd.read_csv("../../original_dataset/product_category_name_translation.csv")

product_id_column = "product_id"
product_category_name_column = "product_category_name"
product_name_length_column = "product_name_lenght"
product_description_length_column = "product_description_lenght"
product_photos_qty_column = "product_photos_qty"
product_weight_g_column = "product_weight_g"
product_length_cm_column = "product_length_cm"
product_height_cm_column = "product_height_cm"
product_width_cm_column = "product_width_cm"

c_name_column = "product_category_name"
c_name_en_column = "product_category_name_english"


# --- Data Cleaning ------------------
def clean_id(prod_id):
    return str(prod_id).strip().lower()

def clean_product_category_name(category_name):
    return str(category_name).strip().lower()

def cast_to_int(column):
    return column.astype("Int64", errors="ignore")

def cast_to_float(column):
    return column.astype("float64", errors="ignore")

df = (
    df.dropna(subset=[
        product_id_column,
        product_category_name_column,
        product_photos_qty_column,
        product_weight_g_column,
        product_length_cm_column,
        product_height_cm_column,
        product_width_cm_column
    ])
    .drop(columns=[product_name_length_column, product_description_length_column])
)

df[product_id_column] = df[product_id_column].apply(clean_id)
df[product_category_name_column] = df[product_category_name_column].apply(clean_product_category_name)
df[product_photos_qty_column] = cast_to_int(df[product_photos_qty_column])
df[product_weight_g_column] = cast_to_float(df[product_weight_g_column])
df[product_length_cm_column] = cast_to_float(df[product_length_cm_column])
df[product_height_cm_column] = cast_to_float(df[product_height_cm_column])
df[product_width_cm_column] = cast_to_float(df[product_width_cm_column])

df = df[
    (df[product_photos_qty_column] >= 0) &
    (df[product_weight_g_column] >= 0) &
    (df[product_length_cm_column] >= 0) &
    (df[product_height_cm_column] >= 0) &
    (df[product_width_cm_column] >= 0)
]

df = (
    df.merge(df_c[[c_name_column, c_name_en_column]],
             left_on=product_category_name_column,
             right_on=c_name_column,
             how='left')
    .drop(columns=[c_name_column])
    .rename(columns={c_name_en_column: product_category_name_column})
    .drop_duplicates()
)

df.to_csv("cleaned_products_dataset.csv", index=False)