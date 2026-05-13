from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DecimalType, TimestampType, DateType, DoubleType,
)

SCHEMAS: dict[str, StructType] = {
 
    "orders": StructType([
        StructField("order_id",                     StringType(),    nullable=True),
        StructField("customer_id",                  StringType(),    nullable=True),
        StructField("order_status",                 StringType(),    nullable=True),
        StructField("order_purchase_timestamp",     TimestampType(), nullable=True),
        StructField("order_approved_at",            TimestampType(), nullable=True),
        StructField("order_delivered_carrier_date", TimestampType(), nullable=True),
        StructField("order_delivered_customer_date",TimestampType(), nullable=True),
        StructField("order_estimated_delivery_date",TimestampType(), nullable=True),
    ]),
 
    "order_items": StructType([
        StructField("order_id",             StringType(),     nullable=True),
        StructField("order_item_id",        IntegerType(),    nullable=True),
        StructField("product_id",           StringType(),     nullable=True),
        StructField("seller_id",            StringType(),     nullable=True),
        StructField("shipping_limit_date",  TimestampType(),  nullable=True),
        StructField("price",               DecimalType(10,2), nullable=True),
        StructField("freight_value",       DecimalType(10,2), nullable=True),
    ]),
 
    "customers": StructType([
        StructField("customer_id",              StringType(), nullable=True),
        StructField("customer_unique_id",       StringType(), nullable=True),
        StructField("customer_zip_code_prefix", StringType(), nullable=True),
        StructField("customer_city",            StringType(), nullable=True),
        StructField("customer_state",           StringType(), nullable=True),
    ]),
 
    "products": StructType([
        StructField("product_id",                   StringType(),  nullable=True),
        StructField("product_category_name",        StringType(),  nullable=True),
        StructField("product_name_lenght",          IntegerType(), nullable=True),
        StructField("product_description_lenght",   IntegerType(), nullable=True),
        StructField("product_photos_qty",           IntegerType(), nullable=True),
        StructField("product_weight_g",             IntegerType(), nullable=True),
        StructField("product_length_cm",            IntegerType(), nullable=True),
        StructField("product_height_cm",            IntegerType(), nullable=True),
        StructField("product_width_cm",             IntegerType(), nullable=True),
    ]),
 
    "sellers": StructType([
        StructField("seller_id",                StringType(), nullable=True),
        StructField("seller_zip_code_prefix",   StringType(), nullable=True),
        StructField("seller_city",              StringType(), nullable=True),
        StructField("seller_state",             StringType(), nullable=True),
    ]),
 
    "order_payments": StructType([
        StructField("order_id",              StringType(),     nullable=True),
        StructField("payment_sequential",    IntegerType(),    nullable=True),
        StructField("payment_type",          StringType(),     nullable=True),
        StructField("payment_installments",  IntegerType(),    nullable=True),
        StructField("payment_value",        DecimalType(10,2), nullable=True),
    ]),
 
    "order_reviews": StructType([
        StructField("review_id",                StringType(),    nullable=True),
        StructField("order_id",                 StringType(),    nullable=True),
        StructField("review_score",             IntegerType(),   nullable=True),
        StructField("review_comment_title",     StringType(),    nullable=True),
        StructField("review_comment_message",   StringType(),    nullable=True),
        StructField("review_creation_date",     TimestampType(), nullable=True),
        StructField("review_answer_timestamp",  TimestampType(), nullable=True),
    ]),
 
    "geolocation": StructType([
        StructField("geolocation_zip_code_prefix", StringType(),  nullable=True),
        StructField("geolocation_lat",             DoubleType(),  nullable=True),
        StructField("geolocation_lng",             DoubleType(),  nullable=True),
        StructField("geolocation_city",            StringType(),  nullable=True),
        StructField("geolocation_state",           StringType(),  nullable=True),
    ]),
 
    "product_category_name_translation": StructType([
        StructField("product_category_name",         StringType(), nullable=True),
        StructField("product_category_name_english", StringType(), nullable=True),
    ]),
}

# CSV source file mapping
CSV_FILES: dict[str, str] = {
    "orders":                           "olist_orders_dataset.csv",
    "order_items":                      "olist_order_items_dataset.csv",
    "customers":                        "olist_customers_dataset.csv",
    "products":                         "olist_products_dataset.csv",
    "sellers":                          "olist_sellers_dataset.csv",
    "order_payments":                   "olist_order_payments_dataset.csv",
    "order_reviews":                    "olist_order_reviews_dataset.csv",
    "geolocation":                      "olist_geolocation_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}

# Business key(s) của từng bảng — dùng để dedup và DQ PK check
BUSINESS_KEYS: dict[str, list[str]] = {
    "orders":                           ["order_id"],
    "order_items":                      ["order_id", "order_item_id"],
    "customers":                        ["customer_id"],
    "products":                         ["product_id"],
    "sellers":                          ["seller_id"],
    "order_payments":                   ["order_id", "payment_sequential"],
    "order_reviews":                    ["review_id", "order_id"],
    "geolocation":                      ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"],
    "product_category_name_translation": ["product_category_name"],
}

NOT_NULL_COLS: dict[str, list[str]] = {
    "orders":                           ["order_id", "customer_id"],
    "order_items":                      ["order_id", "order_item_id"],
    "customers":                        ["customer_id"],
    "products":                         ["product_id"],
    "sellers":                          ["seller_id", "seller_zip_code_prefix"],
    "order_payments":                   ["order_id", "payment_sequential"],
    "order_reviews":                    ["review_id", "order_id"],
    "geolocation":                      ["geolocation_zip_code_prefix",
                                         "geolocation_lat", "geolocation_lng"],
    "product_category_name_translation": ["product_category_name"],
}

PARTITION_COLS: dict[str, str | None] = {
    "orders":                           "order_purchase_timestamp",
    "order_items":                      "shipping_limit_date",
    "customers":                        None,
    "products":                         None,
    "sellers":                          None,
    "order_payments":                   None,
    "order_reviews":                    None,
    "geolocation":                      None,
    "product_category_name_translation": None,
}

