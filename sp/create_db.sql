CREATE DATABASE DB;
USE DB;


CREATE TABLE geolocation (
     geolocation_zip_code_prefix VARCHAR(10) PRIMARY KEY,
     geolocation_lat FLOAT,
     geolocation_lng FLOAT,
     geolocation_city VARCHAR(100),
     geolocation_state VARCHAR(10)
);


CREATE TABLE customers (
    customer_id CHAR(32) PRIMARY KEY,
    customer_unique_id CHAR(32) UNIQUE,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),
    customer_zip_code_prefix VARCHAR(10) NOT NULL,

    FOREIGN KEY (customer_zip_code_prefix) REFERENCES geolocation(geolocation_zip_code_prefix)
);


CREATE TABLE sellers (
     seller_id CHAR(32) PRIMARY KEY,
     seller_city VARCHAR(100),
     seller_state VARCHAR(10),
     seller_zip_code_prefix VARCHAR(10) NOT NULL,

     FOREIGN KEY (seller_zip_code_prefix) REFERENCES geolocation(geolocation_zip_code_prefix)
);


CREATE TABLE products (
      product_id CHAR(32) PRIMARY KEY,
      product_category_name VARCHAR(100),
      product_photos_qty FLOAT,
      product_weight_g FLOAT,
      product_length_cm FLOAT,
      product_height_cm FLOAT,
      product_width_cm FLOAT
);


CREATE TABLE orders (
    order_id CHAR(32) PRIMARY KEY,
    customer_id CHAR(32) NOT NULL,
    order_status VARCHAR(20),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,

    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);


CREATE TABLE order_items (
    order_id CHAR(32) NOT NULL,
    order_item_id INT NOT NULL,
    product_id CHAR(32),
    seller_id CHAR(32),
    shipping_limit_date DATETIME,
    price FLOAT,
    freight_value FLOAT,
    PRIMARY KEY (order_id, order_item_id),

    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);


CREATE TABLE order_payments (
    order_id CHAR(32) NOT NULL,
    payment_sequential INT,
    payment_type VARCHAR(20),
    payment_installments INT,
    payment_value FLOAT,

    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);


CREATE TABLE order_reviews (
    review_id CHAR(32) PRIMARY KEY,
    order_id CHAR(32) NOT NULL,
    review_score INT,
    review_comment_title VARCHAR(255),
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,

    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);


