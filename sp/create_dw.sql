CREATE DATABASE DW;
USE DW;

-- SCD 1
CREATE TABLE geolocation_dim (
     geolocation_key CHAR(36) PRIMARY KEY,
     zip_code_prefix VARCHAR(10),
     city VARCHAR(100),
     state VARCHAR(10),
     latitude DOUBLE,
     longitude DOUBLE,
     created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
     modified_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);


-- SCD 1
CREATE TABLE current_customer_dim (
    durable_customer_key CHAR(36) PRIMARY KEY,
    customer_unique_id CHAR(32),
    customer_geolocation_key CHAR(36),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    modified_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (customer_geolocation_key) REFERENCES geolocation_dim(geolocation_key)
);


-- SCD 2
CREATE TABLE all_customer_dim (
    customer_key CHAR(36) PRIMARY KEY,
    durable_customer_key CHAR(36),
    customer_id CHAR(32),
    customer_geolocation_key CHAR(36),
    effective_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    expired_at DATETIME DEFAULT '9999-12-31 23:59:59',
    current_flag VARCHAR(20) DEFAULT 'is_effective',

    FOREIGN KEY (durable_customer_key) REFERENCES current_customer_dim(durable_customer_key),
    FOREIGN KEY (customer_geolocation_key) REFERENCES geolocation_dim(geolocation_key)
);


-- SCD 1
CREATE TABLE seller_dim (
    seller_key CHAR(36) PRIMARY KEY,
    seller_id CHAR(32),
    seller_geolocation_key CHAR(36),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    modified_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (seller_geolocation_key) REFERENCES geolocation_dim(geolocation_key)
);


-- SCD 1
CREATE TABLE current_product_dim (
    durable_product_key CHAR(36) PRIMARY KEY,
    product_id CHAR(32),
    category VARCHAR(100),
    photos_quantity INT,
    height FLOAT,
    length FLOAT,
    weight FLOAT,
    width FLOAT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    modified_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);


-- SCD 2
CREATE TABLE all_product_dim (
    product_key CHAR(36) PRIMARY KEY,
    durable_product_key CHAR(36),
    product_id CHAR(32),
    category VARCHAR(100),
    photos_quantity FLOAT,
    weight FLOAT,
    height FLOAT,
    length FLOAT,
    width FLOAT,
    effective_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    expired_at DATETIME DEFAULT '9999-12-31 23:59:59',
    current_flag VARCHAR(20) DEFAULT 'is_effective',

    FOREIGN KEY (durable_product_key) REFERENCES current_product_dim(durable_product_key)
);


-- SCD 0
CREATE TABLE date_dim (
    date_key DATE PRIMARY KEY, -- Format: YYYY-MM-DD
    day INT,
    month INT,
    month_name VARCHAR(20),
    year INT,
    day_of_week INT,
    day_of_week_name VARCHAR(20),
    week INT,
    quarter INT
);


-- SCD 0
CREATE TABLE time_dim (
    time_key TIME PRIMARY KEY, -- Format: HH:MM:ss
    hour INT,
    minute INT,
    period_of_day VARCHAR(20)
);


-- SCD 1
CREATE TABLE sale_indicator_dim (
    sale_indicator_key CHAR(36) PRIMARY KEY,
    order_status VARCHAR(20),
    payment_type VARCHAR(20),
    payment_sequential INT,
    payment_installments INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    modified_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);


CREATE TABLE item_sale_fact (
    product_key CHAR(36),
    durable_product_key CHAR(36),
    customer_key CHAR(36),
    durable_customer_key CHAR(36),
    seller_key CHAR(36),
    sale_indicator_key CHAR(36),

    purchase_date_key DATE,
    purchase_time_key TIME,
    shipping_limit_date_key DATE,
    shipping_limit_time_key TIME,

--     order_id and order_item_id = composite key
    order_id CHAR(32), -- DD
    order_item_id INT, -- DD

    price FLOAT,
    freight_value FLOAT,
    total_amount FLOAT,

    FOREIGN KEY (product_key) REFERENCES all_product_dim(product_key),
    FOREIGN KEY (durable_product_key) REFERENCES current_product_dim(durable_product_key),
    FOREIGN KEY (customer_key) REFERENCES all_customer_dim(customer_key),
    FOREIGN KEY (durable_customer_key) REFERENCES current_customer_dim(durable_customer_key),
    FOREIGN KEY (seller_key) REFERENCES seller_dim(seller_key),
    FOREIGN KEY (sale_indicator_key) REFERENCES sale_indicator_dim(sale_indicator_key),

    FOREIGN KEY (purchase_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY (purchase_time_key) REFERENCES time_dim(time_key),
    FOREIGN KEY (shipping_limit_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY (shipping_limit_time_key) REFERENCES time_dim(time_key)
);

CREATE TABLE item_lifecycle_fact (
    product_key CHAR(36),
    durable_product_key CHAR(36),
    customer_key CHAR(36),
    durable_customer_key CHAR(36),
    seller_key CHAR(36),
    sale_indicator_key CHAR(36),

    purchase_date_key DATE,
    approval_date_key DATE,
    delivery_date_key DATE,
    arrival_date_key DATE,
    estimated_arrival_date_key DATE,

    purchase_time_key TIME,
    approval_time_key TIME,
    delivery_time_key TIME,
    arrival_time_key TIME,
    estimated_arrival_time_key TIME,

--     order_id and order_item_id = composite key
    order_id CHAR(32), -- DD
    order_item_id INT, -- DD

    purchase_approval_lag INT,  -- in days
    purchase_delivery_lag INT,
    purchase_arrival_lag INT,
    purchase_estimated_arrival_lag INT,
    approval_delivery_lag INT,
    approval_arrival_lag INT,
    approval_estimated_arrival_lag INT,
    delivery_arrival_lag INT,
    delivery_estimated_arrival_lag INT,
    arrival_estimated_arrival_lag INT,

    FOREIGN KEY (product_key) REFERENCES all_product_dim(product_key),
    FOREIGN KEY (durable_product_key) REFERENCES current_product_dim(durable_product_key),
    FOREIGN KEY (customer_key) REFERENCES all_customer_dim(customer_key),
    FOREIGN KEY (durable_customer_key) REFERENCES current_customer_dim(durable_customer_key),
    FOREIGN KEY (seller_key) REFERENCES seller_dim(seller_key),
    FOREIGN KEY (sale_indicator_key) REFERENCES sale_indicator_dim(sale_indicator_key),

    FOREIGN KEY (purchase_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY (approval_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY (delivery_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY (arrival_date_key) REFERENCES date_dim(date_key),
    FOREIGN KEY (estimated_arrival_date_key) REFERENCES date_dim(date_key),

    FOREIGN KEY (purchase_time_key) REFERENCES time_dim(time_key),
    FOREIGN KEY (approval_time_key) REFERENCES time_dim(time_key),
    FOREIGN KEY (delivery_time_key) REFERENCES time_dim(time_key),
    FOREIGN KEY (arrival_time_key) REFERENCES time_dim(time_key),
    FOREIGN KEY (estimated_arrival_time_key) REFERENCES time_dim(time_key)
);
