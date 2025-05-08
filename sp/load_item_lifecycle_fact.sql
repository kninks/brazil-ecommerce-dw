DROP PROCEDURE IF EXISTS load_item_lifecycle_fact;

DELIMITER $$

CREATE PROCEDURE load_item_lifecycle_fact()
BEGIN
    -- Create a temporary table to store the data before inserting into the fact table
    CREATE TEMPORARY TABLE tmp_item_lifecycle_fact AS
    SELECT
        oik.product_key AS product_key,
        oik.durable_product_key AS durable_product_key,
        ok.customer_key AS customer_key,
        ok.durable_customer_key AS durable_customer_key,
        oik.seller_key AS seller_key,
        si.sale_indicator_key AS sale_indicator_key,
        DATE(ok.order_purchase_timestamp) AS purchase_date_key,
        CONCAT(DATE_FORMAT(ok.order_purchase_timestamp, '%H:%i'), ':00') AS purchase_time_key,
        DATE(ok.order_approved_at) AS approval_date_key,
        CONCAT(DATE_FORMAT(ok.order_approved_at, '%H:%i'), ':00') AS approval_time_key,
        DATE(ok.order_delivered_carrier_date) AS delivery_date_key,
        CONCAT(DATE_FORMAT(ok.order_delivered_carrier_date, '%H:%i'), ':00') AS delivery_time_key,
        DATE(ok.order_delivered_customer_date) AS arrival_date_key,
        CONCAT(DATE_FORMAT(ok.order_delivered_customer_date, '%H:%i'), ':00') AS arrival_time_key,
        DATE(ok.order_estimated_delivery_date) AS estimated_arrival_date_key,
        CONCAT(DATE_FORMAT(ok.order_estimated_delivery_date, '%H:%i'), ':00') AS estimated_arrival_time_key,
        oik.order_id,
        oik.order_item_id,
        DATEDIFF(ok.order_approved_at, ok.order_purchase_timestamp) AS purchase_approval_lag,
        DATEDIFF(ok.order_delivered_customer_date, ok.order_purchase_timestamp) AS purchase_delivery_lag,
        DATEDIFF(ok.order_delivered_customer_date, ok.order_purchase_timestamp) AS purchase_arrival_lag,
        DATEDIFF(ok.order_estimated_delivery_date, ok.order_purchase_timestamp) AS purchase_estimated_arrival_lag,
        DATEDIFF(ok.order_delivered_carrier_date, ok.order_approved_at) AS approval_delivery_lag,
        DATEDIFF(ok.order_delivered_customer_date, ok.order_approved_at) AS approval_arrival_lag,
        DATEDIFF(ok.order_estimated_delivery_date, ok.order_approved_at) AS approval_estimated_arrival_lag,
        DATEDIFF(ok.order_delivered_customer_date, ok.order_delivered_carrier_date) AS delivery_arrival_lag,
        DATEDIFF(ok.order_estimated_delivery_date, ok.order_delivered_carrier_date) AS delivery_estimated_arrival_lag,
        DATEDIFF(ok.order_estimated_delivery_date, ok.order_delivered_customer_date) AS arrival_estimated_arrival_lag
    FROM (
        SELECT
        o.order_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        ac.customer_key,
        ac.durable_customer_key
        FROM DB.orders o
            INNER JOIN DW.all_customer_dim ac ON o.customer_id = ac.customer_id
    ) ok INNER JOIN (
        SELECT
            oi.order_id,
            oi.order_item_id,
            ap.product_key,
            ap.durable_product_key,
            s.seller_key
        FROM DB.order_items oi
            INNER JOIN DW.all_product_dim ap ON oi.product_id = ap.product_id AND ap.current_flag = 'is_current'
            INNER JOIN DW.seller_dim s ON oi.seller_id = s.seller_id
    ) oik ON oik.order_id = ok.order_id
        INNER JOIN DB.order_payments op ON oik.order_id = op.order_id
        INNER JOIN DW.sale_indicator_dim si ON (
        op.payment_type = si.payment_type
            AND op.payment_installments = si.payment_installments
            AND op.payment_sequential = si.payment_sequential
            AND ok.order_status = si.order_status
        );

    -- Insert new records into the fact table
    INSERT INTO DW.item_lifecycle_fact (
        product_key,
        durable_product_key,
        customer_key,
        durable_customer_key,
        seller_key,
        sale_indicator_key,
        purchase_date_key,
        purchase_time_key,
        approval_date_key,
        approval_time_key,
        delivery_date_key,
        delivery_time_key,
        arrival_date_key,
        arrival_time_key,
        estimated_arrival_date_key,
        estimated_arrival_time_key,
        order_id,
        order_item_id,
        purchase_approval_lag,
        purchase_delivery_lag,
        purchase_arrival_lag,
        purchase_estimated_arrival_lag,
        approval_delivery_lag,
        approval_arrival_lag,
        approval_estimated_arrival_lag,
        delivery_arrival_lag,
        delivery_estimated_arrival_lag,
        arrival_estimated_arrival_lag
    )
    SELECT
        product_key,
        durable_product_key,
        customer_key,
        durable_customer_key,
        seller_key,
        sale_indicator_key,
        purchase_date_key,
        purchase_time_key,
        approval_date_key,
        approval_time_key,
        delivery_date_key,
        delivery_time_key,
        arrival_date_key,
        arrival_time_key,
        estimated_arrival_date_key,
        estimated_arrival_time_key,
        order_id,
        order_item_id,
        purchase_approval_lag,
        purchase_delivery_lag,
        purchase_arrival_lag,
        purchase_estimated_arrival_lag,
        approval_delivery_lag,
        approval_arrival_lag,
        approval_estimated_arrival_lag,
        delivery_arrival_lag,
        delivery_estimated_arrival_lag,
        arrival_estimated_arrival_lag
    FROM tmp_item_lifecycle_fact;

    DROP TEMPORARY TABLE IF EXISTS tmp_item_lifecycle_fact;
END $$

DELIMITER ;