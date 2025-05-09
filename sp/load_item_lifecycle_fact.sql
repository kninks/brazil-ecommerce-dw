DROP PROCEDURE IF EXISTS load_item_lifecycle_fact;

DELIMITER $$

CREATE PROCEDURE load_item_lifecycle_fact()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE batch_size INT DEFAULT 10000;
    DECLARE offset_val INT DEFAULT 0;
    DECLARE rows_in_batch INT;

    WHILE NOT done DO
        DROP TEMPORARY TABLE IF EXISTS tmp_item_lifecycle_fact;

        -- Create a temporary table to store the data before inserting into the fact table
        CREATE TEMPORARY TABLE tmp_item_lifecycle_fact AS
        SELECT
            ap.product_key AS product_key,
            ap.durable_product_key AS durable_product_key,
            ac.customer_key AS customer_key,
            ac.durable_customer_key AS durable_customer_key,
            s.seller_key AS seller_key,
            si.sale_indicator_key AS sale_indicator_key,
            DATE(o.order_purchase_timestamp) AS purchase_date_key,
            CONCAT(DATE_FORMAT(o.order_purchase_timestamp, '%H:%i'), ':00') AS purchase_time_key,
            DATE(o.order_approved_at) AS approval_date_key,
            CONCAT(DATE_FORMAT(o.order_approved_at, '%H:%i'), ':00') AS approval_time_key,
            DATE(o.order_delivered_carrier_date) AS delivery_date_key,
            CONCAT(DATE_FORMAT(o.order_delivered_carrier_date, '%H:%i'), ':00') AS delivery_time_key,
            DATE(o.order_delivered_customer_date) AS arrival_date_key,
            CONCAT(DATE_FORMAT(o.order_delivered_customer_date, '%H:%i'), ':00') AS arrival_time_key,
            DATE(o.order_estimated_delivery_date) AS estimated_arrival_date_key,
            CONCAT(DATE_FORMAT(o.order_estimated_delivery_date, '%H:%i'), ':00') AS estimated_arrival_time_key,
            o.order_id,
            batched_oi.order_item_id,
            DATEDIFF(o.order_approved_at, o.order_purchase_timestamp) AS purchase_approval_lag,
            DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS purchase_delivery_lag,
            DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) AS purchase_arrival_lag,
            DATEDIFF(o.order_estimated_delivery_date, o.order_purchase_timestamp) AS purchase_estimated_arrival_lag,
            DATEDIFF(o.order_delivered_carrier_date, o.order_approved_at) AS approval_delivery_lag,
            DATEDIFF(o.order_delivered_customer_date, o.order_approved_at) AS approval_arrival_lag,
            DATEDIFF(o.order_estimated_delivery_date, o.order_approved_at) AS approval_estimated_arrival_lag,
            DATEDIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date) AS delivery_arrival_lag,
            DATEDIFF(o.order_estimated_delivery_date, o.order_delivered_carrier_date) AS delivery_estimated_arrival_lag,
            DATEDIFF(o.order_estimated_delivery_date, o.order_delivered_customer_date) AS arrival_estimated_arrival_lag
        FROM (
            SELECT *
            FROM DB.order_items oi
            ORDER BY oi.order_id, oi.order_item_id
            LIMIT batch_size OFFSET offset_val
        ) batched_oi
        INNER JOIN DB.orders o ON batched_oi.order_id = o.order_id
        INNER JOIN DW.all_customer_dim ac ON o.customer_id = ac.customer_id
        INNER JOIN DW.all_product_dim ap ON batched_oi.product_id = ap.product_id AND ap.current_flag = 'is_current'
        INNER JOIN DW.seller_dim s ON batched_oi.seller_id = s.seller_id
        INNER JOIN DB.order_payments op ON batched_oi.order_id = op.order_id
        INNER JOIN DW.sale_indicator_dim si ON (
            op.payment_type = si.payment_type
            AND op.payment_installments = si.payment_installments
            AND op.payment_sequential = si.payment_sequential
            AND o.order_status = si.order_status
        );

        SET rows_in_batch = (SELECT COUNT(*) FROM tmp_item_lifecycle_fact);

        IF rows_in_batch = 0 THEN
            SET done = TRUE;
        ELSE
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

            SET offset_val = offset_val + batch_size;
        END IF;

        DROP TEMPORARY TABLE IF EXISTS tmp_item_lifecycle_fact;
    END WHILE;
END $$

DELIMITER ;