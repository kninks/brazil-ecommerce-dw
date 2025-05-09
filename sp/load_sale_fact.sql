DROP PROCEDURE IF EXISTS load_item_sale_fact;

DELIMITER $$

CREATE PROCEDURE load_item_sale_fact()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE batch_size INT DEFAULT 10000;
    DECLARE offset_val INT DEFAULT 0;
    DECLARE rows_in_batch INT;

    WHILE NOT done DO
        DROP TEMPORARY TABLE IF EXISTS tmp_item_sale_fact;

        CREATE TEMPORARY TABLE tmp_item_sale_fact AS
        SELECT
            oik.product_key AS product_key,
            oik.durable_product_key AS durable_product_key,
            ok.customer_key AS customer_key,
            ok.durable_customer_key AS durable_customer_key,
            oik.seller_key AS seller_key,
            si.sale_indicator_key AS sale_indicator_key,
            DATE(ok.order_purchase_timestamp) AS purchase_date_key,
            CONCAT(DATE_FORMAT(ok.order_purchase_timestamp, '%H:%i'), ':00') AS purchase_time_key,
            DATE(oik.shipping_limit_date) AS shipping_limit_date_key,
            CONCAT(DATE_FORMAT(oik.shipping_limit_date, '%H:%i'), ':00') AS shipping_limit_time_key,
            oik.order_id AS order_id,
            oik.order_item_id AS order_item_id,
            oik.price AS price,
            oik.freight_value AS freight_value,
            oik.price + oik.freight_value AS total_amount
        FROM (
             SELECT
                 oi.order_id,
                 oi.order_item_id,
                 ap.product_key,
                 ap.durable_product_key,
                 s.seller_key,
                 oi.shipping_limit_date,
                 oi.price,
                 oi.freight_value
             FROM DB.order_items oi
                 INNER JOIN DW.all_product_dim ap ON oi.product_id = ap.product_id AND ap.current_flag = 'is_current'
                 INNER JOIN DW.seller_dim s ON oi.seller_id = s.seller_id
             ORDER BY oi.order_id, oi.order_item_id
             LIMIT batch_size OFFSET offset_val
        ) oik
            INNER JOIN (
                SELECT
                    o.order_id,
                    o.order_status,
                    o.order_purchase_timestamp,
                    ac.customer_key,
                    ac.durable_customer_key
                FROM DB.orders o
                     INNER JOIN DW.all_customer_dim ac ON o.customer_id = ac.customer_id
            ) ok ON oik.order_id = ok.order_id
            INNER JOIN DB.order_payments op ON oik.order_id = op.order_id
            INNER JOIN DW.sale_indicator_dim si ON (
                op.payment_type = si.payment_type
                    AND op.payment_installments = si.payment_installments
                    AND op.payment_sequential = si.payment_sequential
                    AND ok.order_status = si.order_status
            );

        SET rows_in_batch = (SELECT COUNT(*) FROM tmp_item_sale_fact);

        -- Insert new records into the fact table
        IF rows_in_batch = 0 THEN
            SET done = TRUE;
        ELSE
            INSERT INTO DW.item_sale_fact (
                product_key,
                durable_product_key,
                customer_key,
                durable_customer_key,
                seller_key,
                sale_indicator_key,
                purchase_date_key,
                purchase_time_key,
                shipping_limit_date_key,
                shipping_limit_time_key,
                order_id,
                order_item_id,
                price,
                freight_value,
                total_amount
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
                shipping_limit_date_key,
                shipping_limit_time_key,
                order_id,
                order_item_id,
                price,
                freight_value,
                total_amount
            FROM tmp_item_sale_fact;

            SET offset_val = offset_val + batch_size;
        END IF;

        DROP TEMPORARY TABLE IF EXISTS tmp_item_sale_fact;
    END WHILE;
END $$

DELIMITER ;