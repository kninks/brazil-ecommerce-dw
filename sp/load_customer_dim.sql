DROP PROCEDURE IF EXISTS load_customer_dim;

DELIMITER $$

CREATE PROCEDURE load_customer_dim()
BEGIN
    -- Insert new customers into current dim
    INSERT INTO DW.current_customer_dim (
        durable_customer_key,
        customer_unique_id,
        customer_geolocation_key,
        created_at,
        modified_at
    )
    SELECT
        UUID(),
        c.customer_unique_id,
        gd.geolocation_key,
        NOW(),
        NOW()
    FROM (
         SELECT *
         FROM DB.customers
         GROUP BY customer_unique_id
    ) c
         INNER JOIN DW.geolocation_dim gd ON c.customer_zip_code_prefix = gd.zip_code_prefix
         LEFT JOIN DW.current_customer_dim ccd ON c.customer_unique_id = ccd.customer_unique_id
    WHERE ccd.customer_unique_id IS NULL;

    -- Update existing customers on current dim
    UPDATE DW.current_customer_dim ccd
        INNER JOIN DB.customers c ON ccd.customer_unique_id = c.customer_unique_id
        INNER JOIN DW.geolocation_dim gd ON c.customer_zip_code_prefix = gd.zip_code_prefix
    SET
        ccd.customer_geolocation_key = gd.geolocation_key,
        ccd.modified_at = NOW()
    WHERE ccd.customer_geolocation_key <> gd.geolocation_key;

    -- Update expired_date and is current flag on all dim
    WITH customer_with_key AS (
        SELECT
            ccd.durable_customer_key,
            gd.geolocation_key
        FROM DB.customers c
             INNER JOIN DW.current_customer_dim ccd ON ccd.customer_unique_id = c.customer_unique_id
             INNER JOIN DW.geolocation_dim gd ON gd.zip_code_prefix = c.customer_zip_code_prefix
    )
    UPDATE DW.all_customer_dim acd
        INNER JOIN customer_with_key cwk ON acd.durable_customer_key = cwk.durable_customer_key
    SET
        acd.expired_at = NOW(),
        acd.current_flag = 'is_expired'
    WHERE
        acd.current_flag = 'is_current'
        AND acd.customer_geolocation_key <> cwk.geolocation_key;

    -- Insert new and updated customers into all dim
    INSERT INTO DW.all_customer_dim (
        customer_key,
        durable_customer_key,
        customer_id,
        customer_geolocation_key,
        effective_at,
        expired_at,
        current_flag
    )
    SELECT
        UUID(),
        ccd.durable_customer_key,
        c.customer_id,
        gd.geolocation_key,
        NOW(),
        '9999-12-31 23:59:59',
        'is_current'
    FROM DB.customers c
         INNER JOIN DW.geolocation_dim gd ON c.customer_zip_code_prefix = gd.zip_code_prefix
         INNER JOIN DW.current_customer_dim ccd ON c.customer_unique_id = ccd.customer_unique_id;
END $$

DELIMITER ;