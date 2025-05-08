DROP PROCEDURE IF EXISTS load_seller_dim;

DELIMITER $$

CREATE PROCEDURE load_seller_dim()
BEGIN
    INSERT INTO DW.seller_dim (
        seller_key,
        seller_id,
        seller_geolocation_key,
        created_at,
        modified_at
    )
    SELECT
        IFNULL(d.seller_key, UUID()) AS seller_key,
        s.seller_id,
        geo_d.geolocation_key,
        IFNULL(d.created_at, NOW()) AS created_at,
        NOW() AS modified_at
    FROM DB.sellers s
         LEFT JOIN DW.geolocation_dim geo_d ON s.seller_zip_code_prefix = geo_d.zip_code_prefix
         LEFT JOIN DW.seller_dim d ON s.seller_id = d.seller_id

    ON DUPLICATE KEY UPDATE
    DW.seller_dim.seller_geolocation_key = VALUES(seller_geolocation_key),
        modified_at = IF(
            DW.seller_dim.seller_geolocation_key <> VALUES(seller_geolocation_key),
            NOW(),
            DW.seller_dim.modified_at
        );
END $$

DELIMITER ;