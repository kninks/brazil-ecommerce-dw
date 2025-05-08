DROP PROCEDURE IF EXISTS load_geolocation_dim;

DELIMITER $$

CREATE PROCEDURE load_geolocation_dim()
BEGIN
    INSERT INTO DW.geolocation_dim (
        geolocation_key,
        zip_code_prefix,
        city,
        state,
        latitude,
        longitude,
        created_at,
        modified_at
    )
    SELECT
        IFNULL(d.geolocation_key, UUID()) AS geolocation_key,
        g.geolocation_zip_code_prefix,
        g.geolocation_city,
        g.geolocation_state,
        g.geolocation_lat,
        g.geolocation_lng,
        IFNULL(d.created_at, NOW()) AS created_at,
        NOW() AS modified_at
    FROM DB.geolocation g
    LEFT JOIN DW.geolocation_dim d ON g.geolocation_zip_code_prefix = d.zip_code_prefix

    ON DUPLICATE KEY UPDATE
        DW.geolocation_dim.city = VALUES(city),
        DW.geolocation_dim.state = VALUES(state),
        DW.geolocation_dim.latitude = VALUES(latitude),
        DW.geolocation_dim.longitude = VALUES(longitude),
        DW.geolocation_dim.modified_at = IF(
            DW.geolocation_dim.city <> VALUES(city)
                OR DW.geolocation_dim.state <> VALUES(state)
                OR DW.geolocation_dim.latitude <> VALUES(latitude)
                OR DW.geolocation_dim.longitude <> VALUES(longitude),
            NOW(),
            DW.geolocation_dim.modified_at
        );
END $$

DELIMITER ;