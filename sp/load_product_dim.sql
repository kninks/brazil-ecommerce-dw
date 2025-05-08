DROP PROCEDURE IF EXISTS load_product_dim;

DELIMITER $$

CREATE PROCEDURE load_product_dim()
BEGIN
    -- Insert new products into current dim
    INSERT INTO DW.current_product_dim (
        durable_product_key,
        product_id,
        category,
        photos_quantity,
        height,
        length,
        width,
        weight,
        created_at,
        modified_at
    )
    SELECT
        UUID(),
        p.product_id,
        p.product_category_name,
        p.product_photos_qty,
        p.product_height_cm,
        p.product_length_cm,
        p.product_width_cm,
        p.product_weight_g,
        NOW(),
        NOW()
    FROM DB.products p
        LEFT JOIN DW.current_product_dim cd ON p.product_id = cd.product_id
    WHERE cd.product_id IS NULL;

    -- Update existing products on current dim
    UPDATE DW.current_product_dim cd
    INNER JOIN DB.products p ON cd.product_id = p.product_id
    SET
        cd.category = p.product_category_name,
        cd.photos_quantity = p.product_photos_qty,
        cd.height = p.product_height_cm,
        cd.length = p.product_length_cm,
        cd.width = p.product_width_cm,
        cd.weight = p.product_weight_g,
        cd.modified_at = NOW()
    WHERE (
       cd.category <> p.product_category_name OR
       cd.photos_quantity <> p.product_photos_qty OR
       cd.height <> p.product_height_cm OR
       cd.length <> p.product_length_cm OR
       cd.width <> p.product_width_cm OR
       cd.weight <> p.product_weight_g
    );

    -- Update expired_date and is current flag on all dim
    UPDATE DW.all_product_dim ad
    INNER JOIN DB.products p ON ad.product_id = p.product_id
    SET
        ad.expired_at = NOW(),
        ad.current_flag = 'is_expired'
    WHERE
        ad.current_flag = 'is_current' AND (
            ad.category <> p.product_category_name OR
            ad.photos_quantity <> p.product_photos_qty OR
            ad.height <> p.product_height_cm OR
            ad.length <> p.product_length_cm OR
            ad.width <> p.product_width_cm OR
            ad.weight <> p.product_weight_g
        );

    -- Insert new and updated products into all dim
    INSERT INTO DW.all_product_dim (
        product_key,
        durable_product_key,
        product_id,
        category,
        photos_quantity,
        height,
        length,
        width,
        weight,
        effective_at,
        expired_at,
        current_flag
    )
    SELECT
        UUID(),
        cd.durable_product_key,
        p.product_id,
        p.product_category_name,
        p.product_photos_qty,
        p.product_height_cm,
        p.product_length_cm,
        p.product_width_cm,
        p.product_weight_g,
        NOW(),
        '9999-12-31 23:59:59',
        'is_current'
    FROM DB.products p
    JOIN DW.current_product_dim cd ON p.product_id = cd.product_id
    LEFT JOIN DW.all_product_dim ad ON p.product_id = ad.product_id AND ad.current_flag = 'is_current'
    WHERE
        ad.product_key IS NULL OR
        ad.category <> p.product_category_name OR
        ad.photos_quantity <> p.product_photos_qty OR
        ad.height <> p.product_height_cm OR
        ad.length <> p.product_length_cm OR
        ad.width <> p.product_width_cm OR
        ad.weight <> p.product_weight_g;
END $$

DELIMITER ;