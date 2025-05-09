INSERT INTO DW.date_dim (
    date_key, day, month, month_name, year,
    day_of_week, day_of_week_name, week, quarter
)
WITH RECURSIVE date_range AS (
    SELECT DATE('2016-01-01') AS dt
UNION ALL
SELECT DATE_ADD(dt, INTERVAL 1 DAY)
FROM date_range
WHERE dt < '2025-12-31'
    )
SELECT
    dt AS date_key,
    DAY(dt) AS day,
    MONTH(dt) AS month,
    DATE_FORMAT(dt, '%M') AS month_name,
    YEAR(dt) AS year,
    WEEKDAY(dt) AS day_of_week, -- 0 = Monday
    DATE_FORMAT(dt, '%W') AS day_of_week_name,
    WEEK(dt, 1) AS week,        -- mode 1 = week starts Monday
    QUARTER(dt) AS quarter
FROM date_range;