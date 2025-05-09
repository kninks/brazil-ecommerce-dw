INSERT INTO DW.time_dim (
    time_key, hour, minute, period_of_day
)
WITH RECURSIVE time_series AS (
    SELECT MAKETIME(0, 0, 0) AS t
    UNION ALL
    SELECT ADDTIME(t, '00:01:00')
    FROM time_series
    WHERE t < MAKETIME(23, 59, 0)
)
SELECT
    t AS time_key,
    HOUR(t) AS hour,
    MINUTE(t) AS minute,
    CASE
    WHEN HOUR(t) BETWEEN 5 AND 11 THEN 'morning'
    WHEN HOUR(t) BETWEEN 12 AND 17 THEN 'afternoon'
    WHEN HOUR(t) BETWEEN 18 AND 21 THEN 'evening'
    ELSE 'night'
END AS period_of_day
FROM time_series;
