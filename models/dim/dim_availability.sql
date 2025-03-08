SELECT 
    listing_id,
    date,
    available,
    REPLACE(REPLACE(price,'$'), ',') :: NUMBER(10,2) AS price,
    CASE WHEN minimum_nights = 0 THEN 1 ELSE minimum_nights END AS minimum_nights,
    maximum_nights
FROM
    {{ ref('src_calendar') }}