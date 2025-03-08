WITH availability_trends AS (
    SELECT 
        h.host_is_superhost,
        l.listing_id,
        COUNT(CASE WHEN a.available = TRUE THEN 1 END) AS available_days,
        COUNT(a.date) AS total_days,
        ROUND((COUNT(CASE WHEN a.available = TRUE THEN 1 END) / COUNT(a.date)) * 100, 2) AS availability_rate
    FROM {{ ref('fact_listings') }} l
    JOIN {{ ref('dim_hosts') }} h ON l.host_id = h.host_id
    JOIN {{ ref('dim_availability') }} a ON l.listing_id = a.listing_id
    GROUP BY h.host_is_superhost, l.listing_id
)

SELECT 
    host_is_superhost,
    ROUND(AVG(availability_rate), 2) AS avg_availability_rate,
    COUNT(listing_id) AS total_listings
FROM availability_trends
GROUP BY host_is_superhost
ORDER BY host_is_superhost DESC
