WITH host_performance AS (
    SELECT 
        h.host_is_superhost,
        ROUND(AVG(l.price), 2) AS avg_price,
        ROUND(AVG(l.review_scores_rating), 2) AS avg_review_score,
        SUM(CASE WHEN a.available = TRUE THEN 1 ELSE 0 END) AS booked_days,
        COUNT(a.date) AS total_days,
        ROUND((SUM(CASE WHEN a.available = TRUE THEN 1 ELSE 0 END) / COUNT(a.date)) * 100, 2) AS occupancy_rate
    FROM {{ ref('fact_listings') }} l
    JOIN {{ ref('dim_hosts') }} h ON l.host_id = h.host_id
    JOIN {{ ref('dim_availability') }} a ON l.listing_id = a.listing_id
    GROUP BY h.host_is_superhost
)

SELECT 
    host_is_superhost,
    avg_price,
    avg_review_score,
    booked_days,
    total_days,
    occupancy_rate
FROM host_performance
ORDER BY host_is_superhost DESC
