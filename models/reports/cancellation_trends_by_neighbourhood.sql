WITH cancellations AS (
    SELECT 
        l.neighbourhood,
        COUNT(a.date) AS total_days,
        SUM(CASE WHEN a.available = FALSE AND l.number_of_reviews = 0 THEN 1 ELSE 0 END) AS potential_cancellations,
        ROUND((SUM(CASE WHEN a.available = FALSE AND l.number_of_reviews = 0 THEN 1 ELSE 0 END) / COUNT(a.date)) * 100, 2) AS cancellation_rate
    FROM {{ ref('fact_listings') }} l
    JOIN {{ ref('dim_availability') }} a ON l.listing_id = a.listing_id
    GROUP BY l.neighbourhood
)

SELECT 
    neighbourhood,
    total_days,
    potential_cancellations,
    cancellation_rate
FROM cancellations
ORDER BY cancellation_rate DESC
