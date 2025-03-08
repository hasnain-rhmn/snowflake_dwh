WITH review_availability AS (
    SELECT 
        l.listing_id,
        l.review_scores_rating,
        COUNT(CASE WHEN a.available = TRUE THEN 1 END) AS available_days,
        COUNT(a.date) AS total_days,
        ROUND((COUNT(CASE WHEN a.available = TRUE THEN 1 END) / COUNT(a.date)) * 100, 2) AS availability_rate
    FROM {{ ref('fact_listings') }} l
    JOIN {{ ref('dim_availability') }} a ON l.listing_id = a.listing_id
    GROUP BY l.listing_id, l.review_scores_rating
)

SELECT 
    review_scores_rating,
    ROUND(AVG(availability_rate), 2) AS avg_availability_rate,
    COUNT(listing_id) AS total_listings
FROM review_availability
GROUP BY review_scores_rating
ORDER BY review_scores_rating DESC
