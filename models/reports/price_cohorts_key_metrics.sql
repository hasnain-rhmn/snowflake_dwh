WITH price_distribution AS (
    SELECT 
        price,
        listing_id,
        listing_name,
        neighbourhood,
        room_type,
        NTILE(4) OVER (ORDER BY price) AS price_quartile
    FROM {{ ref('fact_listings') }}
),

labeled_quartiles AS (
    SELECT 
        listing_id,
        listing_name,
        neighbourhood,
        room_type,
        price,
        CASE 
            WHEN price_quartile = 1 THEN 'Budget Friendly'
            WHEN price_quartile = 2 THEN 'Mid-Range'
            WHEN price_quartile = 3 THEN 'Premium'
            ELSE 'Luxury'
        END AS price_cohort
    FROM price_distribution
),

availability_metrics AS (
    SELECT 
        lq.price_cohort,
        COUNT(DISTINCT lq.listing_id) AS total_listings,
        SUM(CASE WHEN da.available = TRUE THEN 1 ELSE 0 END) AS booked_days,
        COUNT(da.date) AS total_days,
        ROUND((SUM(CASE WHEN da.available = TRUE THEN 1 ELSE 0 END) / COUNT(da.date)) * 100, 2) AS occupancy_rate,
        ROUND(AVG(lq.price), 2) AS avg_price
    FROM labeled_quartiles lq
    JOIN {{ ref('dim_availability') }} da ON lq.listing_id = da.listing_id
    GROUP BY lq.price_cohort
)

SELECT 
    price_cohort,
    total_listings,
    avg_price,
    booked_days,
    total_days,
    occupancy_rate
FROM availability_metrics
ORDER BY avg_price
