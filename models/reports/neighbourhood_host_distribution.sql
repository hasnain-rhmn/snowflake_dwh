WITH host_neighbourhood_metrics AS (
    SELECT 
        l.neighbourhood,
        COUNT(DISTINCT h.host_id) AS total_hosts,
        COUNT(DISTINCT CASE WHEN h.host_is_superhost = TRUE THEN h.host_id END) AS superhosts,
        COUNT(DISTINCT CASE WHEN hlc.host_listings_count::INTEGER > 1 THEN h.host_id END) AS professional_hosts,
        COUNT(DISTINCT l.listing_id) AS total_properties
    FROM {{ ref('fact_listings') }} l
    JOIN {{ ref('dim_hosts') }} h ON l.host_id = h.host_id
    LEFT JOIN {{ ref('dim_hosts') }} hlc ON l.host_id = hlc.host_id
    GROUP BY l.neighbourhood
)

SELECT 
    neighbourhood,
    total_hosts,
    superhosts,
    professional_hosts,
    total_properties
FROM host_neighbourhood_metrics
ORDER BY total_properties DESC
