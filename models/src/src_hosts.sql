WITH hosts AS (
    SELECT 
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        host_listings_count
    FROM 
        {{ source('airbnb', 'listings') }}
)

SELECT 
    *
FROM 
    hosts