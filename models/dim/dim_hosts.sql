WITH hosts AS (
    SELECT 
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        host_listings_count,
        ROW_NUMBER() OVER (PARTITION BY host_id ORDER BY host_since DESC) AS row_num
    FROM 
        {{ ref('src_hosts') }}
)

SELECT 
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    host_listings_count
FROM hosts
WHERE row_num = 1