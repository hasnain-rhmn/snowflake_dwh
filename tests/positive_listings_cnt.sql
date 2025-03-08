-- tests/positive_balance.sql
SELECT *
FROM {{ ref('dim_hosts') }}
WHERE host_listings_count < 0
