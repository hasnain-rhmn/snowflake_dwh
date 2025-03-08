WITH listings AS (
    SELECT 
        id as listing_id,
        name AS listing_name,
        host_id,
        neighbourhood,
        room_type,
        price,
        minimum_nights,
        number_of_reviews,
        review_scores_rating,
        availability_365
    FROM {{ source('airbnb', 'listings') }}
)

SELECT 
    * 
FROM 
    listings