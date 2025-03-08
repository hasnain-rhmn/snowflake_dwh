SELECT 
    listing_id,
    listing_name,
    host_id,
    neighbourhood,
    room_type,
    REPLACE(REPLACE(price,'$'), ',') :: NUMBER(10,2) AS price,
    CASE WHEN minimum_nights = 0 THEN 1 ELSE minimum_nights END AS minimum_nights,
    number_of_reviews,
    review_scores_rating,
    availability_365
FROM
    {{ ref('src_listings') }}