with calendar AS 
    (
        SELECT  
            *
        FROM
            {{ source('airbnb', 'calendar') }}
    )

SELECT 
    listing_id,
    date,
    available,
    price,
    minimum_nights,
    maximum_nights
FROM 
    calendar