select
    LISTING_ID,
    DATE as review_date,
    REVIEWER_NAME,
    COMMENTS as review_text
from
    {{ source('airbnb', 'reviews') }}