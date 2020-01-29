select
    stars,
    avg(food_photos),
    avg(drink_photos),
    avg(inside_photos),
    avg(outside_photos),
    avg(all_photos)
from
    business_with_processed_photos
group by
    stars;
    