create external table if not exists ext_processed_business_photos (
    business_id string,
    food int,
    drink int,
    inside int,
    outside int,
    all_photos int)
row format delimited 
fields terminated by ','
stored as textfile location '/user/Analysis2/middle_set';

create external table if not exists processed_business_photos (
    business_id string,
    food int,
    drink int,
    inside int,
    outside int,
    all_photos int)
row format delimited
    fields terminated by '\u001f'
    stored as orc;

insert overwrite table processed_business_photos
select
    business_id, food, drink, inside, outside, all_photos
from
    ext_processed_business_photos;

    

create table if not exists business_with_processed_photos (
    business_id string,
    name string,
    address string,
    city string,
    state string,
    postal_code string,
    latitude float,
    longitude float,
    stars float,
    review_count int,
    is_open int,
    attributes string,
    categories string,
    hours string,
    food_photos int,
    drink_photos int,
    inside_photos int,
    outside_photos int,
    all_photos int
    )
row format delimited
    fields terminated by '\u001f'
    stored as orc;

insert overwrite table business_with_processed_photos
select
    b.business_id,
    name, 
    address, 
    city, 
    state, 
    postal_code, 
    latitude, 
    longitude, 
    stars, 
    review_count, 
    is_open, 
    attributes, 
    categories, 
    hours,
    food,
    drink,
    inside,
    outside,
    all_photos
from 
    business b
join 
    processed_business_photos p on b.business_id = p.business_id;