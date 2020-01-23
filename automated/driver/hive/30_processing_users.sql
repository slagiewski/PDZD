create table if not exists users_labeled (
    average_stars float,
    compliment_cool int,
    compliment_cute int,
    compliment_funny int,
    compliment_hot int,
    compliment_list int,
    compliment_more int,
    compliment_note int,
    compliment_photos int,
    compliment_plain int,
    compliment_profile int,
    compliment_writer int,
    cool int,
    elite string,
    fans int,
    friends string,
    funny int,
    name string,
    review_count int,
    useful int,
    user_id string,
    yelping_since string,
    days_since_registered int,
    days_since_registered_label string
    )
row format delimited
    fields terminated by '\u001f'
    stored as orc;

insert into users_labeled
select
    average_stars,
    compliment_cool,
    compliment_cute,
    compliment_funny,
    compliment_hot,
    compliment_list,
    compliment_more,
    compliment_note,
    compliment_photos,
    compliment_plain,
    compliment_profile,
    compliment_writer,
    cool,
    elite,
    fans,
    friends,
    funny,
    name,
    review_count,
    useful,
    user_id,
    yelping_since,
    datediff(${base_date}, yelping_since),
    case
        when datediff(${base_date}, yelping_since) < 1 then 'below_day'
        when datediff(${base_date}, yelping_since) < 7 then 'day'
        when datediff(${base_date}, yelping_since) < 31 then'week'
        when datediff(${base_date}, yelping_since) < 93 then 'month'
        when datediff(${base_date}, yelping_since) < 183 then 'quarter'
        when datediff(${base_date}, yelping_since) < 365 then 'half_year'
        when datediff(${base_date}, yelping_since) < 730 then 'year'
        else 'over_year' 
    end 
from users;