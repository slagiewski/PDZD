create external table if not exists geodata (
    city string,
    latitude float,
    longitude float)
row format delimited 
    fields terminated by ',' 
    stored as textfile 
    location '/user/DataSources/CSV/geodata';

create table if not exists OSMdata (
    city string,
    latitude float,
    longitude float)
row format delimited 
    fields terminated by ','
    stored as orc;

insert overwrite table OSMdata
select
    *
from geodata;

create external table if not exists review (
    user_id string,
    business_id string,
    text string,
    date string,
    compliment_count int)
row format delimited 
fields terminated by ',' 
stored as textfile location '/user/DataSources/CSV/review';

create table if not exists reviews (
    user_id string,
    business_id string,
    text string,
    date string,
    compliment_count int)
row format delimited 
    fields terminated by ',' 
    stored as orc;

insert overwrite table reviews
select
    *
from
    review;

create external table if not exists checkin (
    business_id string,
    date string)
row format delimited
    fields terminated by ','
    stored as textfile
    location '/user/DataSources/CSV/checkin';

create table if not exists checkins (
    business_id string,
    date string)
row format delimited
    fields terminated by ','
    stored as orc;

insert overwrite table checkins
select
    *
from
    checkin;

create external table if not exists photo (
    business_id string,
    caption string,
    label string,
    photo_id string)
row format delimited
    fields terminated by ','
    stored as textfile
    location '/user/DataSources/CSV/photo';

create table if not exists photos (
    business_id string,
    caption string,
    label string,
    photo_id string)
row format delimited
    fields terminated by ','
    stored as orc;

insert overwrite table photos
select
    *
from
    photo;

create external table if not exists biznes (
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
    hours string)
row format delimited
    fields terminated by ','
    stored as textfile
    location '/user/DataSources/CSV/business';

create table if not exists business (
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
    hours string)
row format delimited
    fields terminated by ','
    stored as orc;

insert overwrite table business
select
    *
from
    biznes;

create external table if not exists user (
    name string,
    compliment_profile string,
    compliment_hot string,
    compliment_funny string,
    friends string,
    fans string,
    funny string,
    yelping_since string,
    compliment_photos string,
    compliment_more string,
    compliment_plain string,
    compliment_writer string,
    review_count string,
    user_id string,
    useful string,
    compliment_cute string,
    average_stars string,
    cool string,
    compliment_cool string,
    compliment_note string,
    compliment_list string,
    elite string)
row format delimited
    fields terminated by ','
    stored as textfile
    location '/user/DataSources/CSV/user';

create table if not exists users (
    name string,
    compliment_profile string,
    compliment_hot string,
    compliment_funny string,
    friends string,
    fans string,
    funny string,
    yelping_since string,
    compliment_photos string,
    compliment_more string,
    compliment_plain string,
    compliment_writer string,
    review_count string,
    user_id string,
    useful string,
    compliment_cute string,
    average_stars string,
    cool string,
    compliment_cool string,
    compliment_note string,
    compliment_list string,
    elite string)
row format delimited
    fields terminated by ','
    stored as orc;

insert overwrite table users
select
    *
from
    user;

create external table if not exists tip (
    user_id string,
    business_id string,
    text string,
    date string,
    compliment_count int)
row format delimited
    fields terminated by ','
    stored as textfile
    location '/user/DataSources/CSV/tip';

create table if not exists tips (
    user_id string,
    business_id string,
    text string,
    date string,
    compliment_count int)
row format delimited
    fields terminated by ','
    stored as orc;

insert overwrite table tips
select
    *
from
    tip;

