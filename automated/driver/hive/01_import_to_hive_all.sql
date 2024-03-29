create external table if not exists ext_geodata (
    city string,
    latitude float,
    longitude float)
row format delimited 
    fields terminated by ','
    stored as textfile 
    location '/user/DataSources/CSV/geodata';

create table if not exists geodata (
    city string,
    latitude float,
    longitude float)
row format delimited 
    fields terminated by '\u001f'
    stored as orc;

insert overwrite table geodata
select
    *
from ext_geodata;

create external table if not exists ext_review (
    -- business_id,cool,date,funny,review_id,stars,text,useful,user_id
    business_id string,
    cool int,
    `date` string,
    funny int,
    review_id string,
    stars float,
    text string,
    useful int,
    user_id string)
row format delimited 
fields terminated by '\u001f' -- ASCII 31: unit separator, NOT PRINTABLE: needed because hive cannot handle quoted delimiters, every common separator exists in reviews file
stored as textfile location '/user/DataSources/CSV/review'
tblproperties ("skip.header.line.count"="1");

create table if not exists review (
    user_id string,
    business_id string,
    `text` string,
    `date` string,
    votes int)
row format delimited 
    fields terminated by '\u001f' 
    stored as orc;

insert overwrite table review
select
    user_id, business_id, `text`, `date`, (cool + funny + useful)
from
    ext_review;


create external table if not exists ext_checkin (
    business_id string,
    `date` string)
row format delimited
    fields terminated by '\u001f'
    stored as textfile
    location '/user/DataSources/CSV/checkin'
    tblproperties ("skip.header.line.count"="1");

create table if not exists checkin (
    business_id string,
    `date` string)
row format delimited
    fields terminated by '\u001f'
    stored as orc;

insert overwrite table checkin
select
    *
from
    ext_checkin;

create external table if not exists ext_photo (
    business_id string,
    caption string,
    label string,
    photo_id string
    )
row format delimited
    fields terminated by '\u001f'
    stored as textfile
    location '/user/DataSources/CSV/photo'
    tblproperties ("skip.header.line.count"="1");

create table if not exists photo (
    business_id string,
    caption string,
    label string,
    photo_id string)
row format delimited
    fields terminated by '\u001f'
    stored as orc;

insert overwrite table photo
select
    business_id, caption, label, photo_id
from
    ext_photo;

create external table if not exists ext_business (
    address string,
    attributes string,
    attr_AcceptsInsurance string,
    attr_AgesAllowed string,
    attr_Alcohol string,
    attr_Ambience string,
    attr_BYOB string,
    attr_BYOBCorkage string,
    attr_BestNights string,
    attr_BikeParking string,
    attr_BusinessAcceptsBitcoin string,
    attr_BusinessAcceptsCreditCards string,
    attr_BusinessParking string,
    attr_ByAppointmentOnly string,
    attr_Caters string,
    attr_CoatCheck string,
    attr_Corkage string,
    attr_DietaryRestrictions string,
    attr_DogsAllowed string,
    attr_DriveThru string,
    attr_GoodForDancing string,
    attr_GoodForKids string,
    attr_GoodForMeal string,
    attr_HairSpecializesIn string,
    attr_HappyHour string,
    attr_HasTV string,
    attr_Music string,
    attr_NoiseLevel string,
    attributes_Open24Hours string,
    attr_OutdoorSeating string,
    attr_RestaurantsAttire string,
    attributes_RestaurantsCounterService string,
    attr_RestaurantsDelivery string,
    attr_RestaurantsGoodForGroups string,
    attr_RestaurantsPriceRange2 string,
    attr_RestaurantsReservations string,
    attr_RestaurantsTableService string,
    attr_RestaurantsTakeOut string,
    attr_Smoking string,
    attr_WheelchairAccessible string,
    attr_WiFi string,
    business_id string,
    categories string,
    city string,
    h_Friday string,
    h_Monday string,
    h_Saturday string,
    h_Sunday string,
    h_Thursday string,
    h_Tuesday string,
    h_Wednesday string,
    hours string,
    is_open int,
    latitude float,
    longitude float,
    name string,
    postal_code string,
    review_count int,
    stars float,
    state string
)
row format delimited
    fields terminated by '\u001f'
    stored as textfile
    location '/user/DataSources/CSV/business'
    tblproperties ("skip.header.line.count"="1");

create table if not exists business (
    address string,
    attributes string,
    categories string,
    city string,
    hours string,
    is_open int,
    latitude float,
    longitude float,
    name string,
    postal_code string,
    review_count int,
    stars float,
    state string,
    business_id string
    )
row format delimited
    fields terminated by '\u001f'
    stored as orc;

insert overwrite table business
select
    address,
    attributes,
    categories,
    city,
    hours,
    is_open,
    latitude,
    longitude,
    name,
    postal_code,
    review_count,
    stars,
    state,
    business_id
from
    ext_business;

create external table if not exists ext_user (                                                                                                                                                                                                           
    average_stars string,
    compliment_cool string,
    compliment_cute string,
    compliment_funny string,
    compliment_hot string,
    compliment_list string,
    compliment_more string,
    compliment_note string,
    compliment_photos string,
    compliment_plain string,
    compliment_profile string,
    compliment_writer string,
    cool string,
    elite string,
    fans string,
    friends string,
    funny string,
    name string,
    review_count string,
    useful string,
    user_id string,
    yelping_since string)
row format delimited
    fields terminated by '\u001f'
    stored as textfile
    location '/user/DataSources/CSV/user'
    tblproperties ("skip.header.line.count"="1");

create table if not exists users (
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
    yelping_since string)
row format delimited
    fields terminated by '\u001f'
    stored as orc;

insert overwrite table users
select
    *
from
    ext_user;

create external table if not exists ext_tip (
    business_id string,
    compliment_count int,
    `date` string,
    text string,
    user_id string
)
row format delimited
    fields terminated by '\u001f'
    stored as textfile
    location '/user/DataSources/CSV/tip'
    tblproperties ("skip.header.line.count"="1");

create table if not exists tip (
    user_id string,
    business_id string,
    text string,
    `date` string,
    compliment_count int)
row format delimited
    fields terminated by '\u001f'
    stored as orc;

insert overwrite table tip
select
    user_id, business_id, text, `date`, compliment_count
from
    ext_tip;

