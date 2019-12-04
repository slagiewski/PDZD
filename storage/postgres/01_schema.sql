drop table if exists business cascade;
drop table if exists category cascade;
drop table if exists business_category cascade;
drop table if exists business_photo_category cascade;
drop table if exists business_hours cascade;
drop table if exists "user" cascade;
drop table if exists friend cascade;
drop table if exists photo cascade;
drop table if exists review cascade;
drop table if exists tip cascade;
drop table if exists checkin cascade;

create table business (
    business_id varchar(32) primary key,
    name text,
    address text,
    city text,
    state text,
    postal_code text,
    latitude double precision,
    longitude double precision,
    centre_distance double precision, -- enum
    centre_distance_range text,
    centre_direction text,  -- future category?
    stars real,
    review_count integer
);

create table category (
    id serial primary key,
    name text
);

create table business_category (
    business_id varchar(32) references business(business_id),
    category_id integer references category(id)
);

create table business_photo_category (
    business_id varchar(32) references business(business_id),
    category text,
    photo_count integer
);

create table business_hours (
    business_id varchar(32) references business(business_id),
    day_of_week varchar(10),
    open_at time,
    close_at time,
    is_24h boolean,
    primary key (business_id, day_of_week)
);


-----------------------------------------------

create table "user" (
    user_id varchar(32) primary key,
    review_count integer,
    yelping_since timestamp,
    account_age text, -- enum: day | week | month | quarter | 6-month | year | more
    compliments jsonb,
    vote_ratings jsonb
);

create table friend (
    user_a varchar(32),
    user_b varchar(32),
    primary key (user_a, user_b) 
);

-----------------------------------------------

create table photo (
    photo_id varchar(32) primary key,
    business_id varchar(32) references business(business_id),
    caption text,
    category text
);

create index photo_category_idx on photo (category);
------

create table review (
    review_id serial primary key,
    user_id varchar(32),
    business_id varchar(32) references business(business_id),
    stars real,
    date timestamp,
    text text,
    useful integer,
    funny integer,
    cool integer
);
-----
create table tip (
    tip_id serial primary key,
    user_id varchar(32),
    business_id varchar(32) references business(business_id),
    text text,
    date timestamp,
    compliment_count integer
);
------
create table checkin (
    id serial primary key,
    business_id varchar(32) references business(business_id),
    date timestamp
);
