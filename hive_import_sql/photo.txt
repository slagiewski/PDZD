create external table if not exists photo
(
business_id string,
caption string,
label string,
photo_id string
)
row format delimited
fields terminated by ','
stored as textfile
location '/user/DataSources/CSV/photo';


create table if not exists photos
(
business_id string,
caption string,
label string,
photo_id string
)
row format delimited
fields terminated by ','
stored as orc;


insert overwrite table photos Select * from photo;