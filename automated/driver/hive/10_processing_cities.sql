create table if not exists distance (
    business_id string,
    city string,
    distance double,
    stars float
);


insert into table distance 
    select b.business_id,
    b.city,
    acos(sin(b.latitude) *sin(o.latitude) + cos(b.latitude) *cos(o.latitude)* cos(b.longitude -  o.longitude)),
	b.stars
    from  business b join geodata o on b.city == o.city;


create table if not exists centre_distance
(
    business_id string,
    city string,
    distance double,
    stars float,
    label string
)
stored as orc;

insert into centre_distance 
select 
    business_id, 
    city, 
    distance, 
    stars, 
    case
        when distance > 15 then '(15,...)'
        when distance > 10 then'(10,15]'
        when distance > 7 then'(7,10]'
        when distance > 5 then '(5,7]'
        when distance > 3 then'(3,5]'
        when distance > 1 then'(1,3]'
        when distance > 0 then '[0,1]'
        else 
            null
end from distance;
