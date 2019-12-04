insert into business
	(business_id, name, address, city, postal_code, centre_distance, centre_distance_range, stars, review_count)
values
	('dasdasdasdad', 'name1', 'address', 'city', '42-232', 2.334, '(1, 2]', 4, 24);

insert into "user"
	(user_id, review_count, yelping_since, account_age, compliments, vote_ratings)
values
	('sdsddasdasd', '2', '1999-01-08 04:05:06', '2', '{}', '{}'),
    ('sdsddasdas2', '0', '2001-01-08 04:05:06', '0', '{}', '{}');

insert into tip
	(tip_id, user_id, date, business_id, text, compliment_count)
values
	(1, 'sdsddasdasd', '1999-01-08 04:05:06', 'dasdasdasdad', 'text', 23);

insert into review
	(review_id, user_id, date, business_id, stars, text, useful, funny, cool)
values
	(1, 'sdsddasdasd', '1999-01-08 04:05:06', 'dasdasdasdad', 3, 'nice', 2, 3, 4);

insert into photo
	(photo_id, business_id, caption, category)
values
	('pdqweqda', 'dasdasdasdad', 'caption', 'category1');

insert into checkin
	(id, business_id, date)
values
	(1, 'dasdasdasdad', '1999-01-08 04:05:06');

insert into category
	(id, name)
values
	(1, 'category1'),
    (2, 'category2');

insert into "friend"
	(user_a, user_b)
values
    ('sdsddasdas2', 'sdsddasdasd');

insert into business_photo_category
	(business_id, category, photo_count)
values
	('dasdasdasdad', 'category1', 1);

insert into business_hours
	(business_id, day_of_week, open_at, close_at, is_24h)
values
	('dasdasdasdad', 'Monday', '04:00 AM', '8:00 PM', False),
	('dasdasdasdad', 'Tuesday', '04:00 AM', '8:00 PM', False),
	('dasdasdasdad', 'Wednesday', '04:00 AM', '8:00 PM', False),
	('dasdasdasdad', 'Thursday', '04:00 AM', '8:00 PM', False),
	('dasdasdasdad', 'Friday', '04:00 AM', '8:00 PM', False),
	('dasdasdasdad', 'Saturday', '05:00 AM', '12:00 AM', False);

insert into business_category
	(business_id, category_id)
values
	('dasdasdasdad', 1);
