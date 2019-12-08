from uuid import uuid4
# create 'business', 'identity', 'location', 'reviews', 'attributes', 'categories', 'hours'
def convert_business(business):
    converted = {
        b'location:name': business['name'],
        b'location:address': business['address'],
        b'location:city': business['city'],
        b'location:state': business['state'],
        b'location:postal_code': business['postal_code'],
        b'location:lon': str(business['longitude']),
        b'location:lat': str(business['latitude']),

        b'reviews:stars': str(business['stars']),
        b'reviews:count': str(business['review_count']),
        
        b'categories:list': business['categories']
    }
    if business['hours'] is not None:
        for day, hours in business['hours'].items():
            converted['hours:'+day] = hours
    if business['attributes'] is not None:
        for key, value in business['attributes'].items():
            converted['attributes:'+key] = value
    return converted

def business_id(business):
    return business['business_id']

# create 'user', 'data', 'votes_sent', 'compliments_received'
def convert_user(user):
    return {
        b'data:name': user['name'],
        b'data:review_count': str(user['review_count']),
        b'data:avg_start': str(user['average_stars']),
        b'data:yelping_since': str(user['yelping_since']),
        b'data:friends': str(user['friends']),
        b'data:elite_years': str(user['elite']),


        b'votes_sent:useful': str(user['useful']),
        b'votes_sent:funny': str(user['funny']),
        b'votes_sent:cool': str(user['cool']),
        
        b'compliments_received:hot': str(user['compliment_hot']),
        b'compliments_received:more': str(user['compliment_more']),
        b'compliments_received:profile': str(user['compliment_profile']),
        b'compliments_received:cute': str(user['compliment_cute']),
        b'compliments_received:list': str(user['compliment_list']),
        b'compliments_received:note': str(user['compliment_note']),
        b'compliments_received:plain': str(user['compliment_plain']),
        b'compliments_received:cool': str(user['compliment_cool']),
        b'compliments_received:funny': str(user['compliment_funny']),
        b'compliments_received:writer': str(user['compliment_writer']),
        b'compliments_received:photos': str(user['compliment_photos'])
    }

def user_id(user):
    return user['user_id']

# create 'review', 'refs', 'content', 'votes'
def convert_review(review): 
    return {
        b'refs:user_id': review['user_id'],
        b'refs:business_id': review['business_id'],
        b'content:stars': str(review['stars']),
        b'content:data': review['date'],
        b'content:text': review['text'],
        b'votes:useful': str(review['useful']),
        b'votes:funny': str(review['funny']),
        b'votes:cool': str(review['cool'])
    }

def review_id(review):
    return review['review_id']

# create 'tip', 'refs', 'data'
def convert_tip(tip):
    return {
        b'refs:user_id': tip['user_id'],
        b'refs:business_id': tip['business_id'],
        b'data:text': tip['text'],
        b'data:date': tip['date'],
        b'data:compliment_count': str(tip['compliment_count']),
    }

def tip_id(tip):
    return random_id()

# create 'check_in', 'data'
def convert_checkin(checkin):
    return {
        b'data:business_id': checkin['business_id'],
        b'data:dates': checkin['date']
    }

def checkin_id(checkin):
    return random_id()

# create 'photo', 'data', 'refs'
def convert_photo(photo):
    return {
        b'refs:business_id': photo['business_id'],
        b'data:caption': photo['caption'],
        b'data:label': photo['label']
    }

def photo_id(photo):
    return photo['photo_id']

def random_id():
    return uuid4().hex