select 
    days_since_registered_label,
    avg(average_stars)
from
    users_labeled
group by
    days_since_registered_label;