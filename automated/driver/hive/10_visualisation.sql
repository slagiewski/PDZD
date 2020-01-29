select
    label,
    avg(stars)
from 
    centre_distance
group by
    label;