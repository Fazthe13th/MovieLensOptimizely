drop table if exists public.dirctor_worked_on_horror_scifi;
create table public.dirctor_worked_on_horror_scifi as 
select director.directors,
scifi_movie.original_title as scifi_movie_name,
horror_movie.original_title as horror_movie_name,
(case when horror_movie.genre is not null then True else false end) as horror ,
(case when scifi_movie.genre is not null then True else false end) as scifi,
COALESCE(horror_movie.revenue,0) as horror_revenue,
COALESCE(scifi_movie.revenue,0) as scifi_revenue
from
(select DISTINCT j.item->>'name' as directors, t.id
from public.credits t,
LATERAL jsonb_array_elements(t.crew) AS j(item)
where  t.crew is not null and (j.item->>'job') = 'Director') as director
left join
(select DISTINCT j.item->>'name' as genre, t.id, t.original_title, t.revenue
from public.movies_metadata t,
LATERAL jsonb_array_elements(t.genres) AS j(item)
where  t.genres is not null and (j.item->>'id') = 27) as horror_movie
on director.id = horror_movie.id
left join
(select DISTINCT j.item->>'name' as genre, t.id, t.original_title, t.revenue
from public.movies_metadata t,
LATERAL jsonb_array_elements(t.genres) AS j(item)
where  t.genres is not null and (j.item->>'id') = 878) as scifi_movie
on scifi_movie.id = director.id;