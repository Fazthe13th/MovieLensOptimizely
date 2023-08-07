drop table if exists public.popular_g_per_year;
create table public.popular_g_per_year as
select DISTINCT j.item->>'name' as genre,
 count(1) as movie_count, 
 EXTRACT(YEAR FROM  t.release_date) as year,
avg(t.popularity) as avg_popular
from public.movies_metadata t,
LATERAL jsonb_array_elements(t.genres) AS j(item)
where EXTRACT(YEAR FROM  t.release_date) is not null
group by (j.item->>'id'),EXTRACT(YEAR FROM  t.release_date),j.item->>'name'
order by year;