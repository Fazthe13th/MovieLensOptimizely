drop table if exists public.best_rated_movie_by_year;
create table public.best_rated_movie_by_year AS
select  max(m.original_title) as movie_name,max(r.rating) as max_rating,EXTRACT(YEAR FROM  m.release_date) as year from public.movies_metadata m
inner join public.links l  on (m.imdb_id_clean = l.imdbid)
inner join public.ratings r on (l.movieid = r.movieid)
where EXTRACT(YEAR FROM  m.release_date) is not null
group by EXTRACT(YEAR FROM  m.release_date)
order by year;