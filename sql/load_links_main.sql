truncate table public.links;
insert into public.links(
movieId,
imdbId,
tmdbId)
select "movieId"::int, "imdbId"::bigint, "tmdbId"::bigint from pipeline.m_links;
drop table pipeline.m_links;