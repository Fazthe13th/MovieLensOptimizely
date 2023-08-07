truncate table public.ratings;
insert into public.ratings(
userId,
movieId,
rating,
timestamp)
select "userId"::int,
"movieId"::int,
rating::numeric,
timestamp::bigint from pipeline.m_ratings;
drop table pipeline.m_ratings;