truncate table public.movies_metadata;
insert into public.movies_metadata(
        adult, belongs_to_collection, budget, genres, homepage, id, imdb_id, 
        original_language, original_title, overview, popularity, poster_path, 
        production_companies, production_countries, release_date, revenue, 
        runtime, spoken_languages, status, tagline, title, video, vote_average, 
        vote_count)
select
	adult::bool,
    replace(replace(replace(regexp_replace(belongs_to_collection, '"', '', 'g'),'''', '"'),'None','""'),'nan','')::json AS belongs_to_collection,
    budget::numeric,
    replace(replace(regexp_replace(genres, '"', '', 'g'),'''', '"'),'None','""')::jsonb AS genres, 
    homepage::text, 
    id::bigint, 
    imdb_id::text, 
    original_language::varchar(5), 
    original_title::text, 
    overview::text, 
    popularity::numeric, 
    poster_path::text,
    replace(regexp_replace(production_companies, '"', '', 'g'),'''', '"')::jsonb AS production_companies,
    replace(replace(regexp_replace(production_countries, '"', '', 'g'),'''', '"'),'None','""')::jsonb AS production_countries, 
    release_date::date,
    revenue::numeric, 
    runtime::int,
    replace(replace(regexp_replace(spoken_languages, '"', '', 'g'),'''', '"'),'None','""')::jsonb AS spoken_languages,
    status::varchar(30), 
    tagline::text, 
    title::varchar(100), 
    video::bool, 
    vote_average::numeric, 
    vote_count::int
from
    pipeline.m_metadata
	where adult in ('True','False');

update public.movies_metadata
		   set imdb_id_clean = REGEXP_REPLACE(imdb_id, '^tt([0-9]+)', '\1')::bigint;
drop table pipeline.m_metadata;

  