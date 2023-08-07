-- credits:
create table credits(cast_json JSONB,
crew JSONB,
id bigint);


-- keywords:
create table keywords(
id bigint ,
keywords JSONB)

-- links:
create table links(
movieId int,
imdbId bigint,
tmdbId bigint);

-- movies_metadata:

create table movies_metadata(
adult bool,
belongs_to_collection json,
budget numeric,
genres JSONB,
homepage text,
id bigint,
imdb_id text,
original_language varchar(5),
original_title text,
overview text,
popularity numeric,
poster_path text,
production_companies JSONB,
production_countries JSONB,
release_date date,
revenue numeric,
runtime int,
spoken_languages JSONB,
status varchar(30),
tagline text,
title varchar(100),
video bool,
vote_average numeric,
vote_count int);
alter table public.movies_metadata add column imdb_id_clean bigint default null;
-- ratings:
create table ratings(
userId int,
movieId int,
rating numeric,
timestamp bigint);