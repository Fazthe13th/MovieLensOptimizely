truncate table public.credits;
insert into public.credits(cast_json,
crew,
id)
select 
replace(replace(regexp_replace("cast", '"', '', 'g'),'''', '"'),'None','""')::jsonb AS cast_json,
replace(replace(regexp_replace(crew, '"', '', 'g'),'''', '"'),'None','""')::jsonb AS crew,
id::bigint
from pipeline.m_credits;
drop table pipeline.m_credits;