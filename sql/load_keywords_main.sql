truncate table public.keywords;
insert into public.keywords(
id,
keywords)
select 
id::bigint,
replace(replace(regexp_replace(keywords, '"', '', 'g'),'''', '"'),'None','""')::jsonb AS keywords
from pipeline.m_keywords;
drop table pipeline.m_keywords;