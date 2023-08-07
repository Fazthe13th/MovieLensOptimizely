-- which genre has highest popularity in 2017
select * from public.popular_g_per_year pgpy where year = 2017
and avg_popular= (select max(avg_popular) as avg_popular  from public.popular_g_per_year where year = 2017);

+-------+-----------+----+-------------------+
|genre  |movie_count|year|avg_popular        |
+-------+-----------+----+-------------------+
|Fantasy|28         |2017|40.9125583928571429|
+-------+-----------+----+-------------------+

-- Top 5 most profitable directors by revenue worked ONLY on horror genre movies
SELECT directors,
       horror_movie_name,
       sum(horror_revenue) as total_revenue
from public.dirctor_worked_on_horror_scifi
where scifi = FALSE and horror = TRUE
group by directors, horror_movie_name, scifi_movie_name
having sum(horror_revenue) != 0 order by total_revenue desc limit 5;

+---------------+------------------+-------------+
|directors      |horror_movie_name |total_revenue|
+---------------+------------------+-------------+
|Uli Edel       |The Little Vampire|13555988     |
|Ram Gopal Varma|Phoonk            |2760000      |
|Uwe Boll       |BloodRayne        |2405420      |
|Peter Masterson|Night Game        |337812       |
|Tibor Takács   |I, Madman         |151203       |
+---------------+------------------+-------------+


-- what are is the highest rated movies of year 1910 to 1919
select movie_name, max_rating as rating, year from public.best_rated_movie_by_year
where year between 1910 and 1919;

+----------------------------------------------------------------------------+------+----+
|movie_name                                                                  |rating|year|
+----------------------------------------------------------------------------+------+----+
|The Wonderful Wizard of Oz                                                  |5     |1910|
|Winsor McCay, the Famous Cartoonist of the N.Y. Herald and His Moving Comics|5     |1911|
|Trädgårdsmästaren                                                           |5     |1912|
|Traffic in Souls                                                            |5     |1913|
|Tillie's Punctured Romance                                                  |4.5   |1914|
|Дети века                                                                   |5     |1915|
|Пиковая дама                                                                |5     |1916|
|Wild and Woolly                                                             |5     |1917|
|Отец Сергий                                                                 |5     |1918|
|Unheimliche Geschichten                                                     |5     |1919|
+----------------------------------------------------------------------------+------+----+


