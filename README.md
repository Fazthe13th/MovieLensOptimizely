# MovieLensOptimizely
Assigment from Optimizely for Senior Data Engineer position

## About The Project
![dag_img](https://github.com/Fazthe13th/MovieLensOptimizely/assets/19173590/58ab188e-743b-4ad9-9944-995e3903126b)

<!-- GETTING STARTED -->
## Tasks in hand

### Implement a data ingestion task that loads some or all of the data files into tables in any SQL database such as Postgres, MySQL, SQLite or equivalent.
Implemented a data pipeline using <b>Airflow</b>. 
This pipeline loads Movielens dataset, which was downlaoded from Kaggle into `/datasets` folder, into postgres database.
Please note, sample dataset was uploaded to this folder, otherwise git size would increase significantly. <b>Replace with original datasets from Kaggle if you want to run this Dag</b>

This task consists of <b>load_data_temp</b> and <b>load_data_in_main_table</b> [Airflow task groups](https://docs.astronomer.io/learn/task-groups):
* Loads the CSVs in `Pandas` df.
* Cleans the df before loading.
* Uses psotgres `copy_expert` feature to load data faster in postgres into temp tables in `pipeline` schema of DB.
* Later loads from temp tables to `public` scehma with proper datatype.

### Provide a data transformation/aggregation task that generates at least one custom table derived from the loaded data which allows answering common questions about the dataset.
In the next step of this pipeline, which is `create_custom_tables`, I have created three report tables, namely:
* `public.best_rated_movie_by_year` which is created by the <b>best_rated_movie_by_year</b> task.
* `public.dirctor_worked_on_horror_scifi` which is created by the <b>directors_worked_on_horror_or_scifi</b> task.
* `public.popular_g_per_year` which is created by the <b>popular_g_per_year</b> task.


SQLs for the custom tables are in
```
   cd sql/custom_tables/
   ```
### Some example SQL queries that answer some of the questions above based on the custom reports you generated
```
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
```

## Questions and Answers
**[How would you package the pipeline code for deployment?](#id00)** <a id="id00"></a><!-- ID: 00 -->

What is currently do to deploy a pipeline code to production, as I use Airflow as ETL tool, I `git push` to `development` branch t of our git after tesing in dev environment.

Create a merge request to `main` branch and pull it to production environment.

If there is any new python package necessary, we add the packages to `requirements.txt` and build new Airflow image for deployment.

**[How would you schedule a pipeline that runs the ingestion and the transformation tasks sequentially every day?](#id01)** <a id="id01"></a><!-- ID: 01 -->

I would use a cron job to schedule a pipeline. Like in the pipeline for the assignment, I scheduled the pipeline to run every day at midnight.
```
schedule_interval = '0 0 * * *'
```
**[How would you ensure the quality of the data generated from the pipeline?](#id02)** <a id="id02"></a><!-- ID: 02 -->

I would write some checks to ensure data quality. Checks like:
* Is number of rows are same between source and destination? if not need to find reason.
* If there are any numeric values I would sum the column in source and destination and check if both are same or not.
* We can manage log tables in pipeline to ensure data is being properly processed.







