from airflow import DAG
from airflow.models import XCom
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd
from airflow.models import DAG, Variable
import datetime
from airflow.operators.python import get_current_context
import pathlib
import re
import json
print("Test")

docs="""
MovieLens ETL
steps:
1. import dataset from kaggel
2. load into postgres
3. create custom tables

"""
db_name = 'development_db'

destination = '70_server' # airflow posgresql connection, configured at airflow admin pannel
path = str(pathlib.Path(__file__).parent.resolve()) + '/datasets/'

pg_hook_destination = PostgresHook(postgres_conn_id=destination)

result = PostgresHook(postgres_conn_id=destination).get_uri().split("/")
print("this is connection string result --->>", result)
result[3] = db_name
result[0] = "postgresql+psycopg2:"
dw_connection_string = "/".join(result)
print('this is connection string : ', dw_connection_string)

schedule_interval = '0 0 * * *' #every day midnight
# delay_time = '60 min'

selection_delay = 1

default_args={
    'owner' : 'bits_pipeline',
    'start_date' :pendulum.datetime(2023,8,3, tz="Asia/Dhaka")
}


def load_json(json_string):
    try:
        return json.loads(json_string)
    except json.JSONDecodeError:
        return None
    
# Define a function to clean JSON strings with inaccurate double quotes
def clean_json_string(json_string):
    # Remove leading and trailing whitespace
    json_string = json_string.strip()

    # If the JSON string starts with single quote, replace with double quote
    if json_string.startswith("'"):
        json_string = '"' + json_string[1:]

    # If the JSON string ends with single quote, replace with double quote
    if json_string.endswith("'"):
        json_string = json_string[:-1] + '"'

    # Replace single quotes with double quotes
    json_string = json_string.replace("'", '"')

    return json_string

# custom function to load data to pg
def pd_to_psql(df, uri, table_name, schema_name=None, if_exists='fail', sep=','):
    if not 'psycopg2' in uri:
        raise ValueError('need to use psycopg2 uri eg postgresql+psycopg2://psqlusr:psqlpwdpsqlpwd@localhost/psqltest. install with `pip install psycopg2-binary`')
    table_name = table_name.lower()
    if schema_name:
        schema_name = schema_name.lower()
   
    import sqlalchemy
    import io

    if schema_name is not None:
        sql_engine = sqlalchemy.create_engine(uri, connect_args={'options': '-csearch_path={}'.format(schema_name)})
    else:
        sql_engine = sqlalchemy.create_engine(uri)
    sql_cnxn = sql_engine.raw_connection()
    cursor = sql_cnxn.cursor()

    df[:0].to_sql(table_name, sql_engine, schema=schema_name, if_exists=if_exists, index=False)

    fbuf = io.StringIO()
    df.to_csv(fbuf, index=False, header=False, sep=sep, encoding='utf-8')
    fbuf.seek(0)
    cursor.copy_expert( 
        f'COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER)',
        fbuf,
    )
    # cursor.copy_from(fbuf, table_name, sep=sep, null='')
    sql_cnxn.commit()
    cursor.close()
    return True
#dag start

with DAG(dag_id='MovieLens',
         schedule_interval=schedule_interval,
         default_args=default_args,
         max_active_runs=1,
         catchup=False,description="""
        MovieLens data prepare
         """, tags=["MovireLens ETL",'Evan']) as dag:

    dag.doc_md=docs
    
    @task(task_id='start')
    def start(ti=None):
        """Generate Run Id"""
        context = get_current_context()
        run_id = str(context['dag_run'].run_id).replace(
            '-', '_').replace(':', '_').replace('.', '_').replace('+', '_')
        run_id_formated = "MovieLens"+str(run_id)
        ti.xcom_push(key='MovieLens_dagrun_id', value=run_id_formated)
        print('this is run id processed : ', run_id_formated)
        print('this is run id  : ', run_id)
    start = start()


    with TaskGroup('load_data_temp') as load_data_temp:
        @task(task_id='load_movies_metadata')
        def load_movies_metadata(ti=None):
            csv_file_path = path + 'movies_metadata.csv'
            # preprocess_csv(csv_file_path)
            # Load the data from the CSV file into a Pandas DataFrame
            df_metadata = pd.read_csv(csv_file_path,error_bad_lines=False)
            
            # pd.set_option('display.max_columns', None)
            # pd.set_option('display.max_rows', None)
            # pd.set_option('display.width', None)
            # pd.set_option('display.max_colwidth', -1)
            # cleaning
            df_metadata.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r",r'"'], value=["","",""], regex=True, inplace=True)
            df_metadata['belongs_to_collection'] = df_metadata['belongs_to_collection'].astype(str).apply(clean_json_string)
            df_metadata['genres'] = df_metadata['genres'].astype(str).apply(clean_json_string)
            df_metadata['production_companies'] = df_metadata['production_companies'].astype(str).apply(clean_json_string)
            df_metadata['production_countries'] = df_metadata['production_countries'].astype(str).apply(clean_json_string)
            df_metadata['spoken_languages'] = df_metadata['spoken_languages'].astype(str).apply(clean_json_string)
            df_metadata['belongs_to_collection'] = df_metadata['belongs_to_collection'].apply(load_json)
            df_metadata['genres'] = df_metadata['genres'].apply(load_json)
            df_metadata['production_companies'] = df_metadata['production_companies'].apply(load_json)
            df_metadata['production_countries'] = df_metadata['production_countries'].apply(load_json)
            df_metadata['spoken_languages'] = df_metadata['spoken_languages'].apply(load_json)

            df_metadata_table_name = "m_metadata" #the table name
            
            print(df_metadata.dtypes)
            # load
            pd_to_psql(df_metadata, dw_connection_string, f'{df_metadata_table_name}', 
                        if_exists='replace', schema_name='pipeline',sep=',')
            

        load_movies_metadata = load_movies_metadata()

        @task(task_id='load_movies_keywords')
        def load_movies_keywords(ti=None):
            csv_file_path = path + 'keywords.csv'
            # Load the data from the CSV file into a Pandas DataFrame
            df_keywords = pd.read_csv(csv_file_path,error_bad_lines=False)
            df_keywords_table_name = "m_keywords" #the table name
            # default cleaning
            df_keywords.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r",r'"'], value=["","",""], regex=True, inplace=True)
            df_keywords['keywords'] = df_keywords['keywords'].astype(str).apply(clean_json_string)
            df_keywords['keywords'] = df_keywords['keywords'].apply(load_json)
            # load
            pd_to_psql(df_keywords, dw_connection_string, f'{df_keywords_table_name}', 
                        if_exists='replace', schema_name='pipeline',sep=',')

        load_movies_keywords = load_movies_keywords()

        @task(task_id='load_movies_credits')
        def load_movies_credits(ti=None):
            csv_file_path = path + 'credits.csv'
            # Load the data from the CSV file into a Pandas DataFrame
            df_credits = pd.read_csv(csv_file_path,error_bad_lines=False)
            df_credits.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r",r'"'], value=["","",""], regex=True, inplace=True)
            df_credits['cast'] = df_credits['cast'].astype(str).apply(clean_json_string)
            df_credits['crew'] = df_credits['crew'].astype(str).apply(clean_json_string)
            df_credits['cast'] = df_credits['cast'].apply(load_json)
            df_credits['crew'] = df_credits['crew'].apply(load_json)
            df_credits_table_name = "m_credits" #the table name

            # load
            pd_to_psql(df_credits, dw_connection_string, f'{df_credits_table_name}', 
                        if_exists='replace', schema_name='pipeline',sep=',')

        load_movies_credits = load_movies_credits()

        @task(task_id='load_movies_links')
        def load_movies_links(ti=None):
            csv_file_path = path + 'links.csv'
            # Load the data from the CSV file into a Pandas DataFrame
            df_links = pd.read_csv(csv_file_path,error_bad_lines=False)
            print(df_links)
            df_links_table_name = "m_links" #the table name
            # default cleaning
            df_links.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r",r'"'], value=["","",""], regex=True, inplace=True)
            # load
            pd_to_psql(df_links, dw_connection_string, f'{df_links_table_name}', 
                        if_exists='replace', schema_name='pipeline',sep=',')

        load_movies_links = load_movies_links()

        
        @task(task_id='load_ratings')
        def load_ratings(ti=None):
            csv_file_path = path + 'ratings.csv'
            # Load the data from the CSV file into a Pandas DataFrame
            df_ratings = pd.read_csv(csv_file_path,error_bad_lines=False)
            print(df_ratings)
            df_ratings_table_name = "m_ratings" #the table name
            # default cleaning
            df_ratings.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r",r'"'], value=["","",""], regex=True, inplace=True)
            # load
            pd_to_psql(df_ratings, dw_connection_string, f'{df_ratings_table_name}', 
                        if_exists='replace', schema_name='pipeline',sep=',')

        load_ratings = load_ratings()
    with TaskGroup('load_data_in_main_table') as load_data_in_main_table:
        load_movie_meta_main = PostgresOperator(task_id='load_movie_meta_main',
                                                postgres_conn_id=destination,
                                                sql="sql/load_movie_meta_main.sql",
                                                database=db_name)
        load_keywords_main = PostgresOperator(task_id='load_keywords_main',
                                                postgres_conn_id=destination,
                                                sql="sql/load_keywords_main.sql",
                                                database=db_name)
        load_credits_main = PostgresOperator(task_id='load_credits_main',
                                                postgres_conn_id=destination,
                                                sql="sql/load_credits_main.sql",
                                                database=db_name)
        load_ratings_main = PostgresOperator(task_id='load_ratings_main',
                                                postgres_conn_id=destination,
                                                sql="sql/load_ratings_main.sql",
                                                database=db_name)
        load_links_main = PostgresOperator(task_id='load_links_main',
                                                postgres_conn_id=destination,
                                                sql="sql/load_links_main.sql",
                                                database=db_name)
    with TaskGroup('create_custom_tables') as create_custom_tables:
        best_rated_movie_by_year = PostgresOperator(task_id='best_rated_movie_by_year',
                                                postgres_conn_id=destination,
                                                sql="sql/custom_tables/best_rated_movie_by_year.sql",
                                                database=db_name)
        directors_worked_on_horror_or_scifi = PostgresOperator(task_id='directors_worked_on_horror_or_scifi',
                                                postgres_conn_id=destination,
                                                sql="sql/custom_tables/directors_worked_on_horror_or_scifi.sql",
                                                database=db_name)
        popular_g_per_year = PostgresOperator(task_id='popular_g_per_year',
                                                postgres_conn_id=destination,
                                                sql="sql/custom_tables/popular_g_per_year.sql",
                                                database=db_name)
    stop = EmptyOperator(task_id='stop', trigger_rule='all_done')

    start >> load_data_temp >> load_data_in_main_table>> create_custom_tables >>  stop
    


