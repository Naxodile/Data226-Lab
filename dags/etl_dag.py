from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import json
import requests
import pandas as pd

# Cities to load. Override without code changes by creating an Airflow Variable
# named "cities" containing JSON, e.g.
# [{"city": "San Jose", "lat": 37.3382, "lon": -121.8863}, {"city": "Los Angeles", "lat": 34.0522, "lon": -118.2437}]
DEFAULT_CITIES = [
    {"city": "San Jose",    "lat": 37.3382, "lon": -121.8863},
    {"city": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
]


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(city_info):
    """Runs once per city (dynamic task mapping)."""
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": city_info["lat"],
        "longitude": city_info["lon"],
        "past_days": 60,
        "forecast_days": 0,  # only past weather
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code"
        ],
        "timezone": "America/Los_Angeles"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    data["city"] = city_info["city"]
    return data


@task
def transform(data):
    """Runs once per city; returns that city's rows."""
    df = pd.DataFrame({
        "city": data["city"],
        "latitude": data["latitude"],
        "longitude": data["longitude"],
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })
    # NaN -> None so Snowflake receives NULL
    df = df.astype(object).where(pd.notnull(df), None)
    return df.values.tolist()


@task
def load(records_per_city):
    """Receives one list of rows per city, loads all cities in a single transaction."""
    records = [row for city_rows in records_per_city for row in city_rows]

    con = return_snowflake_conn()
    target_table = "weather_data"

    # DDL first: Snowflake commits implicitly on DDL, so keep it outside the transaction
    con.execute("USE DATABASE ASSIGNMENTS;")
    con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table} (
      city varchar,
      latitude float,
      longitude float,
      date date,
      temp_max float,
      temp_min float,
      precipitation float,
      weather_code int,
      PRIMARY KEY (latitude, longitude, date));""")
    # table created by an earlier single-city version has no city column
    con.execute(f"ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS city varchar;")

    try:
        con.execute("BEGIN;")
        con.execute(f"DELETE FROM {target_table}")   # full refresh of all cities
        for r in records:
            print("loading", r[0], r[3])
            con.execute(
                f"""INSERT INTO {target_table}
                    (city, latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                r,
            )
        con.execute("COMMIT;")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise e


with DAG(
    dag_id = 'etl_dag',
    start_date = datetime(2026,9,16),
    catchup=False,
    tags=['etl'],
    schedule = '0 2 * * *',   # daily at 02:00
    max_active_runs = 1
) as dag:
    cities = Variable.get("cities", default_var=DEFAULT_CITIES, deserialize_json=True)

    data = extract.expand(city_info=cities)      # one extract task per city
    lines = transform.expand(data=data)          # one transform task per city
    load_task = load(lines)                      # single load of all cities

    # dbt runs only after the load finishes successfully
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="dbt_weather_analytics",
        wait_for_completion=False,
    )

    load_task >> trigger_dbt
