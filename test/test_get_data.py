import pandas as pd
from sqlalchemy import create_engine
import psycopg2

connection = psycopg2.connect(
    host='localhost',
    database='test_sasa_db',
    user='postgres',
    password='postgres'
)

def _get_data_from_postgres( n=1000):
    query = f"""
    SELECT "tes3"."reviewId",
    "tes3"."content" FROM tes3
    LEFT JOIN sentiment
    ON ("tes3"."reviewId"="sentiment"."reviewId")
    WHERE "sentiment"."sentiments" IS NULL 
    LIMIT {n};
    """
    return pd.read_sql(query, connection)

def _get_data_from_postgress():
    query = f"""
    SELECT * FROM tes3;
    """
    return pd.read_sql(query, connection)


print(_get_data_from_postgres())