import psycopg2
from psycopg2 import Error
from sqlalchemy import create_engine
import os
from sqlalchemy import exc
from transformers import Pipeline

# Database connection parameters
db_params = {
    'host': 'sasa-db-instance.cto8fqajoc9d.ap-southeast-1.rds.amazonaws.com',
    'database': 'sasa-db',
    'user': 'postgres',
    'password': 'pisanggoreng123',
}

try:
    # Attempt to connect to the database
    connection = psycopg2.connect(**db_params)

    # If the connection is successful, print a success message
    print(f"Successfully connected to the database: {db_params['database']}")

    # Don't forget to close the connection when you're done
    connection.close()

except Error as e:
    # If there is an error, print the error message
    print(f"Error connecting to the database: {e}")

###########################################################################################################
# def _postgres_conn(self):
#     connection_info = {
#         "host": os.environ.get("PG_HOST", "sasa-db-instance.cto8fqajoc9d.ap-southeast-1.rds.amazonaws.com"),
#         "port": os.environ.get("PG_PORT", 5432),
#         "db": os.environ.get("PG_DATABASE", "sasa-db"),
#         "user": os.environ.get("PG_USER", "postgres"),
#         "password": os.environ.get("PG_PASS", "pisanggoreng123"),
#     }
#     self.db_engine = create_engine(
#         "postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}".format(
#             **connection_info
#         )
#     )

#     self.db_conn = psycopg2.connect(
#         host=connection_info["host"],
#         port=connection_info["port"],
#         database=connection_info["db"],
#         user=connection_info["user"],
#         password=connection_info["password"],
#     )
#     print("-" * 5 * 20)
#     self.db_conn.cursor()
#     print("PostgreSQL connection successful!")
#     print("-" * 5 * 20)

# _postgres_conn()

# engine = create_engine('postgresql://postgres:postgres@localhost/test_sasa_db')

# def insert_to_database (df, db_table=None, engine=None):
#     print(f"Inserting {len(df)} reviews into the database table '{db_table}'")

#     try:
#         df.to_sql(name= db_table, con=engine, if_exists='append', index=False)
    
#     except exc.IntegrityError as e:
#         pass

#     print("Insertion completed.")
#     return True