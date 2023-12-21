from sqlalchemy import create_engine
from sqlalchemy import text

def initialize_database(conn_string):
    engine = create_engine(conn_string, echo=True)
    with engine.connect() as conn:
        with open("/opt/airflow/db/init.sql") as init_file:
            queries = init_file.read()
            for query in queries.split(";"):
                if query:
                    print(f"Executing: {query}")
                    conn.execute(text(query))