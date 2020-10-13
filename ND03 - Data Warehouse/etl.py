import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, trunc_table_queries

def truncate_tables(cur, conn):
    """
    truncate_tables function will delete all the tables data, Tables names are defined in the sql_queries.py scripts
    """
    for query in trunc_table_queries:
        cur.execute(query)
        conn.commit()

def load_staging_tables(cur, conn):

    """
    load_staging_tables function reads the JSON files stored on the S3 buckets and stages the data into the temporary tables.
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):

    """
    insert_tables function will INSERT data into the tables by the reading the staging tables.
    """
    i=1
    for query in insert_table_queries:
        print(i)
        cur.execute(query)
        conn.commit()
        i=i+1

def main():

    """
    etl.py scripts is used for perform all the etl tasks required for the project. Python functions connects to AWS DB (Redshift).
    Truncates all the staging tables, reads the songs and log data stored on the S3 buckets and loads into staging, Fact
    and Dimension tables.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    truncate_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()