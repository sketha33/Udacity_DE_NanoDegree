import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    drop_tables function will drops all the tables, Tables names are defined in the sql_queries.py scripts
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except:
            print('Error when executing the Query: ' + query)
            conn.commit


def create_tables(cur, conn):
    """
    create_tables function will creates all the tables, Tables are defined in the sql_queries.py scripts
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    create_tables.py process will connect to the DB (AWS Redshift) based on the configurations specified in the dhw.cfg file.
    drops and recreates all the tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()