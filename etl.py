import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, truncate_table_queries

def truncate_tables(cur, conn):
    """
    truncates all tables via sql statements listed in "truncate_table_queries"
    """
    for query in truncate_table_queries:
        #print('Loading data by: '+query)
        cur.execute(query)
        conn.commit()

def load_staging_tables(cur, conn):
    """
    uses COPY statemens listed copy_table_queries to copy data from files to staging tables
    """
    for query in copy_table_queries:
        #print('Loading data by: '+query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    inserts data via sql statements listed insert_table_queries
    """
    for query in insert_table_queries:
        #print('Loading data by: '+query) 
        cur.execute(query)
        conn.commit()

def main():
    """
    Main module function
    creates a connection to a launched cluster and
    executes functions to truncate all tables, load staging tables and insert data
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Executing truncate_tables...')
    truncate_tables(cur, conn)
    print('Executing load_staging_tables...')
    load_staging_tables(cur, conn)
    print('Executing insert_tables...')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()