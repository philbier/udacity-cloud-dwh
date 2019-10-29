import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    drops all tables via sql statements listed in "drop_table_queries"
    """
    for query in drop_table_queries:
        #print('Drop table with: '+query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    creates all tables via sql statements listed in "create_table_queries"
    """
    for query in create_table_queries:
        #print('Create table with: '+query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main Module function
    creates a connection to a launched cluster and
    executes functions to drop and create tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Executing drop_tables...')
    drop_tables(cur, conn)
    print('Executing create_tables...')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()