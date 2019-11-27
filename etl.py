import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, show_table_queries
import pandas as pd



def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading Staging tables ... \n')
    load_staging_tables(cur, conn)
    
    print('Inserting Data Into Tables ...  \n')
    insert_tables(cur, conn)
    
    
    print('ETL Completed!  \n')
    


    conn.close()


if __name__ == "__main__":
    main()