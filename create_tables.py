import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function will delete any tables if they exist
    from the drop_table_queries list in sql_queries.py
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function will create tables if they do not exist
    from the create_table_queries list in sql_queries.py
    """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(e)
        


def main():
    """
    This function will connect to cluster,
    delete any tables if they exist,
    create tables, and close connection to cluster.

    You need to run create_cluster.ipynb first and 
    input 'Endpoint' into dwh.cfg [CLUSTER], host,
    """
    print("Establishing connection ...  \n")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Deleting tables ... \n')
    drop_tables(cur, conn)
    
    print('Creating tables ...  \n')
    create_tables(cur, conn)
    
    print('Creating Tables Completed!  \n')
    conn.close()


if __name__ == "__main__":
    main()