'''
This script is only run once to create the tables for the first time. After that, the script create_tables.py is used to drop and create the tables.
'''


import cassandra
from sql_queries import create_table_queries, drop_table_queries


from cassandra.cluster import Cluster     

def create_keyspace(session):
    """
    Creates a keyspace in Cassandra.
    """
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS iot
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)

def drop_tables(session):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        session.execute(query)
    

def create_tables(session):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        session.execute(query)

def main():
    """
    - Establishes connection with the Cassandra cluster.
    
    - Creates a new keyspace.
    
    - Creates all tables needed.
    
    - Finally, closes the connection.
    """
    cluster = Cluster(['127.0.0.1'])  # Provide contact points
    session = cluster.connect()
    
    create_keyspace(session)
    session.set_keyspace('iot')
    
    drop_tables(session)
    create_tables(session)
    
    cluster.shutdown()
    print('Tables created successfully')
    print('===========================')
    print('')
    print('Run the ETL pipeline now')
    print('Execute the following command:')
    print('python3 etl.py')

if __name__ == "__main__":
    main()
