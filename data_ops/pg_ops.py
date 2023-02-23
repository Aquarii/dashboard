from psycopg2 import pool
from config import configs

db_cred = configs['DB']

connection_pool = pool.SimpleConnectionPool(
    1, 
    1, 
    db_cred
)


class Connection:
    def __init__(self) -> None:
        self.connection = connection_pool.getconn()
    
    def __enter__(self):
        return self.connection
    
    def __exit__(self):
        self.connection.commit()
        connection_pool.putconn(self.connection)