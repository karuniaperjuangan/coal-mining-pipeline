from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
from db.conn import Connection

class ClickHouseConnection(Connection):

    def connect(self):
        try:
            self._connection = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.dbname
            )
            # Test the connection
            self._connection.execute('SELECT 1')
            print("ClickHouse connected.")
        except ClickHouseError as e:
            print(f"ClickHouse connection error: {e}")
            self._connection = None

    def close(self):
        self._connection = None
        print("ClickHouse connection reference cleared.")

    def execute_query(self, query: str, params: tuple = None, settings:dict=None):
        if not self._connection:
            raise Exception("ClickHouse connection is not established.")
        try:
            return self._connection.execute(query, params or (),settings=settings)
        except ClickHouseError as e:
            print(f"ClickHouse query error: {e}")
            return None

    def get_conn(self):
        if not self._connection:
            self.connect()
        return self._connection
    
    def insert_dataframe(self,query,dataframe):
        try:
            return self._connection.insert_dataframe(query,dataframe,settings={'use_numpy': True})
        except ClickHouseError as e:
            print(f"ClickHouse query error: {e}")
            return None