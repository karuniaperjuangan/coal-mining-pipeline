import mysql.connector
from mysql.connector import Error

from db.conn import Connection

class MySQLConnection(Connection):

    def connect(self):
        try:
            self._connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.dbname
            )
            if self._connection.is_connected():
                print("MySQL connected.")
        except Error as e:
            print(f"MySQL connection error: {e}")
            self._connection = None

    def close(self):
        if self._connection and self._connection.is_connected():
            self._connection.close()
            print("MySQL connection closed.")

    def execute_query(self, query: str, params: tuple = None, settings=None):
        if not self._connection or not self._connection.is_connected():
            raise Exception("MySQL connection is not established.")
        
        cursor = self._connection.cursor()
        try:
            cursor.execute(query, params or ())
            if query.strip().lower().startswith("select"):
                result = cursor.fetchall()
                return result
            else:
                self._connection.commit()
                return cursor.rowcount
        finally:
            cursor.close()

    def get_conn(self):
        if not self._connection:
            self.connect()
        return self._connection