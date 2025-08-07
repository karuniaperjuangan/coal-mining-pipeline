import abc

class Connection(abc.ABC):

    def __init__(self, host: str, port: int, user: str, password: str, dbname: str):
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            self.dbname = dbname
            self._connection = None  

    @abc.abstractmethod
    def connect(self):
          pass
          
    @abc.abstractmethod
    def close(self):  
          pass    
    
    @abc.abstractmethod
    def execute_query(self, query: str, params: tuple = None, settings:dict=None):
          pass
    
    @abc.abstractmethod
    def get_conn(self):
          pass