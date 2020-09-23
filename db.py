import mysql.connector
import mysql.connector.pooling
from mysql.connector import errorcode
from config import Config

class MysqlConnector:

    pool = None

    def __init__(self):
        try:
            self.pool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "pooled_conn",
                                                      pool_size = Config.CON_NUMBER,
                                                      **Config.getDatabaseConfig())
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            raise err

    def getConnection(self):
        con = self.pool.get_connection()
        con.autocommit = True
        return con

    def performSelect(self, query, bindVariables = None):
        connection = self.getConnection()
        cursor = connection.cursor()
        cursor.execute(query, bindVariables)
        lista = cursor.fetchall()
        cursor.close()
        connection.close()
        return lista
    
    def performQuery(self, query, bindVariables = None):
        connection = self.getConnection()
        cursor = connection.cursor()
        cursor.execute(query, bindVariables)
        connection.commit()
        cursor.close()
        connection.close()