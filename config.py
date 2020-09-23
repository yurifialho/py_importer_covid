import os

class Config:

    DATABASE_URL = 'DATABASE_URL'
    DATABASE_USER = 'DATABASE_USER'
    DATABASE_PASS = 'DATABASE_PASS'
    DATABASE_NAME = 'DATABASE_NAME'

    THREAD_NUMBER = 50
    CON_NUMBER = 32

    @staticmethod
    def getEnvVariables(key):
        return os.getenv(key)

    @staticmethod
    def getDatabaseConfig(defult=True):
        config = {
            "user": 'root',
            "password": '**********',
            "host": 'localhost',
            "database": 'fbd2'  
        }
        return config
        
