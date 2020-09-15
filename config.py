import os

class Config:

    DATABASE_URL = 'DATABASE_URL'
    DATABASE_USER = 'DATABASE_USER'
    DATABASE_PASS = 'DATABASE_PASS'
    DATABASE_NAME = 'DATABASE_NAME'

    @staticmethod
    def getEnvVariables(key):
        return os.getenv(key)
