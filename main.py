import csv, os, requests, sys, threading

from util import Log
from concurrent.futures import ThreadPoolExecutor
from config import Config
from db import MysqlConnector
from importer import ComprasImporter, ProviderImporter

class Main():

    cnpjErro = []

    mysqlConnector = None

    ccTh = 0
    inTh = 0
    lnTh = 0

    def __init__(self):
        con = self.getConnector()
        self.pImporter = ProviderImporter(con)
        self.cImporter = ComprasImporter(con)

    def run(self):
        print("Running")
        self.readAndLoadCsv()
        
    def getConnector(self):
        if self.mysqlConnector == None:
            self.mysqlConnector = MysqlConnector()
        
        return self.mysqlConnector

    def readAndLoadCsv(self):
        print("Reading....")
        with ThreadPoolExecutor(max_workers=30) as pool:
            with open("./dados1.csv", newline='', encoding='utf8') as csvfile:
                spamreader  = csv.reader(csvfile, delimiter=';', quotechar='"')
                for row in spamreader:
                    if spamreader.line_num == 1:
                        self.cImporter.createLoadTableCovid(row, truncate=True)
                        self.pImporter.createLoadTableProvider(truncate=True)
                    else:
                        pool.submit(self.processRowFile, (row))
                        
                    self.lnTh = spamreader.line_num
                    Log.info(self.lnTh, self.ccTh, self.inTh)

    def processRowFile(self, row):
        self.cImporter.insertInLoadTableCovid(row)
        if not self.pImporter.checkHasInsertedProvider(row[10]):
            self.pImporter.bringProviderAndInsert(row[10])
            return 1   

if __name__ == "__main__":
    main = Main()
    main.run()
    
