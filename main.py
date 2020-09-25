import csv, os, requests, sys, threading
import concurrent.futures

from util import Log
from concurrent.futures import ThreadPoolExecutor
from config import Config
from db import MysqlConnector
from importer import ComprasImporter, ProviderImporter, TipoOcorrenciaImporter, OcorrenciaProvider

class Main():

    mysqlConnector = None

    def __init__(self):
        self.lineRead = 0 
        self.lineProcess = 0
        self.cnpjProvides = 0
        self.provideProcess = 0
        self.ocorrencias = 0
        con = self.getConnector()
        self.pImporter = ProviderImporter(con)
        self.cImporter = ComprasImporter(con)
        self.tipoOcorrImporter = TipoOcorrenciaImporter(con)
        self.ocorrenciaImporter = OcorrenciaProvider(con)

    def run(self):
        print("Running")
        #self.readAndLoadCsv()
        #self.readAndImportProviders()
        self.readAndImportOcorrencias()
        #self.importAuxTables()
        
    def getConnector(self):
        if self.mysqlConnector == None:
            self.mysqlConnector = MysqlConnector()
        
        return self.mysqlConnector

    def importAuxTables(self):
        print("Importing Aux Tables...")

        #Tipo Ocorrencia
        self.tipoOcorrImporter.createLoadTableTipoOcorrencia()
        total = self.tipoOcorrImporter.bringProviderAndInsert()
        print("Imported [%d]" % (total))

    def readAndLoadCsv(self):
        with open("./dados1.csv", newline='', encoding='utf8') as csvfile:
            spamreader  = csv.reader(csvfile, delimiter=';', quotechar='"')
            with ThreadPoolExecutor(max_workers=Config.THREAD_NUMBER) as pool:
                future = []
                for row in spamreader:
                    self.lineReadlineRead = spamreader.line_num
                    Log.info(self.lineRead, self.lineProcess, self.cnpjProvides, self.provideProcess)
                    if spamreader.line_num == 1:
                        self.cImporter.createLoadTableCovid(row, truncate=True)
                    else:
                        future.append(pool.submit(self.cImporter.insertInLoadTableCovid,(row)))
                for future in concurrent.futures.as_completed(future) : 
                    self.lineProcess += future.result()
                    Log.info(self.lineRead, self.lineProcess, self.cnpjProvides, self.provideProcess)
    
    def readAndImportProviders(self):
        cnpjs = self.cImporter.bringAllProviderInserted()
        self.cnpjProvides = len(cnpjs)

        Log.info(self.lineRead, self.lineProcess, self.cnpjProvides, self.provideProcess)
        if self.cnpjProvides > 0:
            with ThreadPoolExecutor(max_workers=30) as pool: #Config.THREAD_NUMBER
                future = []
                self.pImporter.createLoadTableProvider(truncate=False)
                for cnpj in cnpjs:
                    future.append(pool.submit(self.pImporter.bringProviderAndInsert,(cnpj[0])))
                for future in concurrent.futures.as_completed(future) : 
                        r = future.result()
                        if r >= 0:
                            self.provideProcess += r
                        else:
                            self.cnpjProvides += r
                        Log.info(self.lineRead, self.lineProcess, self.cnpjProvides, self.provideProcess)
        
    def readAndImportOcorrencias(self):
        cnpjs = self.pImporter.bringAllProviderInserted()

        Log.info(self.lineRead, self.lineProcess, self.cnpjProvides, self.provideProcess, self.ocorrencias)
        if len(cnpjs) > 0:
            with ThreadPoolExecutor(max_workers=30) as pool: #Config.THREAD_NUMBER
                future = []
                self.ocorrenciaImporter.createLoadTableOcorrencia(truncate=True)
                for cnpj in cnpjs:
                    future.append(pool.submit(self.ocorrenciaImporter.bringAndInsert,(cnpj[0])))
                for future in concurrent.futures.as_completed(future) : 
                        self.ocorrencias += future.result()
                        Log.info(self.lineRead, self.lineProcess, self.cnpjProvides, self.provideProcess, self.ocorrencias)

if __name__ == "__main__":
    main = Main()
    main.run()
    
