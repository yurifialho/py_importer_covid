import csv, os, requests, sys, threading
from concurrent.futures import ThreadPoolExecutor
from config import Config
from db import MysqlConnector

class Main():

    dbUrl = "localhost"
    dbUser = "root"
    dbPass = ""

    providerColuns = ['cnpj', 'nome', 'ativo', 'id_municipio', 'municipio_desc', 'uf', 'id_cnae','cnae_desc', 'habilitado_licitar', 'id_natureza_juridia', 'natureza_juridica_desc','id_porte_empresa', 'porte_empresa_desc']
    cnpjErro = []

    mysqlConnector = None

    ccTh = 0
    inTh = 0
    lnTh = 0

    def __init__(self):
        self._lock = threading.Lock()

    def run(self):
        print("Running")
        self.readAndLoadCsv()
        
    def getConnector(self):
        if self.mysqlConnector == None:
            self.mysqlConnector = MysqlConnector(dbUrl=self.dbUrl, dbUser=self.dbUser, dbPass=self.dbPass)
        
        return self.mysqlConnector

    def readAndLoadCsv(self):
        print("Reading....")
        with ThreadPoolExecutor(max_workers=10) as pool:
            with open("../DATA/dados1.csv", newline='', encoding='utf8') as csvfile:
                spamreader  = csv.reader(csvfile, delimiter=';', quotechar='"')
                for row in spamreader:
                    if spamreader.line_num == 1:
                        #self.createLoadTableCovid(row)
                        self.createLoadTableProvider(truncate=False)
                    else:
                        #pool.apply(func=self.processRowFile, args=(row,))
                        pool.submit(self.processRowFile, (row))
                        
                    self.lnTh = spamreader.line_num
                    self.sendConsole("Inserting [%d] - Processed [%d] - - Inserted [%d]" % (self.lnTh, self.ccTh, self.inTh))
            #pool.close()
            #pool.join()
        

    def processRowFile(self, row):
        #self.insertInLoadTableCovid(row)
        if not self.checkHasInsertedProvider(row[10]):
            self.bringProviderAndInsert(row[10])
            return 1

    
    ## Rest Connections

    def bringProviderAndInsert(self, cnpj):
        url = "http://compras.dados.gov.br/fornecedores/v1/fornecedores.json?cnpj="+cnpj
        try:
            response = requests.get(url)
            if response.status_code == 200:
                json = response.json()
                if not json['count'] == 0:
                    fornecedor = json['_embedded']['fornecedores'][0]
                    f_arr = []
                    f_arr.append(fornecedor['cnpj'])
                    f_arr.append(fornecedor['nome'])
                    f_arr.append(fornecedor['ativo'])
                    f_arr.append(fornecedor['id_municipio'])
                    f_arr.append((fornecedor['_links']['municipio']['title']).split(":")[1])
                    f_arr.append(fornecedor['uf'])
                    f_arr.append(fornecedor['id_cnae'])
                    f_arr.append((fornecedor['_links']['cnae']['title']).split(":")[1])
                    f_arr.append(fornecedor['habilitado_licitar'])
                    f_arr.append(fornecedor['id_natureza_juridica'])
                    f_arr.append((fornecedor['_links']['natureza_juridica']['title']).split(":")[1])
                    f_arr.append(fornecedor['id_porte_empresa'])
                    f_arr.append((fornecedor['_links']['porte_empresa']['title']).split(":")[1])
                    self.insertInLoadTableProvider(f_arr)
                    with self._lock:
                        self.inTh += 1
        except Exception as er:
            print(er)
            self.cnpjErro.append(cnpj)


    ## Database Loads

    def createLoadTableCovid(self,header, truncate = True):
        sqlCreateTable = "CREATE TABLE IF NOT EXISTS LOAD_TABLE_CODID19 ( "
        sqlCreateTable += " VARCHAR(500) NULL, ".join(header)
        sqlCreateTable += " VARCHAR(500) NULL) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8;"
        try:
            con = self.getConnector()
            con.performQuery(sqlCreateTable)
            if truncate:
                con.performQuery("TRUNCATE TABLE LOAD_TABLE_CODID19")
        except Exception as err:
            raise err

    def insertInLoadTableCovid(self, line):
        sqlInsert = "INSERT INTO LOAD_TABLE_CODID19 VALUES( "
        c = 0
        for i in range(len(line)):
            if i < len(line)-1:
                sqlInsert += '%s,'
            else: 
                sqlInsert += '%s'
            c += 1
        sqlInsert += ");"
        try:
            con = self.getConnector()
            con.performQuery(sqlInsert, line)
        except Exception as err:
            raise err
    
    def createLoadTableProvider(self, truncate = True):
        sqlCreateTable = "CREATE TABLE IF NOT EXISTS LOAD_TABLE_PROVIDER ( "
        sqlCreateTable += " VARCHAR(500) NULL, ".join(self.providerColuns)
        sqlCreateTable += " VARCHAR(500) NULL) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8;"
        try:
            con = self.getConnector()
            con.performQuery(sqlCreateTable)
            if truncate:
                con.performQuery("TRUNCATE TABLE LOAD_TABLE_PROVIDER")
        except Exception as err:
            raise err
    
    def insertInLoadTableProvider(self, line):
        sqlInsert = "INSERT INTO LOAD_TABLE_PROVIDER VALUES( "
        c = 0
        for i in range(len(line)):
            if i < len(line)-1:
                sqlInsert += '%s,'
            else: 
                sqlInsert += '%s'
            c += 1
        sqlInsert += ");"
        try:
            con = self.getConnector()
            con.performQuery(sqlInsert, line)
        except Exception as err:
            raise err
    
    def checkHasInsertedProvider(self, cnpj):
        with self._lock:
            self.ccTh += 1
            self.sendConsole("Inserting [%d] - Processed [%d] - - Inserted [%d]" % (self.lnTh, self.ccTh, self.inTh))
            sqlQuery = "select cnpj from LOAD_TABLE_PROVIDER where cnpj = %s"
            con = self.getConnector()
            result = con.performSelect(sqlQuery, [cnpj])
            if len(result) == 0:
                return False
            else:
                return True

    # Consoler Log
    def sendConsole(self, txt):
        #sys.stdout.write("\rCNPJ_ERROR = ["+','.join(self.cnpjErro)+"]\n")
        sys.stdout.write("\r%s" % txt)
        sys.stdout.flush()

if __name__ == "__main__":
    main = Main()
    main.run()
    
