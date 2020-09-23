import threading, requests

from db import MysqlConnector
from util import Log

class ComprasImporter():

    __tableName = "load_table_covid19"

    def __init__(self, connection):
        self.__con = connection

    ## Database Connections

    def createLoadTableCovid(self,header, truncate = True):
        sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+self.__tableName+" ( "
        sqlCreateTable += " VARCHAR(500) NULL, ".join(header)
        sqlCreateTable += " VARCHAR(500) NULL) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8;"
        try:
            
            self.__con.performQuery(sqlCreateTable)
            if truncate:
                self.__con.performQuery("TRUNCATE TABLE "+self.__tableName)
        except Exception as err:
            raise err

    def insertInLoadTableCovid(self, line):
        sqlInsert = "INSERT INTO "+self.__tableName+" VALUES( "
        c = 0
        for i in range(len(line)):
            if i < len(line)-1:
                sqlInsert += '%s,'
            else: 
                sqlInsert += '%s'
            c += 1
        sqlInsert += ");"
        try:
            self.__con.performQuery(sqlInsert, line)
        except Exception as err:
            raise err

class ProviderImporter():

    __providerColuns = ['cnpj', 'nome', 'ativo', 'id_municipio', 'municipio_desc', 'uf', 
                        'id_cnae','cnae_desc', 'habilitado_licitar', 'id_natureza_juridia', 
                        'natureza_juridica_desc','id_porte_empresa', 'porte_empresa_desc']
    __tableName = "load_table_provider"

    def __init__(self, connection):
        self.__con = connection

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
        except Exception as er:
            print(er)

    ## Database Connections

    def checkHasInsertedProvider(self, cnpj):
        sqlQuery = "select cnpj from "+self.__tableName+" where cnpj = %s"
        
        result = self.__con.performSelect(sqlQuery, [cnpj])
        if len(result) == 0:
            return False
        else:
            return True

    def insertInLoadTableProvider(self, line):
        sqlInsert = "INSERT INTO "+self.__tableName+" VALUES( "
        c = 0
        for i in range(len(line)):
            if i < len(line)-1:
                sqlInsert += '%s,'
            else: 
                sqlInsert += '%s'
            c += 1
        sqlInsert += ");"
        try:
            self.__con.performQuery(sqlInsert, line)
        except Exception as err:
            raise err
    
    def createLoadTableProvider(self, truncate = True):
        sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+self.__tableName+" ( "
        sqlCreateTable += " VARCHAR(500) NULL, ".join(self.__providerColuns)
        sqlCreateTable += " VARCHAR(500) NULL) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8;"
        try:
            self.__con.performQuery(sqlCreateTable)
            if truncate:
                self.__con.performQuery("TRUNCATE TABLE "+self.__tableName)
        except Exception as err:
            raise err