import threading, requests

from db import MysqlConnector
from util import Log

class OcorrenciaProvider():
    __providerOcorrColuns = ['cnpj', 'descricao', 'id_tipo_ocorrencia', 'motivo', 'impedido_licitar', 'id_prazo', 
                        'desc_prazo', 'data_aplicacao','data_inicial', 'data_final', 'valor_multa']

    __tableName = "load_table_ocorrencia"

    def __init__(self, connection):
        self.__con = connection

    def getDescPrazo(self, id):
        if id == 1:
            return "Determinado"
        elif id == 2:
            return "2 Anos"
        elif id == 3:
            return "5 Anos"
        elif id == 4:
            return "Indeterminado"
        else:
            return ""
    
    ## Rest Connections

    def bringAndInsert(self, cnpj):
        url = "http://compras.dados.gov.br/fornecedores/v1/ocorrencias_fornecedores.json?cnpj="+cnpj
        urlDetalhe = "http://compras.dados.gov.br/fornecedores/doc/ocorrencia_fornecedor/"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                json = response.json()
                if not json['count'] == 0:
                    for ocorr in json['_embedded']['ocorrenciasFornecedores']:
                        idOcorr = ocorr['id']
                        response2 = requests.get(urlDetalhe+str(idOcorr)+".json")
                        if response2.status_code == 200:
                            jsonDetalhe = response2.json()
                            f_arr = []
                            f_arr.append(jsonDetalhe['cnpj'])
                            f_arr.append(jsonDetalhe['descricao'])
                            f_arr.append(jsonDetalhe['id_tipo_ocorrencia'])
                            f_arr.append(jsonDetalhe['motivo'])
                            f_arr.append(jsonDetalhe['impedido_licitar'])
                            f_arr.append(jsonDetalhe['id_prazo'])
                            if not jsonDetalhe['id_prazo'] == None:
                                f_arr.append(self.getDescPrazo(int(jsonDetalhe['id_prazo'])))
                            else:
                                f_arr.append(None)
                            f_arr.append(jsonDetalhe['data_aplicacao'])
                            f_arr.append(jsonDetalhe['data_inicial'])
                            f_arr.append(jsonDetalhe['data_final'])
                            f_arr.append(jsonDetalhe['valor_multa'])
                            
                            self.insertInLoadTableOcorrencia(f_arr)
                    return int(json['count'])
            return 0
        except Exception as er:
            print(er)

    ## Database Connections

    def createLoadTableOcorrencia(self, truncate = True):
        sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+self.__tableName+" ( "
        sqlCreateTable += " VARCHAR(1000) NULL, ".join(self.__providerOcorrColuns)
        sqlCreateTable += " VARCHAR(1000) NULL) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8;"
        try:
            self.__con.performQuery(sqlCreateTable)
            if truncate:
                self.__con.performQuery("TRUNCATE TABLE "+self.__tableName)
        except Exception as err:
            raise err
    
    def insertInLoadTableOcorrencia(self, line):
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

class TipoOcorrenciaImporter():

    __tableName = "load_table_tipo_ocorrencia"

    def __init__(self, connection):
        self.__con = connection

    ## Rest Connections

    def bringProviderAndInsert(self):
        url = "http://compras.dados.gov.br/fornecedores/v1/tipos_ocorrencia.json"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                json = response.json()
                if not json['count'] == 0:
                    for tipo in json['_embedded']['tiposOcorrencia']:
                        f_arr = []
                        f_arr.append(tipo['id'])
                        f_arr.append(tipo['descricao'])
                        self.insertInLoadTableTipoOcorrencia(f_arr)
                    return int(json['count'])
            return 0
        except Exception as er:
            print(er)

    ## Database Connections

    def createLoadTableTipoOcorrencia(self, truncate = True):
        sqlCreateTable = "CREATE TABLE IF NOT EXISTS "+self.__tableName+" ( "
        sqlCreateTable += " ID INT, DESCRICAO VARCHAR(100)"
        sqlCreateTable += " ) ENGINE = InnoDB DEFAULT CHARACTER SET = utf8;"
        try:
            
            self.__con.performQuery(sqlCreateTable)
            if truncate:
                self.__con.performQuery("TRUNCATE TABLE "+self.__tableName)
        except Exception as err:
            raise err
    
    def insertInLoadTableTipoOcorrencia(self, line):
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
            return 1
        except Exception as err:
            raise err
    
    def bringAllProviderInserted(self):
        #sqlQuery = "select distinct cnpj from "+self.__tableName
        sqlQuery = "select distinct cnpj from load_table_covid19 c where not exists (select p.cnpj from load_table_provider p where p.cnpj = c.cnpj)"
        
        return self.__con.performSelect(sqlQuery)


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
            response = requests.get(url, timeout=3)
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
                    try:
                        f_arr.append((fornecedor['_links']['cnae']['title']).split(":")[1])
                    except:
                        f_arr.append("")
                    f_arr.append(fornecedor['habilitado_licitar'])
                    f_arr.append(fornecedor['id_natureza_juridica'])
                    f_arr.append((fornecedor['_links']['natureza_juridica']['title']).split(":")[1])
                    f_arr.append(fornecedor['id_porte_empresa'])
                    f_arr.append((fornecedor['_links']['porte_empresa']['title']).split(":")[1])
                    self.insertInLoadTableProvider(f_arr)
                    return 1
                else:
                    return -1
            return 0
        except Exception:
            return 0

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
    
    def bringAllProviderInserted(self):
        sqlQuery = "select distinct cnpj from "+self.__tableName
        
        return self.__con.performSelect(sqlQuery)