import sys

class Log():
    # Consoler Log
    @staticmethod
    def info(lineRead = 0, lineProcess = 0, providerRead = 0, providerProcess = 0, ocorrProcess = 0):
        '''
            Send to console the progress fo load.
            Update the same line when print.

            @author Yuri Fialho, Nivando Cavalcante
        '''
        sys.stdout.write("\rReaded [%d] - Buy Covid [%d/%d] - Provider [%d/%d] - Ocorrencias [%d]" % (lineRead, lineProcess, lineRead, providerProcess, providerRead, ocorrProcess))
        sys.stdout.flush()