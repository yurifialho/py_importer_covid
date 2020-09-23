import sys

class Log():
    # Consoler Log
    @staticmethod
    def info(readed, processed, inserted):
        '''
            Send to console the progress fo load.
            Update the same line when print.

            @author Yuri Fialho, Nivando Cavalcante
        '''
        sys.stdout.write("\rReaded [%d] - Buy Covid [%d] - Provider [%d]" % (readed, processed, inserted))
        sys.stdout.flush()