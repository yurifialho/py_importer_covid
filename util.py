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
        sys.stdout.write("\rInserting [%d] - Processed [%d] - - Inserted [%d]" % (readed, processed, inserted))
        sys.stdout.flush()