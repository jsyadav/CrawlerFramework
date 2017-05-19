import os
from jpype import *

class TextImporter(object):
    
    def __init__(self):
        self.package =  JPackage('com.serendio.diskoverer.core.textimport') 
   
    def processFile(self, fname):
        """Given a filename, extract the file contents and return back the plain text
        """
        try:
            file = open(fname,'rb')
        except :
            return None
        filesplits = os.path.splitext(fname)
        print filesplits
        if not file :
            return None
        ext_importer_map = {'.txt' : self.package.FileTxt,
                            '.doc' : self.package.FileWord,
                            '.ppt' : self.package.FilePowerPoint,
                            '.pdf' : self.package.FilePDF,
                            '.rtf' : self.package.FileRTF,
                            '.html': self.package.FileHTML,
                            '.htm' : self.package.FileHTML,
                            '.xls' : self.package.FileXLS,
                            }

        importer = ext_importer_map.get(filesplits[1])
        if not importer:                # unknown file extension
            return ''
        File = JClass('java.io.File')
        f = File(str(file.name))
        ff = importer(f)
        if ff:
            ff.process()
            text = ff.getText()
            return text
        else:
            return ''
    

if __name__ == '__main__':
    #startJVM('/usr/local/jdk1.6.0_05/jre/lib/i386/client/libjvm.so','-Djava.ext.dirs=/home/sathya/serendio/core/bin/lib/diskoverer.jar:/home/sathya/serendio/core/jars/')
    startJVM(getDefaultJVMPath(),"-ea",'-Djava.ext.dirs=/home/cnu/project/VoomNew/New/diskoverer/jars/diskoverer.jar:/home/cnu/project/VoomNew/New/diskoverer/jars/:.') 
    ti = TextImporter()
    text = ti.processFile('/home/cnu/Documents/py_metaclassprog.pdf')
    shutdownJVM()
    print text
