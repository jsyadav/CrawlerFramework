"""
This module serves the need for:
1. Applying rules(checks) to documents while crawling. 
2. Perform actions on the documents based on the results obtained by rule application. 

More checks and actions can be added to the respective Checks and Actions classes
"""


class ChecksAndActions( object ):
    
    def __init__( self, doc ):
        """CAUTION:  doc is passed by ref python dict type. 
        Hence, a mutable object, use it carefully in this
        module. An unwarranted change to the doc in this module
        may result into unwanted bugs elsewhere.
        """
        self.doc = doc

class Checks( ChecksAndActions ):

    def isKeywordAssociated( self, check_param ):
        """
        takes in the doc - looks for a key called keywords and 
        check if the check_param is there in it - returns a boolean
        """
        if not self.doc.get('keywords'):
            return False

        if check_param not in self.doc['keywords']:
            return False

        return True
    
    def isFolderAssociated(self, check_param):
        """
        takes check_param which is associated with the doc
        """
        return True

class Actions( ChecksAndActions ):

    def tagAPost( self, action_param ):
        """
        will take a doc - create/update a key called tags 
        for the doc and put in a tag = action_param
        """
        self.doc['tags'] = self.doc.get('tags', [])
        self.doc['tags'].append(action_param)
