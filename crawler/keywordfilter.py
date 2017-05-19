
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

import re
from sets import Set
import StringIO
from utils import pshlex

import traceback
import logging
log = logging.getLogger('KeywordFilter')

prio = {'or':5, 'and':4,'not':3,'(':2,')':1}

class KeywordFilter:
    """
    Filters an article based on the keywords with boolean logic.
    """
    def createindex(self, text):
        '''Creates an index of the text with the words in the document into a dictionary'''
        self.index = set()
        [self.index.add(x) for x in pshlex.shlex(StringIO.StringIO(unicode(text,'utf-8','ignore')) ,posix=True)]

    #@logit(log, 'checkfilter')
    def checkFilter(self, data, keywords):
        """
        Takes in data and keywords
        data and keywords should be in unicode or ascii.
        returns a list/set of keywords that are found else return an empty list
        """
        self.data = data.lower()
        self.keywords=keywords
        self.createindex(self.data)
        matched_keywords = []
        for keyword in self.keywords:
            if self._check(keyword.lower()):
                matched_keywords.append(keyword)
        return  matched_keywords

    def _check(self, keyword):
        ''' This method converts the keyword into postfix expression.
        However, the expression is computed on the fly rather than
        doing one more pass.
        This requires a patched shlex to handle unicode correctly.
        '''
        postfix = list()
        stack = list()
        for x in pshlex.shlex( StringIO.StringIO(keyword)):
            if x in ['and','or']:
                while len( stack ) and prio[x] <= prio[ stack[-1]]:
                    self.__computeValue(stack, postfix)
                stack.append(x)
            elif x == 'not':
                stack.append(x)
            elif x == '(':
                stack.append(x)
            elif x == ')':
                while stack[-1] != '(':
                    self.__computeValue(stack, postfix)
                stack.pop()
            else:
                postfix.append(x)

        while len(stack):
            if(stack[-1] == "("):
                stack.pop()
            else:
                self.__computeValue(stack, postfix)

        t = postfix.pop()
        if not postfix:
            if type(t) != type(True):
                return self._checkForWord(t)
            else:
                return t

        # Default operator is AND
        op1 = self._checkForWord(t)
        while len(postfix):
            opo = self._checkForWord(postfix.pop())
            op1 = op1 and opo
        return op1

    def __computeValue(self, stack, postfix):
        op = stack.pop()
        if op == 'and':
            op1 = postfix.pop()
            op2 = postfix.pop()
            postfix.append( self._checkForWord(op1) and self._checkForWord(op2) )
        elif op == 'or':
            op1 = postfix.pop()
            op2 = postfix.pop()
            postfix.append( self._checkForWord(op1) or self._checkForWord(op2) )
        elif op == 'not':
            op1 = postfix.pop()
            postfix.append( not self._checkForWord(op1) )
        else:
            postfix.append( op )

    def _checkForWord(self, word):
        if isinstance(word,bool):
            return word
        found = False
        if word.endswith('*'):
            found = self.__getSuffixWordWildcard(word[0:-1])
        elif word.startswith('*'):
            found = self.__getPrefixWordWildcard(word[1:])
        elif word.endswith('"') and word.startswith('"'):
            log.debug("Checking for phrase %s"%(word))
#            found = word[1:-1] in self.data
	    found = re.search("\\b%s\\b"%re.escape(word[1:-1]),self.data,re.DOTALL and re.IGNORECASE)
        else:
            log.debug("Checking for word %s"%(word))
            found = word in self.index
        return bool(found)

    def __getSuffixWordWildcard(self, word):
        log.debug("Checking for words beginning with %s"%(word))
        for item in self.index:
            if item.startswith(word):
                return True
        return False

    def __getPrefixWordWildcard(self, word):
        log.debug("Checking for words ending with %s"%(word))
        for item in self.index:
            if item.endswith(word):
                return True
        return False
