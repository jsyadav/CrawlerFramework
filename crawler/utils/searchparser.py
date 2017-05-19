
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

"""Search query parser

version 2006-03-09

This search query parser uses the excellent Pyparsing module 
(http://pyparsing.sourceforge.net/) to parse search queries by users.
It handles:

* 'and', 'or' and implicit 'and' operators;
* parentheses;
* quoted strings;
* wildcards at the end of a search term (help*);

Requirements:
* Python
* Pyparsing

If you run this script, it will perform a number of tests. To use is as a
module, you should use inheritance on the SearchQueryParser class and overwrite
the Get... methods. The ParserTest class gives a very simple example of how this
could work.

-------------------------------------------------------------------------------
Copyright (c) 2006, Estrate, the Netherlands
All rights reserved.

Redistribution and use in source and binary forms, with or without modification,
are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation 
  and/or other materials provided with the distribution.
* Neither the name of Estrate nor the names of its contributors may be used
  to endorse or promote products derived from this software without specific
  prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; 
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON 
ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT 
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

CONTRIBUTORS:
- Steven Mooij
- Rudolph Froger
- Paul McGuire

TODO:
- add more docs
- ask someone to check my English texts
- add more kinds of wildcards ('*' at the beginning and '*' inside a word)?
"""
from pyparsing import Word, alphanums, Keyword, Group, Combine, Forward, Suppress, Optional, OneOrMore, oneOf, printables
from sets import Set

class SearchQueryParser:

    def __init__(self):
        self._methods = {
            'and': self.evaluateAnd,
            'or': self.evaluateOr,
            'not': self.evaluateNot,
            'parenthesis': self.evaluateParenthesis,
            'quotes': self.evaluateQuotes,
            'word': self.evaluateWord,
            'infixwordwildcard': self.evaluateInfixWordWildcard,
            'prefixwordwildcard': self.evaluatePrefixWordWildcard,
            'suffixwordwildcard': self.evaluateSuffixWordWildcard,
        }
        self._parser = self.parser()
    
    def parser(self):
        """
        This function returns a parser.
        The grammar should be like most full text search engines (Google, Tsearch, Lucene).
        
        Grammar:
        - a query consists of alphanumeric words, with an optional '*' wildcard
          at the end of a word
        - a sequence of words between quotes is a literal string
        - words can be used together by using operators ('and' or 'or')
        - words with operators can be grouped with parenthesis
        - a word or group of words can be preceded by a 'not' operator
        - the 'and' operator precedes an 'or' operator
        - if an operator is missing, use an 'and' operator
        """
        operatorOr = Forward()
        
        ## operatorWord = Group(Combine(Word(alphanums) + Suppress('*'))).setResultsName('wordwildcard') | \
        ##                     Group(Word(alphanums)).setResultsName('word')
        characters = printables.replace('"','').replace('*','').replace('(','').replace(')','')
        ##operatorWord = Group(Combine(Word(characters) + Suppress('*'))).setResultsName('wordwildcard') | \
        ##                     Group(Word(characters)).setResultsName('word')    # For including alphanums and punctuation
        operatorWord = Group(Combine(Suppress('*') + Word(characters) + Suppress('*'))).setResultsName('infixwordwildcard') | \
                       Group(Combine(Word(characters) + Suppress('*'))).setResultsName('suffixwordwildcard') | \
                       Group(Combine(Suppress('*') + Word(characters))).setResultsName('prefixwordwildcard') | \
                             Group(Word(characters)).setResultsName('word')    # For including alphanums and punctuation
     
        operatorQuotesContent = Forward()
        operatorQuotesContent << (
            (operatorWord + operatorQuotesContent) | operatorWord
        )
        
        operatorQuotes = Group(
            Suppress('"') + operatorQuotesContent + Suppress('"')
        ).setResultsName("quotes") | operatorWord
        
        operatorParenthesis = Group(
            (Suppress("(") + operatorOr + Suppress(")"))
        ).setResultsName("parenthesis") | operatorQuotes

        operatorNot = Forward()
        operatorNot << (Group(
            Suppress(Keyword("not", caseless=True)) + operatorNot
        ).setResultsName("not") | operatorParenthesis)

        operatorAnd = Forward()
        operatorAnd << (Group(
            operatorNot + Suppress(Keyword("and", caseless=True)) + operatorAnd
        ).setResultsName("and") | Group(
            operatorNot + OneOrMore(~oneOf("and or") + operatorAnd)
        ).setResultsName("and") | operatorNot)
        
        operatorOr << (Group(
            operatorAnd + Suppress(Keyword("or", caseless=True)) + operatorOr
        ).setResultsName("or") | operatorAnd)
        return operatorOr.parseString

    def evaluateAnd(self, argument):
        return self.evaluate(argument[0]).intersection(self.evaluate(argument[1]))

    def evaluateOr(self, argument):
        return self.evaluate(argument[0]).union(self.evaluate(argument[1]))

    def evaluateNot(self, argument):
        return self.GetNot(self.evaluate(argument[0]))

    def evaluateParenthesis(self, argument):
        return self.evaluate(argument[0])

    def evaluateQuotes(self, argument):
        """Evaluate quoted strings

        First is does an 'and' on the indidual search terms, then it asks the
        function GetQuoted to only return the subset of ID's that contain the
        literal string.
        """
        r = Set()
        search_terms = []
        for item in argument:
            search_terms.append(item[0])
            if len(r) == 0:
                r = self.evaluate(item)
            else:
                r = r.intersection(self.evaluate(item))
        return self.GetQuotes(' '.join(search_terms), r)

    def evaluateWord(self, argument):
        return self.GetWord(argument[0])

    def evaluateInfixWordWildcard(self, argument):
        return self.GetInfixWordWildcard(argument[0])
        
    def evaluatePrefixWordWildcard(self, argument):
        return self.GetPrefixWordWildcard(argument[0])

    def evaluateSuffixWordWildcard(self, argument):
        return self.GetSuffixWordWildcard(argument[0])

    def evaluate(self, argument):
        #print argument
        return self._methods[argument.getName()](argument)

    def Parse(self, query):
        print self._parser(query)[0]
        return self.evaluate(self._parser(query)[0])

    def GetWord(self, word):
        return Set()

    def GetInfixWordWildcard(self, word):
        return Set()

    def GetPrefixWordWildcard(self, word):
        return Set()

    def GetSuffixWordWildcard(self, word):
        return Set()
    
    def GetQuotes(self, search_string, tmp_result):
        return Set()

    def GetNot(self, not_set):
        return Set().difference(not_set)


class ParserTest(SearchQueryParser):
    """Tests the parser with some search queries
    tests containts a dictionary with tests and expected results.
    """
    ## tests = {
    ##     'pyparsing': Set([1]),
    ##     'python or pyparsing':Set([1]),
    ##     'python and ruby':Set([]),
    ##     '"arsing module"':Set([1]),
    ##     '"executing simple grammars, vs. the traditional lex/yacc"':Set([1]),
    ##     '"executing simple grammars vs the traditional lexyacc"':Set([]),
    ##     'traditional and regul* or expression*':Set([1]),
    ##     }

    tests = {
        'value and index': Set([1]),
        '"requires a sequence value"':Set([1]),
        'configuration or (external and tales)': Set([1]),
        'configuration and external and TALES': Set([1]),
        'Zope 2.7*':Set([1]),
        '(TALES and expression) or predicate': Set([1]),
        'TALES and (expression or predicate)': Set([1]),
        '"requires sequence"': Set([]),
        }
    ## docs = {
    ##     1: 'The pyparsing module is an alternative approach to creating and executing simple grammars, vs. the traditional lex/yacc approach, or the use of regular expressions. The pyparsing module provides a library of classes that client code uses to construct the grammar directly in Python code.'
    ## }
        
    ## index = {'Python': Set([1]),
    ##          'The': Set([1]),
    ##          'a': Set([1]),
    ##          'alternative': Set([1]),
    ##          'an': Set([1]),
    ##          'and': Set([1]),
    ##          'approach': Set([1]),
    ##          'classes': Set([1]),
    ##          'client': Set([1]),
    ##          'code': Set([1]),
    ##          'construct': Set([1]),
    ##          'creating': Set([1]),
    ##          'directly': Set([1]),
    ##          'executing': Set([1]),
    ##          'expressions': Set([1]),
    ##          'grammar': Set([1]),
    ##          'grammars': Set([1]),
    ##          'in': Set([1]),
    ##          'is': Set([1]),
    ##          'lexyacc': Set([1]),
    ##          'library': Set([1]),
    ##          'module': Set([1]),
    ##          'of': Set([1]),
    ##          'or': Set([1]),
    ##          'provides': Set([1]),
    ##          'pyparsing': Set([1]),
    ##          'regular': Set([1]),
    ##          'simple': Set([1]),
    ##          'that': Set([1]),
    ##          'the': Set([1]),
    ##          'to': Set([1]),
    ##          'traditional': Set([1]),
    ##          'use': Set([1]),
    ##          'uses': Set([1]),
    ##          'vs': Set([1])
    ##          }

    def createindex(self, text):
        self.docs = {1:text}
        self.index = {}
        for word in self.docs[1].split():
            self.index[word] = Set([1])
        
            
    def GetWord(self, word):
        if (self.index.has_key(word)):
            return self.index[word]
        else:
            return Set()

    def GetWordWildcard(self, word):
        result = Set()
        for item in self.index.keys():
            if word == item[0:len(word)]:
                result = result.union(self.index[item])
        return result

    def GetQuotes(self, search_string, tmp_result):
        result = Set()
        for item in tmp_result:
            if self.docs[item].count(search_string):
                result.add(item)
        return result
    
    def GetNot(self, not_set):
        all = Set(self.docs.keys())
        return all.difference(not_set)

    def Test(self):
        text = """Evaluation is specified by a sequence of ValueProviders associated with the index. A ValueProvider is a device that returns a value for an object. If the return value is not None, then it is interpreted as the object's value with respect to this ValueProvider.
A ValueProvider can have an associated IgnorePredicate, a TALES expression. When the IgnorePredicate yields true for a value, then it is ignored. You may e.g. specify that empty strings should be ignored.
A ValueProvider can have an associated Normalizer, a TALES expression. The Normalizer is applied to not ignored values to transform them in a standard form, e.g. the normalizer for a keyword index can transform a string into a one element tuple containing the string as a keyword index requires a sequence value.
The most essential ValueProviders are AttributeLookups. An AttributeLookup determines the object's value through the lookup of an attribute. The AttributeLookup's configuration determines whether acquisition can be used, whether a callable value should be called and whether exceptions should be ignored.
ExpressionEvaluators are another type of ValueProviders. These are TALES expressions defining an objects value. ExpressionEvaluators often avoid to define simple scripts just for indexing purposes.
Warning: Until Zope 2.7, TALES expressions have been trusted when used outside of Zope; from Zope 2.8 on, TALES expression evaluation is always subject to Zope's security restrictions even when used outside of Zope. This may have strange consequences when you perform index management (e.g. mass reindexing) in an external script (run outside of Zope). In such cases, you should probably let the script login as a Zope user with sufficient priviledges."""

        self.createindex(text)
        all_ok = True
        for item in self.tests.keys():
            print item            
            r = self.Parse(item)            
            e = self.tests[item]
            print 'Result: %s' % r
            print 'Expect: %s' % e
            if e == r:
                print 'Test OK'
            else:
                all_ok = False
                print '>>>>>>>>>>>>>>>>>>>>>>Test ERROR<<<<<<<<<<<<<<<<<<<<<'
            print ''
        return all_ok
            
if __name__=='__main__':
    if ParserTest().Test():
        print 'All tests OK'
    else:
        print 'One or more tests FAILED'
