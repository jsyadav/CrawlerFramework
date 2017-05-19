import re
import cPickle
import os
import nltk


class NLP(object):
    """Tagger, tokenizer and other methods
    """
    def __init__(self,tagger_file="utils/brill.tagger",chunker_file="utils/chunker.pkl"):
        tagger_file = open(tagger_file,'rb')
        chunker_file  = open(chunker_file,'rb')
        self.pstokenizer = nltk.tokenize.PunktSentenceTokenizer()
        self.brill_tagger = cPickle.load(tagger_file)
	self.chunker = cPickle.load(chunker_file)
        self.contractions_unwound={"ain't":"ai not", "aren't":"are not",
                                   "isn't":"is not", "wasn't":"was not",
                                   "weren't":"were not", "didn't":"did not",
                                   "doesn't":"does not", "don't":"do not",
                                   "hadn't":"had not", "hasn't":"has not",
                                   "haven't":"have not", "can't":"can not",
                                   "couldn't":"could not", "needn't":"need not",
                                   "shouldn't":"should not", "shan't":"shall not"}
        self.contractions_unwound={"ain't":"ai not", "aren't":"are not",
                               
                                   "won't":"would not", "wouldn't":"would not",
                                   "i'm":"i am", "you're":"you are",
                                   "he's":"he is", "she's":"she is",
                                   "it's":"it is", "we're":"we are",
                                   "they're":"they are", "i've":"i have",
                                   "you've":"you have", "we've":"we have",
                                   "they've":"they have", "who've":"who have",
                                   "what've":"what have", "when've":"when have",
                                   "where've":"where have", "why've":"why have",
                                   "how've":"how have", "i'd":"i would",
                                   "you'd":"you would", "he'd":"he would",
                                   "she'd":"she would", "we'd":"we would",
                                   "they'd":"they would", "i'll":"i will",
                                   "you'll":"you will", "he'll":"he will",
                                   "she'll":"she will", "we'll":"we will",
                                   "they'll":"they will", "cant":"can not",
                                   "wont":"would not", "dont":"do not",
                                   }
        self.contractions_re = re.compile(r"\b(%s)\b" % "|".join(map(re.escape, self.contractions_unwound.keys())),
                                          re.I + re.U + re.DOTALL)

    def tag(self, sentence):
        """Given a sentence, return a tagged sentence
        """
        tagged = self.brill_tagger.tag(sentence.split())
        tagged_sentence = " ".join([nltk.tag.tuple2str(tok) for tok in tagged])
        return tagged_sentence
    
    def expand_contractions(self,sentence):
        return self.contractions_re.sub(lambda mo: \
                                                self.contractions_unwound[mo.group().strip().lower()], sentence)


    def parse(self,sentence):
	"""Given a sentence gives shollow parsed(chunked) sentence in BIO format"""
	tagged_sent = self.tag(sentence)
	split_sent  =[nltk.tag.str2tuple(each) for each in tagged_sent.split(' ')]
	return self.chunker.parse(split_sent)

    def tokenize(self, sentence, expand_contractions=False):
        """Given a sentence, tokenize the sentence based on words and other puntuations
        """
        if expand_contractions:
            sentence = expand_contractions(sentence)
        return nltk.tokenize.wordpunct_tokenize(sentence)

    def strip_tags(self, tag_text):
        """Given a tagged text, remove the tags and output a plain sentence
        """
        toks = tag_text.split()
        words = [tok.split('/')[0] for tok in toks]
        return ' '.join(words)
    
    def split_sentences(self, text):
        """Given a piece of text, split it into sentences
        """
        return self.pstokenizer.tokenize(text)

    def split_paragraphs(self, text):
        """Given a piece of text, split it into paragraphs
        """
        return nltk.tokenize.line_tokenize(text)
    
    def getAlternatives(self,text):
        """Given a sentence return stemmized tokens
        """
        portedStemmer = nltk.stem.PorterStemmer()
        return portedStemmer.stem_word(text)

    def spellCheck(self,text):
        """Given a text ,corrects misspelled words and returns back corrected text
        """
        return text
        
