import nlp
import re


class ChunkPreprocess:

    def __init__(self):
        self.nlp = nlp.NLP()

    def parse(self, eachsent):
        '''give parse output from simple sent,
        do all preprocessing before parsing, 
        and convert BIO of parser to to parsed 
        output required,
        B =B is Begining of chunk
        I=I is Inside of chunk
        O=O is Outside of chunk
        Input=[(word,Pos-tag,chunk)...]
        output=[(chunkname,phrase)..]
        For example:-
        input=
        [('I', 'PRP', 'B-NP'),
        ('went', 'VBD', 'B-VP'),
        ('the', 'DT', 'B-NP'),
        ('market', 'NN', 'I-NP')]
        output=[('I':'NP','went':VBD','the market':NP)]
        Algo:- Combine the continous sequence of I-POS2 comes after B-POS1 
        if POS1 equals to POS2, and delete the I-POS2
        '''
            
        tempdict = self.nlp.parse(eachsent)
        itemlist = []
        for each in tempdict:
            itemlist.append(list(each))
    
        itemlist_copy = []    
        end_index = 0
        
        for index,each in enumerate(itemlist):
            if not each[2]:
                each[2] = ''        
            mat=re.search(r'(^[BIO])-?(.*)',each[2])
        
            if mat and mat.group(1) == 'B'  and index+1!=len(itemlist):
                start_index=index
                end_index= self._find_last_index(start_index,itemlist,mat.group(2))
                word= self._compute_word(start_index,end_index,itemlist)
                if word:
                    each[0] = word
                itemlist_copy.append(each)
            elif mat and mat.group(1) == 'O':
                itemlist_copy.append(each)
                end_index = 0
            elif mat and mat.group(1) == 'I' and (index>end_index or end_index == 0):
                itemlist_copy.append(each)
		#end_index = 0
                
            elif  mat and index+1==len(itemlist) and mat.group(1)=='B':
                itemlist_copy.append(each)
                end_index = 0
	    
        return self._change_format(itemlist_copy)        

 
    def _find_last_index(self,start_index,itemlist,chunk_type):
        '''This function is used to find out last index of continous series (I-POS), 
        from the BIO chunks format of sentence'''
    
        count = start_index
        for index,each in enumerate(itemlist):
            if not each[2]:
		each[2] = ''
            mat = re.search(r'(^[BIO])-?(.*)',each[2])
            if mat and  index > start_index and mat.group(1) == 'I'\
                    and mat.group(2) == chunk_type :
                count = count+1
            elif mat and (mat.group(1) != 'I' or mat.group(2) != chunk_type)\
                    and index>start_index:
                return count
            elif index > start_index:
                return count
        return count	
        
    
    def _compute_word(self,start_index,end_index,itemlist):
        '''This function is used to find delete all the elements between the 
        first(B-POS) and start index(last I-POS), from the BIO chunks of sentence'''
        word=''
        for index,each in enumerate(itemlist):
            if index >= start_index and index <= end_index:
                word = word+' '+each[0]
           
        return word

    def _change_format(self,itemlist):
        '''This function is used to change from three column format to two column format 
        and to remove BIO labels from the sentence dictionary'''
    
        
        dict = [[each[0],each[2]] for each in itemlist]
        for each in dict:
            mat = re.search(r'^[BIO]-?(.*)',each[1])
            if mat.group(1):
                each[1] = mat.group(1)
            else:
                each[1] = 'O'
    
        return dict    
