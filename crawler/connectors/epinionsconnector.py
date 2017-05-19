
'''
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
'''

#ASHISH YADAV 
#JV
#Skumar
#prerna
#modified by prerna on 27dec

import urllib
from urllib2 import urlparse
import re
from datetime import datetime
import md5
import logging
import copy


from baseconnector import BaseConnector
from utils.utils import stripHtml,get_hash
from utils.urlnorm import normalize
from utils.decorators import logit
from tgimport import *
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo


log = logging.getLogger('EpinionsConnector')

class EpinionsConnector(BaseConnector):
    """For epinions.com
    """
    
    def fetch(self):
        """Fetch of EpinionsConnector
        """
        try:
            self.genre = 'Review'
            self.parenturi = self.currenturi
            code =None
            if self.currenturi.endswith('display_~reviews'):
                code = urlparse.urlparse(self.currenturi)[2]
                self.currenturi = 'http://www.epinions.com'+code +\
                    '/pp_~1/sort_~date/sort_dir_~des/sec_~opinion_list'
                  
            else:
                code =  urlparse.urlparse(self.currenturi)[2].split('/')[2]
                self.currenturi = 'http://www.epinions.com/reviews/'+code +\
                    '/pp_~1/sort_~date/sort_dir_~des/sec_~opinion_list'
                 
            if not code:
                return False
            if not self.__setSoup():
                log.info(self.log_msg('Soup not set , cannot proceed'))
                return False
            self.__getParentPage()
            c =0
            while True:
                parent_page_soup = copy.copy( self.soup )
                if not self.__addReviews():
                    break
                
                try:
                    self.currenturi = 'http://www.epinions.com' + parent_page_soup.\
                                        find('a',href=True , text='Next').parent['href']
                    if not self.__setSoup():
                        break
##                    c+=1
##                    if c ==2:
##                        break
                except:
                    log.info(self.log_msg('Next Page not found'))
                    break
            return True
        except:
            log.exception(self.log_msg('exception in fetch'))
            return False
   
    def __addReviews(self):
        """ Add the Reviews found on the page
        """
        try:
            review_links=[each.find('a')['href']for each in self.soup.findAll('h2','review_title')]
        except:
            log.info(self.log_msg('REview links not found'))
            return False
        if not review_links: #either no reviews exist or page is search pag as product doesnot exist anymore
            log.info(self.log_msg('No Reviews found in this page %s'%self.currenturi))
            f=open('check.html','w')
            f.write(self.soup.prettify())
            f.close()
            return False
        for review_link in review_links:
            review_link_uri = 'http://www.epinions.com' + review_link
            self.currenturi = normalize(review_link_uri)
            self.__setSoup()
            review_identifier = review_link_uri.split('/')[-1]
            if checkSessionInfo(self.genre, self.session_info_out,review_identifier,
                                self.task.instance_data.get('update'),parent_list=[self.parenturi]) and \
            not self.task.instance_data.get('pick_comments'):
                log.info(self.log_msg('Session info Returns True'))
                return False
            page={}
            try:
                page['et_author_link'] = 'http://www.epinions.com'+self.soup.find('span',text=re.compile('\s*Epinions.com ID:\s*')).\
                                    findParent('div').find('a')['href']
                self.currenturi = page['et_author_link']
                if self.__setSoup():
                    try:
                        page['et_author_name'] = stripHtml(self.soup.\
                                    find('span',text=re.compile('\s*Epinions.com ID:\s*')).\
                                    findParent('tr').find('b').renderContents())
                    except:
                        log.exception(self.log_msg('Author name not found'))
##                    try:
##                        date_str = stripHtml(self.soup.find('span',text=re.compile('\s*Member Since:\s*')).\
##                                    findParent('tr').find('b').renderContents()).replace('\'','')
##                        page['edate_author_member_since'] = datetime.strftime(datetime.\
##                                    strptime(date_str,"%b %d %y"),"%Y-%m-%dT%H:%M:%SZ")
##                    except:
##                        log.exception(self.log_msg('Author member since not found'))
                    try:
                       
                        span_tag = [stripHtml(each.renderContents()) for each in self.\
                                    soup.find('b',text='Activity Summary').\
                                    findParent('table').findAll('span','rkr')]
                        for each in span_tag:
                            if each.startswith('Reviews Written:'):
                                page['ei_author_reviews_count'] = int(re.sub('[^\d]','',re.sub('^Reviews Written:','',each).strip()))
                            if each.startswith('Member Visits:'):
                                page['ei_author_member_visits_count'] = int(re.sub('[^\d]','',re.sub('^Member Visits:','',each).strip()))
                            if each.startswith('Total Visits: '):
                                page['ei_author_total_member_visits_count'] = int(re.sub('[^\d]','',re.sub('^Total Visits: ','',each).strip()))
                    except:
                        log.exception(self.log_msg('Author info cannot be extracted'))
                    author_info = {'et_author_real_name':'Member:','et_author_homepage':'Homepage:','et_author_location':'Location:'}
                    for each in author_info.keys():
                        try:
                            #log.info(stripHtml(self.soup.find('span',text=re.compile('\s*' + author_info[each]+ '\s*')).findParent('tr').findAll('td')[-1].renderContents()))
                            td_tag = self.soup.find('span',text=re.compile('\s*' + author_info[each]+ '\s*$')).\
                                        findParent('tr').findAll('td')[-1]
                            if not each=='et_author_homepage':
                                page[each] = stripHtml(td_tag.renderContents())
                            else:
                                url = td_tag.find('a')['href']
                                url = re.search('destin_~(http.*$)',url).group(1)
                                page[each] = url.replace('%3A',':').replace('%252F','/')
                        except:
                            log.exception(self.log_msg('Author %s cannot be extracted'%each))
                    pass
                    try:
                        web_sites_found = False
                        web_sites = ''
                        tr_tags = self.soup.find('span',text=re.compile('\s*Favorite Websites:\s*')).\
                                    findParent('table').findAll('tr')
                        for each in tr_tags:
                            try:
                                td_tags = each.findAll('td')
                                if len(td_tags)>0:
                                    td_tag = td_tags[0]
                                    if td_tag:
                                        td_str = stripHtml( td_tag.renderContents() )
                                        if web_sites_found and not td_str=='':
                                            break
                                        if td_str=='Favorite Websites:':
                                            web_sites_found = True
                                if web_sites_found and len(td_tags)>1:
                                    url = td_tags[-1].find('a')['href']
                                    url = re.search('destin_~(http.*$)',url).group(1)
                                    url =  url.replace('%3A',':').replace('%252F','/')
                                    web_sites = web_sites + url  + '\n'
                            except:
                                log.exception(self.log_msg('Web sites cannot be found'))
                            if not web_sites.strip()=='':
                                page['et_author_favorite_websites'] = web_sites
                    except:
                        log.exception(self.log_msg('Author Favouraite Websites not found'))
                    try:
                        trust_tag = self.soup.find('span',text='Web of Trust').\
                                    findParent('table')
                        trusts = stripHtml(trust_tag.find('b',text=re.compile('trusts:$')).\
                                    findParent('tr').findNext('tr').renderContents()).splitlines()
                        page['et_author_trusts'] = '\n'.join([x.strip() for x in trusts \
                                        if not re.search('View all',x) and x.strip()])
                        
                        trusted_by = stripHtml(trust_tag.find('b',text=re.compile('trusted by:$')).\
                                    findParent('tr').findNext('tr').renderContents()).splitlines()
                        page['et_author_trusted_by'] = '\n'.join([x.strip() for x in trusted_by \
                                        if not re.search('View all',x) and x.strip()])
                        
                    except:
                        log.exception(self.log_msg('Author trusts not found'))
            except:
                log.exception(self.log_msg('Author info cannot be extracted'))
            self.currenturi = review_link_uri
            if not self.__setSoup():
                continue
            page['uri'] = self.currenturi
            try:
                page['title'] = stripHtml(self.soup.findAll('h1',{'class':re.compile('title')})[-1].\
                                renderContents())
            except:
                log.exception(self.log_msg('review title could not be parsed'))
                page['title'] = ''
            try:
                rating_str = re.search('\d+',self.soup.find('div',id ='single_review_area').\
                                            find('span',attrs ={'class':re.compile('iReviewStarsMedYellow medStars\d+')})['class']\
                                            ).group()
                rating =  float(rating_str)/10                            
                page['ef_rating_overall'] = rating
            except:
                log.exception(self.log_msg('review ratings could not be parsed'))
           #pros , cons and the bottom line
            try:
                page['et_data_pros'] = re.sub('^Pros:','',stripHtml(self.soup.find('b', text =re.compile('Pros:')).findParent('span').renderContents())).strip()
            except:
                log.exception(self.log_msg("review pros couldn't be  extracted"))    
            try:
                page['et_data_cons'] = re.sub('^Cons:','',stripHtml(self.soup.find('b', text =re.compile('Cons:')).findParent('span').renderContents())).strip()
            except:
                log.exception(self.log_msg("review Cons couldn't be  extracted"))      
            try:
                page['et_data_bottomline'] = re.sub('^The Bottom Line','',stripHtml(self.soup.find('b', text =re.compile('The Bottom Line')).findParent('span').renderContents())).strip()   
            except:
                log.exception(self.log_msg("review bottom line couldn't be extracted"))
##            try:
##                page['et_data_recommended'] = stripHtml(self.soup.find('b',\
##                                text = re.compile('Recommended:')).next.next.__str__()) 
##            except:
##                log.exception(self.log_msg('data recommended not found'))
##            try:
##                page['et_product_purchased'] = stripHtml(self.soup.find('b',\
##                                text = re.compile('What product did you purchase or try to purchase?')).\
##                                next.__str__())
##            except:
##                log.exception(self.log_msg('product purchased not found'))                
            try:
                data_tag = stripHtml(self.soup.find('div','user_review_full').renderContents())
                
                page['data'] = re.split('Recommended:',data_tag)[0].strip()
            except:    
                page['data'] = ''
                log.exception(self.log_msg('review data could not be extracted'))
            try:
                #log.info(page)
                review_hash = get_hash( page )
            except:
                log.debug(self.log_msg("Error occured while making the hask for\
                        the review, continuing to the next review"))
                continue
            result=updateSessionInfo(self.genre, self.session_info_out,review_identifier, review_hash,
                                     'Review', self.task.instance_data.get('update'),\
                                    parent_list=[self.parenturi])
            if result['updated']:
                parent_list = [self.parenturi]
                page['parent_path']=copy.copy(parent_list)
                parent_list.append(review_identifier)
                page['path']=parent_list
                page['priority']=self.task.priority
                page['level']=self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id  
                page['client_name'] = self.task.client_name
                page['last_updated_time'] = page['pickup_date']
                page['versioned'] = False
                page['entity'] = 'Review'
                page['category'] = self.task.instance_data.get('category','')
                try:
                    date = stripHtml([date for date in self.soup.findAll('span',{'class':'rgr'}) \
                            if stripHtml(date.renderContents()).find('Written: ')>-1][0].renderContents())
                    date = re.search('Written: (\w{3} \d{2} \'\d{2})',date).group(1)
                    date = date.replace("'",'').strip()
                    page['posted_date'] = datetime.strftime(datetime.strptime(date,"%b %d %y"),\
                                            "%Y-%m-%dT%H:%M:%SZ")
                except:
                    log.exception(self.log_msg('review post_date could not be parsed'))
                    page['posted_date'] = page.get('posted_date',datetime.strftime(datetime.utcnow(),\
                                            "%Y-%m-%dT%H:%M:%SZ"))
                page['task_log_id']=self.task.id
                page['uri_domain'] = urlparse.urlparse(page['uri'])[1]
                self.pages.append(page)
            
            parent_soup = copy.copy( self.soup )
            try:
                comment_url = filter(lambda x: x.renderContents().\
                                startswith('Read all comments ') , self.soup.\
                                findAll('a',href=True))
                log.info(self.log_msg('comment_url:%s'%comment_url))                
                if self.task.instance_data.get('pick_comments') and comment_url:
                    self.__addComments('http://www.epinions.com'+comment_url[0]['href'],\
                                [self.parenturi,review_identifier])
                else:
                    log.info(self.log_msg('comments link could not be found/No\
                     comments found for the review/pick_comments disabled'))
            except:
                log.info(self.log_msg('exception while adding comments'))
            self.soup = copy.copy(parent_soup)
        return True

    @logit(log , '__addComments')
    def __addComments(self,comment_url,parent_list):
        #as of now epinions keeps a maximum of 30 comments, and on a single page.
        self.currenturi = comment_url
        if not self.__setSoup():
            return False
        log.info(self.log_msg('comment_url :: %s'%(comment_url)))
        comments = self.soup.findAll('a',href=True , text='Reply to this comment')
        log.info(self.log_msg('no. of comments found = ' + str(len(comments))))
        for comment in comments:
            try:
                comment = comment.parent.parent
                try:
                    data = stripHtml(str(re.sub('.*?</a>\)?','',comment.renderContents())))
                except:
                    log.info(self.log_msg('comment data could not be extracted'))
                    data = ''
                    
                comment_identifier = data[:50] #use first 50 characters as comment identifier
                if not checkSessionInfo(self.genre, self.session_info_out, 
                                        comment_identifier, self.task.instance_data.get('update'),
                                        parent_list=parent_list):
                    page = {}
                    try:
                        title = stripHtml(re.sub(re.compile('\(<a href.*$',re.DOTALL),'',\
                                comment.renderContents()))
                    except:
                        log.info(self.log_msg('comment title could not be extracted'))
                        title = ''
                    try:
                        author_name = stripHtml(''.join(re.findall('by <a .*?</a>',\
                                        comment.renderContents())))
                        author_name = re.sub('^by  ','',author_name)
                    except:
                        log.info(self.log_msg('comment author_name could not be extracted'))
                        author_name = ''
                    try:    
                        hash_data = data + title + author_name + comment_url
                        comment_hash = md5.md5(hash_data.encode('utf-8','ignore')).hexdigest()
                    except:
                        log.info(self.log_msg("Exception occured while making hash for the comment"))

                    result=updateSessionInfo(self.genre, self.session_info_out, comment_identifier, comment_hash, 
                                             'Comment', self.task.instance_data.get('update'), 
                                             parent_list=parent_list)
                    if result['updated']:
                        try:
                            date_level = comment.parent.parent
                            posted_date = stripHtml(date_level.find('td',{'valign':'top' , 'align':'center'}).find('span',{'class':'d-r'}).renderContents())
                            posted_date = re.sub('\n' ,' ' , posted_date)
                            page['posted_date'] = datetime.strftime(datetime.strptime(posted_date.rsplit(' ', 1)[0] , "%b %d '%y %I:%M %p") , "%Y-%m-%dT%H:%M:%SZ")

                        except:
                            log.info(self.log_msg('comment posted_date could not be extracted'))
                            page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")

                        page['data'] = data
                        if author_name:
                            page['et_author_name'] = author_name
                        page['parent_path']=copy.copy(parent_list)
                        parent_list.append(comment_identifier)
                        page['path']=parent_list
                        page['title'] = title
                        page['priority']=self.task.priority
                        page['level']=self.task.level
                        page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                        page['last_updated_time'] = page['pickup_date']
                        page['connector_instance_log_id'] = self.task.connector_instance_log_id
                        page['connector_instance_id'] = self.task.connector_instance_id
                        page['workspace_id'] = self.task.workspace_id
                        page['client_id'] = self.task.client_id  # TODO: Get the client from the project 
                        page['client_name'] = self.task.client_name
                        page['versioned'] = False
                        page['uri'] = comment_url
                        page['uri_domain'] = urlparse.urlparse(comment_url)[1]
                        page['task_log_id']=self.task.id
                        page['entity'] = 'Comment'
                        page['category'] = self.task.instance_data.get('category' ,'')
                        self.pages.append(page)
                        log.debug(self.log_msg("added comment for the review: %s" %parent_list[1]))
            except:
                log.exception(self.log_msg('exception in add_comments'))
                
    @logit(log,'__getparentpage')
    def __getParentPage(self):
        """This will get the parentPageInfo
        """
        page = {}
        try:
            page['title']= page['data'] =stripHtml(self.soup.find('h1',{'class':'product_title'}).renderContents())
        except:
            log.exception(self.log_msg('could not parse post title'))
            page['title'] = page['data']=''
        try:
            rating_str = re.search('\d+$',self.soup.find('span',attrs={'class':re.compile('iReviewStarsMedYellow medStars\d+')})['class']).group()
            rating = float(rating_str)/10
            page['ef_product_rating_overall'] = rating
        except:
            log.exception(self.log_msg('coult not parse overall rating'))
        try:
            post_hash = get_hash(page)
            if not checkSessionInfo(self.genre, self.session_info_out, 
                                    self.parenturi, self.task.instance_data.get('update')):
                id=None
                if self.session_info_out=={}:
                    id=self.task.id
                result=updateSessionInfo(self.genre, self.session_info_out,self.parenturi, post_hash,
                                         'Post', self.task.instance_data.get('update'), Id=id)

                if result['updated']:
                    page['path']=[self.parenturi]
                    page['parent_path']=[]
                    page['uri'] = normalize(self.currenturi)
                    page['uri_domain'] = unicode(urlparse.urlparse(page['uri'])[1])
                    page['priority']=self.task.priority
                    page['level']=self.task.level
                    page['pickup_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['posted_date'] = datetime.strftime(datetime.utcnow(),"%Y-%m-%dT%H:%M:%SZ")
                    page['connector_instance_log_id'] = self.task.connector_instance_log_id
                    page['connector_instance_id'] = self.task.connector_instance_id
                    page['workspace_id'] = self.task.workspace_id
                    page['client_id'] = self.task.client_id  # TODO: Get the client from the project       
                    page['client_name'] = self.task.client_name
                    page['last_updated_time'] = page['pickup_date']
                    page['versioned'] = False
                    page['data'] = ''
                    page['task_log_id']=self.task.id
                    page['entity'] = 'Post'
                    page['category']=self.task.instance_data.get('category','')
                    self.pages.append(page)
                    
                    log.info(self.log_msg('Parent Page added'))
        except Exception,e:
            log.exception(self.log_msg("parent post couldn't be parsed"))
            
    @logit(log, "_setSoup")
    def __setSoup( self, url = None, data = None, headers = {} ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        if url:
            self.currenturi = url
        try:
            log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
            res = self._getHTML( data = data, headers=headers  )
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('Page not for  :%s'%url))
            raise e
#for reviews , epinions allows a permalink which is being used as a idetinfier for each of the reviews ,
#for comments , no permalinks are given due to which comments , are being identified using hash on (review data , author name , title etc)
