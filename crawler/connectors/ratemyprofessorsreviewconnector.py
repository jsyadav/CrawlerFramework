import re
import logging
from urlparse import urlparse
from datetime import datetime
import cgi
import copy
import traceback

from utils.utils import stripHtml,get_hash
from utils.decorators import logit
from utils.urlnorm import normalize
from baseconnector import BaseConnector
from utils.sessioninfomanager import checkSessionInfo, updateSessionInfo

log = logging.getLogger('RateMyProfessorReviewConnector')
class RateMyProfessorsReviewConnector(BaseConnector):
    '''
    Sample Uri : http://ratemyprofessors.com/SelectTeacher.jsp?the_dept=Business&orderby=TLName&letter=A&sid=1270
    '''
    @logit(log,'Fetch')
    def fetch(self):
        '''
        Fetch Data from self.currenturi
        '''
        try:
            baseuri = 'http://ratemyprofessors.com/'
            self.parenturi = self.currenturi
            #log.info(self.log_msg('URL :: '+str(self.currenturi)))
            self.genre = 'Review'
            self.__setSoup()
            # Sorted reverse alphabetically
            if urlparse(self.currenturi)[2].startswith('/SelectTeacher'):
                # For getting meta information about a professor
                log.info(self.log_msg("Trying to get information from: " + self.currenturi))
                reviews_by_alphabet = [baseuri + x['href'] for x in self.soup.find('div', {'class':'pagination'}).findAll("a")][::-1]
                get_meta = lambda soup, regex: soup.find("div", {"class":re.compile(regex)}).renderContents()
                while True:
                    if not self.__setSoup():
                        log.info(self.log_msg('Error while setting Soup'))
                        break
                    ratingTable = self.soup.find('div', {'id':'ratingTable'})
                    if ratingTable:
                        threads = ratingTable.findAll("div", {"class":re.compile("entry")})
                    else:
                        log.info(self.log_msg('No information found in Page. Site changed!'))
                        break
                    
                    for thread in threads:
                        #log.info(thread)
                        temp_task = self.task.clone()
                        try:
                            professor = thread.find("div", {"class":"profName"})
                            link = baseuri + professor.find('a')['href']
                            #print link
                            temp_task.instance_data['uri'] = normalize(link)
                            temp_task.pagedata['et_professor_name'] = stripHtml(professor.renderContents())
                        except:
                            log.exception(self.log_msg("Couldn't create sub task. Moving on to next review"))
                            continue
                        try:
                            # :D Hot?
                            is_hot = get_meta(thread, 'profHot')
                        except:
                            log.info(self.log_msg('Hotness not found'))
                            is_hot = ''
                        temp_task.pagedata['et_is_hot'] = is_hot

                        try:
                            department = get_meta(thread, 'profDept')
                        except:
                            log.info(self.log_msg('Department not found'))
                            department = ''
                        temp_task.pagedata['et_department'] = department

                        try:
                            prof_rating = get_meta(thread, 'profRatings')
                            temp_task.pagedata['ef_total_ratings'] = prof_rating
                        except:
                            log.info(self.log_msg('Total Rating not found'))
                            
                        try:
                            prof_avg = get_meta(thread, 'profAvg')
                            temp_task.pagedata['ef_rating_overall'] = prof_avg
                        except:
                            log.info(self.log_msg('Overall rating not found'))

                        try:
                            prof_easiness = get_meta(thread, 'profEasy')
                            temp_task.pagedata['ef_avg_rating_easiness'] = prof_easiness
                        except:
                            log.info(self.log_msg("Easiness not found"))
                                     
                        self.linksOut.append(temp_task)
                    if len(reviews_by_alphabet) > 0:
                        self.currenturi = reviews_by_alphabet.pop(0)
                return True
            
            elif urlparse(self.currenturi)[2].startswith('/ShowRatings'):
                log.info(self.log_msg ('Reviews  to be captured'))
                self.parenturi = self.currenturi
                print ">>>>>>>>>>>>>>>>>>>>", self.currenturi
                self.genre = "Review"
                if not self.__setSoup():
                    log.info(self.log_msg('Soup not set'))
                    return False
                self.__getParentPage()
                links = [baseuri + x['href'] for x in self.soup.find('div', {'class':'pagination'}).findAll("a")]
                self.__addReviews()
                for link in links:
                    log.info("Getting review for: " + link)
                    self.currenturi = link
                    if not self.__setSoup():
                        log.info(self.log_msg('Soup not set'))
                        break
                    self.__addReviews()
                return True
        except:
            log.exception(self.log_msg('Exception in Fetch: ' + self.currenturi))   
            return False

    @logit(log,'__getParentPage')
    def __getParentPage(self):
        '''
        Get All Parent Page Information
        '''
        #log.info('URL::'+self.currenturi)
        page = {}
        try:
            for field in ['et_professor_name', 'et_is_hot',
                          'et_department', 'et_total_ratings',
                          'ef_rating_overall', 'ef_avg_rating_easiness']:
                default_value = ''
                if field.startswith('ef'):
                    default_value = 0
                page[field] = self.task.pagedata.get(field, default_value)
            
            print ">>>>>>>", self.task.pagedata
            if checkSessionInfo( self.genre, self.session_info_out, self.parenturi,\
                                       self.task.instance_data.get( 'update' ) ):
                log.info(self.log_msg('Session Info Returns True for Parent Page'))
            post_hash = get_hash(page)
            id=None
            if self.session_info_out=={}:
                id=self.task.id
            result=updateSessionInfo( self.genre, self.session_info_out, \
                                            self.parenturi, post_hash,'Post', \
                                    self.task.instance_data.get('update'), Id=id)
            if not result['updated']:
                log.debug(self.log_msg("Parent page not stored"))
                return False
            page['path']=[self.parenturi]
            page['parent_path']=[]
            page['task_log_id'] = self.task.id
            page['versioned'] = self.task.instance_data.get('versioned',False)
            page['category'] = self.task.instance_data.get('category','generic')
            page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                    ,"%Y-%m-%dT%H:%M:%SZ")
            page['client_name'] = self.task.client_name
            page['entity'] = 'post'
            page['data'] = ''
            page['title'] = page['et_professor_name']
            page['uri'] = normalize(self.parenturi)
            page['uri_domain'] = urlparse(page['uri'])[1]
            page['priority'] = self.task.priority
            page['level'] = self.task.level
            page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
            page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
            page['connector_instance_log_id'] = self.task.connector_instance_log_id
            page['connector_instance_id'] = self.task.connector_instance_id
            page['workspace_id'] = self.task.workspace_id
            page['client_id'] = self.task.client_id
            self.pages.append( page )
            log.debug(self.log_msg("Parent page added"))
            return True
        except:
            log.exception(self.log_msg('Exception in GetParentPage'))
            return False

    @logit(log,'__addReviews')
    def __addReviews(self):
        '''
        Extracting Reviews from the page
        '''
        #log.info('URL::'+self.currenturi)
        reviews = self.soup.findAll('div', {'class':re.compile('entry (odd|even)')})
        if len(reviews) == 0:
            log.info(self.log_msg('No reviews Found on a page'))
            return True
        for review in reviews:
            page = {}
            unique_id = review.find("a")["name"]
            try:
                date_str = review.find("div", {"class":"date"}).renderContents()
                date = datetime.strftime(datetime.strptime(date_str,'%m/%d/%y'),'%Y-%m-%dT%H:%M:%SZ')
                page['posted_date'] = date
            except:
                log.exception(self.log_msg('posted date not found, set to current date time'))
                page['posted_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
            page['uri'] = self.currenturi
            try:
                comment_section = review.find("div", {"class":"comment"})
                data = stripHtml(comment_section.find("p", {"class":"commentText"}).renderContents())
                page['data'] = data
            except:
                log.info(self.log_msg('Page Data not found'))
                page['data'] = ' '
                
            if len(page['data']) >50:
                page['title'] = page['data'][:50]+'...'
            else:
                page['title'] = page['data']
                
            try:             
                page['et_class'] = stripHtml(review.find("div", {"class":"class"}).renderContents())
            except:
                log.info(self.log_msg('Class not found for: ' + unique_id))
                page['et_class'] = ''
                
            try:
                page['ei_easiness'] = int(review.find("p", {"class":re.compile("rEasy status")}).find("span").renderContents())
            except:
                log.info(self.log_msg('Easiness not found for: ' + unique_id))

            try:
                page['ei_helpfulness'] = int(review.find("p", {"class":re.compile("rHelpful status")}).find("span").renderContents())
            except:
                log.info(self.log_msg('Helpfulness not found for: ' + unique_id))
                
            try:
                page['ei_clarity'] = int(review.find("p", {"class":re.compile("rClarity status")}).find("span").renderContents())
            except:
                log.info(self.log_msg('Clarity info not found'))

            #log.info(info)
            try:
                page['ei_rater_interest'] = int(review.find("p", {"class":re.compile("rClarity status")}).find("span").renderContents())
            except:
                log.info(self.log_msg('Rater Interest not found'))
            
            try:
                page['et_quality'] = stripHtml(review.find("p", {"class":re.compile("Quality")}).renderContents())
            except:
                log.info(self.log_msg('Average quality not found'))
                
            try:
                #log.info(info)
                #uniq_key = info[7].find('a')['href'].split('=')[-1]
                uniq_key = get_hash( {'data':page['data'],'title':page['title']})
                review_hash = get_hash(page)

                if checkSessionInfo(self.genre, self.session_info_out, uniq_key, self.task.instance_data.get('update'),parent_list=[self.currenturi]):
                    continue
                id=None
                if self.session_info_out=={}:
                    id = self.task.id
                result = updateSessionInfo(self.genre, self.session_info_out, uniq_key, review_hash, 'Review', self.task.instance_data.get('update'),parent_list=[self.parenturi])
                if not result['updated']:
                    continue
                parent_list = [self.parenturi]
                page['parent_path'] = copy.copy(parent_list)
                parent_list.append( uniq_key )
                page['path']=parent_list
                page['task_log_id'] = self.task.id
                page['versioned'] = self.task.instance_data.get('versioned',False)
                page['category'] = self.task.instance_data.get('category','generic')
                page['last_updated_time'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                page['client_name'] = self.task.client_name
                page['entity'] = 'Review'
                page['uri'] = normalize( self.currenturi )
                page['uri_domain'] = urlparse(page['uri'])[1]
                page['priority'] = self.task.priority
                page['level'] = self.task.level
                page['pickup_date'] = datetime.strftime(datetime.utcnow()\
                                                        ,"%Y-%m-%dT%H:%M:%SZ")
                page['connector_instance_log_id'] = self.task.connector_instance_log_id
                page['connector_instance_id'] = self.task.connector_instance_id
                page['workspace_id'] = self.task.workspace_id
                page['client_id'] = self.task.client_id
                self.pages.append( page )
                log.info( self.log_msg("Review added") )
            except:
                log.exception( self.log_msg('Error with session info' ) )

    @logit(log, "setSoup")
    def __setSoup( self ):
        """
            It will set the uri to current page, written in seperate
            method so as to avoid code redundancy
        """
        try:
            log.info(self.log_msg( 'for uri %s'%(self.currenturi) ))
            res = self._getHTML()
            if res:
                self.rawpage = res[ 'result' ]
            else:
                log.info(self.log_msg('self.rawpage not set.... so Sorry..'))
                return False
            self._setCurrentPage()
            return True
        except Exception, e:
            log.exception(self.log_msg('Page not for  :%s'%self.currenturi))
            raise e
        
