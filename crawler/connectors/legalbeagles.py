[config]
connector_data = unicode(simplejson.dumps({'source':'.com','source_type':'forum','is_queryable':False,'capabilities':[{'name':'pick_comments','default_value':False},{'name':'update','default_value':False}]}))
protocol = u'http'
name = u'GetHumanConnector'
fields = unicode(simplejson.dumps([{'name':'data','type':'non-extracted'},{'name':'title','type':'non-extracted'},{'name':'uri','type':'non-extracted'},{'name':'posted_date','type':'non-extracted'},{'name':'et_author_name','type':'extracted'}]))
url_segment = u'gethuman.com'
#fetcher_data = unicode(simplejson.dumps({'url':['http://www.baliforum.com/travel/postlist.php?Cat=0&Board=UBB1&page=0']}))
#fetcher_data = unicode(simplejson.dumps({'url':['http://broncosfreaks.com/forums/showthread.php?t=25873']}))
#fetcher_data = unicode(simplejson.dumps({'url':['http://www.baliforum.com/travel/showflat.php?Cat=0&Number=315060&an=0&page=0']}))
#fetcher_data = unicode(simplejson.dumps({'url':['http://broncosfreaks.com/forums/forumdisplay.php?f=3&order=desc&page=0']}))
#fetcher_data = unicode(simplejson.dumps({'url':['http://broncosfreaks.com/forums/showthread.php?t=25921']}))
fetcher_data = unicode(simplejson.dumps({'url':['http://www.legalbeagles.info/forums/showthread.php?t=12583&page=2']}))
