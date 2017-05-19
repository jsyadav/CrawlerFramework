"""
Copyright (c)2008-2009 Serendio Software Private Limited
All Rights Reserved

This software is confidential and proprietary information of Serendio Software. It is disclosed pursuant to a non-disclosure agreement between the recipient and Serendio. This source code is provided for informational purposes only, and Serendio makes no warranties, either express or implied, in this. Information in this program, including URL and other Internet website references, is subject to change without notice. The entire risk of the use or the results of the use of this program remains with the user. Complying with all applicable copyright laws is the responsibility of the user. Without limiting the rights under copyright, no part of this program may be reproduced, stored in, or introduced into a retrieval system, or distributed or transmitted in any form or by any means (electronic, mechanical, photocopying, recording, on a website, or otherwise) or for any purpose, without the express written permission of Serendio Software.

Serendio may have patents, patent applications, trademarks, copyrights, or other intellectual property rights covering subject matter in this program. Except as expressly provided in any written license agreement from Serendio, the furnishing of this program does not give you any license to these patents, trademarks, copyrights, or other intellectual property.
"""
import pkg_resources
pkg_resources.require("SQLAlchemy>=0.3.10")

import uuid

from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.databases.postgres import PGArray
from sqlalchemy.databases.postgres import PGBinary
from datetime import datetime

from custom import Enum, UUID
from consistency import ALConsistency

metadata = MetaData()


def updateInfoAndMap(e, mapper):
    metadata.bind = e
    clear_mappers()
    mapper(MissedPost, missed_posts_table, extension=ALConsistency())

    mapper(CrawlerMetrics,crawler_metrics_table, extension=ALConsistency())
    mapper(ConnectorInstanceLog, connector_instance_log_table, extension=ALConsistency(),
           properties=dict(tasks=relation(TaskLog, backref='cinstance_log', passive_deletes=True)))#1:N

    #mapper(ConnectorInstanceLog, connector_instance_log_table,
    #       properties=dict(tasks=relation(TaskLog, backref='cinstance_log')))#1:N


    mapper(TaskLog, task_log_table, extension=ALConsistency(),
           properties=dict(related_uris=relation(RelatedURI, backref='tasklog', passive_deletes=True)))#1:N

    mapper(RelatedURI, related_uri_table, extension=ALConsistency())
    # custom mappers end



    # ** I couldn't find a way to interate over (Mapped Instance object : Table) association objects in a sqlalchemy session, using metadata module or anything else.
    #    This information is needed by ALConsistency for ensuring data consistency at application level. So whenever you declare a mapper object for some table, You will need to update the association information here too.

    # identity mappers begin
    mapper(Visit, visits_table, extension=ALConsistency())

    mapper(VisitIdentity, visit_identity_table, extension=ALConsistency(), 
           properties=dict(users=relation(User, backref='visit_identity')))

    mapper(User, users_table, extension=ALConsistency(),
           properties=dict(_password=users_table.c.password))

    mapper(Group, groups_table, extension=ALConsistency(),
           properties=dict(users=relation(User,secondary=user_group_table, backref='groups', passive_deletes=True),
                           workspace_users=relation(WorkspaceUser,backref='groups', passive_deletes=True)))

    mapper(Permission, permissions_table, extension=ALConsistency(),
           properties=dict(groups=relation(Group,
                           secondary=group_permission_table, backref='permissions')))
    # identity mappers end

    # custom mappers begin
    mapper(Client, client_table, extension=ALConsistency(),
           properties=dict(workspaces=relation(Workspace, backref='client', passive_deletes=True),#1:N
                           users=relation(User, backref='client', passive_deletes=True)))#1:N

    mapper(Workspace, workspace_table, extension=ALConsistency(),
           properties=dict(keywords=relation(Keyword, backref='workspace'),#1:N
                           connector_instances=relation(ConnectorInstance, backref='workspace'),#1:N
                           tags=relation(Tag, backref='workspace'),#1:N
                           posts=relation(Post, backref='workspace')#1:N
                           )
    )

    mapper(WorkspaceUser, workspace_user_table, extension=ALConsistency(),
           properties=dict(workspace=relation(Workspace, backref='workspace_user_group'),
                           user=relation(User, backref='workspace_user_group', passive_deletes=True),
                           group=relation(Group, backref='workspace_user_group', passive_deletes=True))
    )

    mapper(Connector, connector_table, extension=ALConsistency(),
           properties=dict(connector_instances=relation(ConnectorInstance, backref='connector')))#1:N

    mapper(ConnectorInstance, connector_instance_table, extension=ALConsistency(),
           properties=dict(cinstance_logs=relation(ConnectorInstanceLog, backref='cinstance'),#1:N
                           posts=relation(Post, backref='connector_instance', passive_deletes=True))
    )

    mapper(Keyword, keyword_table,extension=ALConsistency())
    mapper(Function, functions_table, extension=ALConsistency())
    mapper(Rule, rules_table,extension=ALConsistency())
    mapper(Language, languages_table ,extension=ALConsistency())

    mapper(Post, posts_table, extension=ALConsistency(),
           properties=dict(keywords=relation(Keyword, secondary=post_keyword_table, backref='posts', passive_deletes=True),#M:N
                           comments=relation(Comment, backref = 'posts', passive_deletes=True),
                           publish_post=relation(PublishedPost, backref = 'posts',uselist=False, passive_deletes=True),
                           extracted_entity_values=relation(ExtractedEntityValue, backref='posts', passive_deletes=True))
    )

    mapper(Tag, tags_table, extension=ALConsistency(),
           properties=dict(#keywords=relation(Keyword, secondary=folder_keyword_table, backref='folders'),#M:N
                           posts=relation(Post, secondary=post_tag_table, backref='tags'),#M:N
                           parent=relation(Tag, backref='children')))#1:N

    mapper(Comment, comments_table, extension=ALConsistency())
    mapper(PublishedPost, published_post_table , extension=ALConsistency())

    mapper(ExtractedEntityName, extracted_entity_names_table, extension=ALConsistency(),
           properties=dict(extracted_entity_values=relation(ExtractedEntityValue, backref='extracted_entity_name', passive_deletes=True)))

    mapper(ExtractedEntityValue, extracted_entity_values_table, extension=ALConsistency())

    mapper(ParentExtractedEntityName, parent_extracted_entity_names_table,extension=ALConsistency(),
           properties=dict(parent_extracted_entity_values=relation(ParentExtractedEntityValue, backref='extracted_entity_name', passive_deletes=True)))

    mapper(ParentExtractedEntityValue, parent_extracted_entity_values_table, extension=ALConsistency())

    mapper(AlertsIssued, alerts_issued_table, extension=ALConsistency())

    mapper(AlertDefinition, alert_definition_table, extension=ALConsistency(),
           properties=dict(alerts_issued=relation(AlertsIssued, backref='alert_definition', passive_deletes=True)))

    mapper(UserGroup, user_group_table, extension=ALConsistency())

    # custom mappers end


    mapper(Product, product_tbl, extension=ALConsistency(), properties={
        'category':relation(Workspace, backref='products'),
        'synonyms':relation(ProductSynonym, passive_deletes=True),
        'urls':relation(ProductUrl, passive_deletes=True),#J, backref='product'),
        }
    )


    mapper(ProductHierarchy, product_hierarchy_tbl, extension=ALConsistency())
    mapper(ProductSynonym, product_synonym_tbl, extension=ALConsistency())
    mapper(ProductUrl, product_url_tbl, extension=ALConsistency())
#J
#       properties={'connector_instance':
#                       relation(ConnectorInstance, backref='product_url')})

    mapper(Feature, feature_tbl, extension=ALConsistency(), properties={
        'category':relation(Workspace, backref='features'),
        'synonyms':relation(FeatureSynonym, passive_deletes=True),
        }
    )
    mapper(CategoryStats, category_stats_tbl, extension=ALConsistency())
    mapper(FeatureHierarchy, feature_hierarchy_tbl, extension=ALConsistency())
    mapper(PostSentiment, post_sentiments_tbl, extension=ALConsistency())
    mapper(PostData, post_data_tbl, extension=ALConsistency())
    mapper(PostScore, post_scores_tbl, extension=ALConsistency()) 
    mapper(Sentiment, sentiments_tbl, extension=ALConsistency())
    mapper(FeatureSynonym, feature_synonym_tbl, extension=ALConsistency())
    mapper(Word, words_tbl, extension=ALConsistency())
    mapper(DeletedPost, deleted_posts_table, extension=ALConsistency())
    mapper(SentimentMatView, sentiment_matview_tbl, extension=ALConsistency())

    mapper(Site, sites_tbl, extension=ALConsistency())
    mapper(Field, fields_tbl, extension=ALConsistency())
    mapper(FieldType, field_types_tbl, extension=ALConsistency())
    mapper(FilterTemplate, filter_template_tbl, extension=ALConsistency())

    mapper(KOLScore, kol_scores_table, extension=ALConsistency())
    mapper(Taxonomy, taxonomies_table, extension=ALConsistency())
    
        
def getNextUUID():
    return str(uuid.uuid4())

crawler_metrics_table = Table('crawler_metrics', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('connector_instance_id', UUID),
    Column('articles_crawled', Integer),
    Column('articles_added', Integer),
    Column('content_fetched', Integer),
    Column('created_date', DateTime, default=datetime.utcnow),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="logs"
)

bit_flag_values_table = Table('bit_flag_values', metadata,
    Column('position', Integer, primary_key=True),
    Column('value', Text),
    schema = 'common'
)
connector_instance_log_table = Table('connector_instance_logs', metadata,
    Column('id',UUID, primary_key=True,default=getNextUUID),
    Column('connector_instance_id', UUID, ForeignKey('common.connector_instances.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('start_time', DateTime, default=datetime.utcnow),
    Column('created_date', DateTime, default=datetime.utcnow),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="logs"
)

missed_posts_table = Table('missed_posts', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('post_id', Integer),
    Column('operation', Enum(['add', 'update', 'delete'])),#need an enum
    Column('operation_timestamp', DateTime, default=datetime.utcnow),                               
    Column('_tid', DateTime, default=datetime.utcnow),
    schema = 'logs'
)
task_log_table = Table('task_logs',metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),

    #slight denormalization  but this will avoid the join between the task_log, connector_instance_log, connector_instance - to get workspace_id
    Column('workspace_id', UUID, ForeignKey('common.workspaces.id', onupdate='CASCADE', ondelete='CASCADE')),

    Column('connector_instance_log_id', UUID, ForeignKey('logs.connector_instance_logs.id',
                                                            onupdate='CASCADE', ondelete='CASCADE')),

    Column('num_of_new_posts', Integer), #number of new posts in current task
    Column('session_info', Text), #refer - en-passant - in design.txt # has var called numposts- total number of posts
    Column('level', Integer),
    Column('uri', Text),
    Column('fetch_status', Boolean),
    Column('fetch_message', Unicode(255)),
    Column('filter_status', Boolean),
    Column('extract_status', Boolean),
    Column('misc_status', Unicode(255)),
    Column('enqueue_time', DateTime),
    Column('dequeue_time', DateTime),
    Column('completion_time', DateTime),
    Column('delete_status', Boolean, default=False),                 
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="logs"
)

related_uri_table = Table('related_uris', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('task_log_id', UUID, ForeignKey('logs.task_logs.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    Column('related_uri', Text),
    Column('created_date', DateTime, default=datetime.utcnow),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="logs"

)

class ConnectorInstanceLog(object):
    """
    This creates a log id for every individual crawl of a connector_instance
    """
    pass

class TaskLog(object):
    """
    This stores all the urls that have been crawled, keyed by workspace, connector_instance for a particular run
    """
    pass

class RelatedURI(object):
    """
    This stores all the related URI's from a task like
    pages 2,3,4....  or next or prev....
    refer - design.txt - 'redirect/multipage' section
    """
    pass

class MissedPost(object):
    """
    """
    pass

class CrawlerMetrics(object):
    """                                                                                                                                    
    Stores Crawler Metrics for connector_instances                                                                                         
    """
    pass

# custom classes end

# custom schema begins
client_table = Table('clients', metadata,
                     Column('id', UUID, primary_key=True,default=getNextUUID),
                     Column('name', Unicode(100), unique=True),
                     Column('contact', Unicode(255)),
                     Column('logo_url', Unicode(255)),
                     Column('source', Unicode(255)),
                     Column('created_date', DateTime, default=datetime.utcnow),
                     Column('updated_date', DateTime),
                     Column('active_status', Boolean, default=True),
                     Column('_tid', DateTime, default=datetime.utcnow),
                     schema="common"
)

workspace_table = Table('workspaces', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('name', Unicode(100), nullable=False),
    Column('description', Unicode(400)),
    Column('client_id', UUID, ForeignKey('common.clients.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('active_status', Boolean, default=True),
    Column('delete_status', Boolean, default=False),
    Column('created_date', DateTime, default=datetime.utcnow),
    Column('updated_date', DateTime),
    Column('delivery_type', Unicode(5)),
    Column('type', Unicode(5)),
    Column('metadata',Text) ,
    Column('dashboard_string', Text),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="common"
)

workspace_user_table= Table('workspace_user', metadata, 
    Column('workspace_user_id', UUID, primary_key=True,default=getNextUUID),
    Column('workspace_id', UUID, ForeignKey('common.workspaces.id',onupdate='CASCADE', ondelete='CASCADE')),
    Column('user_id', UUID, ForeignKey('common.tg_user.user_id',onupdate='CASCADE', ondelete='CASCADE')),
    Column('group_id', UUID, ForeignKey('common.tg_group.group_id',onupdate='CASCADE', ondelete='CASCADE')),
    Column('settings', Text),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="common"
)

connector_table  = Table('connectors',metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('url_segment', Unicode(255), unique = True),
    Column('name', Unicode(255), nullable=False), 
    Column('protocol', Unicode(10)),
    Column('fields',Text),

    Column('connector_data',Text),
    Column('created_date', DateTime, default=datetime.utcnow),
    Column('updated_date', DateTime),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="common"

    #type/category - news/blog... - to be used in save article
    #versioned(bool) may depend on the catefory
)

connector_instance_table = Table('connector_instances',metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('connector_id', UUID, ForeignKey('common.connectors.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('workspace_id', UUID, ForeignKey('common.workspaces.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('name', Unicode(300), nullable=False),
    Column('display_fields', Text),
    Column('frequency', Integer, default=86400),

    # uri + query_term + user_name + pass + apply_keywords + metapage
    Column('instance_data',Text),
    Column('active_status', Boolean, default=True),
    Column('delete_status', Boolean, default=False),
    Column('created_date', DateTime, default=datetime.utcnow),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="common"
    #DONT ALLOW EDIT OF CONNECTOR INSTANCE
)

keyword_table = Table('keywords', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('workspace_id', UUID, ForeignKey('common.workspaces.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('keyword',Text, nullable=False),
    Column('highlight', Boolean, default=True),
    Column('filter1', Boolean, default=True),
    Column('exclude1', Boolean, default=True),
    Column('active_status', Boolean, default=True),
    Column('delete_status', Boolean, default=False),
    Column('created_date', DateTime, default=datetime.utcnow),
    Column('updated_date', DateTime),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="common"
)

tags_table = Table('tags', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('workspace_id', UUID, ForeignKey('common.workspaces.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('parent_tag_id', UUID, ForeignKey('common.tags.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('name',Unicode(30),nullable=False),
    Column('persistent', Boolean, default=False),
    Column('assign_all', Boolean, default=False),                     
    Column('active_status', Boolean, default=True),
    Column('delete_status', Boolean, default=False),
    Column('created_date', DateTime, default=datetime.utcnow),
    Column('updated_date', DateTime),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="common"
)

functions_table = Table('functions', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('name', Unicode),
    schema = 'common'                 
)

rules_table = Table('rules', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('workspace_id', UUID, ForeignKey('common.workspaces.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('operation_id', UUID, ForeignKey('common.functions.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('operation_param', Unicode),
    Column('action_id', UUID, ForeignKey('common.functions.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('action_param', Unicode),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema = 'common'                    
)

languages_table = Table('languages',metadata,
                        Column('id', UUID, primary_key=True,default=getNextUUID),
                        Column('language', Unicode),
                        Column('_tid', DateTime, default=datetime.utcnow),
                        schema='common'
                        )

# Solr to pg
posts_table = Table('posts', metadata,
                    Column('id', UUID, primary_key=True,default=getNextUUID),
                    Column('connector_instance_id', UUID, ForeignKey('common.connector_instances.id', onupdate='CASCADE', ondelete='CASCADE')),
                    Column('workspace_id', UUID, ForeignKey('common.workspaces.id', onupdate='CASCADE', ondelete='CASCADE')),
                    Column('previous_version_id', UUID, ForeignKey('common.posts.id')),
                    Column('level', Integer),
                    Column('posted_date', DateTime, default=datetime.utcnow),
                    Column('pickup_date', DateTime, default=datetime.utcnow),
                    Column('last_updated_time', DateTime, default=datetime.utcnow),
                    Column('version_number', Integer),
                    Column('title', Text),
                    Column('data', Text),
                    Column('uri', Text),
                    Column('path', Text),
                    Column('uuid', UUID, default=getNextUUID),
                    Column('parent_path', Text),
                    Column('referenced_posts', Text),
                    Column('source', Text),#Enum(['reviews','forums', 'news', 'blogs', 'others'])),#Text),#need an enum
                    Column('source_type', Enum(['review','forum', 'news', 'blog', 'others','search','rss','email', 'cafe','microblog','internal','QnA','video'])),#Text),#need an enum
                    Column('entity', Enum(['post', 'html page', 'review', 'thread','comment','question','answer', 'reply', 'blog',
                                           'microblog', 'news', 'video'])),#Text), #need an enum
                    Column('active_status', Boolean, default=True),
                    Column('delete_status', Boolean, default=False),
                    Column('orig_data', Binary),
                    Column('language_id',Integer ,ForeignKey('common.languages.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
                    Column('is_latest',Boolean),
                    Column('translated_title', Text),
                    Column('translated_data', Unicode),
                    Column('relevancy', Float),
                    Column('_tid', DateTime, default=datetime.utcnow),
                    schema = 'common'
                    )

post_tag_table = Table('post_tag', metadata,
    Column('post_id', UUID, ForeignKey('common.posts.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    Column('tag_id', UUID, ForeignKey('common.tags.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    schema = 'common'
)

post_keyword_table = Table('post_keyword', metadata,
    Column('post_id', UUID, ForeignKey('common.posts.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    Column('keyword_id', UUID, ForeignKey('common.keywords.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    schema = 'common'
)

comments_table = Table('comments', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('post_id', UUID, ForeignKey('common.posts.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    Column('comment', Text),
    Column('user_id', Integer , ForeignKey('common.tg_user.user_id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    Column('created_date', DateTime, default=datetime.utcnow),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema = 'common'                       
)

extracted_entity_names_table = Table('extracted_entity_names', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('name', Text),
    Column('data_type', Enum(['text', 'date', 'integer', 'float', 'numeric'])),#Text),#need an enum
    Column('_tid', DateTime, default=datetime.utcnow),
    schema = 'common'
)

extracted_entity_values_table = Table('extracted_entity_values', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('extracted_entity_name_id', UUID, ForeignKey('common.extracted_entity_names.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    Column('post_id', UUID, ForeignKey('common.posts.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    Column('pickup_date', DateTime, default=datetime.utcnow),
    Column('value', Text),
    Column('mentions', Integer),                                      
    Column('_tid', DateTime, default=datetime.utcnow),
    schema = 'common'
)

parent_extracted_entity_names_table = Table('parent_extracted_entity_names', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('name', Text),
    Column('data_type', Enum(['text', 'date', 'integer', 'float', 'numeric'])),#Text),#need an enum
    Column('_tid', DateTime, default=datetime.utcnow),
    schema = 'common'
)

parent_extracted_entity_values_table = Table('parent_extracted_entity_values', metadata,
    Column('id', UUID, primary_key=True,default=getNextUUID),
    Column('extracted_entity_name_id', UUID, ForeignKey('common.parent_extracted_entity_names.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    Column('connector_instance_id', UUID, ForeignKey('common.connector_instances.id', onupdate = 'CASCADE', ondelete = 'CASCADE')),
    Column('pickup_date', DateTime, default=datetime.utcnow),
    Column('value', Text),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema = 'common'
)
# Solr to pg

# custom schema ends

# identity schema begins
visits_table = Table('visit', metadata,
    Column('visit_key', String(40), primary_key=True),
    Column('created', DateTime, nullable=False, default=datetime.utcnow),
    Column('expiry', DateTime),
    schema="logs"
)

visit_identity_table = Table('visit_identity', metadata,
    Column('visit_key', String(40), primary_key=True),
    Column('user_id', UUID, ForeignKey('common.tg_user.user_id'), index=True),
    schema="logs"
)

groups_table = Table('tg_group', metadata,
    Column('group_id', UUID, primary_key=True,default=getNextUUID),
    Column('group_name', Unicode(16), unique=True),
    Column('display_name', Unicode(255)),
    Column('created', DateTime, default=datetime.utcnow),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="common"
)

users_table = Table('tg_user', metadata,
    Column('user_id', UUID, primary_key=True,default=getNextUUID),
    Column('user_name', Unicode(100)),
    Column('email_address', Unicode(255), unique=True),
    Column('display_name', Unicode(255)),
    Column('password', Unicode(40)),
    Column('department', Unicode(255)),
    Column('telephone', Unicode(50)),
    Column('external_id', Integer),
    Column('active_status', Boolean, default=True),
    Column('delete_status', Boolean, default=False),
    Column('client_id', UUID, ForeignKey('common.clients.id', onupdate='CASCADE', ondelete='CASCADE')),
    Column('created', DateTime, default=datetime.utcnow),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="common"
)

permissions_table = Table('permission', metadata,
    Column('permission_id', UUID, primary_key=True,default=getNextUUID),
    Column('permission_name', Unicode(16), unique=True),
    Column('description', Unicode(255)),
    Column('_tid', DateTime, default=datetime.utcnow),
    schema="common"
)

user_group_table = Table('user_group', metadata,
    Column('user_id', UUID, ForeignKey('common.tg_user.user_id',
        onupdate='CASCADE', ondelete='CASCADE'), primary_key=True),
    Column('group_id', UUID, ForeignKey('common.tg_group.group_id',
        onupdate='CASCADE', ondelete='CASCADE'), primary_key=True),
    schema="common"
)

group_permission_table = Table('group_permission', metadata,
    Column('group_id', UUID, ForeignKey('common.tg_group.group_id',
        onupdate='CASCADE', ondelete='CASCADE')),
    Column('permission_id', UUID, ForeignKey('common.permission.permission_id',
        onupdate='CASCADE', ondelete='CASCADE')),
    schema="common"
)

alert_definition_table = Table('alert_definition',metadata,
                                Column('id', UUID, primary_key=True,default=getNextUUID),
                                Column('workspace_id',UUID,ForeignKey('common.workspaces.id',
                                                                         onupdate='CASCADE', ondelete='CASCADE'),unique=True),
                                Column('user_id', UUID, ForeignKey('common.tg_user.user_id',
                                                                      onupdate='CASCADE', ondelete='CASCADE'),unique=True),
                                Column('alert_name', Unicode(255)),
                                Column('alert_field', Unicode(255)),
                                Column('count_type', Unicode(255)),
                                Column('post_count_type', Unicode(255)),
                                Column('threshold', Integer),
                                Column('filters', Text),
                               Column('last_alert_post_id', DateTime),
                               Column('count',Integer),
                               Column('email_addrs', Text),
                               Column('active_status', Boolean,default=True),
                               Column('delete_status', Boolean,default=False),
                               Column('last_issued_alert_date', DateTime),
                               Column('frequency', Integer),
                               Column('frequency_check', Integer),
                               Column('emails_send_interval_filter',Text,default='{}'),
                               Column('_tid', DateTime, default=datetime.utcnow),
                               schema="common"
                               )

alerts_issued_table = Table('alerts_issued',metadata,
                            Column('id', UUID, primary_key=True,default=getNextUUID),
                            Column('alert_definition_id', Integer ,ForeignKey('common.alert_definition.id',
                                                                     onupdate='CASCADE', ondelete='CASCADE'),unique=True),
                            Column('alert_date', DateTime, default=datetime.utcnow),
                            Column('last_alert_post_id', DateTime),
                            Column('count',Integer),
                            Column('_tid', DateTime, default=datetime.utcnow),
                            schema="common")


published_post_table = Table('published_post',metadata,
                             Column('id', UUID, primary_key=True,default=getNextUUID),
                             Column('post_id',Integer,ForeignKey('common.posts.id',
                                                                 onupdate='CASCADE', ondelete='CASCADE'),unique=True),
                             Column('publish_date', DateTime, default=datetime.utcnow),
                             Column('_tid', DateTime, default=datetime.utcnow),
                             schema="common")

# identity schema ends

# custom classes begin
class Client(object):
    """
    Stores client specific information like contact, logo etc
    """
    pass


class Workspace(object):
    """
    Relationship stores workspace information. Workspace is the top level
    segeration of information. Workspace names should be unique within a client.
    """
    pass

class Connector(object):
    """
    Relationship to store connector information. fields store
    the list of fields that will be extracted by this connector.
    instancedata has the list of urls, concept filter, username, password
    and all other fields that are specified per instance of this connector.
    """
    pass

class ConnectorInstance(object):
    """
    This defines a particular instance of a connector in a workspace. It stores
    all the instance data required for that connector to acquire data.
    """
    def by_workspace(cls, workspaceid):
        """
        A class method that can be used to get the list of connectors
        defined in a workspace based on the workspace id
        """
        return list(cls.query.filter_by(workspace_id=workspaceid))
    by_workspace = classmethod(by_workspace)


class Keyword(object):
    """
    This stores the keywords for a particular workspace for filtering
    """
    pass

class Function(object):
    """
    """
    pass

class Rule(object):
    """
    """
    pass

class Language(object):
    """
    specifies different languages installed
    """
    pass


class Post(object):
    """
    """
    pass

class Tag(object):
    """
    """
    pass

class Comment(object):
    """
    """
    pass

class ExtractedEntityName(object):
    """
    """
    pass

class ExtractedEntityValue(object):
    """
    """
    pass

class ParentExtractedEntityName(object):
    """
    """
    pass

class ParentExtractedEntityValue(object):
    """
    """
    pass

class AlertsIssued(object):
    '''
    A class having information on issued alerts
    '''
    pass

class AlertDefinition(object):
    '''
    A class having Alerts definition
    '''
    pass
#
# custom classes end

# identity classes begin
class Visit(object):
    """
    A visit to your site
    """
    def lookup_visit(cls, visit_key):
        return cls.query.get(visit_key)
    lookup_visit = classmethod(lookup_visit)


class VisitIdentity(object):
    """
    A Visit that is link to a User object
    """
    pass


class Group(object):
    """
    An ultra-simple group definition.
    """
    def by_group_name(cls, group_name):
        """Class method to search for groups based on names
        """
        return cls.query.filter_by(group_name=group_name).first()
    by_name = classmethod(by_group_name)


class User(object):
    """
    Reasonably basic User definition.
    Probably would want additional attributes.
    """

    def permissions(self):
        p = set()
        for g in self.groups:
            p |= set(g.permissions)
        return p
    permissions = property(permissions)

    def by_email_address(cls, email):
        """
        A class method that can be used to search users
        based on their email addresses since it is unique.
        """
        return cls.query.filter_by(email_address=email).first()
    by_email_address = classmethod(by_email_address)

    def by_user_name(cls, username):
        """
        A class method that permits to search users
        based on their user_name attribute.
        """
        return cls.query.filter_by(user_name=username).first()
    by_user_name = classmethod(by_user_name)

    def _set_password(self, password):
        """
        encrypts password on the fly using the encryption
        algo defined in the configuration
        """
        self._password = password

    def _get_password(self):
        """
        returns password
        """
        return self._password

    password = property(_get_password, _set_password)


class Permission(object):
    """
    A relationship that determines what each Group can do
    """
    pass

class UserGroup(object):
    """
    """
    pass
# identity classes end

class WorkspaceUser(object):
    """
    """
    pass 

class PublishedPost(object):
    """
    """
    pass

class Base(object):
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            setattr(self, key, value)
            
    def __repr__(self):
        cls = self.__class__
        return "%s(%s)" % (
            cls.__name__,
            ", ".join(["%s=%r" % (c, getattr(self, c)) for c in cls.c.keys()])
        )

# category_tbl = Table('categories', metadata,
#                      Column('id', UUID, primary_key=True,default=getNextUUID),
#                      Column('name', Unicode(100)),
#                      Column('parent_id', Integer), # While creating root category, set parent_id to null
# )

# class Category(Base):
#     pass

# category_hierarchy_tbl = Table('category_hierarchies', metadata,
#                                Column('id', UUID, primary_key=True,default=getNextUUID),
#                                Column('category_id', UUID, ForeignKey('categories.id', onupdate='CASCADE', ondelete='CASCADE')),
#                                Column('ancestor_id', UUID, ForeignKey('categories.id', onupdate='CASCADE', ondelete='CASCADE')),
# )

# class CategoryHierarchy(Base):
#     pass

product_tbl = Table('products', metadata,
                    Column('id', UUID, primary_key=True,default=getNextUUID),
                    Column('name', Unicode(256)),
                    Column('parent_id', Integer), # While creating root category, set parent_id to null
#                    Column('category_id', UUID, ForeignKey('categories.id', onupdate='CASCADE', ondelete='CASCADE')),
                    Column('category_id', UUID, ForeignKey('common.workspaces.id', onupdate='CASCADE', ondelete='CASCADE')),
                    #Column('products_array', PGArray(int)),  # Having this fails
                    Column('_tid', DateTime, default=datetime.utcnow),
                    schema="voom"
)

class Product(Base):
    pass

product_hierarchy_tbl = Table('product_hierarchies', metadata,
                              Column('id', UUID, primary_key=True,default=getNextUUID),
                              Column('product_id', UUID, ForeignKey('voom.products.id', onupdate='CASCADE', ondelete='CASCADE')),
                              Column('ancestor_id', UUID, ForeignKey('voom.products.id', onupdate='CASCADE', ondelete='CASCADE')),
                              Column('_tid', DateTime, default=datetime.utcnow),
                              schema="voom"
)

class ProductHierarchy(Base):
    pass

product_synonym_tbl=Table('product_synonyms', metadata,
                          Column('id',UUID, primary_key=True,default=getNextUUID),
                          Column('product_id', UUID, ForeignKey('voom.products.id', onupdate='CASCADE', ondelete='CASCADE')),
                          Column('name', Unicode(256)),
                          Column('_tid', DateTime, default=datetime.utcnow),
                          schema="voom"
)

class ProductSynonym(Base):
    pass

product_url_tbl=Table('product_urls', metadata,
                      Column('id',UUID, primary_key=True,default=getNextUUID),
                      Column('product_id', UUID, ForeignKey('voom.products.id', onupdate='CASCADE', ondelete='CASCADE')),
                      Column('url', Text),
                      Column('connector_instance_id', Integer, 
                             ForeignKey('common.connector_instances.id', 
                                        onupdate='CASCADE', ondelete='CASCADE')),
                      Column('_tid', DateTime, default=datetime.utcnow),
                      schema="voom"
)

class ProductUrl(Base):
    pass


feature_tbl = Table('features', metadata,
                    Column('id', UUID, primary_key=True,default=getNextUUID),
                    Column('name', Unicode(256)),
                    Column('parent_id', Integer), # While creating root category, set parent_id to null
                    Column('dimension_polarity', Boolean, default=True),
                    Column('weight', Float, default=1.0),
                    Column('category_id', UUID, ForeignKey('common.workspaces.id', onupdate='CASCADE', ondelete='CASCADE')),
                    #Column('features_array', PGArray(int)),  # Having this fails
                    Column('ignore_feature_flag', Boolean, default= True),
                    Column('_tid', DateTime, default=datetime.utcnow),
                    schema="voom"

#                    Column('category_id', UUID, ForeignKey('categories.id', onupdate='CASCADE', ondelete='CASCADE')),
)

class Feature(Base):
    pass

feature_hierarchy_tbl = Table('feature_hierarchies', metadata,
                              Column('id', UUID, primary_key=True,default=getNextUUID),
                              Column('feature_id', UUID, ForeignKey('voom.features.id', onupdate='CASCADE', ondelete='CASCADE')),
                              Column('ancestor_id', UUID, ForeignKey('voom.features.id', onupdate='CASCADE', ondelete='CASCADE')),
                              Column('_tid', DateTime, default=datetime.utcnow),
                              schema="voom"
)

class FeatureHierarchy(Base):
    pass


post_sentiments_tbl=Table('post_sentiments', metadata,
                          Column('id', UUID, primary_key=True,default=getNextUUID),
                          Column('post_id', UUID, ForeignKey('common.posts.id', onupdate='CASCADE', ondelete='CASCADE')),
                          Column('product_id', UUID, ForeignKey('voom.products.id', onupdate='CASCADE', ondelete='CASCADE')),
                          Column('feature_id', UUID, ForeignKey('voom.features.id', onupdate='CASCADE', ondelete='CASCADE')),
#                          Column('sentiment', Integer),
                          Column('snippet', Text),
                          Column('sentiment_id', UUID, ForeignKey('voom.sentiments.id', onupdate='CASCADE', ondelete='CASCADE')),
                          Column('positive_prob', Float),
                          Column('negative_prob', Float),
                          Column('bit_array',Text),
                          Column('negative_words',Text),
                          Column('positive_words',Text),
                          Column('curated', Boolean, default=False),
                          Column('_tid', DateTime, default=datetime.utcnow),
                          schema="voom"
)

class PostSentiment(Base):
    pass

post_data_tbl = Table('post_data', metadata,
                      Column('id', UUID, primary_key=True,default=getNextUUID),
                      Column('post_id', UUID, ForeignKey('common.posts.id', onupdate='CASCADE', ondelete='CASCADE')),
                      Column('new_content', Text),
                      Column('rank', Float, default=0),
                      Column('product_id', UUID, ForeignKey('voom.products.id', onupdate='CASCADE', ondelete='CASCADE')),
                      Column('_tid', DateTime, default=datetime.utcnow),
                      schema="voom"
                      )

class PostData(Base):
    pass
                      
post_scores_tbl=Table('post_scores', metadata,
                      Column('id', UUID, primary_key=True,default=getNextUUID),
                      Column('post_id', UUID, ForeignKey('common.posts.id', onupdate='CASCADE', ondelete='CASCADE')),
                      Column('product_id', UUID, ForeignKey('voom.products.id', onupdate='CASCADE', ondelete='CASCADE')),
                      Column('unweighted_score', Float),
                      Column('weighted_score', Float),
                      Column('_tid', DateTime, default=datetime.utcnow),
                      schema="voom"
)

class PostScore(Base):
    pass

sentiments_tbl = Table('sentiments', metadata,
                       Column('id', UUID, primary_key=True,default=getNextUUID),
                       Column('name', Unicode(20)),
                       Column('_tid', DateTime, default=datetime.utcnow),
                       schema="voom"
)

class Sentiment(Base):
    pass

feature_synonym_tbl=Table('feature_synonyms', metadata,
                          Column('id',UUID, primary_key=True,default=getNextUUID),
                          Column('feature_id', UUID, ForeignKey('voom.features.id', onupdate='CASCADE', ondelete='CASCADE')),
                          Column('name', Unicode(256)),
                          Column('_tid', DateTime, default=datetime.utcnow),
                          schema="voom"
)

class FeatureSynonym(Base):
    pass

words_tbl = Table('words', metadata,
                  Column('id', UUID, primary_key=True,default=getNextUUID),
                  Column('word', Unicode(300)),
                  Column('category_id', UUID, ForeignKey('common.workspaces.id', onupdate='CASCADE', ondelete='CASCADE')),
#                  Column('category_id', UUID, ForeignKey('categories.id', onupdate='CASCADE', ondelete='CASCADE')),
                  Column('sentiment_id', UUID, ForeignKey('voom.sentiments.id', onupdate='CASCADE', ondelete='CASCADE')),
                  Column('type', Unicode(20)), # 'sentimentword' or 'dimensionword'
                  Column('language_id', UUID, ForeignKey('common.languages.id', onupdate='CASCADE', ondelete='CASCADE')),
                  Column('_tid', DateTime, default=datetime.utcnow),
                  schema="voom"
)

#----
taxonomies_table = Table('taxonomies', metadata,
                       Column('id', UUID, primary_key=True, default=getNextUUID),
                       Column('name', Unicode(256)),
                       Column('product_info', Text),
                       Column('feature_info', Text),
                       Column('words_info', Text),
                       Column('description', Text),
                       Column('delete_status', Boolean, default=False),
                       Column('last_updated_time', DateTime, default=datetime.utcnow),
                       Column('created_time', DateTime, default=datetime.utcnow),
                       schema="voom"
                       )
                       
class Taxonomy(Base):
    """Predefined taxonomies
    """
    def get_taxonomy(cls, name):
        return cls.query.filter_by(taxonomy_name=name).first()
    get_taxonomy = classmethod(get_taxonomy)
#----
    
class Word(Base):
    pass

deleted_posts_table = Table('deleted_posts', metadata,
                    Column('id', UUID, primary_key=True, default=getNextUUID),
                    Column('workspace_id', UUID),
                    Column('post_id', UUID),
                    Column('deletion_time', DateTime, default=datetime.utcnow),
                    schema="logs"
                    )
class DeletedPost(Base):
    pass

sentiment_matview_tbl = Table('sentiment_matview', metadata,
                              Column('id', UUID, primary_key=True,default=getNextUUID),
                              Column('post_id', UUID),
                              Column('category_id',UUID),
                              Column('title', Text),
                              Column('rank', Float),
                              Column('source', Text),
                              Column('date', DateTime),
                              Column('orig_content',Text),
                              Column('type', Text),
                              Column('metadata', Text),
                              Column('post_url', Text),
                              Column('snippet', Text),
                              Column('post_sentiment_id', UUID),
                              Column('sentiment_id', UUID),
                              Column('product_id', UUID),
                              Column('feature_id',UUID),
                              Column('product_name',Text),
                              Column('feature_name',Text),
                              Column('products_array', PGArray(int)),
                              Column('features_array', PGArray(int)),                              
                              Column('feature_weight', Float),
                              Column('negative_words',PGArray(Text)),
                              Column('positive_words',PGArray(Text)),
                              Column('curated',Boolean, default=False),
                              Column('weighted_sentiment', Float),
                              Column('unweighted_sentiment', Float),
                              Column('weighted_score', Float),
                              Column('unweighted_score', Float),
                              Column('_tid', DateTime, default=datetime.utcnow),
                              schema='voom', 
                              )
class SentimentMatView(Base):
    pass

# Voom UI config tables

sites_tbl = Table('sites', metadata,
                  Column('id', UUID, primary_key=True,default=getNextUUID),
                  Column('url_key', Unicode(300)),
                  Column('config', Unicode),
                  Column('_tid', DateTime, default=datetime.utcnow),
                  schema='voomui')

class Site(Base):
    pass

fields_tbl = Table('fields', metadata,
                   Column('id', UUID, primary_key=True,default=getNextUUID),
                   Column('field_name', Unicode(300)),
                   Column('label_name', Unicode(300)),
                   Column('site_id', ForeignKey('voomui.sites.id',
                                                onupdate='CASCADE',
                                                ondelete='CASCADE')),
                   Column('_tid', DateTime, default=datetime.utcnow),
                   schema='voomui'
                   )

class Field(Base):
    pass

field_types_tbl = Table('field_types', metadata,
                        Column('id', UUID, primary_key=True,default=getNextUUID),
                        Column('field_id', ForeignKey('voomui.fields.id',
                                                      onupdate='CASCADE',
                                                      ondelete='CASCADE')),
                        Column('type', Unicode(10)),
                        Column('_tid', DateTime, default=datetime.utcnow),
                        schema='voomui'
                        )

class FieldType(Base):
    pass

filter_template_tbl = Table('filter_templates', metadata,
                             Column('id', UUID, primary_key=True,default=getNextUUID),
                             Column('workspace_id', ForeignKey('common.workspaces.id',
                                                               onupdate='cascade',
                                                               ondelete='cascade')),
                             Column('user_id', ForeignKey('common.tg_user.user_id',
                                                           onupdate='cascade',
                                                           ondelete='cascade')),
                             Column('name', Unicode(300)),
                             Column('type', Unicode(300)),
                             Column('string', Unicode()),
                             Column('created_date', DateTime, default=datetime.utcnow),
                             Column('_tid', DateTime, default=datetime.utcnow),
                             schema='voomui')

class FilterTemplate(Base):
    pass

category_stats_tbl = Table('category_stats', metadata,
                           Column('id', UUID, primary_key=True,default=getNextUUID),
                           Column('category_id', ForeignKey('common.workspaces.id',
                                                            onupdate='cascade',
                                                            ondelete='cascade')),
                           Column('date', Date()),
                           Column('product_id', ForeignKey('voom.products.id',
                                                           onupdate='cascade',
                                                           ondelete='cascade')),
                           Column('product_name', Unicode(256)),
                           Column('product_url_id', ForeignKey('voom.product_urls.id',
                                                               onupdate='cascade',
                                                               ondelete='cascade')),
                           Column('connector_instance_id', ForeignKey('common.connector_instances.id',
                                                                      onupdate='cascade',
                                                                      ondelete='cascade')),
                           Column('source', Unicode(100)),
                           Column('source_type', Unicode(100)),
                           Column('post_count', Integer, default=0),
                           Column('post_sentiments_count', Integer, default=0),
                           Column('comments', Text),
                           Column('_tid', DateTime, default=datetime.utcnow),
                           schema='voom')

class CategoryStats(Base):
    pass


"""
Sentiment View - changed

CREATE OR REPLACE VIEW voom.sentiment_view as 
SELECT q.post_id, q.category_id, q.title, q.source, q.date, q.type, q.post_url, q.rank, q.post_sentiment_id, q.sentiment_id, q.product_id, q.feature_id, q.curated, q.snippet, q.product_name, q.feature_name, q.products_array, q.features_array, q.feature_weight, q.unweighted_sentiment, q.weighted_sentiment, pscr.weighted_score AS weighted_post_score, pscr.unweighted_score AS unweighted_post_score
   FROM ( SELECT p.id AS post_id, p.workspace_id AS category_id, p.title, p.source, p.posted_date AS date, p.source_type AS type, p.uri AS post_url, pdata.rank, ps.id AS post_sentiment_id, ps.sentiment_id, ps.product_id, ps.feature_id, ps.curated, ps.snippet, pr.name AS product_name, f.name AS feature_name, pr.products_array, f.features_array, f.weight AS feature_weight, 
                CASE
                    WHEN ps.sentiment_id = 1 THEN 1.0
                    WHEN ps.sentiment_id = 2 THEN (-1.0)
                    ELSE 0::numeric
                END AS unweighted_sentiment, 
                CASE
                    WHEN ps.sentiment_id = 1 THEN f.weight::double precision
                    WHEN ps.sentiment_id = 2 THEN (-1)::double precision * f.weight
                    ELSE 0::double precision
                END AS weighted_sentiment
           FROM posts p, post_sentiments ps, products pr, features f, post_data pdata
          WHERE p.id = ps.post_id AND ps.product_id = pr.id AND ps.feature_id = f.id AND p.id = pdata.post_id AND p.delete_status = false) q
   LEFT JOIN post_scores pscr ON pscr.product_id = q.product_id AND pscr.post_id = q.post_id;
   
"""



'''
Some definitions : 
KOl for any user is specific to source (eg. twitter) + workspace/topic (cellphones)
Hence this can be broken down to two components :
1) One component which is independent of topic
2) Another component which is specific to user and the topic/workspace I am interested to know his influence in.

hence there are 2 sets of tables for each source namely :
user info(twitter_users_table),workspace/topic info(twitter_user_data_table)

and One table that stores overall KOL information

Update(16/11/2010): Moving to a much simpler model, as we are not storing KOL returned by Klout as it is. And only keeping one table 
                    to keep scores.
'''


kol_scores_table = Table('kol_scores', metadata,
                         Column('source',Text),
                         Column('author_name', Text), #Can be the user_id/screen_name/email_address. 
                         Column('workspace_id', UUID, ForeignKey('common.workspaces.id', 
                                                                 onupdate='CASCADE', 
                                                                 ondelete='CASCADE')),
                         Column('author_real_name', Text),
                         Column('api_source', Text),
                         Column('api_error', Boolean, default=False), 
                         Column('author_profile', Text),
                         Column('workspace_id',UUID),
                         Column('score', Float, nullable=False),
                         Column('last_updated', DateTime, default=datetime.utcnow),
                         Column('renew_date', DateTime),
                         PrimaryKeyConstraint(*['source', 'author_name', 'workspace_id']),
                         schema="common"
)

class KOLScore(object):                                                                                                          
    '''                                                                                                                             
    Stores KOL for a user per source_type,workspace_id for a user, and also topic related user information                         
    '''                                                                                                                             
    pass

# custom classes end

# custom mappers begin


# ** I couldn't find a way to interate over (Mapped Instance object : Table) association objects in a sqlalchemy session, using metadata module or anything else.
#    This information is needed by ALConsistency for ensuring data consistency at application level. So whenever you declare a mapper object for some table, You will need to update the association information here too.



clear_mappers()
mapper(MissedPost, missed_posts_table, extension=ALConsistency())

mapper(CrawlerMetrics,crawler_metrics_table, extension=ALConsistency())
mapper(ConnectorInstanceLog, connector_instance_log_table, extension=ALConsistency(),
       properties=dict(tasks=relation(TaskLog, backref='cinstance_log', passive_deletes=True)))#1:N

#mapper(ConnectorInstanceLog, connector_instance_log_table,
#       properties=dict(tasks=relation(TaskLog, backref='cinstance_log')))#1:N


mapper(TaskLog, task_log_table, extension=ALConsistency(),
       properties=dict(related_uris=relation(RelatedURI, backref='tasklog', passive_deletes=True)))#1:N

mapper(RelatedURI, related_uri_table, extension=ALConsistency())
# custom mappers end



# ** I couldn't find a way to interate over (Mapped Instance object : Table) association objects in a sqlalchemy session, using metadata module or anything else.
#    This information is needed by ALConsistency for ensuring data consistency at application level. So whenever you declare a mapper object for some table, You will need to update the association information here too.

# identity mappers begin
mapper(Visit, visits_table, extension=ALConsistency())

mapper(VisitIdentity, visit_identity_table, extension=ALConsistency(),
       properties=dict(users=relation(User, backref='visit_identity')))

mapper(User, users_table, extension=ALConsistency(),
       properties=dict(_password=users_table.c.password))

mapper(Group, groups_table, extension=ALConsistency(),
       properties=dict(users=relation(User,secondary=user_group_table, backref='groups', passive_deletes=True),
                       workspace_users=relation(WorkspaceUser,backref='groups', passive_deletes=True)))

mapper(Permission, permissions_table, extension=ALConsistency(),
       properties=dict(groups=relation(Group,
                       secondary=group_permission_table, backref='permissions')))
# identity mappers end

# custom mappers begin
mapper(Client, client_table, extension=ALConsistency(),
       properties=dict(workspaces=relation(Workspace, backref='client', passive_deletes=True),#1:N
                       users=relation(User, backref='client', passive_deletes=True)))#1:N

mapper(Workspace, workspace_table, extension=ALConsistency(),
       properties=dict(keywords=relation(Keyword, backref='workspace'),#1:N
                       connector_instances=relation(ConnectorInstance, backref='workspace'),#1:N
                       tags=relation(Tag, backref='workspace'),#1:N
                       posts=relation(Post, backref='workspace')#1:N
                       )
)

mapper(WorkspaceUser, workspace_user_table, extension=ALConsistency(),
       properties=dict(workspace=relation(Workspace, backref='workspace_user_group'),
                       user=relation(User, backref='workspace_user_group', passive_deletes=True),
                       group=relation(Group, backref='workspace_user_group', passive_deletes=True))
)

mapper(Connector, connector_table, extension=ALConsistency(),
       properties=dict(connector_instances=relation(ConnectorInstance, backref='connector')))#1:N

mapper(ConnectorInstance, connector_instance_table, extension=ALConsistency(),
       properties=dict(cinstance_logs=relation(ConnectorInstanceLog, backref='cinstance'),#1:N
                       posts=relation(Post, backref='connector_instance', passive_deletes=True))
)

mapper(Keyword, keyword_table,extension=ALConsistency())
mapper(Function, functions_table, extension=ALConsistency())
mapper(Rule, rules_table,extension=ALConsistency())
mapper(Language, languages_table ,extension=ALConsistency())

mapper(Post, posts_table, extension=ALConsistency(),
       properties=dict(keywords=relation(Keyword, secondary=post_keyword_table, backref='posts', passive_deletes=True),#M:N
                       comments=relation(Comment, backref = 'posts', passive_deletes=True),
                       publish_post=relation(PublishedPost, backref = 'posts',uselist=False, passive_deletes=True),
                       extracted_entity_values=relation(ExtractedEntityValue, backref='posts', passive_deletes=True))
)

mapper(Tag, tags_table, extension=ALConsistency(),
       properties=dict(#keywords=relation(Keyword, secondary=folder_keyword_table, backref='folders'),#M:N
                       posts=relation(Post, secondary=post_tag_table, backref='tags'),#M:N
                       parent=relation(Tag, backref='children')))#1:N

mapper(Comment, comments_table, extension=ALConsistency())
mapper(PublishedPost, published_post_table , extension=ALConsistency())

mapper(ExtractedEntityName, extracted_entity_names_table, extension=ALConsistency(),
       properties=dict(extracted_entity_values=relation(ExtractedEntityValue, backref='extracted_entity_name', passive_deletes=True)))

mapper(ExtractedEntityValue, extracted_entity_values_table, extension=ALConsistency())

mapper(ParentExtractedEntityName, parent_extracted_entity_names_table,extension=ALConsistency(),
       properties=dict(parent_extracted_entity_values=relation(ParentExtractedEntityValue, backref='extracted_entity_name', passive_deletes=True)))

mapper(ParentExtractedEntityValue, parent_extracted_entity_values_table, extension=ALConsistency())

mapper(AlertsIssued, alerts_issued_table, extension=ALConsistency())

mapper(AlertDefinition, alert_definition_table, extension=ALConsistency(),
       properties=dict(alerts_issued=relation(AlertsIssued, backref='alert_definition', passive_deletes=True)))

mapper(UserGroup, user_group_table, extension=ALConsistency())

# custom mappers end


mapper(Product, product_tbl, extension=ALConsistency(), properties={
    'category':relation(Workspace, backref='products'),
    'synonyms':relation(ProductSynonym, passive_deletes=True),
    'urls':relation(ProductUrl, passive_deletes=True),#J, backref='product'),
    }
)


mapper(ProductHierarchy, product_hierarchy_tbl, extension=ALConsistency())
mapper(ProductSynonym, product_synonym_tbl, extension=ALConsistency())
mapper(ProductUrl, product_url_tbl, extension=ALConsistency())
#J
#       properties={'connector_instance':
#                       relation(ConnectorInstance, backref='product_url')})

mapper(Feature, feature_tbl, extension=ALConsistency(), properties={
    'category':relation(Workspace, backref='features'),
    'synonyms':relation(FeatureSynonym, passive_deletes=True),
    }
)
mapper(CategoryStats, category_stats_tbl, extension=ALConsistency())
mapper(FeatureHierarchy, feature_hierarchy_tbl, extension=ALConsistency())
mapper(PostSentiment, post_sentiments_tbl, extension=ALConsistency())
mapper(PostData, post_data_tbl, extension=ALConsistency())
mapper(PostScore, post_scores_tbl, extension=ALConsistency())
mapper(Sentiment, sentiments_tbl, extension=ALConsistency())
mapper(FeatureSynonym, feature_synonym_tbl, extension=ALConsistency())
mapper(Word, words_tbl, extension=ALConsistency())
mapper(DeletedPost, deleted_posts_table, extension=ALConsistency())
mapper(SentimentMatView, sentiment_matview_tbl, extension=ALConsistency())

mapper(Site, sites_tbl, extension=ALConsistency())
mapper(Field, fields_tbl, extension=ALConsistency())
mapper(FieldType, field_types_tbl, extension=ALConsistency())
mapper(FilterTemplate, filter_template_tbl, extension=ALConsistency())

mapper(KOLScore, kol_scores_table, extension=ALConsistency())
mapper(Taxonomy, taxonomies_table, extension=ALConsistency())


ALConsistency._update_mapping(
    {
        Visit : visits_table,
        VisitIdentity : visit_identity_table,
        User : users_table,
        Group : groups_table,
        Permission : permissions_table,
        Client : client_table,
        Workspace : workspace_table,
        WorkspaceUser : workspace_user_table,
        Connector : connector_table,
        ConnectorInstance : connector_instance_table,
        Keyword : keyword_table,
        Function : functions_table,
        Rule : rules_table,
        Language : languages_table,
        Post : posts_table,
        Tag : tags_table,
        Comment : comments_table,
        PublishedPost : published_post_table,
        ExtractedEntityName : extracted_entity_names_table,
        ExtractedEntityValue : extracted_entity_values_table,
        ParentExtractedEntityName : parent_extracted_entity_names_table,
        ParentExtractedEntityValue : parent_extracted_entity_values_table,
        AlertsIssued : alerts_issued_table,
        AlertDefinition :alert_definition_table,
        UserGroup : user_group_table,
        Product : product_tbl,
        ProductHierarchy : product_hierarchy_tbl,
        ProductSynonym : product_synonym_tbl,
        ProductUrl : product_url_tbl,
        Feature : feature_tbl,
        FeatureHierarchy : feature_hierarchy_tbl,
        PostSentiment : post_sentiments_tbl,
        PostData : post_data_tbl,
        PostScore : post_scores_tbl,
        Sentiment : sentiments_tbl,
        FeatureSynonym : feature_synonym_tbl,
        Word : words_tbl,
        DeletedPost : deleted_posts_table,
        SentimentMatView : sentiment_matview_tbl,
        Site : sites_tbl,
        Field : fields_tbl,
        FieldType : field_types_tbl,
        FilterTemplate : filter_template_tbl,
        KOLScore : kol_scores_table,
        CategoryStats : category_stats_tbl,
        MissedPost : missed_posts_table,
        CrawlerMetrics : crawler_metrics_table,
        ConnectorInstanceLog : connector_instance_log_table,
        TaskLog : task_log_table,
        RelatedURI : related_uri_table,
        Taxonomy : taxonomies_table
        }
    )

