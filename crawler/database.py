#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import time
import logging

import sqlalchemy
from sqlalchemy.orm import create_session as orm_create_session
from tgimport import config

log = logging.getLogger("database")

_engine = None


def get_engine():
    """Retrieve the engine based on the current configuration."""
    global _engine
    if not _engine:
        dburi = config.get(section="taskmaster_shard", option="dburi")
        if not dburi:
            raise KeyError("No sqlalchemy database config found!")
        _engine = sqlalchemy.create_engine(dburi)
    if not metadata.is_bound():
        metadata.bind = _engine
    return _engine

def create_session():
    """Create a session that uses the engine from thread-local metadata."""
    if not metadata.is_bound():
        get_engine()
    return orm_create_session()

metadata = sqlalchemy.MetaData()
try:
    from sqlalchemy.orm import scoped_session
    # Create session with autoflush=False
    # and autocommit=True (transactional=False)
    session = scoped_session(create_session)
    mapper = session.mapper  # use session-aware mapper
except ImportError: # SQLAlchemy < 0.4
    from sqlalchemy.ext.sessioncontext import SessionContext
    class Objectstore(object):
        def __init__(self):
            self.context = SessionContext(create_session)
        def __getattr__(self, name):
            return getattr(self.context.registry(), name)
        session = property(lambda s: s.context.registry())
    session = Objectstore()
    context = session.context
    Query = sqlalchemy.Query
    from sqlalchemy.orm import mapper as orm_mapper
    def mapper(cls, *args, **kwargs):
        validate = kwargs.pop('validate', False)
        if not hasattr(getattr(cls, '__init__'), 'im_func'):
            def __init__(self, **kwargs):
                 for key, value in kwargs.items():
                     if validate and key not in self.mapper.props:
                         raise KeyError(
                            "Property does not exist: '%s'" % key)
                     setattr(self, key, value)
            cls.__init__ = __init__
        m = orm_mapper(cls, extension=context.mapper_extension,
            *args, **kwargs)
        class query_property(object):
            def __get__(self, instance, cls):
                return Query(cls, session=context.current)
        cls.query = query_property()
        return m

try:
    from sqlalchemy.ext import activemapper
    activemapper.metadata, activemapper.objectstore = metadata, session
except ImportError:
    pass
try:
    import elixir
    elixir.metadata, elixir.session = metadata, session
except ImportError:
    pass


bind_meta_data = bind_metadata = get_engine # alias names

try:
    set
except NameError: # Python 2.3
    from sets import Set as set

hub_registry = set()

_hubs = dict() # stores the AutoConnectHubs used for each connection URI
