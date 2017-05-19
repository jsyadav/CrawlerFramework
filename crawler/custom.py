#!python

## The MIT License

## Copyright (c) <year> <copyright holders>

## Permission is hereby granted, free of charge, to any person obtaining a copy
## of this software and associated documentation files (the "Software"), to deal
## in the Software without restriction, including without limitation the rights
## to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
## copies of the Software, and to permit persons to whom the Software is
## furnished to do so, subject to the following conditions:

## The above copyright notice and this permission notice shall be included in
## all copies or substantial portions of the Software.

## THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
## IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
## FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
## AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
## LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
## OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
## THE SOFTWARE.


from sqlalchemy import types, exceptions
import uuid 

class Enum(types.TypeDecorator):
    impl = types.Unicode
    
    def __init__(self, values, empty_to_none=False, strict=False):
        """Emulate an Enum type.

        values:
           A list of valid values for this column
        empty_to_none:
           Optional, treat the empty string '' as None
        strict:
           Also insist that columns read from the database are in the
           list of valid values.  Note that, with strict=True, you won't
           be able to clean out bad data from the database through your
           code.
        """

        if values is None or len(values) is 0:
            raise exceptions.AssertionError('Enum requires a list of values')
        self.empty_to_none = empty_to_none
        self.strict = strict
        self.values = values[:]

        # The length of the string/unicode column should be the longest string
        # in values
        size = max([len(v) for v in values if v is not None])
        super(Enum, self).__init__(size)        
        
        
    def process_bind_param(self, value, dialect):
        if self.empty_to_none and value is '':
            value = None
        if value not in self.values:
            raise exceptions.AssertionError('"%s" not in Enum.values' % value)
        return value
        
        
    def process_result_value(self, value, dialect):
        if self.strict and value not in self.values:
            raise exceptions.AssertionError('"%s" not in Enum.values' % value)
        return value

##http://groups.google.com/group/sqlalchemy/browse_thread/thread/ece4a240eb82844b/a3274d8cef62d517?lnk=gst&q=uuid+type#a3274d8cef62d517
class UUID(types.TypeEngine):
    def __init__(self):
        pass

    def get_col_spec(self):
        return "UUID"

    def convert_bind_param(self, value, engine):
        if not value:
            return value
        return str(value)

    def convert_result_value(self, value, engine):
        if not value:
            return value
        return str(value)



# ##http://groups.google.com/group/sqlalchemy/browse_thread/thread/23e8035db59e4d94/901f832e44a03b6d?lnk=gst&q=uuid#901f832e44a03b6d
# class UUID(types.TypeDecorator):
#     '''
#     A column type of uuid that is suitable for object id's that are unique
#     across domains. It is represented as a varchar in the database.
#     todo: useage
#     '''
#     precision=36
#     impl=types.String

#     def process_bind_param(self,value,dialect):
#         #need to  verify value is uuid compatible
#         if value and value.__class__ is uuid.UUID:
#             return str(value)
#         elif value and (value.__class__ is uuid.UUID)==False:
#             raise ValueError, 'value: %s is not a valid uuid.UUID' % value
#         else:
#             return None

#     def process_result_value(self,value,dialect):
#         if value:
#             return uuid.UUID(value)
#         else:
#             return None 

if __name__ == '__main__':
    from sqlalchemy import *
    t = Table('foo', MetaData('sqlite:///'),
              Column('id', UUID,default=uuid.uuid4 ,primary_key=True),
              Column('e', Enum([u'foobar', u'baz', u'quux', None])))
    t.create()

    t.insert().execute(e=u'foobar')
    t.insert().execute(e=u'baz')
    t.insert().execute(e=u'quux')
    t.insert().execute(e=None)
    
    try:
        t.insert().execute(e=u'lala')
        assert False
    except exceptions.AssertionError:
        pass    
    
    print list(t.select().execute())
