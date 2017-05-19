from sqlalchemy.orm.interfaces import MapperExtension 
from collections import defaultdict, namedtuple
from sqlalchemy.exceptions import InvalidRequestError
from database import session

#Implementing consistency at application level

class ALConsistency(MapperExtension): 
    instance_table_mapping = {}
    table_instance_mapping = {}
    
    referential_integrity_info = defaultdict(dict)
    RefInfo = namedtuple('RefInfo','srcTable srcColumnName destTable destColumnName')
    
    def before_insert(self, mapper, connection, instance): 
        """
        :: Check for Referential integrity

        Before inserting the paramater object instance, 
        make sure that all the parent objects referred, if any are VALID OBJECTS, otherwise ROLLBACK.

        """
        ALConsistency._hasParent(instance)


    def before_update(self, mapper, connection, instance):
        """
        :: Implementation - On Update Cascade

        Before updating column(s) of the parameter object instance, 
        If the column value to be updated is a primary key, update current session to cascade updates for 
        the dependent foreign key columns too.
        """
        ## Update cascades are not supported as of now.
        pass

    
    def before_delete(self, mapper, connection, instance):
        """
        :: Implementation - On Delete Cascade
        
        Before deleting the parameter object instance,
        Update the session to casacde deletes any dependent child table columns too.
        """
        ALConsistency._hasChildren(instance)


    @classmethod
    def _hasChildren(cls, instance):
        tbl = ALConsistency.instance_table_mapping[instance.__class__]
        child_columns = ALConsistency.referential_integrity_info[tbl]['referred_by']

        for refInfo in child_columns:
            srcTable, srcColumnName, destTable, destColumnName = refInfo
            parentMappedObject = ALConsistency.table_instance_mapping[srcTable]
            childMappedObject = ALConsistency.table_instance_mapping[destTable]
            
            parentColValue = getattr(instance, srcColumnName)
            d = dict([[destColumnName, parentColValue]])
            if session.query(childMappedObject).filter_by(**d).first() != None:
                raise InvalidDelete(refInfo)
        return False

    
    @classmethod
    def _hasParent(cls, instance):
        tbl = ALConsistency.instance_table_mapping[instance.__class__]
        parent_columns = ALConsistency.referential_integrity_info[tbl]['refers_to']
        
        for refInfo in parent_columns:
            srcTable, srcColumnName, destTable, destColumnName = refInfo
            parentMappedObject = ALConsistency.table_instance_mapping[destTable]
            childMappedObject = ALConsistency.table_instance_mapping[srcTable]

            childColumnValue = getattr(instance, srcColumnName)
            parentColumnValue = None
            
            if childColumnValue:
                parentColumnValue = session.query(parentMappedObject).get(childColumnValue)
                
                if not( childColumnValue and parentColumnValue):
                    raise InvalidInsert(refInfo)
        return True

    @classmethod
    def _update_mapping(cls, instance_table_mapping):
        """
        This function updates mapping between Table Objects - Orm Objects, and the corresponding reverse Mapping
        for ex.,task_logs_table - TaskLogs
        It is used to determine foreign keys, primary keys by accessing corresponding the table object
        Given the Instance object. 
       And given the table object, map to the corresponding Instance object.
        """
        cls.instance_table_mapping.update(instance_table_mapping)
        cls.table_instance_mapping.update(dict([ [v,k] for k,v in instance_table_mapping.items()]))
        cls.table_list = {}

        # Exception is skipped in the code given below, 
        # Because of the way model is incrementally loaded, the parent table(s) which are referred to here
        # might not have been loaded at the time, the function update_mapping is called.
        # Because there is no direct way for me to check, when the last call to this function is made, After that
        # however information here will CORRECTLY REFLECT the refrential integrity constraints.

        # referential_integrity_info contains following info, 
        # {
        #    table_name_1 : 
        #           { 'referred_by' : Set( (childTable1_fullname, childTable1_columnname), ... ) ,
        #             'refers_to' : Set( (parentTable1_fullname, parentTable1_columnname), ... ) 
        #           } , ...
        #  }
        
        for table in cls.table_instance_mapping.keys():
            cls.referential_integrity_info[table].setdefault('refers_to',set())
            cls.referential_integrity_info[table].setdefault('referred_by',set())
            try:
                for fk in table.foreign_keys:    
                    referred_by = cls.RefInfo(fk.column.table, fk.column.name, table, fk.parent.name)
                    cls.referential_integrity_info[fk.column.table].setdefault('referred_by',set())
                    cls.referential_integrity_info[fk.column.table]['referred_by'].add(referred_by)
                    
                    refers_to = cls.RefInfo(table, fk.parent.name, fk.column.table, fk.column.name)
                    cls.referential_integrity_info[table].setdefault('refers_to',set())
                    cls.referential_integrity_info[table]['refers_to'].add(refers_to)
            except InvalidRequestError:
                pass 
                    

class InvalidInsert(Exception):

    def __init__(self, refInfo):
        self.message = "Invalid INSERT Exception : Not allowed as the parent object<%s(%s)> referred to must exist,"\
            "before child <%s(%s)> can be inserted - "%(refInfo.srcTable, refInfo.srcColumnName, 
                                                        refInfo.destTable, refInfo.destColumnName)

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message


class InvalidDelete(Exception):

    def __init__(self, refInfo):
        self.message = "Invalid DELETE Exception : Not allowed as the child <%s(%s)> should be deleted first before "\
                        "the parent object <%s(%s)> can be deleted."\
                        %(refInfo.destTable, refInfo.destColumnName,refInfo.srcTable, refInfo.srcColumnName)

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message
