#!/usr/bin/python3
#
# table.py
#
# Definition for an ORM database table and its metaclass
#

import collections
import orm
import datetime
from orm.easydb import *
from orm.field import Field, Integer, Float, String, Foreign, DateTime, Coordinate

# metaclass of table
# Implement me or change me. (e.g. use class decorator instead)
class MetaTable(type):
    # class variable to keep track of repeating tables
    namesList = []
    table_info = []
    
    def __init__(cls, name, bases, attrs):
        #print("--------Meta---------")
        # class variable to ClassName.namedefinedabove
        # avoid repeating table names
        if name in MetaTable.namesList:
            raise AttributeError("table name is repeated")
        MetaTable.namesList.append(name)
        
        # fields for the class
        cls._fields = []
        for key, value in attrs.items():            
            if isinstance(value, Field):
                cls._fields.append((key, value))
                # call to set the colunms
                # creating an object of class field for each attribute defined
                value.setname(key)

    # Returns an existing object from the table, if it exists.
    #   db: database object, the database to get the object from
    #   pk: int, primary key (ID)
    def get(cls, db, pk):
        # get version and values from easydb 
        values, version = db.get(cls.__name__, pk)
        # attain the fields requiring user input 
        keys = []
        fields = []
        for idx, (key, val) in enumerate(cls._fields):
            if(type(val) == Coordinate):
                #need to pack the first two float into one tuple for location
                zipped = (values[idx], values[idx+1])
                values.pop(idx)
                values[idx] = zipped
            keys.append(key)
            fields.append(val)
        # organize the values given by user
        kwargs = {}
        foreign = False
        for idx, (key, val, value) in enumerate(zip(keys, fields, values)):
            # foreign so first value is refernce to prexisting table 
            if isinstance(val, Foreign):
                values2, version2 = db.get(val.table.__name__, value)
                if(key == 'location'):
                    #hardcode
                    zipped = (values2[idx], values2[idx+1])
                    values2.pop(idx)
                    values2[idx] = zipped
                keys2 = []
                fields2 = []
                for key2, val2 in val.table._fields:
                    keys2.append(key2)
                    fields2.append(val2)
                kwargs2 = {}
                for key2, value2 in zip(keys2, values2):
                    kwargs2[key2] = value2
                obj2 = val.table(db, **kwargs2)
                kwargs[key] = obj2
            else:
                kwargs[key] = value

        # this will invoke Table.__init__(''' arguments ''') 
        # ** kwargs bc we pack the values
        #print("GET kwargs: ", kwargs)
        obj = cls(db, **kwargs)
        #print("obj: ",obj.__dict__)
        return obj

    # Returns a list of objects that matches the query. If no argument is given,
    # returns all objects in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def filter(cls, db, **kwarg):
        foreign = False
        if(kwarg == {}):
            #return all rows
            #op = AL = 1
            row_ids = db.scan(cls.__name__, 1)
        else:
            key, val = zip(*kwarg.items())
            key = key[0]
            val = val[0]
            for cls_key, cls_val in cls.__dict__.items():
                if(isinstance(cls_val, Foreign)):
                    foreign = True
                    foreign_key = cls_key
            #there might be a better way of unpacking dictionary
            if('_' not in key):
                #eq operator = 2
                if(key == 'location'):
                    col = ['location_lat', 'location_lon']
                    temp_ids = []
                    for idx, i in enumerate(col):
                        temp_ids.extend(db.scan(cls.__name__, 2, column_name = i, value = val[idx]))
                    row_ids = []
                    unique_ids = set(temp_ids)
                    for i in unique_ids:
                        if(temp_ids.count(i) == 2):
                            row_ids.append(i)
                else:
                    if(key.title() in MetaTable.namesList):
                        key = 'id'
                        val = val.__dict__['pk']
                    row_ids = db.scan(cls.__name__, 2, column_name=key, value = val)
            else:
                column, op = key.split('__')
                if(column.title() in MetaTable.namesList):
                    #it is a foreign key
                    if(isinstance(val, int)):
                        column = 'id'
                    else:
                        column = 'name'
                if(op == 'ne'):
                    #not equal = 3
                    dt_ex = datetime.datetime.now()
                    if(type(val) == type(dt_ex)):
                        val = str(val)
                    row_ids = db.scan(cls.__name__, 3, column_name = column, value = val)
                    #print(1)
                elif(op == 'gt'):
                    #greater than = 5
                    if(column == 'location'):
                        col = ['location_lat', 'location_lon']
                        temp_ids = []
                        for idx, i in enumerate(col):
                            temp_ids.extend(db.scan(cls.__name__, 5, column_name = i, value = val[idx]))
                        row_ids = []
                        unique_ids = set(temp_ids)
                        for i in unique_ids:
                            if(temp_ids.count(i) == 2):
                                row_ids.append(i)
                    else:
                        row_ids = db.scan(cls.__name__, 5, column_name = column, value = val)
                elif(op == 'lt'):
                    #less than = 4
                    dt_ex = datetime.datetime.now()
                    if(type(val) == type(dt_ex)):
                        val = str(val)
                    row_ids = db.scan(cls.__name__, 4, column_name = column, value = val)
        objects = []
        for pk in row_ids:
            obj = cls.get(db, pk)
            if(foreign == True):
                #assigning the pk value to the reference table
                #foreign key ('user') is obtained before calling db.scan above
                obj.__dict__['_'+foreign_key].__dict__['pk'] = pk
            objects.append(obj)
        return objects
        

    # Returns the number of matches given the query. If no argument is given, 
    # return the number of rows in the table.
    # db: database object, the database to get the object from
    # kwarg: the query argument for comparing
    def count(cls, db, **kwarg):
        if(kwarg == {}):
            result = db.scan(cls.__name__, 1)
            return len(result)
        key, val = zip(*kwarg.items())
        key = key[0]
        val = val[0]
        if('_' not in key):
            #eq = 2
            if(key.title() in MetaTable.namesList):
                key = 'id'
                val = val.__dict__['pk']
            row_ids = db.scan(cls.__name__, 2, column_name=key, value = val)
        else:
            column, op = key.split('__')
            if(column not in cls.__dict__ and column != 'id'):
                raise AttributeError('unknown field')
            if(op == 'lt'):
                #less than = 4
                row_ids = db.scan(cls.__name__, 4, column_name=column, value = val)
            elif(op == 'ne'):
                #not equal = 3
                row_ids = db.scan(cls.__name__, 3, column_name=column, value = val)
            elif(op == 'gt'):
                #greater than = 5
                row_ids = db.scan(cls.__name__, 5, column_name=column, value = val)
            else:
                raise AttributeError('unknown operator')
        return len(row_ids)


# table class
# Implement me.
class Table(object, metaclass=MetaTable):
    def __init__(self, db, **kwargs):
        #get the name of the child class --> table type of the instance  
        self._db = db
        self.tb_name = self.__class__.__name__        
        self.version = None
        self.pk = None  # ID (primary key)
        self.temp = []
        
        #find out required attributes:
        self.requiredAttr = []
        self.insertArg = []
        for attr_name, attr_obj in self._fields:     
            if(type(attr_obj) == Foreign):
                if(attr_obj.blank == True):
                    val = None
                else:
                    val = kwargs[attr_name]
            else:
                if(attr_name not in kwargs and attr_obj.blank == False):              
                    raise AttributeError("Missing required argument Object testcase 1")                
                else:
                    if(attr_name not in kwargs):
                        if(attr_obj.blank == True):
                            val = attr_obj.default
                    else:
                        val = kwargs[attr_name]
            setattr(self, attr_name, val)
            self.insertArg.append(val)
        

    # Save the row by calling insert or update commands.
    # atomic: bool, True for atomic update or False for non-atomic update
    def save(self, atomic=True):
        #print("self field: ", self._fields)
        values = []
        for key, value in self._fields:
            val = getattr(self, key,value)
            if(type(val) == tuple):
                values.append(val[0])
                values.append(val[1])
            elif(type(value) == DateTime):
                values.append(str(val))
            else:
                values.append(val)
        #print("SAVE values: ", values)
        valTypes = [int, str, float]
        for i in values:
            if(type(i) not in valTypes):  
                parentObj = i
                if parentObj.pk is None:
                    if(type(parentObj.insertArg[0]) == tuple):
                        #hardcode
                        temp = parentObj.insertArg.pop(0)
                        parentObj.insertArg.insert(0, temp[1])
                        parentObj.insertArg.insert(0, temp[0])
                        #print("parentObj insertArg: ", parentObj.insertArg)
                        #print("---parent tablename ",parentObj.tb_name)
                    parentObj.pk, parentObj.version = parentObj._db.insert(parentObj.tb_name,  parentObj.insertArg) 
                values[0] = parentObj.pk
                #print("SAVE values: ", values)
                if self.version is None:
                    #print("pk is ",self.pk)
                    self.pk, self.version = self._db.insert(self.tb_name,  values)
                    #print("==pk after is ",self.pk)
                    #print("==version after is ",self.version)
                
                if self.version is not None:
                    #print("version is ",self.version)
                    self.version = self._db.update(self.tb_name, self.pk, values, self.version)
                    #print("update values ", values)
        if self.version is None and self.pk is None:
            self.pk, self.version = self._db.insert(self.tb_name,  values)
        elif self.version is not None and atomic is False:
            self.version = self._db.update(self.tb_name, self.pk, values)
        elif self.version is not None and atomic is not False:
            self.version = self._db.update(self.tb_name, self.pk, values, self.version)

    # Delete the row from the database.
    def delete(self):
        self._db.drop(self.tb_name, self.pk)
        self.pk = None
        self.version = None

