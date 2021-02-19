#!/usr/bin/python3
#
# orm.py
#
# Definition for setup and export function
#

from .easydb import Database
import inspect
import orm


def getTableNames(module_info):
    tb_names = []
    for name in module_info:
        if(name.startswith('_') == False):
            tb_names.append(name)
    return tb_names

# Return a database object that is initialized, but not yet connected.
#   database_name: str, database name
#   module: module, the module that contains the schema
def setup(database_name, module):
    # Check if the database name is "easydb".
    #print("SETUP@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    if database_name != "easydb":
        raise NotImplementedError("Support for %s has not implemented"%(
            str(database_name)))
    ormStr = orm.String()
    ormStrType = orm.String
    
    names = getTableNames(module.__dict__)
    tb_names = []    
    for n in names:
        if(n[0].isupper() == True):
            tb_names.append(n)
    table_schema = ['']*len(tb_names)
    tb_names = []    
    for n in names:
        if(n[0].isupper() == True):
            tb_names.append(n)
    for name, value in inspect.getmembers(module):
        if(name in tb_names):
            attr = []
            #print("value: ", value.__dict__)
            for key, val in value.__dict__.items():
                custom = False
                if(key.startswith('_') == False):
                    #print("key: ", key)
                    #print("val: ", val)
                    #key = name of the attr, value = type of the attr
                    if(type(val) == orm.String):
                        normal_type = str
                    elif(type(val) == orm.Float):
                        normal_type = float
                    elif(type(val) == orm.Integer):
                        normal_type = int
                    elif(type(val) == orm.Foreign):           
                        normal_type = val.ref_table                        
                    else:
                        custom = True
                        if(val.c_name == "Coordinate"):
                            attr.append( ('location_lat', float) )
                            attr.append( ('location_lon', float) )
                            
                        if(val.c_name == "DateTime"):
                            if(key == 'start'):
                                attr.append( ('start', str) )
                            if(key == 'end'):
                                attr.append( ('end', str) )
                    if(custom == False):
                        attr.append( (key, normal_type) )

            idx = tb_names.index(name)
            table_schema[idx] = (name, tuple(attr))        
    #print("FINAL: ", tuple(table_schema))
    return Database(tuple(table_schema))

# Return a string which can be read by the underlying database to create the 
# corresponding database tables.
#   database_name: str, database name
#   module: module, the module that contains the schema
def export(database_name, module):
    #print("EXPORT@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
    # Check if the database name is "easydb".
    if database_name != "easydb":
        raise NotImplementedError("Support for %s has not implemented"%(
            str(database_name)))
    
    names = getTableNames(module.__dict__)
    tb_names = []    
    for n in names:
        if(n[0].isupper() == True):
            tb_names.append(n)
    #print("ordered_tb_names: ", tb_names)
    table_schema = ['']*len(tb_names)
    for name, value in inspect.getmembers(module):
        if(name in tb_names):
            foreign = False
            #print("name: ", name)
            table_str = (name + ' { ')
            for key, val in value.__dict__.items():
                #print("key: ", key)
                #print("val: ", val)
                if(key.startswith('_') == False):
                    if(type(val) == orm.String):
                        table_str += (str(key) + ' : ' + "string; ")
                    elif(type(val) == orm.Float):
                        table_str += (str(key) + ' : ' + "float; ")
                    elif(type(val) == orm.Integer):
                        table_str += (str(key) + ' : ' + "integer; ")
                    elif(type(val) == orm.Foreign):
                        table_str += (str(key) + ' : ' + val.ref_table + '; ' )
                    else:
                        if(val.c_name == "Coordinate"):
                            table_str += "location_lat : float; location_lon : float; "
                        if(val.c_name == "DateTime"):
                            if(key == 'start'):
                                table_str += "start: string; "
                            if(key == 'end'):
                                table_str += "end: string; "
            table_str += '} '
            idx = tb_names.index(name)            
            table_schema[idx] = table_str
    return_val = ''
    #print("table_schema: ", table_schema)
    for t in table_schema:
        return_val += t
    return return_val.strip()

