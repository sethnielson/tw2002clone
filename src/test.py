from persistent_object import persistent_object
from sqlite3_db import sqlite3_db

persistent_object.c_set_db_interface(sqlite3_db("test.db"))

class Test(persistent_object):
    class persistent_data_spec:
        x=int
        y=str
        z=persistent_object
        j=None
        i=None
        
    def __init__(self):
        self.x = 3
        self.y = "hello"
        
import sys
if sys.argv[1] == "init":
    t1 = Test()
    t1.x=4
    t1.y = "bye"
    
    t2 = Test()
    t2.j = [1, "hello", t1, ['a','b','c']]
    
    t1.z=t2
    t1.j = t2.j
    print(t1.j, t2.j)
    t1.j[0] = 0
    t1.i = {'a':1, 'b':[1,2,3], 'c': {1:2, 2:3}, 'd':t2}
elif sys.argv[1] == "reload":
    persistent_object.c_reload_objects()
    t1 = persistent_object.c_get_object_by_id(0)
    print(t1.x, t1.y, t1.z)
    t2 = t1.z
    print(t2.x, t2.y, t2.z)
    print(t1.j)
    print(t2.j)
    print(t1.i)