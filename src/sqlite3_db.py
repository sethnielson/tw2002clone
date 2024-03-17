import sqlite3

from db_interface import db_interface, db_cache_control, db_list, db_dict

class sqlite3_db(db_interface):
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file)
        cur = self.conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS objects(obj_id PRIMARY KEY, obj_class)")
        cur.execute("CREATE TABLE IF NOT EXISTS data(obj_id, field, ref, value, PRIMARY KEY(obj_id, field))")
        self.conn.commit()
        self.cache_control = db_cache_control(self)
        
    def __delete(self, cur, db_id, field=None):
        if field == None:
            cur.execute("DELETE FROM objects WHERE obj_id = ?", (db_id, ))
            cur.execute("DELETE FROM data WHERE obj_id = ?", (db_id, ))
        else:
            # We don't delete references yet. Garbage collection at some point
            # cur.execute("SELECT (ref, value) FROM data WHERE obj_id=? AND field=?", (db_id, field))
            # row = cur.fetchone()
            # if row is None:
            #     returnc_create_db_list
            # ref, value = row
            # if ref:
            #     self.__delete(value)
            cur.execute("DELETE FROM data WHERE obj_id = ? AND  field = ?", (db_id, field))
        
    def __update(self, cur, db_id, field, value):
        # TODO: This is ugly. Get machinery for creating our complex types.
        if isinstance(value, list) and not isinstance(value, db_list):
            value = db_list.REGISTRY.c_create_db_list(self.get_cache_control().save, value)
        elif isinstance(value, dict) and not isinstance(value, db_dict):
            value = db_dict.REGISTRY.c_create_db_dict(self.get_cache_control().save, value)
            
        (value_ref, ref_type) = self.c_db_ref(value)
        
        if ref_type is None and type(value) in self.ALLOWED_SCALAR_TYPES:
            values = (db_id, field, None, value)
        elif ref_type is not None:
            values = (db_id, field, value_ref, ref_type)
            if isinstance(value, list):
                for k in value._db_ctx.db_dirty_fields:
                    if k < len(value):
                        self.__update(cur, value._db_ctx.db_id, k, value[k])
                    else:
                        self.__delete(cur, value._db_ctx.db_id, k)
                value._db_ctx.fields_synchronized()
            elif isinstance(value, dict):
                for k in value._db_ctx.db_dirty_fields:
                    if k in value:
                        self.__update(cur, value._db_ctx.db_id, k, value[k])
                    else:
                        self.__delete(cur, value._db_ctx.db_id, k)
                value._db_ctx.fields_synchronized()
        else:
            raise Exception("Unhandled type {}".format(type(value)))
        
        cur.execute("REPLACE INTO data(obj_id, field, ref, value) VALUES(?, ?, ?, ?)", values)
        return value
            
    def __load(self, cur, db_id, field):
        cur.execute("SELECT ref, value FROM data WHERE obj_id=? and field=?", (db_id, field))
        row = cur.fetchone()
        if row is None:
            raise Exception("Could not load field {}".format(field))
            
        ref, value = row
        if ref is not None:

            # inline dereferencing of aggregate types like list and dictionary
            if value == "__list__":
                l = db_list.REGISTRY.c_get_db_list_by_id(ref)
                if l is not None:
                    return l
                
                l = db_list.REGISTRY.c_create_db_list(self.get_cache_control().save, fixed_id=ref)
                l._db_ctx.set_db_loading_mode()
                
                cur.execute("SELECT MAX(field) FROM data WHERE obj_id=?", (l._db_ctx.db_id,))
                row = cur.fetchone()
                if row is None:
                    # list is empty, I guess
                    return l
                    
                max_index = row[0]
                for i in range(max_index+1): # plus 1 because max_index is an actually used index
                    l.append(self.__load(cur, l._db_ctx.db_id, i))
                l._db_ctx.fields_synchronized()
                return l
            # inline dereferencing of aggregate types like list and dictionary
            if value == "__dict__":
                d = db_dict.REGISTRY.c_get_db_dict_by_id(ref)
                if d is not None:
                    return d
                
                d = db_dict.REGISTRY.c_create_db_dict(self.get_cache_control().save, fixed_id=ref)
                d._db_ctx.set_db_loading_mode()
                
                for row in cur.execute("SELECT field FROM data WHERE obj_id=?", (d._db_ctx.db_id,)):
                    # I think we need a different cursor for calls outside this for loop?
                    field_cur = self.conn.cursor()
                    
                    d_field = row[0]
                    
                    # inefficient. Two select calls instead of one. TODO: streamline
                    d[d_field] = self.__load(field_cur, d._db_ctx.db_id, d_field)
                
                d._db_ctx.fields_synchronized()
                return d
                
            # the de-referencer is used for dereferencing external object
            # it's also used for the UNSET value for... reasons...
            else:
                value, derefed = self.c_db_deref(ref, value)
                if not derefed:
                    raise Exception("Could not dereference {}, {}".format(ref, value))
                return value
        else:
            return value
            
    def get_cache_control(self):
        return self.cache_control
            
    def commit(self):
        self.conn.commit()
        
    def init_object(self, ctx):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO objects(obj_id, obj_class) VALUES(?, ?)", [ctx.db_id, ctx.db_type_name])
        for field in ctx.db_spec:
            self.__update(cur, ctx.db_id, field, self.UNSET)
        
    def get(self, ctx):
        cur = self.conn.cursor()
        fields = list(ctx.db_spec.keys())
        data = {}
        for field in fields:
            data[field] = self.__load(cur, ctx.db_id, field)
        ctx.fields_synchronized()
        return data
        
    def set(self, ctx, data):
        cur = self.conn.cursor()
        
        # update aggregates directly
        if ctx.db_type_name == "__list__":
            for k in ctx.db_dirty_fields:
                if k < len(data):
                    self.__update(cur, ctx.db_id, k, data[k])
                else:
                    self.__delete(cur, ctx.db_id, k)
        elif ctx.db_type_name == "__dict__":
            for k in ctx.db_dirty_fields:
                if k in data:
                    self.__update(cur, ctx.db_id, k, data[k])
                else:
                    self.__delete(cur, ctx.db_id, k)
        else:
            for k in data:
                if k not in ctx.db_spec: raise Exception("Data filed {} is not persistent".format(k))
                if k not in ctx.db_dirty_fields: continue
                data[k] = self.__update(cur, ctx.db_id, k, data[k])
        ctx.fields_synchronized()
        
    def all_objects(self):
        cur = self.conn.cursor()
        for row in cur.execute("SELECT * FROM objects"):
            yield(row)