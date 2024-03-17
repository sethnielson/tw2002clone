import time, atexit

class db_complex_type_ctx:
    """
    This structure is meant to be glue between a complex type (like an object or list)
    and the database. 
    
    - db_id is a unique id across all possible complex types.
    - db_type_name is a hint for databases (e.g., databses that want a separate table for the type)
    - db_spec is hints that can be used to control types. Can be empty, or types can be None if supported
    - fields_cache is a dictionary where the complex type holds cached data.
    - change_notifier is called every time fields are updated. Updates are signaled through fields_changed()
    """
    __next_id = 0
    
    @classmethod
    def c_next_id(cls):
        db_complex_type_ctx.__next_id += 1
        return (db_complex_type_ctx.__next_id - 1)
        
    def __init__(self, db_type_name, db_spec, fields_cache, change_notifier, set_id=None):
        if set_id is None:
            self.db_id = self.c_next_id()
        else:
            self.db_id = set_id
        self.db_type_name = db_type_name
        self.db_spec = db_spec
        self.fields_cache = fields_cache
        self.db_dirty_fields = set([])
        self.change_notifier = change_notifier
        self.notify = True
        
    def set_db_loading_mode(self):
        '''
        In db loading mode, there is no change notificaiton because
        changes are coming from the db, not the object
        
        Automatically turned off after fields_synchronized.
        '''
        self.notify = False
        
        
    def fields_changed(self, fields):
        if not self.notify: return
        
        self.db_dirty_fields.update(fields)
        self.change_notifier(self)
        
    def fields_synchronized(self):
        self.db_dirty_fields.clear()
        self.notify = True
        
        
class db_list(list):
    """
    DB interface wrapper for lists.
    """
    
    class REGISTRY:
        __LIST_LOOKUP = {}
    
        class db_list_ctx(db_complex_type_ctx):
            def __init__(self, l, change_notifier, set_id=None):
                super().__init__("__list__", db_spec={}, fields_cache=l, change_notifier=change_notifier, set_id=set_id)
    
        @classmethod
        def c_create_db_list(cls, cache_control, raw_list=None, fixed_id=None):
            if raw_list is not None:
                l = db_list(raw_list)
            else:
                l = db_list()
                
            # TODO: Lots of circular references here. Simpler solution?
            db_ctx = cls.db_list_ctx(l, cache_control, set_id=fixed_id)
            l._db_ctx = db_ctx
            db_ctx.fields_changed(range(len(l)))
            cls.__LIST_LOOKUP[db_ctx.db_id] = l
            return l
            
        @classmethod
        def c_get_db_list_by_id(cls, db_id):
            return cls.__LIST_LOOKUP.get(db_id, None)
        
    def __iadd__(self, other):
        old_len = len(self)
        ret = super().__iadd__(other)
        new_len = len(self)
        self._db_ctx.fields_changed(range(old_len, new_len))
        return ret
        
    def __imul__(self, i):
        old_len = len(self)
        ret = super().__imul__(i)
        new_len = len(self)
        self._db_ctx.fields_changed(range(old_len, new_len))
        return ret
        
    def __setitem__(self, k, v):
        if k < 0: k = len(self) + k
        ret = super().__setitem__(k, v)
        self._db_ctx.fields_changed([k])
        return ret
        
    def __delitem__(self, k):
        if k < 0: k = len(self) + k
        old_len = len(self)
        ret = super().__delitem__(k, v)
        self._db_ctx.fields_changed(range(k, old_len))
        return ret
        
    def append(self, v):
        ret = super().append(v)
        self._db_ctx.fields_changed([len(self)-1])
        return ret
        
    def insert(self, pos, item):
        if pos < 0: pos = len(self) + pos
        ret = super().insert(pos, item)
        self._db_ctx.fields_changed(range(pos, len(self)))
        return ret
        
    def sort(self, *args, **kargs):
        ret = super().sort(*args, **kargs)
        self._db_ctx.fields_changed(range(len(self)))
        return ret
        
    def reverse(self, *args, **kargs):
        ret = super().reverse(*args, **kargs)
        self._db_ctx.fields_changed(range(len(self)))
        return ret
        
    def remove(self, item):
        index = super().index(item)
        old_len = len(self)
        ret = super().remove(item)
        self._db_ctx.fields_changed(range(index, old_len))
        return ret
        
    def pop(self, index=-1):
        if index < 0: index = len(self) + index
        old_len = len(self)
        ret = super().pop(index)
        self._db_ctx.fields_changed(range(index, old_len))
        return ret
        
    def extend(self, iterable):
        old_len = len(self)
        ret = super().extend(iterable)
        self._db_ctx.fields_changed(range(old_len, len(self)))
        return ret
        
    def clear(self):
        old_len = len(self)
        ret = super().clear()
        self._db_ctx.fields_changed(range(old_len))
        return ret
        
class db_dict(dict):
    """
    DB interface wrapper for dictionaries.
    """
    
    class REGISTRY:
        __DICT_LOOKUP = {}
    
        class db_dict_ctx(db_complex_type_ctx):
            def __init__(self, d, change_notifier, set_id=None):
                super().__init__("__dict__", db_spec={}, fields_cache=d, change_notifier=change_notifier, set_id=set_id)
    
        @classmethod
        def c_create_db_dict(cls, cache_control, raw_dict=None, fixed_id=None):
            if raw_dict is not None:
                d = db_dict(raw_dict)
            else:
                d = db_dict()
                
            # TODO: Lots of circular references here. Simpler solution?
            db_ctx = cls.db_dict_ctx(d, cache_control, set_id=fixed_id)
            d._db_ctx = db_ctx
            db_ctx.fields_changed(d.keys())
            cls.__DICT_LOOKUP[db_ctx.db_id] = d
            return d
            
        @classmethod
        def c_get_db_dict_by_id(cls, db_id):
            return cls.__DICT_LOOKUP.get(db_id, None)
        
    def __setitem__(self, k, v):
        ret = super().__setitem__(k, v)
        self._db_ctx.fields_changed([k])
        return ret
        
    def __delitem__(self, k):
        ret = super().__delitem__(k, v)
        self._db_ctx.fields_changed([k])
        return ret
        
    def pop(self, k):
        ret = super().pop(k)
        self._db_ctx.fields_changed([k])
        return ret
        
    def popitem(self):
        pop_key, pop_value = super().popitem()
        self._db_ctx.fields_changed([pop_key])
        return pop_key, pop_value
        
    def clear(self):
        old_keys = list(self.keys())
        ret = super().clear()
        self._db_ctx.fields_changed(old_keys)
        return ret
        
    def update(self, *args, **kargs):
        ret = super().update(*args, **kargs)
        
        # don't have a simple method for identifying changes yet
        # just update everything for now.
        # no keys should be deleted so this should be fine
        self._db_ctx.fields_changed(self.keys())
        return ret
        
class db_unset_obj:
    def __repr__(self):
        return "<UNSET VALUE>"
        
class db_cache_control:
    """
    Interface for a cache on top of the database. Save adds to a pending set.
    On certain conditions (mode-dependant), data is written out
    
    Note that the timer mode is not guaranteed. It requires some kind of write
    to occur AFTER the timer has expired.
    """
    CACHE_MODE_OFF = "write_immediate"
    CACHE_MODE_MANUAL = "write_on_demand"
    CACHE_MODE_TIMER = "write_on_timer"
    CACHE_MODE_ON_EXIT = "write_on_exit"
    
    def __init__(self, db_interface, mode=CACHE_MODE_OFF):
        self.db = db_interface
        self.change_mode(mode)
        self.timeout = 5*60 # 5 minutes
        self.last_save = time.time()
        self.pending = {}
        
    def change_mode(self, new_mode):
        self.mode = new_mode
        if new_mode == self.CACHE_MODE_ON_EXIT:
            atexit.register(self.write_to_database)
        else:
            atexit.unregister(self.write_to_database)
        
    def change_timeout(self, new_timeout):
        self.timeout = timeout
        
    def save(self, ctx):
        if ctx.db_id not in self.pending:
            # ctx, data should be fixed.
            self.pending[ctx.db_id] = ctx
        if self.mode == self.CACHE_MODE_OFF or (self.mode == self.CACHE_MODE_TIMER and (time.time() - self.last_save) > self.timeout):
            self.write_to_database()
            
    def write_to_database(self):
        print("... writing to disk ...")
        while self.pending:
            db_id, ctx = self.pending.popitem()
            self.db.set(ctx, ctx.fields_cache)
        self.db.commit()
        self.last_save = time.time()
        self.pending = {}

class db_interface:
    UNSET = db_unset_obj()
    
    __REF_MGR = None
    
    ALLOWED_SCALAR_TYPES = set([int, float, str, bytes, type(None)])
    ALLOWED_AGGREGATE_TYPES = set([list])
    
    @classmethod
    def c_set_ref_mgr(cls, mgr):
        cls.__REF_MGR = mgr
        
    @classmethod
    def c_db_ref(cls, v):
        # Let any external reference managers have a first crack
        if cls.__REF_MGR: 
            v, ref_type = cls.__REF_MGR.db_ref(v)
            if ref_type is not None: 
                return (v, ref_type)
            
        # convert UNSET into a value stored in the database ("Blob")
        if v is cls.UNSET: 
            return (-1, "__unset__")
        
        # get internal reference ID of db_lists (lists must be converted first)
        elif isinstance(v, db_list):
            return (v._db_ctx.db_id, "__list__")
        elif isinstance(v, db_dict):
            return (v._db_ctx.db_id, "__dict__")
            
        # make sure any other values are legal scalar types
        elif type(v) not in cls.ALLOWED_SCALAR_TYPES:
            raise Exception("Unsupported database type {}".format(type(v)))
            
        # legal type and unmodified
        return (v, None)
        
    @classmethod
    def c_db_deref(cls, v, ref_type):
        if cls.__REF_MGR:
            v, derefed = cls.__REF_MGR.db_deref(v, ref_type)
            if derefed: 
                return (v, True)
            
        if v == -1 and ref_type == "__unset__": 
            return (cls.UNSET, True)
            
        # note aggregates like lists and dictionaries are dereferenced inline.
        # they are not handled here.
            
        elif ref_type is not None:
            raise Exception("Could not deference ptr of type {}".format(ref_type))
            
        return (v, FALSE)
        
    def init_object(self, ctx):
        pass
        
    def get(self, ctx):
        pass
        
    def set(self, ctx, data):
        pass
        
    def load_objects(self):
        pass
        
    def get_cache_control(self):
        pass
        
