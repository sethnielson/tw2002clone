import time
from db_interface import db_interface, db_complex_type_ctx

class PersistentType(type):  
    __db_access = None
    __class_directory = {}
    __obj_directory = {}
    
    class reference_manager:
        def db_ref(self, v):
            if isinstance(v, persistent_object): return (v.__get_persistent_id__(), "__persistent_object__")
            return (v, None)
            
        def db_deref(self, v, ref_type):
            if ref_type == "__persistent_object__": 
                return (persistent_object.c_get_object_by_id(v), True)
            return (v, False)
    
    @classmethod
    def c_set_db_interface(cls, db_interface):
        db_interface.c_set_ref_mgr(cls.reference_manager())
        cls.__db_access = db_interface
        
    @classmethod
    def c_db(cls):
        return cls.__db_access
        
    @classmethod
    def c_get_class_spec(cls, cls_name):
        return cls.__class_directory.get(cls_name, None)
        
    @classmethod
    def c_set_class_spec(cls, db_cls_name, db_cls, db_spec):
        cls.__class_directory[db_cls_name] = (db_cls, db_spec)
        
    @classmethod
    def c_register_object(cls, obj, obj_id):
        cls.__obj_directory[obj_id] = obj
        
    @classmethod
    def c_lookup_object(cls, obj_id):
        return cls.__obj_directory.get(obj_id, None)
        
    @classmethod
    def c_obj_iter(cls):
        for k,v in cls.__obj_directory.items():
            yield (k,v)
        
    class init_decorator:
        def __init__(self, orig_init):
            self.orig_init = orig_init
            
        def __call__(self, obj_self, *args, **kargs):
            db_type_name = obj_self.__class__.__name__
            cls_lookup = PersistentType.c_get_class_spec(db_type_name)
            
            if cls_lookup is None:
                raise Exception("Class {} not found in storage".format(db_type_name))
                
            db_cls, db_spec = cls_lookup
            if obj_self.__class__ != db_cls:
                print(obj_self, obj_self.__class__, db_cls)
                raise Exception("Class definition mismatch")
            
            db_obj_ctx = db_complex_type_ctx(db_type_name, db_spec, {}, PersistentType.c_db().get_cache_control().save)
            persistence_proxy = obj_self.__class__.persistence_proxy(db_obj_ctx)
            obj_self.__initialize_persistence__(persistence_proxy)
            
            PersistentType.c_db().init_object(persistence_proxy.db_ctx)
            PersistentType.c_register_object(obj_self, obj_self.__get_persistent_id__())
            
            self.orig_init(obj_self, *args, **kargs)
            
    
        
    def __new__(cls, name, bases, attrs):
        persistent_data_spec = attrs.get("persistent_data_spec", None)
        if persistent_data_spec is None:
            raise Exception("Class {} does not define 'persistent_data_spec'.".format(name))
        
        db_spec = {k:getattr(persistent_data_spec,k) for k in dir(persistent_data_spec) if not k.startswith("_")}
        base_specs = [base for base in bases if type(base) is PersistentType]
        while base_specs:
            next_base = base_specs.pop(0)
            for k in dir(next_base.persistent_data_spec):
                if k.startswith("_"): continue
                if k in db_spec:
                    raise Exception("Persistent Subclasses Cannot Overwrite Previous Attributes. Attempt to redefine {}".format(k))
                db_spec[k] = next_base.persistent_data_spec[k]
            base_specs = base_specs + [base for base in next_base.__bases__ if type(base) is PersistentType]
        
        orig_constructor = attrs.get("__init__", lambda self: None)
        new_constructor =  PersistentType.init_decorator(orig_constructor)
        attrs["__init__"] = lambda self, *args, **kargs: new_constructor(self, *args, **kargs)
        cls_inst = super().__new__(cls, name, bases, attrs)
        PersistentType.c_set_class_spec(name, cls_inst, db_spec)
        return cls_inst
        
class persistent_object(metaclass=PersistentType):
    class persistent_data_spec:
        pass
        
    @classmethod
    def c_set_db_interface(cls, db_interface):
        PersistentType.c_set_db_interface(db_interface)
        
    @classmethod
    def c_get_object_by_id(cls, persistent_id):
        return PersistentType.c_lookup_object(persistent_id)
            
    @classmethod
    def c_set_cache_mode(cls, mode):
        cls.persistence_proxy.CACHE_MODE = mode
        
    class persistence_proxy:
        UNSET = db_interface.UNSET
        
        '''
        @staticmethod
        def decorate_list(proxy, l_key, l):
            old_add = l.__add__
            old_set = l.__setitem__
            old_append = l.append
            
            l.__add__ = lambda self, other: [old_add(other), proxy.set_list_changed(l_key, len(l)-len(other), len(l))]
            l.__set__ = lambda self, k, v: [old_set(k, v), proxy.set_list_changed(l_key, k, k+1)]
            l.__append__ = lambda self, v: [old_append(v), proxy.set_list_changed(l_key, len(l)-1, len(l))]
            l.__persistent__ = True'''
        
        def __init__(self, db_ctx):
            self.db_ctx = db_ctx
            
            self.last_save = time.time()
            self.cache = db_ctx.fields_cache
            for k in db_ctx.db_spec:
                if k not in self.cache:
                    self.cache[k] = self.UNSET
                
        def has_persistent_attr(self, k):
            return k in self.db_ctx.db_spec
            
        def get(self, k):
            return self.cache[k]
            
        def reload(self):
            self.db_ctx.fields_cache = PersistentType.c_db().get(self.db_ctx)
            self.cache = self.db_ctx.fields_cache
            
        def save_aggregate(self, k):
            PersistentType.c_db().set_aggregate(self.db_ctx, k, self.cache[k], changespec)
            
        def set(self, k, v):
            #if isinstance(v, list) and not hasattr(v, "__persistent__"):
            #    self.decorate_list(self, k, v)
            self.cache[k] = v
            self.db_ctx.fields_changed([k])
                
        def set_list_changed(self, l_key, start_change, end_change):
            self.change_specs[l].append((start_change, end_change))
            self.save_aggregate(l_key)
                
    @classmethod
    def c_reload_objects(cls):
        cls.__CREATION_MODE = "reload"
        
        # objects must be registered first (so they exist) and then reloaded
        # this solves objects pointing at teach other
        reloaders = []
        for persistence_id, obj_class_name in PersistentType.c_db().all_objects():
            obj_class, db_spec = PersistentType.c_get_class_spec(obj_class_name)
            
            obj = object.__new__(obj_class)
            obj_ctx = db_complex_type_ctx(obj_class_name, db_spec, {}, PersistentType.c_db().get_cache_control().save, set_id=persistence_id)
            persistence_proxy = obj.__class__.persistence_proxy(obj_ctx)
            obj.__initialize_persistence__(persistence_proxy)
            PersistentType.c_register_object(obj, persistence_id)
            reloaders.append(persistence_proxy)
            
        for persistence_proxy in reloaders:
            persistence_proxy.reload()
            
    
    def __getattribute__(self, k):
        if k.startswith("_"): return super().__getattribute__(k)
        proxy = self.__persistent_proxy
        if proxy.has_persistent_attr(k):
            return proxy.get(k)
        else: return super().__getattribute__(k)
        
    def __setattr__(self, k, v):
        if k.startswith("_"): return super().__setattr__(k, v)
        proxy = self.__persistent_proxy
        if proxy.has_persistent_attr(k):
            return proxy.set(k, v)
        else: return super().__setattr__(k, v)
        
    def __initialize_persistence__(self, proxy):
        self.__persistent_proxy = proxy
        
    def __get_persistent_id__(self):
        return self.__persistent_proxy.db_ctx.db_id