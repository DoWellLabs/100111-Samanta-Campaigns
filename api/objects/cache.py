import datetime
from django.utils import timezone

from .bases import Object



class ObjectCache:
    """Temporary storage for `Object`s"""

    def __init__(
            self, 
            __type: type[Object], 
            *, 
            max_size: int = -1,
            clear_when_full: bool = False
        ):
        """
        Create a new cache for `Object`s of the specified type.

        :param __type: The type of `Object`s to store in the cache. This usually a subclass of `Object`.
        :param max_size: The maximum number of objects to store in the cache. Defaults to `-1` (no limit).
        :param clear_when_full: Whether to clear the cache when it is full instead of raising an error. Defaults to `False`.
        """
        if not issubclass(__type, Object):
            raise TypeError("Cache cannot be created for a non-Object type")
        if not isinstance(max_size, int):
            raise TypeError("max_size must be an integer")
        if not isinstance(clear_when_full, bool):
            raise TypeError("clear_when_full must be a boolean")
        self.__objecttype__ = __type
        self.__register__ = {}
        self.max_size = max_size
        self.clear_when_full = clear_when_full

    @property
    def register(self):
        """The contents of the cache"""
        is_corrupt = False

        if not isinstance(self.__register__, dict):
            is_corrupt = True
        # If self.max_size is positive, check if the cache as exceeded the maximum size
        if abs(self.max_size) == self.max_size and self.size > self.max_size:
            is_corrupt = True

        for pkey, (timestamp, obj) in self.__register__.items():
            if is_corrupt:
                break
            if not isinstance(pkey, str):
                is_corrupt = True
                break
            if not isinstance(timestamp, datetime.datetime):
                is_corrupt = True
                break
            if not isinstance(obj, self.__objecttype__):
                is_corrupt = True
                break
        if is_corrupt:
            raise RuntimeError(f"Cache for '{self.__objecttype__}' is corrupt")
        return self.__register__
    
    @property
    def size(self):
        """The number of objects in the cache"""
        return len(self.__register__)
    
    @property
    def is_full(self):
        """Whether the cache is full"""
        return self.size == self.max_size
    
    @property
    def empty(self):
        """Whether the cache is empty"""
        return self.size == 0

    def insert(self, obj: Object):
        """
        Store an object in the cache.

        :param obj: The object to store
        :return: The timestamp of the object's insertion into the cache
        """
        if not isinstance(obj, self.__objecttype__):
            raise TypeError(f"obj must be an instance of {self.__objecttype__}")
        
        if self.size == self.max_size:
            if self.clear_when_full:
                self.clear()
            else:
                raise RuntimeError("Cache is full")
        
        self.__register__[obj.pkey] = (timezone.now(), obj)
        return self.register[obj.pkey][0]


    def update(self, obj: Object):
        """
        Update an object in the cache.

        :param obj: The object to update
        """
        if not isinstance(obj, self.__objecttype__):
            raise TypeError(f"obj must be an instance of {self.__objecttype__}")
        if obj.pkey not in self.register:
            raise ValueError("Object not found")
        self.__register__[obj.pkey] = (timezone.now(), obj)
        return True
    

    def remove(self, obj: Object):
        """
        Remove an object from the cache.

        :param obj: The object to remove
        """
        if not isinstance(obj, self.__objecttype__):
            raise TypeError(f"obj must be an instance of {self.__objecttype__}")
        if obj.pkey not in self.register:
            raise ValueError("Object not found")
        del self.__register__[obj.pkey]
        return True
    

    def clear(self):
        """Empty the cache"""
        self.__register__ = {}
        return None
    

    def all(self):
        """
        Get an ObjectList of all objects in the cache.
        """
        objs = [obj for _, obj in self.register.values()]
        objlist = self.__objecttype__.manager.get_objectlist()
        objlist.extend(objs)
        return objlist
    

    def get(self, key: str):
        """
        Get an object from the cache.

        :param key: The primary key of the object to get
        """
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key not in self.register:
            raise ValueError("Object not found")
        return self.register[key][1]


