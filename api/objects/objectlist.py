import asyncio
from typing import Callable, Iterable
import inflection

from .db import ObjectDatabase
from .utils import import_obj_from_traversal_path



class ObjectList(list):
    """
    A list of `Object`'s similar to a Django `QuerySet`.

    An ObjectList is a list that only contains objects of a particular `Object` type.
    """
    available_lookups = {
        "exact": lambda x, y: x == y,
        "iexact": lambda x, y: str(x).lower() == str(y).lower(),
        "contains": lambda x, y: str(y) in str(x),
        "icontains": lambda x, y: str(y).lower() in str(x).lower(),
        "startswith": lambda x, y: str(x).startswith(y),
        "istartswith": lambda x, y: str(x).lower().startswith(str(y).lower()),
        "endswith": lambda x, y: str(x).endswith(str(y)),
        "iendswith": lambda x, y: str(x).lower().endswith(str(y).lower()),
        "in": lambda x, y: x in y,
        "gt": lambda x, y: x > y,
        "gte": lambda x, y: x >= y,
        "lt": lambda x, y: x < y,
        "lte": lambda x, y: x <= y,
        "range": lambda x, y: y[0] <= x <= y[1],
    }

    def __init__(self, *, object_class, using: ObjectDatabase = None, objects: Iterable = None) -> None:
        """
        Create a new instance of the `ObjectList`

        :param object_class: An Object class or traversal path to an Object class. 
        This defines what type of Object can exist in the ObjectList
        Should be provided if the ObjectList class/sublass does not have the `object_class` attribute set.
        :param using: The database to used by the Objectlist.
        :param objects: An iterable of Objects to be added to the ObjectList.
        """
        from .bases import Object # To address circular import

        if object_class and isinstance(object_class, str):
            object_class = import_obj_from_traversal_path(object_class)
    
        if not issubclass(object_class, Object):
            raise TypeError(f"{self.__class__} must be instantiated with an `Object` class or subclass.")
        self.object_class = object_class

        if using and not isinstance(using, ObjectDatabase):
            raise TypeError(f"using must be an instance of {ObjectDatabase}.")
        self._db = using
        super().__init__()
        if objects:
            self.extend(objects)
        return None
    
    #------------------#
    # OBJECTLIST PROPS #
    #------------------#
    
    @property
    def pkeys(self):
        """Returns a list of the primary keys of all Objects in the list."""
        return [ obj.pkey for obj in self.all() ]
    
    @property
    def empty(self):
        """Returns True if the ObjectList is empty, False otherwise."""
        return len(self.all()) == 0
    
    #------------------#
    # OBJECTLIST + DB  #
    #------------------#

    def all(self, **kwargs):
        """
        Returns ObjectList of all objects in the database.

        :param kwargs: keyword arguments to pass to the database's `fetch()` method.
        """
        # Only method that has a reference to the database
        db_result = self._fetch_all(**kwargs)
        copy = self.copy()
        clone = copy.union(db_result)
        if self.object_class.config.ordering:
            return clone.order_by(*self.object_class.config.ordering)
        return clone


    def none(self):
        """Returns an empty ObjectList"""
        return self.clone()


    def get(self, **kwargs):
        """
        Returns the Object that matches the specified attribute lookup(s).

        :param kwargs: keyword arguments of the form `attribute=value`.
        :raises `object_class.DoesNotExist`: if no match is found.
        :raises `object_class.MultipleObjectsReturned`: if multiple matches are found.
        """
        if not kwargs:
            raise ValueError("No keyword arguments provided.")
        filtered = self.filter(**kwargs)
        if filtered.empty:
            raise self.object_class.DoesNotExist(f"No {self.object_class.__name__} found with attributes: {' '.join([f'`{k}={v}`' for k, v in kwargs.items()])}")
        if filtered.count() > 1:
            raise self.object_class.MultipleObjectsReturned(f"Multiple {inflection.pluralize(self.object_class.__name__)} returned.")
        return filtered.first()


    def filter(self, *, negate: bool = False, **kwargs):
        """
        Returns a list of Objects that match the specified attribute lookup(s), or not if `negate` is True.

        :param negate: If True, return objects that does not match the attribute lookup(s).
        :param kwargs: keyword arguments of the form `attribute=value`.
        """
        if not kwargs:
            return self.all()
        clone = self.clone()
        for obj in self.all():
            is_match = True
            for key, value in kwargs.items():
                if "__" in key:
                    key, *lookups = key.split("__")
                    rel_obj = getattr(obj, key)
                    for index, lookup in enumerate(lookups, start=1):
                        if not lookup:
                            break
                        if hasattr(rel_obj, lookup):
                            rel_obj = getattr(rel_obj, lookup)
                            if index == len(lookups) and rel_obj != value:
                                is_match = False
                                break
                        else:
                            try:
                                lookup_evaluator = self.available_lookups.get(lookup)
                                if not lookup_evaluator:
                                    raise LookupError(f"Invalid lookup: '{lookup}'")
                                
                                if not lookup_evaluator(rel_obj, value):
                                    is_match = False
                                    break
                            except LookupError:
                                raise   
                            except TypeError:
                                raise LookupError(
                                    f"Lookup `{lookup}` is not supported between `{type(rel_obj)}` and `{type(value)}`"
                                )
                            except Exception as exc:
                                raise LookupError(
                                    f"Lookup `{lookup}` failed"
                                ) from exc
                else:
                    if getattr(obj, key) != value:
                        is_match = False
                        break

                if not is_match:
                    break

            if negate:
                if not is_match:
                    clone.append(obj)
            else:
                if is_match:
                    clone.append(obj)
        return clone

    
    def exclude(self, **kwargs):
        """
        Returns an ObjectList of Objects that do not match the specified attributes.
        The same as `filter(**kwargs, negate=True)`

        :param kwargs: keyword arguments of the form `attribute=value`.
        """
        negatives = self.filter(**kwargs, negate=True)
        return negatives


    def delete(self, **kwargs):
        """
        Deletes Objects in the ObjectList from the database and removes them from the ObjectList.

        :param kwargs: keyword arguments to pass to the objects' `delete()` method.
        """
        asyncio.run(self.all().adelete(**kwargs))
        return None


    def bulk_delete(self, *objs, **kwargs):
        """
        Deletes Objects provided as arguments from the database.
        Can be a useful way to delete a large number of objects in a single call.

        :param objs: Objects to be deleted.
        :param kwargs: keyword arguments to pass to the objects' `delete()` method.
        """
        self.__checktypes__(objs)
        clone = self.clone()
        clone.extend(objs) 
        clone.delete(using=self._db, **kwargs)
        return None


    def earliest(self, *attrs):
        """
        Returns the Object with the earliest value of the specified attribute.

        :param attrs: attributes whose values are to be compared.
        """
        objlist = self.all()
        args = []
        if not attrs:
            attrs = self.object_class.config.ordering
        for attr in attrs:
            if attr.startswith("-"):
                args.append(attr[1:])
            else:
                args.append(attr)
        return objlist.order_by(*args).first()


    def latest(self, *attrs):
        """
        Returns the Object with the latest value of the specified attribute.
        
        :param attrs: attributes whose values are to be compared.
        """
        objlist = self.all()
        args = []
        if not attrs:
            attrs = self.object_class.config.ordering
        for attr in attrs:
            if attr.startswith("-"):
                args.append(attr[1:])
            else:
                args.append(attr)
        return objlist.order_by(*args).last()

  
    def exists(self, __obj = None, **kwargs):
        """
        Checks if an Object is present in the ObjectList or if an object with the specified attributes is present in the ObjectList.

        :param __obj: The object whose presence is to be checked. Defaults to None.
        :param kwargs: keyword arguments of the form `attribute=value`.
        """
        if __obj:
            return __obj in self.all()
        return not self.filter(**kwargs).empty
    
    
    
    #------------------#
    # OBJECTLIST ONLY  #
    #------------------#
    
    def first(self):
        """Returns the first Object in the ObjectList."""
        objlist = self.all()
        if objlist.empty:
            return None
        return objlist[0]


    def last(self):
        """Returns the last Object in the ObjectList."""
        objlist = self.all()
        if objlist.empty:
            return None
        return objlist[-1]


    def values(self, *attrs):
        """
        Returns a list of dictionaries containing the values of the specified attributes for each Object in the ObjectList.

        :param attrs: attributes whose values are to be returned.
        """
        objlist = self.all()
        if not attrs:
            return [ obj.__dict__ for obj in objlist ]
        return [ {attr: getattr(obj, attr) for attr in attrs} for obj in objlist ]


    def values_list(self, *attrs):
        """
        Returns a list of tuples containing the values of the specified attributes for each Object in the ObjectList.

        :param attrs: attributes whose values are to be returned.
        """
        objlist = self.all()
        if not attrs:
            return [ tuple(obj.__dict__.values()) for obj in objlist ]
        return [ tuple(getattr(obj, attr) for attr in attrs) for obj in objlist ]


    def count(self, __obj = None, **kwargs):
        """
        Returns the number of Objects in the ObjectList. 
        If an Object is passed, it returns the number of times the object is present in the ObjectList. 
        If keyword arguments are passed, it returns the number of Objects that match the keyword arguments.

        :param __obj: The Object whose count is to be returned. Defaults to None.
        :param kwargs: keyword arguments of the form `attribute=value`.
        """
        if __obj:
            return self.filter(pkey=__obj.pkey).count()
        elif kwargs:
            return self.filter(**kwargs).count()
        return len(self.all())


    def aggregate(self, *aggregations):
        """
        Returns a dictionary containing the specified aggregations.

        :param aggregations: aggregations to be performed on the ObjectList.
        """
        objlist = self.all()
        if not aggregations:
            return {}
        aggregates_dict = {}
        for aggregation in aggregations:
            if not isinstance(aggregation, dict):
                raise TypeError(f"Aggregation must be a dictionary, not {type(aggregation)}")
            for key, value in aggregation.items():
                if not isinstance(key, str):
                    raise TypeError(f"Aggregation key must be a string, not {type(key)}")
                if not callable(value):
                    raise TypeError(f"Aggregation value must be a callable, not {type(value)}")
                aggregates_dict[key] = value(objlist)
        return aggregates_dict
    

    def order_by(self, *attrs):
        """
        Return a new ObjectList with Objects ordered by the specified attributes.

        :param attrs: attributes name to sorted ObjectList by. If an attribute is prefixed with a `-`, the ObjectList is sorted in descending order by that attribute.
        """
        clone = self.clone()
        clone.extend(self)
        for attr in attrs:
            if not isinstance(attr, str):
                raise TypeError(f"Attribute name must be a string, not {type(attr).__name__}")
            if attr.startswith("-"):
                clone.sort(key=lambda x: getattr(x, attr[1:]), reverse=True)
            else:
                clone.sort(key=lambda x: getattr(x, attr))
        return clone
    

    def _fetch_all(self, **kwargs):
        """
        Fetches all Objects from the database and adds them to the list.

        :param kwargs: keyword arguments to pass to the database's `fetch()` method.
        :returns: a ObjectList of Objects fetched from the database.
        """
        if not self._db:
            return self.clone()
        objs = self._db.fetch(self.object_class, **kwargs)
        return self.clone().union(objs) # Already helps ensure objects fetched from DB are of valid type
    

    def append(self, __object) -> None:
        self.__checktypes__((__object,))
        return super().append(__object)


    def extend(self, __iter) -> None:
        self.__checktypes__(__iter)
        return super().extend(__iter)


    def insert(self, __index: int, __object) -> None:
        self.__checktypes__((__object,))
        return super().insert(__index, __object)


    def copy(self):
        """
        Returns a shallow copy of this ObjectList containing the same Objects
        as this ObjectList.
        """
        return self.__copy__()


    def clone(self):
        """
        Returns a shallow and empty copy of the ObjectList.

        The copy will have no reference to the database.
        """
        clone = self.__class__(object_class=self.object_class)
        return clone


    def sort(self, key: Callable, reverse: bool = False):
        return super().sort(key=key, reverse=reverse)


    def distinct(self):
        """Returns a list of unique Objects in the list."""
        clone = self.clone()
        clone.extend(set(self))
        return clone


    def difference(self, __iter):
        """
        Returns a ObjectList of objects that are present in the ObjectList but not in the specified iterable.
        
        :param __iter: The iterable to be compared with.
        """
        clone = self.clone()
        clone.extend(set(self).difference(__iter))
        return clone


    def symmetric_difference(self, __iter):
        """
        Returns a ObjectList of Objects that are unique to either the ObjectList or iterable.

        :param __iter: The iterable to be compared with.
        """
        clone = self.clone()
        clone.extend(set(self).symmetric_difference(__iter))
        return clone


    def union(self, __iter):
        """
        Returns a ObjectList containing Objects added from provided iterable excluding duplicates.

        :param __iter: The iterable to unite with.
        """
        clone = self.clone()
        clone.extend(set(self).union(__iter))
        return clone


    def intersect(self, __iter):
        """
        Returns a ObjectList containing Objects that are present in both the ObjectList and the iterable.

        :param __iter: The iterable to get intersect with.
        """
        clone = self.clone()
        clone.extend(set(self).intersection(__iter))
        return clone


    def remove(self, __obj, **kwargs):
        """
        Removes an Object from the ObjectList or removes all Objects with the specified attributes from the ObjectList.

        :param __obj: The Object to be removed. Defaults to None.
        :param kwargs: keyword arguments of the form `attribute=value`.
        """
        if __obj:
            self.__checktypes__((__obj,))
            return super().remove(__obj)
        elif kwargs:
            filtered = self.filter(**kwargs)
            for obj in filtered:
                return super().remove(obj)
        raise ValueError("No Object or attributes provided.")


    def pop(self, __index: int = -1):
        """
        Removes and returns the Object at the specified index.

        :param __index: The index of the Object to be removed. Defaults to -1.
        """
        return super().pop(__index)


    def clear(self):
        """Removes all Objects from the ObjectList."""
        return super().clear()


    def reverse(self):
        """Reverses the order of Objects in the ObjectList."""
        return super().reverse()


    def index(self, __obj, **kwargs) -> int:
        """
        Returns the index of the specified Object in the ObjectList or returns the index of the first Object with the specified attributes.
        """
        if __obj:
            self.__checktypes__((__obj,))
            return super().index(__obj)
        elif kwargs:
            filtered = self.filter(**kwargs)
            if filtered.empty:
                raise ValueError(f"No {self.object_class.__name__} found with attributes: {' '.join([f'`{k}={v}`' for k, v in kwargs.items()])}")
            return super().index(filtered.first())
        raise ValueError("No Object or attributes provided.")


    async def adelete(self, **kwargs):
        """
        Deletes Objects in the ObjectList from the database asynchronously.

        :param kwargs: keyword arguments to pass to the objects' `adelete()` method.
        """
        kwargs["using"] = kwargs.get("using", self._db)
        tasks = [ obj.adelete(**kwargs) for obj in self if obj.supports_db ]
        if tasks:
            return await asyncio.gather(*tasks)
        return None

    #-----------------------#
    # OBJECTLIST ONLY PROPS #
    #-----------------------#
    values.objectlist_only = True
    values_list.objectlist_only = True
    count.objectlist_only = True
    aggregate.objectlist_only = True
    first.objectlist_only = True
    last.objectlist_only = True
    order_by.objectlist_only = True
    _fetch_all.objectlist_only = True
    union.objectlist_only = True
    difference.objectlist_only = True
    symmetric_difference.objectlist_only = True
    distinct.objectlist_only = True
    intersect.objectlist_only = True
    remove.objectlist_only = True
    pop.objectlist_only = True
    clear.objectlist_only = True
    reverse.objectlist_only = True
    sort.objectlist_only = True
    index.objectlist_only = True
    count.objectlist_only = True
    copy.objectlist_only = True
    clone.objectlist_only = True
    append.objectlist_only = True
    extend.objectlist_only = True
    insert.objectlist_only = True
    adelete.objectlist_only = True

    #-----------------#
    #  MAGIC METHODS  #
    #-----------------#

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} {super().__repr__()}>"

    def __checktypes__(self, __iter) -> None:
        """Checks if all Objects in the iterable are of the correct type."""
        for obj in __iter:
            if not isinstance(obj, self.object_class) or obj.__class__ != self.object_class:
                raise TypeError(f"{self.__class__} can contain only objects of type {self.object_class}")
        return None

    def __copy__(self):
        return self.__class__(object_class=self.object_class, using=self._db, objects=self)

    def __setitem__(self, __index: int, __object) -> None:
        self.__checktypes__((__object,))
        return super().__setitem__(__index, __object)

    def __setslice__(self, __start: int, __end: int, __iter) -> None:
        self.__checktypes__(__iter)
        return super().__setslice__(__start, __end, __iter)
    
    def __iadd__(self, __iter) -> None:
        self.__checktypes__(__iter)
        return super().__iadd__(__iter)
    
    def __add__(self, __iter) -> None:
        self.__checktypes__(__iter)
        return super().__add__(__iter)
    
    def __radd__(self, __iter) -> None:
        self.__checktypes__(__iter)
        return super().__radd__(__iter)
    
    def __mul__(self, __value) -> None:
        return super().__mul__(__value)
    
    def __rmul__(self, __value) -> None:
        return super().__rmul__(__value)
    
    def __imul__(self, __value) -> None:
        return super().__imul__(__value)
    
    def __contains__(self, __object) -> bool:
        # self.__checktypes__((__object,))
        return super().__contains__(__object)
    
    def __getitem__(self, __index: int):
        return super().__getitem__(__index)
    
    def __getslice__(self, __start: int, __end: int):
        return super().__getslice__(__start, __end)
    
    def __iter__(self):
        return super().__iter__()
    
    def __reversed__(self):
        return super().__reversed__()
    
    def __or__(self, __iter):
        return self.union(__iter)
    
    def __and__(self, __iter):
        return self.intersect(__iter)
    
    def __sub__(self, __iter):
        return self.difference(__iter)
    
    def __isub__(self, __iter):
        return self.__sub__(__iter)
    
    def __rsub__(self, __iter):
        return self.__sub__(__iter)
    
    def __xor__(self, __objectlist):
        return self.symmetric_difference(__objectlist)
    
    def __ixor__(self, __objectlist):
        return self.__xor__(__objectlist)
    
    def __rxor__(self, __objectlist):
        return self.__xor__(__objectlist)
    
    
    

def as_manager(
        objectlist_subclass: type[ObjectList],
        using: ObjectDatabase = None,
        *args,
        **kwargs
    ):
    """
    Returns a decorator that creates an `ObjectManager` subclass from 
    the given objectlist subclass and sets the `manager` attribute of the decorated 
    `Object` subclass to an instance of the created `ObjectManager` subclass.

    :param objectlist_subclass: The `ObjectList` subclass from which to create the `ObjectManager` subclass.
    :param using: The database to be used by the created `ObjectManager` subclass.
    :param args: Positional arguments to be passed to the created `ObjectManager` subclass' constructor.
    :param kwargs: Keyword arguments to be passed to the created `ObjectManager` subclass' constructor.

    Example:

    ```python
    from objects.objectlist import ObjectList, as_manager
    from objects.types.db import DBObject

    class BookList(ObjectList):
        def fictional(self):
            return self.filter(genre__iexact="fiction")
        ...

    @as_manager(BookList)
    class Book(DBObject):
        config = DBObject.new_config()
        config.attributes = {
            "title": (str,),
            ...
            "genre": (str,),
            ...
        }
        config.choices = {
            "genre": ("FICTION", "HORROR", "ROMANCE", ...),
            ...
        }
        ...
    
    fictions = Book.manager.fictional()
    ```
    """
    if not issubclass(objectlist_subclass, ObjectList):
        raise TypeError(
            f"Expected objectlist class to be a subclass of {ObjectList}, got {objectlist_subclass}"
        )
    
    from .bases import Object
    def decorator(object_subclass: type[Object]):
        """
        #### `Object` subclass decorator

        Creates an `ObjectManager` subclass from the given objectlist 
        subclass and sets the `manager` attribute of the decorated 
        `Object` subclass to an instance of created `ObjectManager` subclass.

        :param object_subclass: The `Object` subclass to be decorated.
        """
        if not issubclass(object_subclass, Object):
            raise TypeError(
                f"Expected object class to be a subclass of {Object}, got {object_subclass}"
            )
        object_manager_subclass = object_subclass.manager_class.from_objectlist(
            objectlist_subclass, 
            using=using,
            subclass_name=f"{object_subclass.__name__}Manager"
        )
        object_manager_subclass.object_class = object_subclass
        object_subclass.manager = object_manager_subclass(*args, **kwargs)
        return object_subclass
    
    return decorator
