import inflection


class ObjectDatabase:
    """
    Base class for databases used to store Objects.

    This class should not be instantiated directly; instead, create a subclass
    based on the database type you want to use.

    The subclass should implement the following methods:
        - `fetch()`: Fetches Objects from the database. Returns a list of Objects.
        - `insert()`: Inserts Objects into the database. Returns the Object's primary key.
        - `update()`: Updates Objects in the database. Returns True if update was successful, False otherwise.
        - `delete()`: Deletes Objects from the database. Returns True if delete was successful, False otherwise.
    """
    def __init__(self, name: str = None):
        """
        :param name: The name to create the database with. If not specified, defaults to the class name in lowercase and underscored.
        """
        self.name = name or inflection.underscore(self.__class__.__name__)


    def fetch(self, __type, **kwargs):
        """
        Fetches all objects of the specified type.

        :param __type: The type of the objects to fetch. Must be a subclass of Object.
        :returns: A list of Objects that match the given criteria.
        """
        # print("type is", __type)
        from .bases import Object
        if not issubclass(__type, Object):
            raise TypeError(f"Expected Object class or subclass, got {__type} instead.")
        return []   


    def insert(self, obj, **kwargs):
        """
        Inserts an object into the database.

        :param obj: The object to insert into the database.
        :returns: The object's primary key on insertion.
        """
        from .bases import Object
        if not isinstance(obj, Object):
            raise TypeError(f"Expected Object, got {obj.__class__} instead.")
        return obj.pkey


    def update(self, obj, **kwargs):
        """
        Updates object's data in the database.

        :param obj: The object to update in the database.
        :returns: True if update was successful, False otherwise.
        """
        from .bases import Object
        if not isinstance(obj, Object):
            raise TypeError(f"Expected Object, got {obj.__class__} instead.")
        return False

    
    def delete(self, obj, **kwargs):
        """
        Deletes object's data from the database.

        :param obj: The object to delete from the database.
        :returns: True if delete was successful, False otherwise.
        """
        from .bases import Object
        if not isinstance(obj, Object):
            raise TypeError(f"Expected Object, got {obj.__class__} instead.")
        return False
