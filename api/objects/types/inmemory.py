from typing import Any
import uuid

from ..bases import Object, ObjectMeta
from ..cache import ObjectCache


class InMemoryObjectMeta(ObjectMeta):
    """Metaclass for `InMemoryObject`s"""
    def __new__(cls, name, bases, attrs):
        new_class = super().__new__(cls, name, bases, attrs)
        # Set the default cache for the class
        new_class.__cache__ = ObjectCache(new_class)
        return new_class



class InMemoryObject(Object, metaclass=InMemoryObjectMeta):
    """
    Base class for `Object`s that do not interact with the database intended to be stored temporarily in memory.

    A mapping of the Object's attributes to the attribute type should be defined in `config.attributes`.
    Other default sub-configurations available are defined in the Object's `config_class`.

    The following class attributes can be defined in subclasses to customize the Object:
        - `config_class`: These can be used to specify a custom configuration class for the Object.
        This class must be a subclass of `ObjectConfig`. If not specified, the default configuration class is used.

        - `config`: An instance of the `config_class`. This is used to configure the Object's attributes.
        Call the `new_config` classmethod to get a new instance of the `config_class`.

        - `manager`: An instance of the `ObjectManager` class. This is used to manage the Object.
        A new instance of the `ObjectManager` class is automatically created for the Object if this is not specified.
    
    Create a simple Book Object class that can be saved to the database as follows:
    ```
    class Book(DBObject):
        # Get a new configuration for the Book Object class
        config = DBObject.new_config()
        config.attributes = {
            "title": str,
            "author": str,
            "year": str,
            "is_published": bool
        }
        config.required = ("title", "author", "year")
        config.defaults = {
            "is_published": False,
        }
        config.ordering = ("title", "-year", "author")
        config.validators = {
            "title": [is_not_blank, MinMaxLengthValidator(min_length=3, max_length=255).validate],
            "author": [is_not_blank, MinMaxLengthValidator(min_length=3, max_length=255).validate],
            "year": [is_not_blank, MinMaxLengthValidator(min_length=4, max_length=4).validate],
        }
    
    # Create a new Book
    book = Book(title="The Great Gatsby", author="F. Scott Fitzgerald", year="1925")
    # OR
    book = Book.manager.create(title="The Great Gatsby", author="F. Scott Fitzgerald", year="1925")
    # Prints the unique identifier of the Book in memory
    print(book.pkey) 

    # Get details of the Book
    print(book.data)

    # Get a list of Books in memory
    books = Book.manager.all()
    print(books)
    ```
    """  
    __supportsdb__ = False
 
    def __init__(self, **attrs):
        super().__init__(**attrs)
        # Add a unique identifier for in-memory object.
        self._pkey = str(uuid.uuid4())
        # Insert the object into the class' cache.
        self.__class__.__cache__.insert(self)

    
    # Prevent access to the class' cache from instances
    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == f"__cache__":
            raise RuntimeWarning("Reserved attribute: '__cache__'. Not allowed!")
        super().__setattr__(__name, __value)
    

    def __getattribute__(self, __name: str) -> Any:
        if __name == f"__cache__":
            raise RuntimeWarning("Not allowed!")
        return super().__getattribute__(__name)



class Human(InMemoryObject):
    config = InMemoryObject.new_config()
    config.attributes = {
        "name": (str,),
        "age": (int,),
        "is_cool": (bool,),
        "friends": (list, type(None)),
    }
    config.required = ("name", "age")
    config.defaults = {
        "is_cool": True,
    }
    config.ordering = ("name", "-age")

friends = [
    {
        "name": "John",
        "age": 25,
        "is_cool": True,
        "friends":[
            {
                "name": "Jane",
                "age": 25,
                "is_cool": True,
                "friends": [Human(name="Daniel", age=25, is_cool=True)]
            }
        ]
    }
]

# daniel = Human(name="Daniel", age=25, friends=friends)

class Woman(Human):
    config = Human.new_config()
    config.ordering = ("name", "-age")

# tade = Woman(name="Tade", age=25)
