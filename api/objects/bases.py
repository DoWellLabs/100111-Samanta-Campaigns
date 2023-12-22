from typing import Any
from django.core.exceptions import ValidationError as DjangoValidationError

from .manager import ObjectManager
from .config import ObjectConfig
from .signals import pre_init, post_init, class_prepared
from .exceptions import (
    ImproperlyConfigured, 
    UnregisteredAttributeError,
    DoesNotExist, MultipleObjectsReturned
)



class ObjectMeta(type):
    """Metaclass for `Object` classes or subclasses.""" 
    def __new__(cls, name, bases, attrs):
        new_class = super().__new__(cls, name, bases, attrs)
        # Check if config_class is set and is the ObjectConfig class or a subclass of it.
        if not issubclass(new_class.config_class, ObjectConfig):
            raise TypeError(f"config_class must be class or a subclass of, {ObjectConfig.__name__}")
        # Check if manager_class is set and is the ObjectManager class or a subclass of it.
        if not issubclass(new_class.manager_class, ObjectManager):
            raise TypeError(f"manager_class must be class or a subclass of, {ObjectManager.__name__}")
        # Object specific exceptions
        new_class.DoesNotExist = type(f"{new_class.__name__.strip()}DoesNotExist", (DoesNotExist,), {})
        new_class.MultipleObjectsReturned = type(f"Multiple{new_class.__name__.strip()}sReturned", (MultipleObjectsReturned,), {})
        return new_class


    def __init__(cls, name, bases, attrs):
        # Object configuration related checks
        if cls.config:
            # If the config is already in use by another Object class, raise an error.
            if cls.config._objectclass:
                raise ImproperlyConfigured(
                    f"{cls.__name__}.config is already in use by {cls.config._objectclass}. Call {cls.__name__}.new_config() to get a new configuration."
                )
            if cls.config.__class__ != cls.config_class:
                raise TypeError(
                    f"`{cls.__name__}.config` must be a direct instance of `{cls.config_class}`"
                )

            # Add configurations set in superclass to the subclass
            cls_mro = cls.mro()
            cls_mro.reverse()
            # Filter out classes that are not subclasses of Object or do not have a config set.
            cls_mro = filter(lambda c: issubclass(c, Object) and c.config, cls_mro)
            for cls_ in cls_mro:
                for subconfig_name, subconfig_val in cls_.config.get_subconfigs().items():
                    # If the subconfig is already set in the subclass, do not override it.
                    if getattr(cls.config, subconfig_name):
                        continue
                    # Else, set the subconfig in the subclass.
                    setattr(cls.config, subconfig_name, subconfig_val)
            # Validate the class' configuration
            cls.config.validate()
            # Set the config's objectclass to this class. 
            # Now the config is said to be in use by this class and cannot be used by another class.
            # This is to prevent the same config from being used by multiple classes.
            # Also, the configuration's attributes cannot be changed after this.
            cls.config._objectclass = cls

        # Object manager related checks   
        manager = getattr(cls, "manager", None)
        # Set the manager in the case that the manager is not set(properly), or the manager set is not for this Object class.
        if not issubclass(manager.__class__, cls.manager_class) or manager.object_class is not cls:
            cls.manager = cls.manager_class.for_objectclass(cls)()

        super().__init__(name, bases, attrs)
        class_prepared.send(sender=cls)



class Object(metaclass=ObjectMeta):
    """
    Base class representing an object with architecture imitating a Django model.
    
    A mapping of the Object's attributes to the attribute type should be defined in `config.attributes`.
    Other default sub-configurations available are defined in the Object's `config_class`.

    The following class attributes can be defined in subclasses to customize the Object:
        - `config_class`: These can be used to specify a custom configuration class for the Object.
        This class must be a subclass of `ObjectConfig`. If not specified, the default `ObjectConfig` class is used.

        - `manager_class`: This can be used to specify a custom manager class for the Object.
          The manager class must be a subclass of `ObjectManager`.
        
        - `config`: An instance of the `config_class`. This is used to defined the configuration for the Object.
        Each Object class must have its own configuration. Call the `new_config()` classmethod to get a new instance of the Object's `config_class`.
        If the Object class has a `config` set, the sub-configurations of the superclass(if any) are automatically added to its `config`,
        except if the sub-configurations are already set. This is to allow subclasses to inherit the sub-configurations of their superclasses.

        - `manager`: An instance of the `manager_class`. This is used to manage the Object's instances.
        If no manager is set, a new instance of `manager_class` is automatically created for the Object.

    If `__supportsdb__` returns `True`, the following methods must be implemented:
        - `save`: Saves the Object to the database.
        - `delete`: Deletes the Object from the database.
        - `asave`: async version of `save`.
        - `adelete`: async version of `delete`.
    """
    config_class = ObjectConfig
    config: config_class = None
    manager_class = ObjectManager
    manager: manager_class = None # This will be set in the metaclass if it is still not set(properly) in the subclass.
    __supportsdb__ = False

    def __new__(cls, *args, **kwargs):
        # Ensures that the Object class is not instantiated directly.
        if cls == Object:
            raise TypeError("Cannot instantiate `Object` class directly. Please subclass it.")
        new_object = super().__new__(cls)

        # Ensure that the necessary methods are implemented if the Object supports database operations.
        if new_object.supports_db:
            db_methods = ("save", "delete", "asave", "adelete")
            for method in db_methods:
                if not (hasattr(new_object, method) and callable(getattr(new_object, method))):
                    raise ImproperlyConfigured(
                        f"{cls.__name__} supports database operations but does not implement a `{method}` method."
                    )
                
        # Add primary key
        new_object._pkey = None # Primary key should be set in the subclasses as deemed fit
        return new_object
    

    def __init__(self, **attrs):
        """
        Initializes the Object with the given attributes.

        :param attrs: Attributes specified in `config.attributes` to be set on the Object.
        Attributes not specified in `config.attributes` are ignored.

        NOTE:
        If the value for an attribute is None and the type(s) specified for the attribute in `config.attributes`
        does not include None, the attribute will be set to its default value if available. If no default value is found,
        the attribute will then finally be set to None. 

        To allow an attribute to be explicitly set to None, `type(None)` should be included in type(s) defined
        for the attribute in `config.attributes`. Alternatively, do not set a default value for the attribute.
        """
        if not self.config:
            raise ImproperlyConfigured(f"`{self.__class__.__name__}.config` is not set.")
        
        attrs = self.config.filter_attrs(attrs)
        pre_init.send(sender=self.__class__, attrs=attrs)

        for key, value in attrs.items():
            setattr(self, key, value)

        # validations should be run after initialization, 
        # if the objects primary key has not been set
        if not self.pkey:
            self.run_validations()

        post_init.send(sender=self.__class__, instance=self)

    @property
    def supports_db(self):
        """Returns `True` if the Object class supports database operations. Otherwise, returns `False`."""
        return self.__class__.__supportsdb__
    
    @property
    def data(self):
        """Return the Object as a dictionary (as defined in `serialize`)"""
        dict_repr = self.serialize()
        if not isinstance(dict_repr, dict):
            raise ImproperlyConfigured("`serialize` return type must be dict")
        return dict_repr
    
    @property
    def pkey(self):
        """Object's primary key. This is the unique identifier for the Object"""
        return self._pkey
    
    
    def serialize(self):
        """
        Returns a dictionary representation of the Object. 
        You can override this method to customize the Object's serialization.

        It is advised to access the `data` property instead of calling this 
        method directly (even after overriding it).
        """
        serialized = {}
        for attr_name in self.config.attributes.keys():
            serialized[attr_name] = getattr(self, attr_name)
        return {**serialized, "pkey": self.pkey}
    

    @classmethod
    def new_config(cls, *args, **kwargs):
        """
        Returns a new instance of the Object's `config_class` - a new object configuration.

        :param args: Positional arguments to be used to initialize the `config_class`.
        :param kwargs: Keyword arguments to be used to initialize the `config_class`.
        """
        return cls.config_class(*args, **kwargs)
    
    
    @classmethod
    def pkey_exists(cls, pkey: str) -> bool:
        """
        Checks if there is an instance of this Object with the provided primary key.

        :param pkey: primary key
        """
        return cls.manager.exists(pkey=pkey)
    
    
    def set_defaults(self):
        """
        Overrides and sets all Object's attributes to their default values if provided. 
        Otherwise, leaves them as is.
        """
        for attr_name in self.config.attributes.keys():
            default = self.config.get_default_for(attr_name)
            if default:
                setattr(self, attr_name, default)
        return None
    
    
    def run_validations(self):
        """
        Runs all validation on the Object and its attributes

        Validators are not run on NoneType attributes.
        """
        errors = {}
        try:
            self.validate()
        except Exception as exc:
            if exc.args:
                errors["detail"] = str(exc.args[0])
            else:
                errors["detail"] = str(exc)
                
        for attr_name, validators in self.config.validators.items():
            for validator in validators:
                val = getattr(self, attr_name, None)
                if val is not None:
                    try:
                        validator(val)
                    except Exception as exc:
                        if exc.args:
                            errors[attr_name] = str(exc.args[0])
                        else:
                            errors[attr_name] = str(exc)
                        continue
        if errors:
            raise DjangoValidationError(errors)
        return None
    
    
    def validate(self):
        """
        Additional custom validation to be done on the Object.

        Add your custom validation logic here. Validation here is run before the validators defined in `config.validators`.
        raise a `django.core.exceptions.ValidationError` if validation fails.
        """
        return None
    
    # ----------------- #
    #  Magic Functions  #
    # ----------------- #
    
    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == "_pkey":
            if __value is not None and not isinstance(__value, str):
                raise TypeError("Primary key must be of type `str`") 
        else:
            if not __name in self.config.attributes.keys():
                raise UnregisteredAttributeError(f"`{__name}` is not a registered attribute of {self.__class__}.")
            
            if __value is not None:
                allowed_attr_types = self.config.attributes[__name]
                allowed_choices = self.config.choices.get(__name, None)
                if not isinstance(__value, allowed_attr_types):
                    raise TypeError(f"Attribute `{__name}` must be any of these types: {allowed_attr_types}")
                
                if allowed_choices and __value not in allowed_choices:
                    raise ValueError(f"Value `{__value}` is not a valid choice for attribute `{__name}`. Choices are {allowed_choices}")

        return super().__setattr__(__name, __value)
    
    
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} ({id(self)})>"
    
    
    def __mul__(self, __value: int):
        """Returns a ObjectList of the Object multiplied by the given value."""
        # The ObjectList returned should have no connection to the database
        objlist = self.manager.get_objectlist().clone()
        objlist.extend([self for _ in range(__value)])
        return objlist
    
    
    def __rmul__(self, __value: int):
        return self.__mul__(__value)
    
    
    def __imul__(self, __value: int):
        return self.__mul__(__value)


    def __hash__(self):
        if self.pkey:
            return hash(self.pkey)
        return hash(id(self))
