from typing import Mapping, Any
import datetime

from .exceptions import ImproperlyConfigured, UnregisteredAttributeError, AttributeRequired
from .objectlist import ObjectList


class Null:
    """A value that is not set"""
    pass


class ObjectConfigMeta(type):
    """Metaclass for the `ObjectConfig` class and its subclasses."""
    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, "attributes"):
            raise ImproperlyConfigured(f"`attributes` must be defined in {cls}.")
        
        # Saves the original type of the (default) sub-configurations 
        # in `ObjectConfig` class (as they must not be changed later in the Object).
        cls._subconfigs_ogtypes = {}
        for subconfig_name, subconfig_val in cls.get_default_subconfigs().items():
            if not hasattr(cls, f"validate_{subconfig_name}"):
                raise Exception(
                    f"Validation for `{subconfig_name}` was not implemented."
                    f" Implement `validate_{subconfig_name}` in `{cls}`"
                )
            
            if subconfig_val is None:
                raise ImproperlyConfigured(f"`{subconfig_name}` must be defined in `{cls}`. It cannot be `None`.")
            cls._subconfigs_ogtypes[subconfig_name] = subconfig_val.__class__
        return super().__init__(name, bases, attrs)



class ObjectConfig(metaclass=ObjectConfigMeta):
    """
    `Object` configuration. Configurations are designed to be immutable. 
    Once an Object is created/initialized, its configuration cannot be changed.

    The following sub-configurations are available by default:
    - `attributes`: A mapping of the Object's attributes to a tuple of their types. 
    The first type in the tuple is the primary type of the attribute and takes precedence over the other types.
    - `choices`: A mapping of the Object's attributes to their choices.
    - `required`: A tuple of the attributes of the Object that are required on initialization.
    - `defaults`: A mapping of the Object's attributes to their default values.
    - `validators`: A mapping of the Object's attributes to their validators.
    - `ordering`: A tuple of the attributes of the Object to order the ObjectList by when performing a lookup.

    To define a custom configuration for an Object, subclass this class and define the sub-configurations
    as class attributes. Then, set the `config_class` attribute of the Object to the subclass.

    To define sub-configurations that are not available by default, deined them a class attributes in the subclass.
    and implement a validation method for the sub-configuration in the subclass. The validation method should be named
    `validate_<subconfig_name>` where `<subconfig_name>` is the name of the sub-configuration.
    The validation method should check that the sub-configuration is set correctly and raise an exception if not.
    The validation method is only called if the sub-configuration is set or not empty.

    Example:
    ```python
    class JobConfig(ObjectConfig):
        ...
        salary_range = tuple()

        def validate_salary_range(self):
            if len(self.salary_range) != 2:
                raise Exception("Salary range must have 2 values")
            if self.salary_range[0] > self.salary_range[1]:
                raise Exception("Minimum salary must be less than maximum salary")

    class Job(Object):
        config_class = JobConfig
        ...

    class Teaching(Job):
        config = Job.new_config()
        config.salary_range = (10000, 20000)
        ...
    ```
    """
    attributes = dict() # {attr_name: attr_type} e.g. {"name": str, "age": int, ...}
    choices = dict() # {attr_name: (choice1, choice2, ...)}
    # unique = tuple()
    required = tuple() # (attr_name1, attr_name2, ...)
    defaults = dict() # {attr_name: default_value} e.g. {"name": "John Doe", "age": 18, ...}
    validators = dict() # {attr_name: (validator1, validator2, ...)} e.g. {"name": (lambda x: x != "", lambda x: x != "John Doe"), ...}
    ordering = tuple() # The default attributes to order the ObjectList by when performing a lookup

    def __new__(cls, *args, **kwargs):
        """Creates a new instance of the ObjectConfig class"""
        default_subconfigs = cls.get_default_subconfigs()
        new_config = super().__new__(cls)
        # Update the new instance with the default sub-configurations of the configuration class
        for subconfig_name, subconfig_val in default_subconfigs.items():
            setattr(new_config, subconfig_name, subconfig_val)

        new_config._objectclass = None
        return new_config
    

    def __setattr__(self, __name, __value):
        # Prevent the configuration from being changed after its objectclass has been set 
        # i.e the object class has been initialized.
        if getattr(self, "_objectclass", None):
            raise ImproperlyConfigured(
                f"`{self._objectclass}` has already been initialized and its configuration cannot be changed."
            )
        return super().__setattr__(__name, __value)


    @property
    def objectclass(self):
        """
        Returns the Object class that this configuration is being used by.

        If this configuration is not being used by an Object class, it returns `None`.
        """
        from .bases import Object
        if self._objectclass and not issubclass(self._objectclass, Object):
            raise ImproperlyConfigured(
                f"`{self.__class__}` is not being used by an Object class. rather, it is being used by `{self._objectclass}`."
            )
        return self._objectclass
    

    def is_used(self):
        """
        Returns `True` if this configuration is being used by an Object class. Otherwise, returns `False`.
        """
        return self.objectclass is not None
    

    @classmethod
    def get_default_subconfigs(cls):
        """
        Class method that returns an aggregate of all (default) sub-configurations defined in the 
        configuration class and its superclasses in a dictionary.

        The dictionary contains a mapping of the sub-configurations to their values.

        NOTE: 
        If this method is called on an instance of the class, The sub-configurations returned are 
        the ones defined in the class(and its superclasses) and not on the instance.
        Call `get_subconfigs` on the instance to get the sub-configurations defined on the instance too.
        """
        default_subconfigs = {}
        # Use class' mro to get class and its superclasses. 
        # Retrieve the sub-configurations defined in each class and update the `default_subconfigs` dictionary
        mro = cls.mro()
        # Remove the `object` class from the mro as it is not a subclass of `ObjectConfig`
        mro.remove(object)
        # Reverse the mro so that the sub-configurations defined in the child class are prioritized 
        # over the sub-configurations defined in its superclasses
        mro.reverse()
        
        for class_ in mro:
            for attr_name, attr_val in class_.__dict__.items():
                if attr_name.startswith("_") or callable(attr_val) or isinstance(attr_val, (classmethod, staticmethod, property)):
                    continue
                default_subconfigs[attr_name] = attr_val
        return default_subconfigs
    

    def get_subconfigs(self):
        """
        Returns a dictionary representation of the object configuration.
        This also includes all (default) sub-configurations defined in the class and its superclasses.

        This dictionary contains a mapping of all sub-configurations to their values.
        """
        d = {}
        for attr_name, attr_val in self.__dict__.items():
            if attr_name.startswith("_") or callable(attr_val) or isinstance(attr_val, (classmethod, staticmethod, property)):
                continue
            d[attr_name] = attr_val
        return d
    

    def _check_for_duplicates(self):
        """Checks for duplicates in all sub-configurations that are iterables"""
        for subconfig_name, subconfig_val in self.get_subconfigs().items():
            if isinstance(subconfig_val, (tuple, list, set)):
                if subconfig_val and len(subconfig_val) != len(set(subconfig_val)):
                    raise Exception(f"Duplicates found in `{subconfig_name}`")
        return None
    

    def _check_subconfigs_for_unregistered_attributes(self):
        """Checks that all attributes defined in the sub-configurations are registered in `self.attributes`"""
        for subconfig_name, subconfig_val in self.get_subconfigs().items():
            if subconfig_name == "attributes":
                continue
            if isinstance(subconfig_val, (dict, list, tuple, set)):
                for attr in subconfig_val:
                    attr = attr.strip("-")
                    if attr not in self.attributes.keys():
                        raise UnregisteredAttributeError(f"Attribute `{attr}` in `{subconfig_name}` is not registered in `attributes`.")
        return None
    

    def validate(self):
        """
        Validates the configuration.

        This method should be called after all sub-configurations have been set.
        By default, it will be called automatically by the `Object` class/subclass during class creation.
        So it is advised to not call this method manually.
        """
        self._check_subconfigs_for_unregistered_attributes()
        self._check_for_duplicates()

        for subconfig_name, subconfig_val in self.get_subconfigs().items():
            # Check that the subconfig's value is of the original type defined in the ObjectConfig class
            if not isinstance(subconfig_val, self._subconfigs_ogtypes[subconfig_name]):
                raise TypeError(
                    f"`{subconfig_name}` must be of type {self._subconfigs_ogtypes[subconfig_name]} as defined in `{self.__class__.__name__}`"
                )
            # Run subconfig specific validation
            if subconfig_val:
                getattr(self, f"validate_{subconfig_name}")()
        return None
    

    def validate_attributes(self):
        """Validates the attributes defined in `self.attributes`"""
        for attr_name, attr_types in self.attributes.items():
            if not isinstance(attr_types, tuple):
                raise TypeError(f"Type(s) defined for `{attr_name}` should be defined in a tuple.")
            for attr_type in attr_types:
                try:
                    if issubclass(attr_type, object):
                        continue
                except TypeError:
                    pass
                raise ImproperlyConfigured(
                    f"Type `{attr_type}` defined for `{attr_name}` is not supported.\n"
                    f" Supported types are {self._supported_types}"
                )                
        return None
    

    def validate_choices(self):
        """Validates the choices defined in `self.choices`"""
        for attr_name, choices in self.choices.items():
            if not isinstance(choices, (tuple, list)):
                raise TypeError(f"Choices for `{attr_name}` must be defined in a tuple or list.")
            if len(choices) < 2:
                raise Exception(f"`{attr_name}` must have at least 2 choices. If not, set a default for `{attr_name}` in `defaults`.")
            for i, choice in enumerate(choices, start=1):
                attr_types = self.attributes[attr_name]
                if not isinstance(choice, attr_types):
                    raise TypeError(f"Choice{i} for `{attr_name}` must be of type {attr_types}.")
        return None
    
    
    def validate_required(self):
        """Validates the required attributes defined in `self.required`"""
        for required_attr in self.required:
            # check if required attributes are not given a default value
            if required_attr in self.defaults.keys():
                raise Exception(f"Providing a default value for required attribute `{required_attr}` is not allowed.")
        return None
    

    def validate_defaults(self):
        """Validates the default values defined in `self.defaults`"""
        for attr_name, default_value in self.defaults.items():
            if callable(default_value):
                default_value = default_value()
            if not isinstance(default_value, self.attributes[attr_name]):
                raise TypeError(f"Default value for attribute `{attr_name}` must be of/return type {self.attributes[attr_name]}")
        return None
    

    def validate_validators(self):
        """Validates the validators defined in `self.validators`"""
        for attr_name, attr_validators in self.validators.items():
            if not isinstance(attr_validators, (tuple, list, set)):
                raise TypeError(f"Validators for `{attr_name}` must be defined in a tuple, list or set.")
            for validator in attr_validators:
                if not callable(validator):
                    raise TypeError(f"Validator `{validator}` for `{attr_name}` in is not a callable.")
        return None
    

    def validate_ordering(self):
        """Validates the ordering defined in `self.ordering`"""
        pass


    def filter_attrs(self, attrs: Mapping[str, Any]):
        """
        Filters the given attributes to only include the attributes defined in `self.attributes`.
        If an attribute is not present in the given attributes, it is set to its default value if present.

        :param attrs: The attributes to filter
        :raises AttributeRequired: If a required attribute is not present in the given attributes

        NOTE:
        If the value of an attribute is None and the type(s) specified for the attribute in `self.attributes`
        is not None, the attribute will be set to its default value if available. If no default value is found,
        the attribute will then finally be set to None. 

        To allow an attribute to be explicitly set to None, None should be included in type(s) defined
        for the attribute in `self.attributes`. Alternatively, do not set a default value for the attribute.
        """
        filtered_attrs = {}
        for attr_name in self.attributes.keys():
            attr = attrs.get(attr_name, Null)
            # If the attribute's value is None and the types specified for the attribute 
            # in self.attributes does not include None set the attribute to Null
            if attr is None and not isinstance(None, self.attributes[attr_name]):
                attr = Null
            
            if attr is Null and attr_name in self.required:
                raise AttributeRequired(f"Attribute `{attr_name}` is required.")
            filtered_attrs[attr_name] = attr if attr is not Null else self.get_default_for(attr_name)
        return filtered_attrs
    

    def get_default_for(self, attr_name: str):
        """
        Returns the default value for the given attribute name if present. Otherwise, returns `None`.

        :param attr_name: The name of the attribute to get the default value for
        """
        default = self.defaults.get(attr_name, None)
        if default and callable(default):
            default = default()
        return default



class DBObjectConfig(ObjectConfig):
    """
    `DBObject` configuration.

    The following sub-configurations are available by default:
    - `attributes`: A mapping of the Object's attributes to their types.
    - `choices`: A mapping of the Object's attributes to their choices.
    - `required`: A tuple of the attributes of the Object that are required on initialization.
    - `defaults`: A mapping of the Object's attributes to their default values.
    - `validators`: A mapping of the Object's attributes to their validators.
    - `ordering`: A tuple of the attributes of the Object to order the ObjectList by when performing a lookup.
    - `auto_now_datetimes`: A tuple of the attributes of the Object that should be set to the current datetime on save.
    """
    _migrate = True
    auto_now_datetimes = tuple()
    
    @property
    def migrate(self):
        """
        Whether or not the Object should be migrated to the Datacube database
        """
        return self._migrate
    
    @migrate.setter
    def migrate(self, value):
        if not isinstance(value, bool):
            raise TypeError("migrate must be a boolean")
        self._migrate = value


    def validate_auto_now_datetimes(self):
        for attr_name in self.auto_now_datetimes:
            for attr_type in self.attributes[attr_name]:
                if not issubclass(attr_type, datetime.datetime):
                    raise TypeError(f"Attribute type defined for `{attr_name}` must be of type `datetime.datetime` to be used in `auto_now_datetimes`.") 
        return None
