
class UnregisteredAttributeError(Exception):
    """Raised when an unregistered attribute is found in the class' configuration. Attribute was not added to `config.attributes`."""
    pass


class AttributeRequired(Exception):
    """Raised when a required attribute is not found in or passed into the class."""
    pass


class DoesNotExist(Exception):
    """Raised when an object does not exist in the database."""
    pass


class MultipleObjectsReturned(Exception):
    """Raised when multiple objects are returned from the database for a lookup."""
    pass


class ImproperlyConfigured(Exception):
    """Improperly Configured"""
    pass


class DatabaseError(Exception):
    """Raised when an error occurs while performing a database operation."""
    pass

