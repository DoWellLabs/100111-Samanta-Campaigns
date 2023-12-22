from ..objects.exceptions import DatabaseError


class FetchError(DatabaseError):
    """
    Raised when an error occurs during fetching.
    """
    pass


class InsertionError(DatabaseError):
    """
    Raised when an error occurs during insertion.
    """
    pass


class UpdateError(DatabaseError):
    """
    Raised when an error occurs during update.
    """
    pass


class DeletionError(DatabaseError):
    """
    Raised when an error occurs during deletion.
    """
    pass


class MigrationError(DatabaseError):
    """
    Raised when an error occurs during migration.
    """
    pass
