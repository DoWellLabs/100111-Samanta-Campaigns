from typing import Any

from .exceptions import MetaDataValidationError


class DowellObjectMetaDataValidationMixin:
    """Mixin class for validating Dowell object metadata."""
    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == "metadata":
            if not isinstance(__value, dict):
                raise MetaDataValidationError("Metadata must be a dictionary.")
            if not __value:
                raise MetaDataValidationError("Metadata cannot be empty.")
        return super().__setattr__(__name, __value)
    

class DowellObjectEqualityMixin:
    """Mixin class for checking equality of Dowell objects."""
    def __eq__(self, o: object) -> bool:
        if not isinstance(o, type(self)):
            return False
        return self.id == o.id

