from collections.abc import Callable
from typing import Any
from django.dispatch import Signal


class ObjectSignal(Signal):
    """Object Signal"""

    def __init__(self, name: str, providing_args: list[str] = None, use_caching: bool = False):
        super().__init__(use_caching=use_caching)
        if not isinstance(name, str):
            raise TypeError(f"ObjectSignal name must be a string, not {name.__class__}")
        if providing_args is None:
            providing_args = []
        if not isinstance(providing_args, list):
            raise TypeError(f"ObjectSignal providing_args must be a list, not {providing_args.__class__}")
        
        self.name = name
        self.providing_args = providing_args

    
    def send(self, sender: Any, **named: Any) -> list[tuple[Callable[..., Any], str | None]]:
        if self.name != "class_prepared":
            from .bases import Object
            if not issubclass(sender, Object):
                raise TypeError(f"Sender must be {Object} or a subclass, not {sender}")
        else:
            from .bases import ObjectMeta
            if not isinstance(sender, ObjectMeta):
                raise TypeError(f"Sender must be {ObjectMeta} or a subclass, not {sender.__class__}")
        return super().send(sender, **named)
    

    def send_robust(self, sender: Any, **named: Any) -> list[tuple[Callable[..., Any], str | None]]:
        if self.name != "class_prepared":
            from .bases import Object
            if not issubclass(sender, Object):
                raise TypeError(f"Sender must be {Object} or a subclass, not {sender}")
        else:
            from .bases import ObjectMeta
            if not isinstance(sender, ObjectMeta):
                raise TypeError(f"Sender must be {ObjectMeta}, not {sender.__class__}")
        return super().send_robust(sender, **named)


# Sent when an Object class has been prepared/initialized
class_prepared = ObjectSignal("class_prepared", use_caching=True)

# Sent when an Object instance is about to be initialized
pre_init = ObjectSignal("pre_init", use_caching=True)
# Sent when an Object instance has been initialized
post_init = ObjectSignal("post_init", use_caching=True)

# Sent when an Object instance is about to be saved
pre_save = ObjectSignal("pre_save", use_caching=True)
# Sent when an Object instance has been saved
post_save = ObjectSignal("post_save", use_caching=True)

# Sent when an Object instance is about to be deleted
pre_delete = ObjectSignal("pre_delete", use_caching=True)
# Sent when an Object instance has been deleted
post_delete = ObjectSignal("post_delete", use_caching=True)

