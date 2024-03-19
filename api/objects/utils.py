"""Miscellaneous utility functions."""
from django.apps import apps
import importlib
from typing import Tuple, Type, Any
import logging
from cachetools import TTLCache
import os
import functools


def import_obj_from_traversal_path(traversal_path: str):
    """
    Import an object in any app using its full traversal path.
    This will not work for objects that are not in an app.
    
    The traversal path should be in the format:
       `app_label.module_path.object_name`

    :param traversal_path: Full traversal path of the object to import
    :return: The imported object
    :raises AttributeError: If the object is not found
    """
    app_label, main_path = traversal_path.split('.', 1)
    app_config = apps.get_app_config(app_label)
    module_path = object_name = None

    try:
        sub_paths = main_path.rsplit('.', 1)

        if len(sub_paths) == 1:
            module_path = sub_paths[0]
        else:
            module_path, object_name = sub_paths
        module = importlib.import_module(f'{app_config.label}.{module_path}')
        
        if object_name is None:
            return module
        return getattr(module, object_name)
    except AttributeError as exc:
        raise ImportError(
            f'Could not import object from traversal path: {traversal_path}'
        ) from exc



def check_value_isinstance_of_given_types(valuename: str, value: Any, types: Tuple[Type, ...]):
    """
    Recursively checks if the value (and its subvalues, if any) is an instance of any of the given types.

    :param valuename: Name of the value to be checked.
    :param value: Value to be checked.
    :param types: Tuple of types to check against.
    :raises TypeError: If the value is not an instance of any of the given types.
    """
    if not isinstance(types, tuple):
        raise TypeError("`types` must be a tuple of types.")
    
    if not isinstance(value, (list, tuple, set, frozenset, dict)):
        if not isinstance(value, types):
            raise TypeError(f"'{valuename}' must be any of these types: {types}. Not {value.__class__}.")
    else:
        if isinstance(value, dict):
            for key, val in value.items():
                check_value_isinstance_of_given_types(f"{valuename}['{key}']", val, types)
        else:
            for index, val in enumerate(value):
                check_value_isinstance_of_given_types(f"{valuename}[{index}]", val, types)
    return None



class readonly:    

    def __init__(self, value: Any):
        self.v = value

    def __get__(self):
        return self.v


def get_logger(
        name: str, 
        logfile_path: str, 
        base_level: str = "DEBUG",
        format: str = "%(asctime)s - %(levelname)s - %(message)s",
        date_format: str = "%d/%m/%Y %H:%M:%S (%Z)",
        file_mode: str = 'a+',
    ) -> logging.Logger:
    """
    Get an already setup `logging.Logger` instance

    :param name: The name of the logger.
    :param logfile_path: The name or path of the log file to log messages into. It can be a relative or absolute path.
    If the file does not exist, it will be created.
    :param base_level: The base level for logging message. Defaults to "DEBUG".
    :param format: log message format. Defaults to "%(asctime)s - %(levelname)s - %(message)s".
    :param date_format: Log date format string. Defaults to "%d/%m/%Y %H:%M:%S (%Z)".
    :param file_mode: Log file write mode. Defaults to 'a+'.
    :return: `logging.Logger` instance
    """
    logfile_path = logfile_path.replace('/', '\\')
    logfile_path = os.path.abspath(logfile_path)
    if '\\' in logfile_path:
        os.makedirs(os.path.dirname(logfile_path), exist_ok=True, mode=0o777)

    _, ext = os.path.splitext(logfile_path)
    if ext and ext != '.log':
        raise ValueError('Invalid extension type for log file')
    if not ext:
        logfile_path = f"{logfile_path}.log"

    # make log file handler
    file_handler = logging.FileHandler(
        filename=logfile_path,
        mode=file_mode 
    )
    file_handler.setLevel(base_level.upper())
    formatter = logging.Formatter(fmt=format, datefmt=date_format)
    file_handler.setFormatter(formatter)

    # make the logger
    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    logger.setLevel(base_level.upper())
    return logger


def async_ttl_cache(maxsize: int = 128, ttl_seconds: float = 3600):
    """
    Cache the result of the decorated asynchronous function's 
    call for a specified amount of time

    :param maxsize: The maximum size of the cache
    :param ttl_seconds: The time to live of the cache in seconds. Defaults to 1 hour.
    """
    cache = TTLCache(maxsize=maxsize, ttl=ttl_seconds)

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            key = (args, frozenset(kwargs.items()))
            if key not in cache:
                result = await func(*args, **kwargs)
                cache[key] = result
            return cache[key]

        return wrapper

    return decorator


def ttl_cache(maxsize: int = 128, ttl_seconds: float = 3600):
    """
    Cache the result of the decorated function's call for a specified amount of time

    :param maxsize: The maximum size of the cache
    :param ttl_seconds: The time to live of the cache in seconds. Defaults to 1 hour.
    """
    cache = TTLCache(maxsize=maxsize, ttl=ttl_seconds)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, frozenset(kwargs.items()))
            if key not in cache:
                result = func(*args, **kwargs)
                cache[key] = result
            return cache[key]

        return wrapper

    return decorator


