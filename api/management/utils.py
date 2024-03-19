import functools
from typing import List
from django.apps import apps
import asyncio
import sys
import inspect
from concurrent.futures import ThreadPoolExecutor

from api.objects.types.db import DBObject
from samantha_campaigns.api.objects.utils import import_obj_from_traversal_path



def has_dbobjects_file(app_label: str):
    """
    Returns True if the given app has a 'dbobjects.py' file
    
    :param app_label: label of the app to check
    :return: True if the given app has a 'dbobjects.py' file, False otherwise
    """
    try:
        import_obj_from_traversal_path(f"{app_label}.dbobjects")
        return True
    except ImportError:
        return False


def app_exists(app_label: str):
    """
    Returns True if an app with the given label exists, False otherwise

    :param app_label: Label of the app to check
    :return: True if an app with the given name exists, False otherwise
    """
    try:
        apps.get_app_config(app_label)
        return True
    except LookupError:
        return False


def get_apps_with_dbobjects():
    """
    Returns a list of all apps with a 'dbobjects.py' file having at least one DBObject
    defined in it.

    :return: List of all apps with a 'dbobjects.py' file having at least one DBObject
    """
    all_apps = apps.get_app_configs()
    apps_with_dbobjects = []
    for app in all_apps:
        if has_dbobjects_file(app.label) and get_dbobjects(app.label):
            apps_with_dbobjects.append(app)
    return apps_with_dbobjects


# Cache function results to make it performant
@functools.lru_cache(maxsize=None)
def get_dbobjects(app_label: str) -> List[DBObject]:
    """
    Returns a list of all DBObjects in the given app.

    :param app_label: label of the app to get DBObjects from
    :return: List of all DBObjects in the given app
    """
    dbobjects = []
    dbobjects_module = import_obj_from_traversal_path(f"{app_label}.dbobjects")
    for obj in dbobjects_module.__dict__.values():
        if not (inspect.isclass(obj) and issubclass(obj, DBObject) and obj.config and hasattr(obj, "__module__")):
            continue
        if obj.__module__ == dbobjects_module.__name__:
            dbobjects.append(obj)
    return dbobjects


def migrate_dbobjects_in_app(app_label: str, output_stream=sys.stdout):
    """
    Migrates all DBObjects in the given app.

    :param app_label: label of the app containing DBObjects to be migrated
    :param output_stream: Output stream to write migration status to
    """
    dbobjects = get_dbobjects(app_label)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    executor = ThreadPoolExecutor(max_workers=4)
    
    def make_asynchronous_migrate_method(dbobject: type[DBObject]):
        def migrate_method():
            """
            Migrates the given DBObject to its respective database.
            Raises asyncio.CancelledError if migration fails
            """
            output_stream.write(f"Migrating '{dbobject.__name__}' to '{dbobject.db().name}' database...")
            try:
                dbobject.migrate()
                output_stream.write(f"'{dbobject.__name__}' migrated successfully")
            except Exception as exc:
                output_stream.write(f"Migration of '{dbobject.__name__}' failed")
                raise asyncio.CancelledError from exc

        @functools.wraps(dbobject.migrate)
        async def async_migrate_method():
            return loop.run_in_executor(executor, lambda : migrate_method())
        
        return async_migrate_method
    
    async_migrate_methods = [ 
        make_asynchronous_migrate_method(dbobject) 
        for dbobject in dbobjects 
        if dbobject.config.migrate 
    ]
    tasks = [ async_method() for async_method in async_migrate_methods ]
    loop.run_until_complete(asyncio.gather(*tasks))
    executor.shutdown(wait=True)
    return None


def migrate_dbobjects_in_apps(app_labels: List[str], output_stream=sys.stdout):
    """
    Migrates all DBObjects in the given apps.

    :param app_labels: labels of the apps containing DBObjects to be migrated
    :param output_stream: Output stream to write migration status to
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    executor = ThreadPoolExecutor(max_workers=4)
    
    def make_asynchronous_migrate_method_for_app(app_label: str):
        def migrate_method_for_app():
            """
            Migrates all DBObjects in the given app to their respective databases.
            Raises asyncio.CancelledError if migration fails
            """
            output_stream.write(f"Migrating DBObjects in app '{app_label}'...")
            try:
                return migrate_dbobjects_in_app(app_label, output_stream=output_stream)
            except Exception as exc:
                output_stream.write(f"Migration failed for app '{app_label}'")
                raise asyncio.CancelledError from exc
            
        async def async_migrate_method_for_app():
            return loop.run_in_executor(executor, lambda : migrate_method_for_app())
        
        async_migrate_method_for_app.__name__ = f"async_migrate_method_for_{app_label}"
        async_migrate_method_for_app.__qualname__ = f"async_migrate_method_for_{app_label}"
        return async_migrate_method_for_app
    
    async_migrate_methods_for_apps = [ 
        make_asynchronous_migrate_method_for_app(app_label) 
        for app_label in app_labels 
    ]
    tasks = [ async_method_for_app() for async_method_for_app in async_migrate_methods_for_apps ]
    loop.run_until_complete(asyncio.gather(*tasks))
    executor.shutdown(wait=True)
    return None


def flush_dbobjects_in_app(app_label: str, output_stream=sys.stdout):
    """
    Flushes all DBObjects in the given app.

    :param app_label: label of the app containing DBObjects to be flushed
    :param output_stream: Output stream to write flush status to
    """
    dbobjects = get_dbobjects(app_label)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    executor = ThreadPoolExecutor(max_workers=4)
    
    def make_asynchronous_flush_method(dbobject: type[DBObject]):
        def flush_method():
            """
            Flushes the given DBObject from its database. 
            Raises asyncio.CancelledError if flushing fails
            """
            output_stream.write(f"Flushing '{dbobject.__name__}' from '{dbobject.db().name}' database...")
            try:
                dbobject.flush()
                output_stream.write(f"'{dbobject.__name__}' flushed successfully")
            except Exception as exc:
                output_stream.write(f"Flushing of '{dbobject.__name__}' failed")
                raise asyncio.CancelledError from exc

        @functools.wraps(dbobject.flush)
        async def async_flush_method():
            return loop.run_in_executor(executor, lambda : flush_method())
        
        return async_flush_method
    
    async_flush_methods = [ 
        make_asynchronous_flush_method(dbobject) 
        for dbobject in dbobjects 
        if dbobject.config.migrate 
    ]
    tasks = [ async_method() for async_method in async_flush_methods ]
    loop.run_until_complete(asyncio.gather(*tasks))
    executor.shutdown(wait=True)
    return None


def flush_dbobjects_in_apps(app_labels: List[str], output_stream=sys.stdout):
    """
    Flushes all DBObjects in the given apps.

    :param app_labels: labels of the apps containing DBObjects to be flushed
    :param output_stream: Output stream to write flush status to
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    executor = ThreadPoolExecutor(max_workers=4)
    
    def make_asynchronous_flush_method_for_app(app_label: str):
        def flush_method_for_app():
            """
            Flushes all DBObjects in the given app from their respective databases.
            Raises asyncio.CancelledError if flushing fails
            """
            output_stream.write(f"Flushing DBObjects in app '{app_label}'...")
            try:
                return flush_dbobjects_in_app(app_label, output_stream=output_stream)
            except Exception as exc:
                output_stream.write(f"Flushing failed for app '{app_label}'")
                raise asyncio.CancelledError from exc
            
        async def async_flush_method_for_app():
            return loop.run_in_executor(executor, lambda : flush_method_for_app())
        
        async_flush_method_for_app.__name__ = f"async_flush_method_for_{app_label}"
        async_flush_method_for_app.__qualname__ = f"async_flush_method_for_{app_label}"
        return async_flush_method_for_app
    
    async_flush_methods_for_apps = [ 
        make_asynchronous_flush_method_for_app(app_label) 
        for app_label in app_labels 
    ]
    tasks = [ async_method_for_app() for async_method_for_app in async_flush_methods_for_apps ]
    loop.run_until_complete(asyncio.gather(*tasks))
    executor.shutdown(wait=True)
    return None
