from ..objects.manager import ObjectManager, use_objectlist
from ..objects.objectlist import ObjectList
from .db import DatacubeDB


class datacube_use_objectlist(use_objectlist):
    """
    Custom `use_objectlist` decorator for datacube object managers

    Allows for the creation of datacube compatible manager 
    equivalents of the given objectlist's public methods.
    """
    def make_manager_method(self, method_name: str):
        def manager_method(
                self, 
                dowell_api_key: str = None, 
                limit: int = None,
                offset: int = None,
                workspace_id: str = None, 
                wanted: str = None, 
                *args,
                **kwargs,
                
            ):
            # Fetch all objects in the manager's objectlist (and database), using the necessary keyword arguments
            manager_objlist = self.get_objectlist().all(
                dowell_api_key=dowell_api_key, 
                limit=limit, 
                offset=offset,
                workspace_id=workspace_id,
                wanted=wanted
            )
            # Then, call the method on the objectlist returned
            return getattr(manager_objlist, method_name)(*args, **kwargs)
        return manager_method



@datacube_use_objectlist(ObjectList)
class DatacubeObjectManager(ObjectManager):
    """Manages `DatacubeObject`s."""
    base_db_class = DatacubeDB
    db = None
    
    def create(
            self, 
            *, 
            save: bool = True, 
            dowell_api_key: str = None,
            collection_name: str= None,
            **kwargs
        ):
        """
        Creates a `DatacubeObject` with the specified attributes. 
        Saves it to the database and returns it (if it supports database operations)

        :param save: whether to save the created `DatacubeObject` to the database.
        :param dowell_api_key: the API key to use to save the `DatacubeObject` to the database.
        :param kwargs: keyword arguments of the form `attribute=value` to be used to create the `DatacubeObject`.
        :returns: the created `DatacubeObject`.
        """
        obj = self.object_class(**kwargs)
        if save and obj.supports_db:
            obj.save(dowell_api_key=dowell_api_key, collection_name=collection_name)
        return obj


    def get_or_create(self, *, dowell_api_key: str = None, **kwargs):
        """
        Returns an Object that matches the specified attributes or creates one if it does not exist.

        :param kwargs: keyword arguments of the form `attribute=value` to be used to get or create the `DatacubeObject`.
        :returns: a tuple of the form `(Object, created)`, where `Object` is the retrieved or created Object and `created` is a boolean specifying whether a new Object was created.
        """
        try:
            print("Third check for GET")
            return self.get(dowell_api_key=dowell_api_key, **kwargs), False
        except self.object_class.DoesNotExist:
            return self.create(dowell_api_key=dowell_api_key, **kwargs), True
    

    def update(self, *, dowell_api_key: str = None, **kwargs):
        """
        Updates the attributes of all Objects of the manager with the specified attributes.

        :param dowell_api_key: the API key to use to update the `DatacubeObject`s in the database.
        :param kwargs: keyword arguments of the form `attribute=value` to be used to update the `DatacubeObject`s.
        """
        updated_objs = []
        for obj in self.all(dowell_api_key=dowell_api_key):
            for key, value in kwargs.items():
                setattr(obj, key, value)
            updated_objs.append(obj)
        return self.bulk_save(*updated_objs, dowell_api_key=dowell_api_key)

