import inspect
from django.utils import timezone
import inflection

from ..objects.types import DBObject


class DatacubeObjectConfig(DBObject.config_class):
    """Configuration for `DatacubeObject`s"""
    _use_daily_collection = False
    _collection_name = None
    _preferred_db = None

    @property
    def collection_name(self):
        """The name of the collection the Object should be saved in"""
        col_name = self._collection_name
        if self.objectclass:
            col_name = col_name or inflection.pluralize(inflection.underscore(self.objectclass.__name__))
            if self.use_daily_collection:
                col_name = f"{col_name}_for_{timezone.now().strftime('%Y_%m_%d')}"
        return col_name
    
    @collection_name.setter
    def collection_name(self, value):
        if not isinstance(value, str):
            raise TypeError("collection_name must be a string")
        self._collection_name = value
    
    
    @property
    def use_daily_collection(self):
        """Whether or not the Object's collection should change daily"""
        if not isinstance(self._use_daily_collection, bool):
            raise TypeError("_use_daily_collection must be a boolean")
        return self._use_daily_collection
    
    @use_daily_collection.setter
    def use_daily_collection(self, value):
        if not isinstance(value, bool):
            raise TypeError("use_daily_collection must be a boolean")
        self._use_daily_collection = value
    

    @property
    def preferred_db(self):
        """
        The name of the Datacube database to use for the Object. 

        This is useful for when you want to use a different Datacube database for
        a specific Object type, but don't want to change the one used by
        the manager. Probably because the manager class is also being used by other Object types.

        NOTE: This must be a name of an existing Datacube database
        """
        return self._preferred_db

    @preferred_db.setter
    def preferred_db(self, value):
        if not isinstance(value, str):
            raise TypeError("preferred_db must be a string")
        self._preferred_db = value

