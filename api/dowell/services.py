from typing import Any, List
import requests

from .mixins import DowellObjectEqualityMixin, DowellObjectMetaDataValidationMixin
from .exceptions import (
    MetaDataNotFoundError,
    UserNotFound,
    InvalidWorkspaceID,
)
from .utils import get_dowell_user_info


class DowellService(DowellObjectEqualityMixin, DowellObjectMetaDataValidationMixin):
    """Class for representing a Dowell service."""

    def __init__(
            self, 
            service_id: str = None, 
            workspace_id: str = None, 
            metadata: dict = None
        ):
        """
        Initializes a DowellService object.

        :param service_id: The Dowell service ID of the service.
        :param workspace_id: The Dowell workspace ID of the user for whom the service is to be initialized.
        :param metadata: The metadata of the service from a DowellUser object.
        """
        if not service_id and not metadata:
            raise ValueError("Please provide either a service ID or metadata.")
        if service_id and metadata:
            raise ValueError("Please provide either a service ID or metadata, not both.")
        if service_id and not workspace_id:
            raise ValueError("Please provide a workspace ID.")
        self.id = service_id
        if not metadata:
            self.update(workspace_id)
        else:
            self.metadata = metadata

    def __repr__(self) -> str:
        return f"<DowellService: {self.id}>"
    
    def __str__(self) -> str:
        return self.name
    

    def __setattr__(self, __name: str, __value: Any) -> None:
        obj = super().__setattr__(__name, __value)
        if __name == "metadata":
            self.__update__()
        return obj


    def __getmeta__(self, workspace_id: str):
        """
        Returns the metadata of the service for the user.
        
        :param workspace_id: The Dowell workspace ID of the user.
        """
        try:
            user_metadata = get_dowell_user_info(workspace_id)
        except requests.exceptions.HTTPError:
            raise UserNotFound(workspace_id)
        if not user_metadata:
            raise InvalidWorkspaceID(workspace_id)
        for service in user_metadata["services"]:
            if service["service_id"] == self.id:
                return service
        return None
    

    def __update__(self):
        """Updates the service's details from the metadata."""
        if not self.metadata:
            raise MetaDataNotFoundError("Cannot update details without metadata. Call the update() method ")
        self.id: str = self.metadata["service_id"]
        self.name: str = self.metadata["name"]
        self.type: str = self.metadata["service_type"]
        self.active_status: bool = self.metadata["is_active"]
        self.subservices: List[DowellSubService] = [ DowellSubService(self, metadata=subservice) for subservice in self.metadata["sub_service"] ] if self.metadata["sub_service"] else []
        return None
    
    @property
    def credits_required(self) -> int:
        """Returns the total credits that may be required for using the service (all subservices)."""
        if self.subservices:
            return sum([ subservice.credits_required for subservice in self.subservices ])
        return self.metadata["credits"]


    def update(self, workspace_id: str):
        """
        Updates the service.

        :param workspace_id: The Dowell workspace ID of the user for whom the service is to be updated.
        """
        try:
            self.metadata = self.__getmeta__(workspace_id)
        except MetaDataNotFoundError:
            raise MetaDataNotFoundError("Invalid workspace ID. Could not find service metadata.")
        return None
    

    def get_subservice(self, subservice):
        """
        Returns a subservice object for the service.

        :param subservice: The subservice ID of the subservice in the parent service, subservice name or the subservice object.
        """
        if isinstance(subservice, str):
            return next((sub for sub in self.subservices if sub.id == subservice or sub.name.lower() == subservice.lower()), None)
        elif isinstance(subservice, DowellSubService):
            return next((sub for sub in self.subservices if sub.id == subservice.id), None)
        else:
            raise ValueError("Subservice must be a subservice ID, subservice name or a DowellSubService object.")
    

    def has_subservice(self, subservice):
        """
        Returns whether the service has a particular subservice.

        :param subservice: The subservice ID of the subservice in the parent service, subservice name or the subservice object.
        """
        return bool(self.get_subservice(subservice))



class DowellSubService(DowellObjectEqualityMixin, DowellObjectMetaDataValidationMixin):
    """Class for representing a Dowell subservice."""

    def __init__(
            self, 
            parent_service: DowellService, 
            subservice_id: str = None, 
            metadata: dict = None
        ):
        """
        Initializes a DowellSubService object.

        :param parent_service: The parent DowellService object.
        :param subservice_id: The subservice ID of the subservice in the parent service.
        :param metadata: The metadata of the subservice from a DowellService object.
        """
        if not isinstance(parent_service, DowellService):
            raise ValueError("Parent service must be a DowellService object.")
        if not subservice_id and not metadata:
            raise ValueError("Please provide either a subservice ID or metadata.")
        if subservice_id and metadata:
            raise ValueError("Please provide either a subservice ID or metadata, not both.")
        
        self.parent = parent_service
        if not metadata:
            self.update(subservice_id)
        else:
            self.metadata = metadata


    def __repr__(self) -> str:
        return f"<DowellSubService: {self.id}>"
    
    
    def __str__(self) -> str:
        return self.name
    
    
    def __setattr__(self, __name: str, __value: Any) -> None:
        obj = super().__setattr__(__name, __value)
        if __name == "metadata":
            self.__update__()
        return obj
    

    def __getmeta__(self, subservice_id: str):
        """
        Returns the metadata of the subservice for the user.
        
        :param subservice_id: The subservice ID of the subservice in the parent service.
        """
        if not self.parent.metadata["sub_service"]:
            raise MetaDataNotFoundError("Cannot get subservice metadata without parent service metadata.")
        for subservice in self.parent.metadata['sub_service']:
            if subservice["sub_service_id"] == subservice_id:
                return subservice
        return None


    def __update__(self):
        """Updates the subservice's details from the metadata."""
        if not self.metadata:
            raise MetaDataNotFoundError("Cannot update details without metadata. Call the update() method ")
        self.name: str = self.metadata["sub_service_name"]
        self.id: str = self.metadata["sub_service_id"]
        self.quantity: int = self.metadata["quantity"]
        self.credits_required: int = self.metadata["sub_service_credits"]
        return None


    def update(self, subservice_id: str):
        """
        Updates the subservice for parent DowellService.

        :param subservice_id: The subservice ID of the subservice in the parent service.
        """
        try:
            self.metadata = self.__getmeta__(subservice_id)
        except MetaDataNotFoundError:
            raise MetaDataNotFoundError("Invalid subservice ID. Could not find subservice metadata. Check that parent service metadata is available also.")
        return None
    
    
    def check_active_status_for_user(self, user):
        """
        Check if the subservice is an active service for the user.

        :param user: The DowellUser object or workspace ID of the user.
        :return: The active status of the subservice for the user.
        """
        from .user import DowellUser
        user = user if isinstance(user, DowellUser) else DowellUser(workspace_id=user)
        corresponding_service = user.get_service(self.name)
        if not corresponding_service:
            return True # Some subservices are not related to a service, so they are always active
        return corresponding_service.active_status
    

