
# Dowel user and service related exceptions

class InvalidWorkspaceID(Exception):
    def __init__(self, workspace_id: str = None, *args: object) -> None:
        """
        Initializes an InvalidWorkspaceID exception.

        :param workspace_id: The Dowell workspace ID that is invalid.
        """
        self.workspace_id = workspace_id
        super().__init__(*args)
        return None
    
    def __str__(self) -> str:
        if not self.workspace_id:
            return "Invalid workspace ID."
        return f"Invalid workspace ID '{self.workspace_id}'."
    

class MetaDataValidationError(Exception):
    pass


class MetaDataNotFoundError(Exception):
    pass


class UserNotFound(Exception):
    def __init__(self, workspace_id: str = None, *args: object) -> None:
        """
        Initializes a UserNotFound exception.

        :param workspace_id: The Dowell workspace ID of the user that is not found.
        """
        self.workspace_id = workspace_id
        super().__init__(*args)
        return None
    
    def __str__(self) -> str:
        if not self.workspace_id:
            return "User not found."
        return f"User with workspace ID '{self.workspace_id}' not found."


class ServiceNotfound(Exception):
    def __init__(self, service, *args: object) -> None:
        """
        Initializes a ServiceNotFound exception.

        :param service: The DowellService object or name of the service that is not found.
        """
        self.service = service
        super().__init__(*args)
        return None
    
    def __str__(self) -> str:
        return f"Service '{self.service}' not found."


class SubServiceNotFound(Exception):
    def __init__(self, service, subservice, *args: object) -> None:
        """
        Initializes a SubServiceNotFound exception.

        :param service: The DowellService object for which the subservice is not found.
        :param subservice: The DowellSubService object or name of the subservice that is not found.
        """
        from .services import DowellService
        if not isinstance(service, DowellService):
            raise ValueError("Service must be a DowellService object.")
        self.service = service
        self.subservice = subservice
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Subservice '{self.subservice}' not found in service '{self.service}'."


class SubservicesRequired(Exception):
    def __init__(self, service, *args: object) -> None:
        """
        Initializes a SubservicesRequired exception.

        :param service: The DowellService object for which subservices are required.
        """
        from .services import DowellService
        if not isinstance(service, DowellService):
            raise ValueError("Service must be a DowellService object.")
        self.service = service
        super().__init__(*args)
        return None
    
    def __str__(self) -> str:
        return f"Service '{self.service}' requires subservices. Please provide a list of subservices used."
    

class SubservicesNotRequired(Exception):
    def __init__(self, service, *args: object) -> None:
        """
        Initializes a SubservicesNotRequired exception.

        :param service: The DowellService object for which subservices are not required.
        """
        from .services import DowellService
        if not isinstance(service, DowellService):
            raise ValueError("Service must be a DowellService object.")
        self.service = service
        super().__init__(*args)
        return None
    
    def __str__(self) -> str:
        return f"Service '{self.service}' does not require subservices. Please leave `subservices` as None."
    

class ServiceNotActive(Exception):
    def __init__(self, service, *args: object) -> None:
        """
        Initializes a ServiceNotActive exception.

        :param service: The DowellService object which is not active.
        """
        from .services import DowellService
        if not isinstance(service, DowellService):
            raise ValueError("Service must be a DowellService object.")
        self.service = service
        assert not self.service.active_status, f"Service '{self.service}' is not inactive. Ensure that the service is not active before raising this exception."
        super().__init__(*args)
    
    def __str__(self) -> str:
        return f"Service '{self.service}' is not active. Please activate the service to proceed."


class SubServiceNotActive(Exception):
    def __init__(self, service, subservice, *args: object) -> None:
        """
        Initializes a SubServiceNotActive exception.

        :param service: The DowellService object which is not active.
        :param subservice: The DowellSubService object which is not active.
        """
        from .services import DowellService, DowellSubService
        if not isinstance(service, DowellService):
            raise ValueError("Service must be a DowellService object.")
        if not isinstance(subservice, DowellSubService):
            raise ValueError("Subservice must be a DowellSubService object.")
        if not service.has_subservice(subservice):
            raise ValueError(f"Subservice '{subservice}' must be a subservice of {service}.")
        self.service = service
        self.subservice = subservice
        super().__init__(*args)

    def __str__(self) -> str:
        return f"Subservice '{self.subservice}', of service '{self.service}' is not active. Please activate the subservice to proceed."


class InsufficientCredits(Exception):
    def __init__(self, user, balance: int = None, *args: object) -> None:
        """
        Initializes an InsufficientCredits exception.

        :param user: The DowellUser object which does not have sufficient credits.
        :param balance: The remaining number of credits required to proceed.
        """
        from .user import DowellUser
        if not isinstance(user, DowellUser):
            raise ValueError("User must be a DowellUser object.")
        self.user = user
        self.balance = balance
        super().__init__(*args)
    
    def __str__(self) -> str:
        if not self.balance:
            return f"DowellUser '{self.user}' does not have sufficient credits. Please purchase more credits to proceed."
        return f"DowellUser '{self.user}' does not have sufficient credits. Please purchase {self.balance} credits more to proceed."
    


# Datacube Related Exceptions

class DatacubeError(Exception):
    """Base class for all Datacube errors"""


class ConnectionError(DatacubeError):
    """Error when connecting to the Datacube"""


class DatabaseError(DatacubeError):
    """Error related to the Datacube database connection"""


class DatabaseNotFoundError(DatabaseError):
    """Database connection requested was not found"""


class CollectionNotFoundError(DatabaseError):
    """Database collection requested was not found"""


class AlreadyExistsError(DatabaseError):
    """Database collection requested already exists"""
