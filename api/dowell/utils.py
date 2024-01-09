from django.conf import settings
import requests

from .exceptions import UserNotFound


def get_dowell_user_info(client_admin_id: str):
    """
    Gets the Dowell user info for the user with the client admin id provided

    :param client_admin_id: The Dowell client admin id of the user.
    """
    response = requests.get(
        url=settings.DOWELL_USER_SERVICE_KEY_URL,
        params={
            "type": "get_api_key",
            "workspace_id": client_admin_id,
        }
    )
    if response.status_code == 200:
        if response.json()["success"] == True:
            # print("the response is",response.json()['data'])
            return response.json()['data']
        raise UserNotFound(client_admin_id)
    response.raise_for_status()
    return None


def find_parent_service(subservice: str, user):
    """
    Finds the parent service of a subservice for a user.

    :param subservice_id: The subservice ID/name of the subservice in the parent service.
    :param user: The DowellUser object or workspace ID of user for whom the parent service is to be found.
    :return: The parent DowellService object if found, else None.
    """
    from .user import DowellUser
    user = user if isinstance(user, DowellUser) else DowellUser(workspace_id=user)
    for service in user.services:
        for subserv in service.subservices:
            if subservice in ( subserv.id, subserv.name ):
                return service
    return None
