from django.conf import settings
from django.http.response import HttpResponse
from django.views.decorators.http import require_http_methods
from rest_framework import exceptions, status, response

from api.views import SamanthaCampaignsAPIView
from api.dowell.user import DowellUser
from .dbobjects import Campaign, CampaignMessage
from .utils import construct_dowell_email_template
from .serializers import CampaignSerializer, CampaignMessageSerializer
from rest_framework.response import Response
from .helpers import CustomResponse,CampaignHelper

from api.database import SamanthaCampaignsDB
from samantha_campaigns.settings import PROJECT_API_KEY
from api.dowell.datacube import DowellDatacube
import requests
from api.utils import _send_mail




class UserRegistrationView(SamanthaCampaignsAPIView):
    """
    Endpoint for user registration related operations to create a new users collection.
    """
    def get(self, request):
        """
        Get all collections and check if there's a collection created by the user.

        This method retrieves collections from the database and checks if a collection created by the user exists.
        
        :param request: The HTTP request object.
        :return: A response containing collection data or a message indicating the status of the operation.
        """
        workspace_id = request.query_params.get("workspace_id", None)
        collection_name = f"{workspace_id}_samantha_campaign"

        dowell_datacube = DowellDatacube(db_name=SamanthaCampaignsDB.name, dowell_api_key=PROJECT_API_KEY)

        try:
            response = dowell_datacube.fetch(
                _from=collection_name,
            )
            if not response:
                dowell_datacube.create_collection(name=collection_name)
                dowell_datacube.insert(collection_name, data={"database_created": False})
                return Response(
                    {
                        "success": False,
                        "message": f"The collection {collection_name} did not exist in the Database."
                                f"New collection  {collection_name} has been created."
                    }, status=200)
            else:
                    database_created = any(item.get('database_created', False) for item in response)
                    if not database_created:
                        id_not_created = next((item['_id'] for item in response if not item.get('database_created')), None)
                        return Response(
                            {
                                "success": False,
                                "message": "Database not created",
                                "id": id_not_created
                            },
                            status=status.HTTP_200_OK
                        )
                    else:
                        response_data = {
                            "success": True,
                            "database_created": database_created,
                            "message": "Database already created"
                        }
                        
                        return Response(
                            data=response_data,
                            status=status.HTTP_200_OK
                        )
                
        except Exception as err :
            return CustomResponse(False, str(err), None, status.HTTP_501_NOT_IMPLEMENTED)
             

    def post(self, request):
        """
        Update database with user registration data.

        This method updates the database with user registration data.
        
        :param request: The HTTP request object.
        :return: A response indicating the success or failure of the database update operation.
        """
        try:
            workspace_id = request.query_params.get("workspace_id")
            collection_name = f"{workspace_id}_samantha_campaign"
            id = request.data.get("id")
            print(id, collection_name)

            payload = {
                "api_key": PROJECT_API_KEY,
                "db_name": "Samanta_CampaignDB",
                "coll_name": collection_name,
                "operation": "update",
                "query": {"_id": id},
                "update_data": {"database_created": True}
            }

            response = requests.put("https://datacube.uxlivinglab.online/db_api/crud/", json=payload)
            response_data = response.json()

            print(response_data)

            if not response_data:
                return Response({
                    "success": False,
                    "database_created": False,
                    "message": "Database not updated"
                }, status=200)
            else:
                return Response({
                    "success": True,
                    "database_created": True,
                    "message": "Database updated"
                }, status=200)

        except Exception as err:
            return CustomResponse(False, str(err), None, status.HTTP_400_BAD_REQUEST)


class TestEmail(SamanthaCampaignsAPIView):
    def post(self, request, *args, **kwargs):
        try:
            workspace_id = request.query_params.get("workspace_id")
            user = DowellUser(workspace_id=workspace_id)
            user_api_key = user.api_key

            campaign_id = request.data.get("campaign_id")
            recipient_address = request.data.get("recipient_address")
            sender_address = request.data.get("sender_address")
            sender_name = "SAMANTHA CAMPAIGN"
            recipient_name = request.data.get("recipient_name")

            collection_name = f"{workspace_id}_samantha_campaign"
            dowell_datacube = DowellDatacube(db_name=SamanthaCampaignsDB.name, dowell_api_key=user_api_key)
            campaign_list = dowell_datacube.fetch(
                _from=collection_name,
                filters={"_id": campaign_id}
            )

            if campaign_list and campaign_list[0].get("message"):
                message = campaign_list[0]["message"]
                subject = message.get("subject")
                body = message.get("body")

                _send_mail(
                    subject=subject,
                    body=construct_dowell_email_template(
                        subject=subject,
                        body=body,
                        recipient=recipient_address
                    ),
                    sender_address=sender_address,
                    recipient_address=recipient_address,
                    sender_name=sender_name,
                    recipient_name=recipient_name,
                )
                return Response({
                    "success": True,
                    "message": "Email sent"
                }, status=200)
            else:
                return Response({
                    "success": False,
                    "message": "No message found for the campaign."
                }, status=400)

        except Exception as e:
            return Response({
                "success": False,
                "message": f"Failed to send email. Error: {str(e)}"
            }, status=500)






class CampaignListCreateAPIView(SamanthaCampaignsAPIView):
    
    def get(self, request, *args, **kwargs):
        """
        Get all campaigns created by the user
        """
        workspace_id = request.query_params.get("workspace_id", None)
        page_size = request.query_params.get("page_size", 16)
        page_number = request.query_params.get("page_number", 1)
        collection_name = f"{workspace_id}_samantha_campaign"
        dowell_datacube = DowellDatacube(db_name=SamanthaCampaignsDB.name, dowell_api_key=PROJECT_API_KEY)
        # try:
        #     page_number = int(page_number)
        #     page_size = int(page_size)
        # except ValueError:
        #     raise exceptions.NotAcceptable("Invalid page number or page size.")
        response = dowell_datacube.fetch(
            _from=collection_name,
            limit=50,
            offset=0,
        )
        campaigns = response
        data = []
        # 
        necessities = (
              "id", "title", "type", "image",
        #
              "broadcast_type", "start_date", 
              "end_date", "is_active", "has_launched"
          )
        for campaign in campaigns:
            campaign_data = campaign
            filtered_campaign_data = {}
            for key in necessities:
                if key == 'id':
                    filtered_campaign_data['id'] = campaign_data.get('_id', None)  # Ensure to use '_id' for MongoDB
                elif key in campaign_data:
                    filtered_campaign_data[key] = campaign_data[key]
                else:
                    filtered_campaign_data[key] = None  # Or any default value you prefer
            data.append(filtered_campaign_data)


        
        response_data = {
             "count": len(data),
            #  "page_size": page_size,
            #  "page_number": page_number,
             "results": data,
         }
        # if page_number > 1:
        #      response_data["previous_page"] = f"{request.path}?workspace_id={workspace_id}&page_size={page_size}&page_number={page_number - 1}"
        # if len(data) == page_size:
        #      response_data["next_page"] = f"{request.path}?workspace_id={workspace_id}&page_size={page_size}&page_number={page_number + 1}"

        return Response(
            data=response_data, 
            status=status.HTTP_200_OK
        )
    

    def post(self, request, *args, **kwargs):
        """
        Create a new campaign

        Request Body Format:
        {
            "type": "",
            "broadcast_type": "",
            "title": "",
            "purpose": "",
            "image": "",
            "keyword": "",
            "target_city": "",
            "target_audience": "",
            "range": 100,
            "frequency": "",
            "start_date": "",
            "end_date": "",
            "audiences": [],
            "leads_links": []
        }
        """
        workspace_id = request.query_params.get("workspace_id", None)
        collection_name = f"{workspace_id}_samantha_campaign"

        data = request.data

        if not isinstance(data, dict):
            raise exceptions.NotAcceptable("Request body must be a dictionary.")
        data['default_message'] = True

        serializer = CampaignSerializer(
            data=data,
        )
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.data

        dowell_datacube = DowellDatacube(db_name=SamanthaCampaignsDB.name, dowell_api_key=PROJECT_API_KEY)
        response = dowell_datacube.insert(
            _into=collection_name,
            data=validated_data  # Insert validated data directly
        )
        inserted_id = response.get("inserted_id")
        campaign_title = validated_data.get("title")
        campaign_purpose = validated_data.get("purpose")
        default_message = {
            "subject": campaign_title,
            "body": campaign_purpose,
            "is_default": True
        }

        campaign_helper = CampaignHelper(workspace_id)

        message_serializer = CampaignMessageSerializer(
            data=default_message,
        )
        message_serializer.is_valid(raise_exception=True)
        validated_message = message_serializer.data

        message_response = dowell_datacube.update(
            _in=collection_name,
            filter={
                "_id": inserted_id
            },
            data={"message": validated_message}  # Insert validated data directly
        )
        updated_campaign = dowell_datacube.fetch(
            _from=collection_name,
            filters={
                "_id": inserted_id
            }
        )
        campaign_id = updated_campaign[0]['_id']

        can_launch, reason, percentage_ready = campaign_helper.is_launchable(campaign_id)
        data = {
            "Campaign": updated_campaign,
            "launch_status": {
                "can_launch": can_launch,
                "reason": reason,
                "percentage_ready": percentage_ready
            }
        }

        return Response(
            data=data,
            status=status.HTTP_200_OK
        )


class CampaignRetrieveUpdateDeleteAPIView(SamanthaCampaignsAPIView):
    """Campaign Retrieve and Update API View"""

    def get(self, request, *args, **kwargs):
        """
        Retrieve a campaign by id
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        
        user = DowellUser(workspace_id=workspace_id)
        campaign: Campaign = Campaign.manager.get(
            creator_id=workspace_id, 
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )
        serializer = CampaignSerializer(
            instance=campaign, 
            context={"dowell_api_key": settings.PROJECT_API_KEY}
        )

        can_launch, reason, percentage_ready = campaign.is_launchable(dowell_api_key=settings.PROJECT_API_KEY)
        data = {
            **serializer.data,
            "next_due_date": campaign.next_due_date,
            "has_audiences": campaign.has_audiences,
            "has_message": campaign.get_message(dowell_api_key=settings.PROJECT_API_KEY) is not None,
            "launch_status": {
                "can_launch": can_launch,
                "reason": reason,
                "percentage_ready": percentage_ready
            }
        }
        return response.Response(
            data=data, 
            status=status.HTTP_200_OK
        )


    def put(self, request, *args, **kwargs):
        """
        Update a campaign
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        data = request.data
        if not isinstance(data, dict):
            raise exceptions.NotAcceptable("Request body must be a dictionary.")
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        
        user = DowellUser(workspace_id=workspace_id)
        campaign: Campaign = Campaign.manager.get(
            creator_id=workspace_id, 
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )
        
        serializer = CampaignSerializer(
            instance=campaign, 
            data=data, 
            context={"dowell_api_key": settings.PROJECT_API_KEY}
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()

        return response.Response(
            data=serializer.data, 
            status=status.HTTP_200_OK
        )
    

    def patch(self, request, *args, **kwargs):
        """
        Partially update a campaign
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        data = request.data
        if not isinstance(data, dict):
            raise exceptions.NotAcceptable("Request body must be a dictionary.")
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")

        user = DowellUser(workspace_id=workspace_id)
        campaign: Campaign = Campaign.manager.get(
            creator_id=workspace_id, 
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )
        
        serializer = CampaignSerializer(
            instance=campaign, 
            data=data, 
            partial=True, 
            context={"dowell_api_key": settings.PROJECT_API_KEY}
        )
        serializer.is_valid(raise_exception=True)
        campaign = serializer.save()
        
        can_launch, reason, percentage_ready = campaign.is_launchable(dowell_api_key=settings.PROJECT_API_KEY)
        data = {
            **campaign.data,
            "launch_status": {
                "can_launch": can_launch,
                "reason": reason,
                "percentage_ready": percentage_ready
            }
        }
        return response.Response(
            data=data,
            status=status.HTTP_200_OK
        )


    def delete(self, request, *args, **kwargs):
        """
        Delete a campaign
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        
        user = DowellUser(workspace_id=workspace_id)
        campaign: Campaign = Campaign.manager.get(creator_id=workspace_id, pkey=campaign_id, dowell_api_key=settings.PROJECT_API_KEY)
        campaign.delete(dowell_api_key=settings.PROJECT_API_KEY)

        return response.Response(
            data={
                "detail": "Campaign deleted successfully."
            },
            status=status.HTTP_200_OK
        )



class CampaignActivateDeactivateAPIView(SamanthaCampaignsAPIView):
    """Campaign Activate and Deactivate API View"""

    def get(self, request, *args, **kwargs):
        """
        Activate or deactivate campaign
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
    
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        
        user = DowellUser(workspace_id=workspace_id)
        campaign: Campaign = Campaign.manager.get(
            creator_id=workspace_id,
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )

        if campaign.is_active:
            campaign.deactivate(dowell_api_key=settings.PROJECT_API_KEY)
            msg = f"Campaign: '{campaign.title}', has been deactivated."
        else:
            campaign.activate(dowell_api_key=settings.PROJECT_API_KEY)
            msg = f"Campaign: '{campaign.title}', has been activated."
            
        return response.Response(
            data={
                "detail": msg
            },
            status=status.HTTP_200_OK
        )   



class CampaignAudienceListAddAPIView(SamanthaCampaignsAPIView):
    """Campaign Audience List API View"""

    def get(self, request, *args, **kwargs):
        """
        Get all campaign audiences
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        
        user = DowellUser(workspace_id=workspace_id)
        campaign = Campaign.manager.get(
            creator_id=workspace_id, 
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )
        
        return response.Response(
            data=campaign.data["audiences"], 
            status=status.HTTP_200_OK
        )

    
    def post(self, request, *args, **kwargs):
        """
        Add audiences to a campaign

        Request Body Format:
        ```
        {
            "audiences": []
        }
        ```
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        data = request.data
        if not isinstance(data, dict):
            raise exceptions.NotAcceptable("Request body must be a dictionary.")
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")

        audiences = data.get("audiences", [])
        if not isinstance(audiences, list):
            raise exceptions.NotAcceptable("Audiences must be a list")
        if not audiences:
            raise exceptions.NotAcceptable("Audiences must be provided.")

        user = DowellUser(workspace_id=workspace_id)
        campaign = Campaign.manager.get(
            creator_id=workspace_id, 
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )

        for audience in audiences:
            campaign.add_audience(audience)
        campaign.save(dowell_api_key=settings.PROJECT_API_KEY)

        return response.Response(
            data=campaign.data["audiences"], 
            status=status.HTTP_200_OK
        )



@require_http_methods(["GET"])
def campaign_audience_unsubscribe_view(request, *args, **kwargs):
    """
    Unsubscribes an audience from a campaign
    """
    campaign_id = kwargs.get("campaign_id", None)
    audience_id = request.GET.get("audience_id", None)
    msg = "You have successfully unsubscribed from this campaign."

    try:
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        if not audience_id:
            raise exceptions.NotAcceptable("Audience id must be provided.")

        campaign = Campaign.manager.get(
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )

        audience = campaign.audiences.get(id=audience_id)
        try:
            audience.unsubscribe()
        except:
            msg = "You have already been unsubscribed from this campaign."
        finally:
            campaign.save(dowell_api_key=settings.PROJECT_API_KEY)

    except:
        msg = "<h3>Something went wrong! Please check the link and try again.</h3>"
        return HttpResponse(msg, status=400)

    html_response = construct_dowell_email_template(
        subject=f"Unsubscribe from Campaign: '{campaign.title}'",
        body=msg,
        recipient=audience.email
    )
    return HttpResponse(html_response, status=200)



class CampaignMessageCreateRetreiveAPIView(SamanthaCampaignsAPIView):
    """Campaign Message Create and Retrieve API View"""

    def get(self, request, *args, **kwargs):
        """
        Get campaign message 
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        
        collection_name = f"{workspace_id}_samantha_campaign"
        dowell_datacube = DowellDatacube(db_name=SamanthaCampaignsDB.name, dowell_api_key=PROJECT_API_KEY)
        campaign_response = dowell_datacube.fetch(
            _from=collection_name,
            filters={
                "_id": campaign_id
            }
        )
        
        if not campaign_response:
            raise exceptions.NotFound("Campaign not found.")
       
        message = campaign_response[0].get("message", None)  # Get the message field from the response
        return response.Response(
            data=message, 
            status=status.HTTP_200_OK
        )
    
    
    def post(self, request, *args, **kwargs):
        """
        Add a message to a campaign

        Request Body Format:
        ```
        {
            "subject": "",
            "body": "",
            "sender": ""
            "is_default": "",
            "is_html_email: "",
            "html_email_link": "",
        }
        ```
        """
        workspace_id = request.query_params.get("workspace_id", None)
        data = request.data
        if not isinstance(data, dict):
            raise exceptions.NotAcceptable("Request body must be a dictionary.")
        
        campaign_id = kwargs.get("campaign_id", None)
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        
        collection_name = f"{workspace_id}_samantha_campaign"
        dowell_datacube = DowellDatacube(db_name=SamanthaCampaignsDB.name, dowell_api_key=PROJECT_API_KEY)
        campaign = dowell_datacube.fetch(
            _from=collection_name,
            filters={
                "_id": campaign_id
            }
        ) 
        if not campaign:
          raise exceptions.NotFound("Campaign not found.")

        message_serializer = CampaignMessageSerializer(data=data)
        message_serializer.is_valid(raise_exception=True)
        validated_message = message_serializer.validated_data
        

     
        dowell_datacube.update(
            _in=collection_name,
            filter={
                "_id": campaign_id
            },
            data={
                "default_message": False,
                "message": validated_message
                }
        )

        updated_campaign = dowell_datacube.fetch(
            _from=collection_name,
            filters={
                "_id": campaign_id
            }
        ) 
        updated_message = updated_campaign[0].get("message", None)
        
        return response.Response(
            data=updated_message, 
            status=status.HTTP_200_OK
        )




class CampaignMessageUpdateDeleteAPIView(SamanthaCampaignsAPIView):
    """Update and Delete Campaign Message API View"""

    def put(self, request, *args, **kwargs):
        """
        Update campaign message
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        message_id = kwargs.get("message_id", None)
        data = request.data
        if not isinstance(data, dict):
            raise exceptions.NotAcceptable("Request body must be a dictionary.")
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        if not message_id:
            raise exceptions.NotAcceptable("Message id must be provided.")
        
        user = DowellUser(workspace_id=workspace_id)
        message = CampaignMessage.manager.get(
            pkey=message_id, 
            campaign_id=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )
        
        serializer = CampaignMessageSerializer(
            instance=message, 
            data=data, 
            context={"dowell_api_key": settings.PROJECT_API_KEY}
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()

        campaign: Campaign = Campaign.manager.get(
            creator_id=workspace_id, 
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )

        campaign.default_message = False
        campaign.save(dowell_api_key=settings.PROJECT_API_KEY)

        return response.Response(
            data=serializer.data, 
            status=status.HTTP_200_OK
        )


    def patch(self, request, *args, **kwargs):
        """
        Partially update campaign message
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        message_id = kwargs.get("message_id", None)
        data = request.data
        if not isinstance(data, dict):
            raise exceptions.NotAcceptable("Request body must be a dictionary.")
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        if not message_id:
            raise exceptions.NotAcceptable("Message id must be provided.")
        
        user = DowellUser(workspace_id=workspace_id)
        message = CampaignMessage.manager.get(
            pkey=message_id, 
            campaign_id=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )
        
        serializer = CampaignMessageSerializer(
            instance=message, data=data, 
            partial=True, 
            context={"dowell_api_key": settings.PROJECT_API_KEY}
        )
        serializer.is_valid(raise_exception=True)
        serializer.save()

        campaign: Campaign = Campaign.manager.get(
            creator_id=workspace_id, 
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )

        campaign.default_message = False
        campaign.save(dowell_api_key=settings.PROJECT_API_KEY)

        return response.Response(
            data=serializer.data, 
            status=status.HTTP_200_OK
        )



class CampaignLaunchAPIView(SamanthaCampaignsAPIView):

    def get(self, request, *args, **kwargs):
        """
        Launch a campaign 
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        
        user = DowellUser(workspace_id=workspace_id)
        campaign = Campaign.manager.get(
            pkey=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )
        campaign.launch(dowell_api_key=settings.PROJECT_API_KEY)

        return response.Response(
            data={
                "detail": "Campaign launched successfully."
            }, 
            status=status.HTTP_200_OK
        )
    



campaign_list_create_api_view = CampaignListCreateAPIView.as_view()
campaign_retreive_update_delete_api_view = CampaignRetrieveUpdateDeleteAPIView.as_view()
campaign_activate_deactivate_api_view = CampaignActivateDeactivateAPIView.as_view()
campaign_audience_list_add_api_view = CampaignAudienceListAddAPIView.as_view()
campaign_message_create_retrieve_api_view = CampaignMessageCreateRetreiveAPIView.as_view()
campaign_message_update_delete_api_view = CampaignMessageUpdateDeleteAPIView.as_view()
campaign_launch_api_view = CampaignLaunchAPIView.as_view()
user_registration_view = UserRegistrationView.as_view()
test_email_view = TestEmail.as_view()