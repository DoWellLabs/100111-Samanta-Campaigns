import uuid
import datetime
from django.utils import timezone
from typing import Dict

from api.datacube.objects import DatacubeObject
from api.objects.utils import ttl_cache
from api.validators import validate_not_blank, MinMaxLengthValidator


class CampaignRunReport(DatacubeObject):
    """Report containing the events that occurred during the running of a Campaign"""
    config = DatacubeObject.new_config()
    config.use_daily_collection = True

    config.attributes = {
        "campaign_id": (str,),
        "title": (str,),
        "events": (list,),
        "created_at": (datetime.datetime,),
        "updated_at": (datetime.datetime,),
    }
    config.required = (
        "campaign_id",
        "title",
    )
    config.defaults = {
        "created_at": timezone.now,
        "events": [],
    }
    config.validators = {
        "campaign_id": [validate_not_blank],
        "title": [validate_not_blank, MinMaxLengthValidator(min_length=3, max_length=255)],
    }
    config.auto_now_datetimes = ("updated_at",)
    config.ordering = ("-created_at",)

    @ttl_cache(maxsize=12, ttl_seconds=60*60*24) # cache for 24 hours
    def get_campaign(self, dowell_api_key: str):
        """
        The Campaign that the report belongs to
        """
        from campaigns.dbobjects import Campaign
        print("Second check for GET")
        return Campaign.manager.get(pkey=self.campaign_id, dowell_api_key=dowell_api_key)
    

    def get_events(self, sort_by_occurrence: bool = True, latest_first: bool = False):
        """
        Get the events that occurred during the running of the campaign

        :param sort_by_occurrence: Whether to sort the events by occurrence or not
        :param latest_first: Whether to sort the events with most recent first or not
        :return: List of events
        """
        events = self.events
        if sort_by_occurrence:
            events = sorted(events, key=lambda event: event["occurred_at"], reverse=latest_first)
        return events
    

    def add_event(self, event_type: str, data: Dict):
        """
        Add an event to the report

        :param event_type: Type of event that occurred. Supported values are: "info", "warning", "error"
        :param data: Data associated with the event
        """
        if not isinstance(event_type, str):
            raise TypeError("event_type must be a string")
        if event_type.lower() not in ("info", "warning", "error"):
            raise ValueError("event_type must be one of 'info', 'warning', or 'error'")
        if not isinstance(data, dict):
            raise TypeError("data must be a dictionary")
        if not data:
            raise ValueError("data cannot be empty")
        
        event = {
            "id": str(uuid.uuid4()),
            "type": event_type.upper(),
            "data": data,
            "occurred_at": timezone.now(),
        }
        self.events.append(event)
        return None
        
    
    def remove_event(self, event_id: str):
        """
        Remove an event from the report

        :param event_id: ID of the event to remove
        """
        if not isinstance(event_id, str):
            raise TypeError("event_id must be a string")
        if not event_id:
            raise ValueError("event_id cannot be empty")
        
        for event in self.events:
            if event["id"] == event_id:
                self.events.remove(event)
                break
        return None
    

    def serialize(self):
        serialized = super().serialize()
        serialized["events"] = self.get_events(latest_first=False)
        serialized["id"] = serialized.pop("pkey")
        return serialized


