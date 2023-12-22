from django.conf import settings
from django.dispatch import receiver

from .dbobjects import CampaignRunReport
from campaigns.dbobjects import Campaign
from api.objects.signals import pre_delete



@receiver(pre_delete, sender=Campaign)
def pre_campaign_delete(sender: type[Campaign], **kwargs):
    campaign = kwargs.get("instance")
    run_reports = CampaignRunReport.manager.filter(
        campaign_id=campaign.pkey,
        dowell_api_key=settings.PROJECT_API_KEY
    )
    run_reports.delete(dowell_api_key=settings.PROJECT_API_KEY)
