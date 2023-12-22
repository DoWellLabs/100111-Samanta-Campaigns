from rest_framework import exceptions, response, status
from django.conf import settings

from api.views import SamanthaCampaignsAPIView
from api.dowell.user import DowellUser
from .dbobjects import CampaignRunReport



class CampaignRunReportListAPIView(SamanthaCampaignsAPIView):
    """Campaign Run Report List API View"""
    def get(self, request, *args, **kwargs):
        """
        Get all campaign run reports
        """
        workspace_id = request.query_params.get("workspace_id", None)
        campaign_id = kwargs.get("campaign_id", None)
        if not campaign_id:
            raise exceptions.NotAcceptable("Campaign id must be provided.")
        
        user = DowellUser(workspace_id=workspace_id)
        reports = CampaignRunReport.manager.filter(
            campaign_id=campaign_id, 
            dowell_api_key=settings.PROJECT_API_KEY
        )
        data = [ report.data for report in reports ]
        
        return response.Response(
            data=data, 
            status=status.HTTP_200_OK
        )


campaign_run_reports_list_api_view = CampaignRunReportListAPIView.as_view()
