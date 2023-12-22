from django.urls import path

from . import views


app_name = "reports"

urlpatterns = [
    path("", views.campaign_run_reports_list_api_view, name="campaign-run-reports-list"),
]


