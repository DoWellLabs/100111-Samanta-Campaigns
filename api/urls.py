from django.urls import path, include

from . import views

app_name = "api"

urlpatterns = [
    path("campaigns/", include("campaigns.urls"), name="campaigns"),
    path("audiences/", views.audience_list_sort_api_view, name="audience-list-sort"),
]


