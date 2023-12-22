from rest_framework import status, response
from django.views.decorators import csrf
from rest_framework.views import APIView
from django.utils.decorators import method_decorator



@method_decorator(csrf.csrf_exempt, name='dispatch')
class HealthCheckAPIView(APIView):
    """API Status"""
    def get(self, request, *args, **kwargs):
        return response.Response(
            data={"detail": "If you are seeing this, the server is !down"}, 
            status=status.HTTP_200_OK
        )

health_check_api_view = HealthCheckAPIView.as_view()

