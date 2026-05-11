from django.contrib import admin
from django.urls import include, path

from api.urls import overview_url

urlpatterns = [
    overview_url,
    path("admin/", admin.site.urls),
    path("api/", include("api.urls")),
]
