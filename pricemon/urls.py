from django.contrib import admin
from django.urls import include, path

from api.urls import history_url, overview_url

urlpatterns = [
    overview_url,
    history_url,
    path("admin/", admin.site.urls),
    path("api/", include("api.urls")),
]
