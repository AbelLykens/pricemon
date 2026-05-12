from django.contrib import admin
from django.urls import include, path
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)

from api.urls import history_url, overview_url

urlpatterns = [
    overview_url,
    history_url,
    path("admin/", admin.site.urls),
    # OpenAPI schema + interactive docs. Live under /api/v1/ only — not aliased.
    path("api/v1/schema/", SpectacularAPIView.as_view(), name="schema"),
    path(
        "api/v1/docs/",
        SpectacularSwaggerView.as_view(url_name="schema"),
        name="swagger-ui",
    ),
    path(
        "api/v1/redoc/",
        SpectacularRedocView.as_view(url_name="schema"),
        name="redoc",
    ),
    path("api/v1/", include("api.urls")),
    # Unversioned alias for back-compat. New breaking changes go under /api/v2/.
    path("api/", include("api.urls")),
]
