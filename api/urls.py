from django.urls import path

from .views import CurrentPriceView, HealthView, OverviewView, WeightedPricesView

urlpatterns = [
    path("prices/", WeightedPricesView.as_view(), name="weighted-prices"),
    path("current/", CurrentPriceView.as_view(), name="current-prices"),
    path("health/", HealthView.as_view(), name="health"),
]

# Mounted at the project root in pricemon/urls.py
overview_url = path("", OverviewView.as_view(), name="overview")
