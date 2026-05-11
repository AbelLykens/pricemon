from django.urls import path

from .views import (
    CandlesView,
    CurrentPriceView,
    HealthView,
    HistoryView,
    OverviewView,
    WeightedPricesView,
)

urlpatterns = [
    path("prices/", WeightedPricesView.as_view(), name="weighted-prices"),
    path("current/", CurrentPriceView.as_view(), name="current-prices"),
    path("candles/", CandlesView.as_view(), name="candles"),
    path("health/", HealthView.as_view(), name="health"),
]

# Mounted at the project root in pricemon/urls.py
overview_url = path("", OverviewView.as_view(), name="overview")
history_url = path("history/", HistoryView.as_view(), name="history")
