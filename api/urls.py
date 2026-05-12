from django.urls import path

from .views import (
    CandlesAggView,
    CandlesView,
    CurrentPriceView,
    HealthView,
    HistoryView,
    InternalAggregatesView,
    OverviewView,
    SummaryView,
    WeightedPricesView,
)

urlpatterns = [
    path("prices/", WeightedPricesView.as_view(), name="weighted-prices"),
    path("current/", CurrentPriceView.as_view(), name="current-prices"),
    path("candles/", CandlesView.as_view(), name="candles"),
    path("candles/agg/", CandlesAggView.as_view(), name="candles-agg"),
    path("health/", HealthView.as_view(), name="health"),
    # Auth-gated, excluded from the OpenAPI schema (see api/schema_hooks.py).
    path("internal/aggregates/", InternalAggregatesView.as_view(), name="internal-aggregates"),
]

# Mounted at the project root in pricemon/urls.py
overview_url = path("", OverviewView.as_view(), name="overview")
history_url = path("history/", HistoryView.as_view(), name="history")
# Compatibility endpoint for the cheeserobot.org app — path is locked.
summary_json_url = path("price/summary.json", SummaryView.as_view(), name="summary-json")
