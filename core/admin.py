from django.contrib import admin

from .models import Currency, Exchange, MinuteAggregate, TradingPair


@admin.register(Exchange)
class ExchangeAdmin(admin.ModelAdmin):
    list_display = ("slug", "name", "is_active")
    list_filter = ("is_active",)


@admin.register(Currency)
class CurrencyAdmin(admin.ModelAdmin):
    list_display = ("code", "name", "is_quote")
    list_filter = ("is_quote",)


@admin.register(TradingPair)
class TradingPairAdmin(admin.ModelAdmin):
    list_display = ("exchange", "cryptofeed_symbol", "base", "quote", "is_active")
    list_filter = ("exchange", "is_active", "quote")
    search_fields = ("cryptofeed_symbol",)


@admin.register(MinuteAggregate)
class MinuteAggregateAdmin(admin.ModelAdmin):
    list_display = ("pair", "minute_start", "trade_count", "price_vwap", "volume_base")
    list_filter = ("pair__exchange", "pair__quote")
    date_hierarchy = "minute_start"
