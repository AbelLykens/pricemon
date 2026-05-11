from django.db import models


class Exchange(models.Model):
    slug = models.SlugField(max_length=32, unique=True)
    name = models.CharField(max_length=64)
    is_active = models.BooleanField(default=True)

    class Meta:
        ordering = ["slug"]

    def __str__(self) -> str:
        return self.slug


class Currency(models.Model):
    code = models.CharField(max_length=16, unique=True)
    name = models.CharField(max_length=64, blank=True)
    is_quote = models.BooleanField(
        default=False,
        help_text="True if this currency is used as a quote (e.g. USD, EUR).",
    )
    peg_to = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        related_name="pegs",
        help_text=(
            "If this currency is a stablecoin pegged to a fiat (e.g. USDT→USD), "
            "the target fiat. The API uses live stable-to-fiat VWAP to merge "
            "stablecoin-quoted volume into the underlying fiat bucket."
        ),
    )

    class Meta:
        ordering = ["code"]
        verbose_name_plural = "currencies"

    def __str__(self) -> str:
        return self.code


class TradingPair(models.Model):
    """A (exchange, base, quote) instrument that we subscribe to.

    `cryptofeed_symbol` is the normalized BASE-QUOTE form cryptofeed uses
    (e.g. ``"BTC-USD"``). It is what callbacks emit as ``trade.symbol``.
    """

    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE, related_name="pairs")
    base = models.ForeignKey(Currency, on_delete=models.PROTECT, related_name="base_pairs")
    quote = models.ForeignKey(Currency, on_delete=models.PROTECT, related_name="quote_pairs")
    cryptofeed_symbol = models.CharField(max_length=32)
    is_active = models.BooleanField(default=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["exchange", "base", "quote"],
                name="uniq_pair_per_exchange",
            ),
            models.UniqueConstraint(
                fields=["exchange", "cryptofeed_symbol"],
                name="uniq_symbol_per_exchange",
            ),
        ]
        ordering = ["exchange__slug", "cryptofeed_symbol"]

    def __str__(self) -> str:
        return f"{self.exchange.slug}:{self.cryptofeed_symbol}"


class MinuteAggregate(models.Model):
    """Per-minute trade statistics for one (exchange, pair).

    `minute_start` is the UTC minute floor: trades whose timestamp falls in
    [minute_start, minute_start + 60s) are aggregated here.
    """

    pair = models.ForeignKey(TradingPair, on_delete=models.CASCADE, related_name="minutes")
    minute_start = models.DateTimeField(db_index=True)
    trade_count = models.PositiveIntegerField()
    volume_base = models.DecimalField(max_digits=38, decimal_places=18)
    volume_quote = models.DecimalField(max_digits=38, decimal_places=18)
    price_min = models.DecimalField(max_digits=38, decimal_places=18)
    price_max = models.DecimalField(max_digits=38, decimal_places=18)
    price_avg = models.DecimalField(max_digits=38, decimal_places=18)
    price_vwap = models.DecimalField(max_digits=38, decimal_places=18)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["pair", "minute_start"],
                name="uniq_minute_per_pair",
            ),
        ]
        indexes = [
            models.Index(fields=["pair", "-minute_start"], name="agg_pair_minute_desc"),
        ]
        ordering = ["-minute_start"]

    def __str__(self) -> str:
        return f"{self.pair} @ {self.minute_start.isoformat()}"
