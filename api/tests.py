from decimal import Decimal

from django.test import SimpleTestCase

from api.outliers import (
    OutlierReport,
    clip_wick,
    consensus_vwap,
    filter_exchange_outliers,
)


def _row(exchange: str, vwap, volume_base="1"):
    return {
        "exchange": exchange,
        "vwap": Decimal(str(vwap)),
        "volume_base": Decimal(str(volume_base)),
    }


class ConsensusVwapTests(SimpleTestCase):
    def test_empty_returns_none(self):
        self.assertIsNone(consensus_vwap([]))

    def test_single_row(self):
        self.assertEqual(consensus_vwap([_row("a", 100)]), Decimal("100"))

    def test_odd_count_picks_middle(self):
        rows = [_row("a", 100), _row("b", 101), _row("c", 102)]
        self.assertEqual(consensus_vwap(rows), Decimal("101"))

    def test_even_count_averages_middle_two(self):
        rows = [_row("a", 100), _row("b", 102)]
        self.assertEqual(consensus_vwap(rows), Decimal("101"))

    def test_unweighted_so_big_volume_outlier_does_not_anchor(self):
        # The failure mode this filter exists for: one exchange has both the
        # biggest volume AND a fat-finger-pulled vwap. The healthy cluster
        # must stay the consensus.
        rows = [
            _row("bitstamp_bad", 68500, volume_base="1000"),
            _row("coinbase", 68900, volume_base="10"),
            _row("kraken", 68910, volume_base="10"),
            _row("binance", 68905, volume_base="10"),
        ]
        median = consensus_vwap(rows)
        self.assertEqual(median, (Decimal("68900") + Decimal("68905")) / 2)


class FilterExchangeOutliersTests(SimpleTestCase):
    def setUp(self):
        self.report = OutlierReport()

    def _filter(self, rows, *, max_dev_pct="0.5"):
        return filter_exchange_outliers(
            rows,
            base="BTC", quote="USD", minute="2026-05-11T14:00:00+00:00",
            max_dev_pct=Decimal(max_dev_pct),
            report=self.report,
        )

    def test_fewer_than_two_passthrough(self):
        rows = [_row("a", 100)]
        self.assertEqual(self._filter(rows), rows)
        self.assertEqual(self.report.excluded, [])

    def test_tight_cluster_keeps_all(self):
        rows = [_row("a", 100), _row("b", 100.1), _row("c", 99.9)]
        kept = self._filter(rows)
        self.assertEqual(len(kept), 3)
        self.assertEqual(self.report.excluded, [])

    def test_drops_single_outlier(self):
        # 3 exchanges agree on ~100, one at 105 (5% off → drops at 0.5% thresh)
        rows = [
            _row("a", 100), _row("b", 100.1), _row("c", 99.9),
            _row("bad", 105),
        ]
        kept = self._filter(rows)
        kept_ex = {r["exchange"] for r in kept}
        self.assertEqual(kept_ex, {"a", "b", "c"})
        self.assertEqual(len(self.report.excluded), 1)
        self.assertEqual(self.report.excluded[0]["exchange"], "bad")

    def test_drops_outlier_even_when_it_has_biggest_volume(self):
        # Regression: volume-weighted median would pick the bad exchange
        # because it has the largest volume. Unweighted median must not.
        rows = [
            _row("bad", 68500, volume_base="1000"),
            _row("a", 68900), _row("b", 68910), _row("c", 68905),
        ]
        kept = self._filter(rows)
        kept_ex = {r["exchange"] for r in kept}
        self.assertEqual(kept_ex, {"a", "b", "c"})
        self.assertEqual(len(self.report.excluded), 1)
        self.assertEqual(self.report.excluded[0]["exchange"], "bad")

    def test_keeps_at_least_one_when_everyone_diverges(self):
        # If every row is outside the threshold (legit volatility +
        # disagreement) we must still return at least one row.
        rows = [_row("a", 100), _row("b", 200)]
        kept = self._filter(rows, max_dev_pct="0.1")
        self.assertGreaterEqual(len(kept), 1)


class ClipWickTests(SimpleTestCase):
    def setUp(self):
        self.report = OutlierReport()

    def test_passthrough_when_within_bounds(self):
        new_min, new_max = clip_wick(
            Decimal("99.5"), Decimal("100.5"), Decimal("100"),
            max_pct=Decimal("2"), report=self.report,
        )
        self.assertEqual(new_min, Decimal("99.5"))
        self.assertEqual(new_max, Decimal("100.5"))
        self.assertEqual(self.report.wicks_clipped, 0)

    def test_clips_low_min_up_to_floor(self):
        # vwap=100, max_pct=2% → floor=98. min=95 → clipped to 98.
        new_min, new_max = clip_wick(
            Decimal("95"), Decimal("100.5"), Decimal("100"),
            max_pct=Decimal("2"), report=self.report,
        )
        self.assertEqual(new_min, Decimal("98"))
        self.assertEqual(new_max, Decimal("100.5"))
        self.assertEqual(self.report.wicks_clipped, 1)

    def test_clips_high_max_down_to_ceil(self):
        new_min, new_max = clip_wick(
            Decimal("99.5"), Decimal("110"), Decimal("100"),
            max_pct=Decimal("2"), report=self.report,
        )
        self.assertEqual(new_max, Decimal("102"))
        self.assertEqual(self.report.wicks_clipped, 1)

    def test_none_inputs_passthrough(self):
        new_min, new_max = clip_wick(
            None, None, Decimal("100"),
            max_pct=Decimal("2"), report=self.report,
        )
        self.assertIsNone(new_min)
        self.assertIsNone(new_max)
        self.assertEqual(self.report.wicks_clipped, 0)

    def test_nonpositive_vwap_passthrough(self):
        new_min, new_max = clip_wick(
            Decimal("50"), Decimal("200"), Decimal("0"),
            max_pct=Decimal("2"), report=self.report,
        )
        self.assertEqual(new_min, Decimal("50"))
        self.assertEqual(new_max, Decimal("200"))
        self.assertEqual(self.report.wicks_clipped, 0)
