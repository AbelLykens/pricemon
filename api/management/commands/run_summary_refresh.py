"""Background daemon: keep ``/price/summary.json`` warm in memcached.

The summary view is a memory-only read — it does not compute anything on
the request path. This daemon owns rebuilding the payload and writing it
to the live (short-TTL) and last-known-good (long-TTL) cache keys.

Operational behaviour:

* One process per host (systemd unit ``pricemon-summary.service``).
* Loop interval configurable via ``--interval`` (default 2 s) — fast enough
  that the live price block tracks the current-state cache.
* Every refresh is wrapped in ``try/except``; failures are logged and the
  loop sleeps a short backoff before the next attempt rather than crashing.
* ``sdnotify`` integration: ``READY=1`` after the first successful refresh,
  ``WATCHDOG=1`` pings every interval. Sustained failures stop the pings,
  letting systemd kill+restart via ``WatchdogSec``.
* Clean shutdown on SIGTERM/SIGINT.
"""

from __future__ import annotations

import logging
import signal
import time

import sdnotify
from django.core.management.base import BaseCommand

from api.views import refresh_summary_body

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Refresh the /price/summary.json body in cache on a loop."

    def add_arguments(self, parser):
        parser.add_argument(
            "--interval",
            type=float,
            default=2.0,
            help="Seconds between successful refreshes (default: 2.0).",
        )
        parser.add_argument(
            "--error-backoff",
            type=float,
            default=5.0,
            help="Seconds to sleep after a refresh failure (default: 5.0).",
        )
        parser.add_argument(
            "--once",
            action="store_true",
            help="Refresh once and exit (for one-off cache warm / tests).",
        )

    def handle(
        self,
        *args,
        interval: float,
        error_backoff: float,
        once: bool,
        **options,
    ):
        notifier = sdnotify.SystemdNotifier()
        stop = {"flag": False}

        def _request_stop(signum, _frame):
            log.info("stop requested via signal %s", signum)
            stop["flag"] = True

        signal.signal(signal.SIGTERM, _request_stop)
        signal.signal(signal.SIGINT, _request_stop)

        # First refresh: do it before READY=1 so the cache is warm by the
        # time systemd considers the unit started. If the very first attempt
        # fails we still send READY (otherwise systemd would think we never
        # came up) and rely on the loop's retry-with-backoff.
        consecutive_failures = 0
        last_ok_ts: float | None = None
        last_len: int = 0
        try:
            body_json = refresh_summary_body()
            last_ok_ts = time.time()
            last_len = len(body_json)
            log.info("initial refresh ok (%d bytes)", last_len)
        except Exception:
            consecutive_failures = 1
            log.exception("initial summary refresh failed")

        notifier.notify("READY=1")
        notifier.notify("STATUS=summary refresh daemon running")

        if once:
            return

        while not stop["flag"]:
            t0 = time.monotonic()
            try:
                body_json = refresh_summary_body()
                last_ok_ts = time.time()
                last_len = len(body_json)
                consecutive_failures = 0
            except Exception:
                consecutive_failures += 1
                log.exception(
                    "summary refresh failed (consecutive=%d)",
                    consecutive_failures,
                )

            # Only ping the watchdog while refreshes are succeeding. If
            # something is consistently broken (DB unreachable, code bug)
            # we want systemd to take the unit down and restart it after
            # a few cycles rather than masking the failure forever.
            if consecutive_failures == 0:
                try:
                    notifier.notify("WATCHDOG=1")
                    notifier.notify(
                        f"STATUS=last_ok={int(last_ok_ts or 0)} bytes={last_len}"
                    )
                except Exception:
                    log.exception("sdnotify failed")
            else:
                try:
                    notifier.notify(
                        f"STATUS=DEGRADED failures={consecutive_failures} "
                        f"last_ok={int(last_ok_ts or 0)}"
                    )
                except Exception:
                    log.exception("sdnotify failed")

            sleep_for = error_backoff if consecutive_failures else interval
            elapsed = time.monotonic() - t0
            remaining = sleep_for - elapsed
            # Wake periodically so SIGTERM is honored within ~0.5s instead
            # of waiting out the full interval.
            while remaining > 0 and not stop["flag"]:
                step = 0.5 if remaining > 0.5 else remaining
                time.sleep(step)
                remaining -= step

        notifier.notify("STOPPING=1")
        log.info("summary refresh daemon exited")
