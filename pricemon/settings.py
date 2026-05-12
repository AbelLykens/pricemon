"""Django settings for pricemon."""

import os
from decimal import Decimal
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv(BASE_DIR / ".env")


def _env(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value  # type: ignore[return-value]


SECRET_KEY = _env("DJANGO_SECRET_KEY", required=True)
DEBUG = _env("DJANGO_DEBUG", "0") == "1"
ALLOWED_HOSTS = [h.strip() for h in _env("DJANGO_ALLOWED_HOSTS", "*").split(",") if h.strip()]


INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "drf_spectacular",
    "core",
    "feeds",
    "api",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "pricemon.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "pricemon.wsgi.application"


DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": _env("POSTGRES_DB", "pricemon"),
        "USER": _env("POSTGRES_USER", "pricemon"),
        "PASSWORD": _env("POSTGRES_PASSWORD", required=True),
        "HOST": _env("POSTGRES_HOST", "localhost"),
        "PORT": _env("POSTGRES_PORT", "5432"),
        "CONN_MAX_AGE": 60,
    }
}


AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]


LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"


REST_FRAMEWORK = {
    "DEFAULT_RENDERER_CLASSES": [
        "rest_framework.renderers.JSONRenderer",
        "rest_framework.renderers.BrowsableAPIRenderer",
    ],
    "DEFAULT_PERMISSION_CLASSES": ["rest_framework.permissions.AllowAny"],
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
}

SPECTACULAR_SETTINGS = {
    "TITLE": "Pricemon API",
    "DESCRIPTION": (
        "Volume-weighted crypto prices aggregated across exchange WebSocket trade feeds. "
        "Endpoints return per-minute aggregates, live (sub-minute) state, and OHLC candles, "
        "with stablecoin-quoted volume merged into the underlying fiat using same-window rates.\n\n"
        "[← Back to overview](/)"
    ),
    "VERSION": "1.0.0",
    "SERVE_INCLUDE_SCHEMA": False,
    # Only document the canonical /api/v1/ tree; the /api/ alias is omitted from the spec.
    "SCHEMA_PATH_PREFIX": r"/api/v1",
    "PREPROCESSING_HOOKS": ["api.schema_hooks.keep_v1_only"],
    "SWAGGER_UI_SETTINGS": {
        "deepLinking": True,
        "persistAuthorization": False,
        "displayOperationId": False,
    },
    "TAGS": [
        {"name": "prices", "description": "Window-based and live volume-weighted prices."},
        {"name": "candles", "description": "Per-minute OHLC, optionally bucketed."},
        {"name": "ops", "description": "Health and meta endpoints."},
    ],
}


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(asctime)s %(levelname)s %(name)s | %(message)s"},
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "default"},
    },
    "root": {"handlers": ["console"], "level": _env("DJANGO_LOG_LEVEL", "INFO")},
    "loggers": {
        "django.db.backends": {"level": "WARNING", "propagate": True},
        "feeds": {"level": _env("FEEDS_LOG_LEVEL", "INFO"), "propagate": True},
    },
}


CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.memcached.PyMemcacheCache",
        "LOCATION": _env("MEMCACHED_LOCATION", "127.0.0.1:11211"),
        "KEY_PREFIX": "pmcur",
        "TIMEOUT": 120,
    }
}

# Per-view response cache TTLs (seconds). Tunable via env so we can dial
# freshness vs. DB load without a redeploy. Set to 0 to disable a view's cache.
API_CACHE_TTL_PRICES = int(_env("API_CACHE_TTL_PRICES", "10"))
API_CACHE_TTL_CANDLES = int(_env("API_CACHE_TTL_CANDLES", "20"))
API_CACHE_TTL_OVERVIEW = int(_env("API_CACHE_TTL_OVERVIEW", "5"))
API_CACHE_TTL_HISTORY = int(_env("API_CACHE_TTL_HISTORY", "30"))


# Pricemon-specific knobs
FEED_WATCHDOG_TIMEOUT_SEC = int(_env("FEED_WATCHDOG_TIMEOUT_SEC", "60"))
FEED_RECONNECT_RETRIES = int(_env("FEED_RECONNECT_RETRIES", "-1"))
# Daemons re-publish a pair's current-state to memcached at most this often.
CURRENT_PUBLISH_MIN_INTERVAL_SEC = float(
    _env("CURRENT_PUBLISH_MIN_INTERVAL_SEC", "0.5")
)
# A current-state entry is considered live if updated within this window.
CURRENT_FRESH_SEC = int(_env("CURRENT_FRESH_SEC", "10"))

# Read-time outlier filtering. Non-destructive: stored data is untouched,
# filters apply only when views aggregate per-(exchange, minute) rows.
OUTLIER_FILTER_ENABLED = _env("OUTLIER_FILTER_ENABLED", "1") == "1"
# Drop an exchange's row from a (base, quote, minute) group if its vwap is
# more than this many percent from the volume-weighted cross-exchange median.
OUTLIER_EXCHANGE_MAX_DEV_PCT = Decimal(_env("OUTLIER_EXCHANGE_MAX_DEV_PCT", "0.5"))
# Clip per-exchange price_min/price_max to vwap ± this many percent before
# they feed into merged OHLC wicks.
OUTLIER_WICK_MAX_PCT = Decimal(_env("OUTLIER_WICK_MAX_PCT", "2.0"))


# --- Cross-host backfill ---
# Server side (fallback role): bearer token that the /api/v1/internal/
# aggregates endpoint requires. Unset => endpoint is disabled (returns 403).
BACKFILL_API_TOKEN = _env("BACKFILL_API_TOKEN", "")

# Client side (primary role): how to reach the fallback's internal endpoint,
# plus the token it expects. Unset => `manage.py backfill_from_fallback`
# refuses to run.
FALLBACK_BASE_URL = _env("FALLBACK_BASE_URL", "")
FALLBACK_BACKFILL_TOKEN = _env("FALLBACK_BACKFILL_TOKEN", "")
# Per-request timeout for backfill HTTP calls (seconds).
FALLBACK_BACKFILL_TIMEOUT_SEC = int(_env("FALLBACK_BACKFILL_TIMEOUT_SEC", "30"))
