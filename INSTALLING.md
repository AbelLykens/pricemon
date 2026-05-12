# Installing Pricemon on Debian 13 (Trixie)

End-to-end setup for a fresh Debian 13 box: Postgres, memcached, gunicorn,
nginx, the Django web service, and one systemd unit per exchange feed.

This guide describes a **live-only fallback instance**: it serves the live
`/current/` endpoint and runs the feed daemons, but does not seed the static
exchange/pair registry and does not import historical archives. The primary
instance remains the source of truth for accumulated history.

Targets the reference layout used in `deploy/` — app at `/opt/pricemon`,
venv at `/opt/venv`, dedicated `pricemon` system user, runtime socket at
`/run/pricemon/gunicorn.sock`. If you change any of those paths, also edit
`deploy/systemd/pricemon-web.service`, `deploy/systemd/pricemon-feed@.service`
and `deploy/nginx/pricemon.conf` before installing them.

All commands assume root (use `sudo -i` or prefix with `sudo`).

## 1. System packages

Debian 13 ships Python 3.13 as `python3`, which matches the venv this repo
was developed against.

```bash
apt update
apt install -y \
    python3 python3-venv python3-dev \
    build-essential pkg-config \
    postgresql postgresql-contrib \
    memcached libmemcached-tools \
    nginx \
    git curl ca-certificates
```

Verify versions:

```bash
python3 --version          # expect 3.13.x
psql --version
nginx -v
```

## 2. Postgres database and role

```bash
sudo -u postgres psql <<'SQL'
CREATE ROLE pricemon WITH LOGIN PASSWORD 'CHANGE_ME_STRONG_PASSWORD';
CREATE DATABASE pricemon OWNER pricemon;
SQL
```

Keep the password — it goes into `.env` as `POSTGRES_PASSWORD`. Local
connections use peer/md5 by default; no `pg_hba.conf` change is needed
when the app and Postgres are on the same host.

## 3. App user and directories

```bash
adduser --system --group --home /opt/pricemon --shell /usr/sbin/nologin pricemon
install -d -o pricemon -g pricemon -m 0750 /opt/pricemon /var/log/pricemon
install -d -o root     -g root     -m 0755 /opt/venv
```

`/run/pricemon` is created automatically by the systemd unit
(`RuntimeDirectory=pricemon`) — don't create it by hand.

## 4. Clone the repo

```bash
cd /opt
rm -rf pricemon          # only if you're starting from a fresh box
git clone https://github.com/AbelLykens/pricemon.git pricemon
chown -R pricemon:pricemon /opt/pricemon
```

## 5. Python venv + dependencies

```bash
python3 -m venv /opt/venv
/opt/venv/bin/pip install --upgrade pip
/opt/venv/bin/pip install -r /opt/pricemon/requirements.txt
```

`requirements.txt` is a frozen capture of the working set on the primary
host (cryptofeed 2.4.1, Django 5.2.x, drf-spectacular, gunicorn, psycopg,
pymemcache, etc.). Pin upgrades happen by re-freezing on the primary.

## 6. Environment file

```bash
cp /opt/pricemon/.env.example /opt/pricemon/.env
chown pricemon:pricemon /opt/pricemon/.env
chmod 0640 /opt/pricemon/.env
$EDITOR /opt/pricemon/.env
```

Required fields:

- `DJANGO_SECRET_KEY` — generate with
  `python3 -c "import secrets; print(secrets.token_urlsafe(64))"`
- `POSTGRES_PASSWORD` — match what you set in step 2
- `DJANGO_ALLOWED_HOSTS` — set to the public hostname(s); `*` only if
  this box sits behind a trusted LB that strips the `Host` header

Everything else has a working default and can be left alone.

## 7. Migrate and collect static

Run as the `pricemon` user so file ownership stays correct:

```bash
sudo -u pricemon /opt/venv/bin/python /opt/pricemon/manage.py migrate
sudo -u pricemon /opt/venv/bin/python /opt/pricemon/manage.py collectstatic --noinput
```

Do **not** run `seed_pairs` on a fallback instance — this box is live-only,
so the exchange/pair registry is intentionally left to be populated by the
feed daemons as live trades come in.

## 8. systemd units

Install all four unit files and reload:

```bash
install -m 0644 /opt/pricemon/deploy/systemd/pricemon-web.service     /etc/systemd/system/
install -m 0644 /opt/pricemon/deploy/systemd/pricemon-feed@.service   /etc/systemd/system/
install -m 0644 /opt/pricemon/deploy/systemd/pricemon-feeds.target    /etc/systemd/system/
systemctl daemon-reload
```

Start and enable the web service:

```bash
systemctl enable --now pricemon-web.service
systemctl status pricemon-web.service        # should be active (running)
ls -l /run/pricemon/gunicorn.sock            # should exist, owned by pricemon
```

## 9. nginx

```bash
install -m 0644 /opt/pricemon/deploy/nginx/pricemon.conf /etc/nginx/sites-available/pricemon.conf
ln -sf /etc/nginx/sites-available/pricemon.conf /etc/nginx/sites-enabled/pricemon.conf
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx
```

The shipped config terminates plain HTTP on port 80 with `server_name _;` —
it assumes TLS terminates on an upstream load balancer that forwards
`X-Forwarded-Proto`. For direct TLS on this box, add a `listen 443 ssl` block
and certs (e.g. via certbot) before reloading.

Smoke test:

```bash
curl -sf http://127.0.0.1/api/v1/health/ && echo OK
```

## 10. Enable per-exchange feed daemons

One systemd instance per exchange. The full set used on the primary:

```bash
for ex in ascendex bequant binance binance_tr binance_us bitfinex bitflyer \
          bitstamp bybit coinbase gateio gemini huobi kraken kucoin okx; do
    systemctl enable --now "pricemon-feed@${ex}.service"
done
systemctl enable pricemon-feeds.target
```

Pick a subset if you want a lighter fallback — each feed is an
independent process and any subset is valid. The list of supported slugs
is whatever `manage.py run_feed --exchange <slug>` accepts.

Check them:

```bash
systemctl list-units 'pricemon-feed@*' --no-legend
journalctl -u pricemon-feed@coinbase.service -n 50 --no-pager
```

A healthy feed logs `READY=1` to systemd within ~30s and then a steady
stream of trade ticks. The unit has `WatchdogSec=120` so a silent feed
will be killed and restarted automatically.

## 11. End-to-end smoke check

```bash
curl -s http://127.0.0.1/api/v1/current/ | head -c 400; echo
curl -s 'http://127.0.0.1/api/v1/prices/?base=BTC&quote=EUR' | head -c 400; echo
```

Open the docs in a browser:

- `http://<host>/api/v1/docs/` — Swagger UI
- `http://<host>/api/v1/redoc/` — Redoc
- `http://<host>/` — overview page
- `http://<host>/history/` — per-minute candle history

## Operational notes

- Restart the web tier after editing templates or views:
  `systemctl restart pricemon-web.service`
- Restart all feeds after editing anything under `feeds/`:
  `systemctl restart pricemon-feeds.target`
- Logs go to journald: `journalctl -u pricemon-web -f`,
  `journalctl -u 'pricemon-feed@*' -f`
- Memcached must be reachable at `MEMCACHED_LOCATION` — the `/current/`
  endpoint and response caching both rely on it. The default config
  (`127.0.0.1:11211`, 64 MB) is fine.
- Postgres tuning: defaults are adequate for a single-box install. The
  hot tables are `feeds_minutebar` (insert-heavy) and the per-exchange
  aggregates; vacuum settings only matter under sustained high write rate.

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `pricemon-web` fails with `DJANGO_SECRET_KEY is required` | `.env` missing or not readable by `pricemon` user |
| `/api/v1/current/` returns empty | memcached not running, or no feed has published yet |
| `pricemon-feed@<x>` restart-loops with `ConnectionRefused` | wait for cryptofeed initial symbol fetch (up to ~4 min for Gemini); also check outbound network |
| `502 Bad Gateway` from nginx | `gunicorn.sock` missing — check `pricemon-web` status |
| Static files 404 | `collectstatic` not run, or nginx `alias` points at wrong path |
