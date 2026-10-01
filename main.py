# ============================================================
# VodiWalker 15.0.0
# Railway Ready
# ============================================================

import asyncio
import ipaddress
import base64
import hashlib
import json
import logging
import os
import re
import secrets
import string
import time
import psutil

from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote, parse_qs

import aiofiles
import httpx
import uvicorn

from fastapi import (
    FastAPI,
    Request,
    HTTPException,
    Depends,
)
from fastapi.responses import (
    Response,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
)
from fastapi.middleware.cors import CORSMiddleware


# ============================================================
# APP
# ============================================================

APP_NAME = "VodiWalker"
APP_VERSION = "27.3.0"

# فقط ویرایش «متن‌های ربات» قفل است؛ تنظیمات و روشن/خاموش ربات آزاد است
BOT_TEXTS_LOCKED = True
BOT_TEXTS_LOCKED_MSG = "ویرایش متن‌های ربات قفل شده است و امکان تغییر ندارد"

SUPPORT_USERNAME = "@VodiWalker"
CHANNEL_USERNAME = "vodiwalkervpn03"

# تنظیماتی که باید بعد از ری‌استارت هم بمانند (قبلاً فقط در حافظه بودند و با هر ری‌استارت پاک می‌شدند)
PERSISTED_EXTRA_SETTINGS = (
    "sub_remark_show_name", "sub_remark_show_volume", "sub_remark_show_id", "sub_remark_show_inbound",
    "sub_info_line_enabled", "sub_info_line_show_volume", "sub_info_line_show_expiry",
    "support_username", "channel_username", "name_style_enabled",
    "last_public_host", "last_public_scheme",
)

_TG_USER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{4,31}$")


def _clean_tg_username(raw) -> str:
    value = str(raw or "").strip()
    for prefix in ("https://t.me/", "http://t.me/", "t.me/", "https://telegram.me/", "@"):
        if value.lower().startswith(prefix):
            value = value[len(prefix):]
    return value.strip().strip("/")


def get_support_username() -> str:
    """آیدی پشتیبان تلگرام (قابل تنظیم از داخل پنل)؛ اگر خالی باشد مقدار پیش‌فرض."""
    raw = _clean_tg_username(CONFIG.get("support_username"))
    return "@" + raw if raw and _TG_USER_RE.match(raw) else SUPPORT_USERNAME


def get_support_url() -> str:
    return "https://t.me/" + get_support_username().lstrip("@")


def get_channel_url() -> str:
    raw = _clean_tg_username(CONFIG.get("channel_username"))
    return "https://t.me/" + (raw if raw and _TG_USER_RE.match(raw) else CHANNEL_USERNAME)
SUPPORT_URL = "https://t.me/VodiWalker"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

logger = logging.getLogger(APP_NAME)


# ============================================================
# TIMEZONE
# ============================================================

try:
    from zoneinfo import ZoneInfo

    IRAN_TZ = ZoneInfo("Asia/Tehran")

except Exception:
    IRAN_TZ = None


# ============================================================
# RAILWAY
# ============================================================

PORT = int(
    os.environ.get(
        "PORT",
        "8000",
    )
)

DATA_DIR = Path(
    os.environ.get(
        "RAILWAY_VOLUME_MOUNT_PATH",
        os.environ.get(
            "DATA_DIR",
            "./data",
        ),
    )
)

DATA_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

DATA_FILE = DATA_DIR / "vodiwalker_state.json"
SECRET_FILE = DATA_DIR / "vodiwalker_secret.key"


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    docs_url=None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# LOCKS
# ============================================================

SAVE_LOCK = asyncio.Lock()
LINKS_LOCK = asyncio.Lock()
SUBS_LOCK = asyncio.Lock()
SESSIONS_LOCK = asyncio.Lock()


# ============================================================
# SECRET
# ============================================================

def load_or_create_secret() -> str:
    env_secret = os.environ.get("SECRET_KEY")

    if env_secret:
        return env_secret

    try:
        if SECRET_FILE.exists():
            existing = (
                SECRET_FILE
                .read_text(
                    encoding="utf-8"
                )
                .strip()
            )

            if existing:
                return existing

        generated = secrets.token_urlsafe(48)

        SECRET_FILE.write_text(
            generated,
            encoding="utf-8",
        )

        return generated

    except Exception as exc:
        logger.warning(
            "Could not persist SECRET_KEY: %s",
            exc,
        )

        return secrets.token_urlsafe(48)


SECRET_KEY = load_or_create_secret()


# ============================================================
# CONFIG
# ============================================================

CONFIG = {
    "port": PORT,
    "secret": SECRET_KEY,
    "host": os.environ.get(
        "RAILWAY_PUBLIC_DOMAIN",
        "localhost",
    ),
   
    "tcp_public_host": os.environ.get("TCP_PUBLIC_HOST", "").strip(),
    "tcp_public_port": os.environ.get("TCP_PUBLIC_PORT", "").strip(),
}


# ============================================================
# STATE
# ============================================================

LINKS: dict = {}
SUBS: dict = {}
# Per-subscription live usage samples. Values come from the real link used_bytes field.
SUB_USAGE_HISTORY = defaultdict(lambda: deque(maxlen=144))
USAGE_PERSIST_TASK = None
SESSIONS: dict = {}
connections: dict = {}
CATEGORIES: dict = {}
DAILY_STATS: dict = {}  # "YYYY-MM-DD" -> {"traffic_bytes":.., "new_links":.., "orders":.., "stars":..}
DAILY_STATS_LOCK = asyncio.Lock()


def _today_key() -> str:
    now = datetime.now(IRAN_TZ) if IRAN_TZ else datetime.now()
    return now.strftime("%Y-%m-%d")


def bump_daily_stat(field: str, amount=1):
    """Increment a counter in today's reporting bucket (best-effort, in-memory)."""
    try:
        key = _today_key()
        bucket = DAILY_STATS.setdefault(
            key, {"traffic_bytes": 0, "new_links": 0, "orders": 0, "stars": 0}
        )
        bucket[field] = bucket.get(field, 0) + amount
        # keep only the last 180 days to avoid unbounded growth
        if len(DAILY_STATS) > 180:
            for old_key in sorted(DAILY_STATS.keys())[: len(DAILY_STATS) - 180]:
                DAILY_STATS.pop(old_key, None)
    except Exception:
        pass

stats = {
    "total_bytes": 0,
    "total_requests": 0,
    "total_errors": 0,
    "start_time": time.time(),
}

_telemetry_lock = asyncio.Lock()
_telemetry_prev = {"ts": time.time(), "rx": 0, "tx": 0}


def _pct(v):
    try:
        return round(float(v), 1)
    except Exception:
        return 0.0


def _human_uptime(seconds):
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours:02d}:{minutes:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"

error_logs = deque(maxlen=100)
activity_logs = deque(maxlen=250)

hourly_traffic = defaultdict(int)
# Real server telemetry samples used by the dashboard charts.
# Samples are collected from psutil; no placeholder/synthetic values are generated.
TELEMETRY_HISTORY = deque(maxlen=90)

http_client: httpx.AsyncClient | None = None


# ============================================================
# PROTOCOL
# ============================================================

PROTOCOLS = (
    "vless-ws",
    "vless-tcp",
    "xhttp-packet-up",
    "xhttp-stream-up",
    "xhttp-stream-one",
    "vmess-ws",
    "trojan-ws",
)

# این پروتکل‌ها روی همان پورت HTTP/WebSocket برنامه (پشت TLS ری‌ورس‌پروکسی یا Railway)
# سرو می‌شن و واقعاً روی سرور پیاده‌سازی شده‌ن.
REAL_TRANSPORT_PROTOCOLS = {
    "vless-ws", "xhttp-packet-up", "xhttp-stream-up",
}
# vless-tcp هم واقعی و پیاده‌سازی‌شده‌ست ولی روی یک پورت TCP خام و جداگانه
# (به‌صورت پیش‌فرض 6543، قابل تغییر با TCP_LISTEN_PORT) — نه پورت HTTP اصلی.
REAL_RAW_TCP_PROTOCOLS = {"vless-tcp"}
# همه‌ی پروتکل‌های دمو/غیرفعال از پنل حذف شده‌اند — هر چیزی که در PROTOCOLS باشد واقعاً کار می‌کند.
NON_FUNCTIONAL_DEMO_PROTOCOLS = {"vmess-ws", "trojan-ws"}

# Protocols that this project actually serves itself. VMess/Trojan entries may
# still be generated as client-side links, but they are NOT advertised as live
# listeners because this backend has no VMess/Trojan inbound parser.
LIVE_PROTOCOLS = REAL_TRANSPORT_PROTOCOLS | REAL_RAW_TCP_PROTOCOLS

PROTOCOL_LABELS = {
    "vless-ws": "VLESS WebSocket",
    "vless-tcp": "VLESS TCP (خام)",
    "xhttp-packet-up": "XHTTP Packet Up",
    "xhttp-stream-up": "XHTTP Stream Up",
    "xhttp-stream-one": "XHTTP Stream One",
    "vmess-ws": "VMess WebSocket",
    "trojan-ws": "Trojan WebSocket",
    "manual": "پروتکل دستی (سفارشی)",
}

PROTOCOL_ALIASES = {
    "vmess": "vmess-ws", "trojan": "trojan-ws", "ss": "shadowsocks",
    "socks": "socks5", "hy2": "hysteria2", "hysteria": "hysteria2",
}

DEFAULT_PROTOCOL = "vless-ws"

# نگاشت هر پروتکل غیر-دستی (manual) به Network/Security واقعی‌ای که در لینک
# نهایی (generate_vless_link) استفاده می‌شود. این فقط برای نمایش صحیح در پنل
# است (تگ‌های "ws/tls" و ...)؛ چون قبلاً این مقادیر همیشه روی مقدار پیش‌فرض
# فیلدهای دستی (tcp/none) می‌افتادند، حتی برای پروتکل‌هایی که واقعاً ws+tls بودند.
PROTOCOL_NETWORK_SECURITY = {
    "vless-ws": ("ws", "tls"),
    "vless-tcp": ("tcp", "none"),
    "xhttp-packet-up": ("xhttp", "tls"),
    "xhttp-stream-up": ("xhttp", "tls"),
    "xhttp-stream-one": ("xhttp", "tls"),
    "vmess-ws": ("ws", "tls"),
    "trojan-ws": ("ws", "tls"),
}

FINGERPRINTS = (
    "chrome",
    "firefox",
    "safari",
    "ios",
    "android",
    "edge",
    "360",
    "qq",
    "random",
    "randomized",
)

DEFAULT_FINGERPRINT = "chrome"

DEFAULT_ALPN_BY_PROTOCOL = {
    "vless-ws": "http/1.1",
    "xhttp-packet-up": "h2,http/1.1",
    "xhttp-stream-up": "h2,http/1.1",
    "xhttp-stream-one": "h2,http/1.1",
}

DEFAULT_PORT = 443
MIN_PORT = 1
MAX_PORT = 65535

DEFAULT_SPEED_LIMIT = 0


# ============================================================
# MANUAL PROTOCOL BUILDER (پروتکل دستی — مثل پنل‌های 3x-ui/Sanaei)
# ============================================================


MANUAL_BASE_PROTOCOLS = ("vless", "vmess", "trojan", "shadowsocks")

MANUAL_BASE_PROTOCOL_LABELS = {
    "vless": "VLESS",
    "vmess": "VMess",
    "trojan": "Trojan",
    "shadowsocks": "Shadowsocks",
}

NETWORKS = ("tcp", "ws", "grpc", "xhttp")

NETWORK_LABELS = {
    "tcp": "TCP",
    "ws": "WebSocket (ws)",
    "grpc": "gRPC",
    "xhttp": "XHTTP",
}

SECURITIES = ("none", "tls", "reality")

SECURITY_LABELS = {
    "none": "بدون امنیت (None)",
    "tls": "TLS",
    "reality": "Reality",
}

XHTTP_MODES = ("auto", "packet-up", "stream-up", "stream-one")
SHADOWSOCKS_METHODS = ("chacha20-ietf-poly1305", "aes-128-gcm", "aes-256-gcm", "2022-blake3-aes-128-gcm", "2022-blake3-aes-256-gcm")

# ترکیب‌هایی که همین پنل واقعاً به‌صورت زنده سرو می‌کند (بدون نیاز به Xray-core
# جداگانه). سایر ترکیب‌ها (مثل هر چیزی با Reality) فقط لینک/کانفیگ برای استفاده
# روی یک نود Xray-core واقعی می‌سازند و به همین دلیل در پنل با یک نشان
# «فقط ساخت لینک» مشخص می‌شوند — این محدودیت صادقانه در UI نشان داده می‌شود.
MANUAL_LIVE_COMBOS = {
    ("ws", "tls"),
    ("ws", "none"),
    ("xhttp", "tls"),
    ("xhttp", "none"),
    ("tcp", "none"),
}


def normalize_protocol(protocol: str | None) -> str:
    value = str(protocol or DEFAULT_PROTOCOL).strip().lower()
    value = PROTOCOL_ALIASES.get(value, value)
    if value == "manual":
        return value
    return value if value in PROTOCOLS else DEFAULT_PROTOCOL


def normalize_network(network: str | None) -> str:
    value = str(network or "tcp").strip().lower()
    return value if value in NETWORKS else "tcp"


def normalize_security(security: str | None) -> str:
    value = str(security or "none").strip().lower()
    return value if value in SECURITIES else "none"


def normalize_xhttp_mode(mode: str | None) -> str:
    value = str(mode or "auto").strip().lower()
    return value if value in XHTTP_MODES else "auto"


def normalize_base_protocol(value: str | None) -> str:
    v = str(value or "vless").strip().lower()
    return v if v in MANUAL_BASE_PROTOCOLS else "vless"


def protocol_display_label(link: dict) -> str:
    """برچسب نمایشی پروتکل برای جدول‌ها و گزارش‌ها.
    برای کانفیگ‌های دستی به‌صورت «VLESS · WebSocket · TLS» نمایش داده می‌شود."""
    protocol = link.get("protocol", DEFAULT_PROTOCOL)
    if protocol != "manual":
        return PROTOCOL_LABELS.get(protocol, protocol)
    base = MANUAL_BASE_PROTOCOL_LABELS.get(normalize_base_protocol(link.get("base_protocol")), "VLESS")
    network = NETWORK_LABELS.get(normalize_network(link.get("network")), "TCP")
    security = SECURITY_LABELS.get(normalize_security(link.get("security")), "بدون امنیت")
    if base == "Shadowsocks":
        return f"Shadowsocks · {network}"
    return f"{base} · {network} · {security}"


# ============================================================
# LOGGING
# ============================================================

def log_activity(
    kind: str,
    message: str,
    level: str = "info",
):
    activity_logs.append(
        {
            "kind": kind,
            "level": level,
            "message": message,
            "time": datetime.now().isoformat(),
        }
    )


# ============================================================
# HELPERS
# ============================================================

def escape_html(value) -> str:
    return (
        str(
            value
            if value is not None
            else ""
        )
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#039;")
    )


def safe_int(
    value,
    default=0,
    minimum=0,
    maximum=None,
):
    try:
        number = int(value)
    except Exception:
        number = default

    if number < minimum:
        number = minimum

    if maximum is not None and number > maximum:
        number = maximum

    return number


def safe_float(
    value,
    default=0.0,
    minimum=0.0,
):
    try:
        number = float(value)
    except Exception:
        number = default

    return max(
        minimum,
        number,
    )


def generate_uuid():
    value = secrets.token_hex(16)

    return (
        f"{value[:8]}-"
        f"{value[8:12]}-"
        f"{value[12:16]}-"
        f"{value[16:20]}-"
        f"{value[20:32]}"
    )


def random_config_name(existing=None):
    existing = existing or set()
    alphabet = string.ascii_lowercase + string.digits
    for _ in range(80):
        length = secrets.randbelow(6) + 8
        name = "".join(secrets.choice(alphabet) for _ in range(length))
        if name not in existing and name and not name[0].isdigit():
            return name
    return secrets.token_hex(6)

def sanitize_config_name(name: str) -> str:
    if not name:
        return random_config_name()
    cleaned = "".join(ch for ch in str(name) if ch.isascii() and ch.isalnum())
    if not cleaned or cleaned[0].isdigit():
        cleaned = ("a" + cleaned) if cleaned else random_config_name()
    return cleaned[:40]

def auto_config_name() -> str:
    return random_config_name()


def now_ir():
    if IRAN_TZ:
        return datetime.now(IRAN_TZ)

    return datetime.now()


def uptime():
    seconds = int(
        time.time()
        - stats["start_time"]
    )

    h = seconds // 3600

    m = (
        seconds
        % 3600
    ) // 60

    s = (
        seconds
        % 60
    )

    return (
        f"{h:02d}:"
        f"{m:02d}:"
        f"{s:02d}"
    )


def fmt_bytes(value: int):
    value = int(
        value or 0
    )

    if value < 1024:
        return f"{value} B"

    if value < 1024 ** 2:
        return (
            f"{value / 1024:.1f} KB"
        )

    if value < 1024 ** 3:
        return (
            f"{value / 1024 ** 2:.2f} MB"
        )

    return (
        f"{value / 1024 ** 3:.2f} GB"
    )


def parse_size_to_bytes(
    value: float,
    unit: str,
):
    if value <= 0:
        return 0

    unit = (
        unit
        or "GB"
    ).upper()

    if unit == "TB":
        return int(
            value
            * 1024 ** 4
        )

    if unit == "GB":
        return int(
            value
            * 1024 ** 3
        )

    if unit == "MB":
        return int(
            value
            * 1024 ** 2
        )

    if unit == "KB":
        return int(
            value
            * 1024
        )

    return int(value)


def parse_speed_to_bytes(
    value: float,
    unit: str,
):
    if value <= 0:
        return 0

    unit = (
        unit
        or "MBIT"
    ).upper()

    if unit == "MBIT":
        return int(
            value
            * 1024
            * 1024
            / 8
        )

    if unit == "KB":
        return int(
            value * 1024
        )

    if unit == "MB":
        return int(
            value
            * 1024
            * 1024
        )

    return int(value)


def is_link_expired(
    link: dict,
):
    expiry = link.get(
        "expires_at"
    )

    if not expiry:
        return False

    try:
        return (
            datetime.now()
            > datetime.fromisoformat(
                expiry
            )
        )

    except Exception:
        return False


def is_link_allowed(
    link: dict | None,
):
    if link is None:
        return False

    if not link.get(
        "active",
        True,
    ):
        return False

    if is_link_expired(link):
        return False

    limit = int(
        link.get(
            "limit_bytes",
            0,
        )
        or 0
    )

    used = int(
        link.get(
            "used_bytes",
            0,
        )
        or 0
    )

    if (
        limit > 0
        and used >= limit
    ):
        return False

    return True


def unique_ips_for_uuid(
    uuid: str,
):
    return {
        connection.get("ip")
        for connection in connections.values()
        if connection.get("uuid") == uuid
        and connection.get("ip")
    }


def client_ip(
    request: Request,
):
    forwarded = request.headers.get(
        "x-forwarded-for"
    )

    if forwarded:
        return (
            forwarded
            .split(",")[0]
            .strip()
        )

    real = request.headers.get(
        "x-real-ip"
    )

    if real:
        return real.strip()

    if request.client:
        return request.client.host

    return "unknown"


def is_ip_allowed(
    link: dict | None,
    uuid: str,
    ip: str,
):
    if link is None:
        return False

    limit = int(
        link.get(
            "ip_limit",
            0,
        )
        or 0
    )

    if limit <= 0:
        return True

    ips = unique_ips_for_uuid(uuid)

    if ip in ips:
        return True

    return len(ips) < limit


def _split_base_url(raw: str):
    """آدرس عمومی ذخیره‌شده رو به (scheme, host) تجزیه می‌کنه. ورودی می‌تونه
    با یا بدون scheme باشه (مثلاً 'panel.example.com' یا 'https://panel.example.com')."""
    raw = (raw or "").strip()
    if not raw:
        return None, None
    scheme = "https"
    rest = raw
    if "://" in raw:
        scheme, rest = raw.split("://", 1)
        scheme = scheme.strip().lower() or "https"
    host = rest.split("/", 1)[0].split(":")[0].strip()
    return (scheme if scheme in ("http", "https") else "https"), (host or None)


_DOMAIN_RE = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)+$")


def _is_real_public_host(host) -> bool:
    """True فقط برای دامنه/آی‌پی عمومی واقعی؛ localhost / 0.0.0.0 / آی‌پی خصوصی / آدرس داخلی ریلوی → False."""
    h = str(host or "").strip().lower().strip("[]")
    if not h or h in ("localhost", "0.0.0.0", "::", "::1"):
        return False
    if h.endswith((".local", ".internal", ".localhost", ".lan")):
        return False
    try:
        return ipaddress.ip_address(h).is_global
    except ValueError:
        pass
    return bool(_DOMAIN_RE.match(h))


def _request_public_host(request) -> str | None:
    try:
        raw = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        host = raw.split(",")[0].split(":")[0].strip().lower()
        return host if _is_real_public_host(host) else None
    except Exception:
        return None


def remember_public_host(request) -> None:
    """آدرس واقعی پنل را از اولین درخواست ادمینِ واردشده به خاطر می‌سپارد (برای ربات و لینک‌هایی که بدون درخواست ساخته می‌شوند)."""
    try:
        host = _request_public_host(request)
        if not host:
            return
        proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "https").split(",")[0].strip().lower()
        if proto not in ("http", "https"):
            proto = "https"
        if CONFIG.get("last_public_host") != host or CONFIG.get("last_public_scheme") != proto:
            CONFIG["last_public_host"] = host
            CONFIG["last_public_scheme"] = proto
            asyncio.get_running_loop().create_task(save_state())
    except Exception:
        pass


def get_public_host_strict(request=None) -> str | None:
    """آدرس واقعی و قابل‌اتصال پنل؛ اگر هیچ آدرس معتبری پیدا نشود None (هرگز localhost/0.0.0.0 نمی‌دهد)."""
    _, override = _split_base_url(CONFIG.get("public_base_url"))
    if override and _is_real_public_host(override):
        return override
    if request is not None:
        host = _request_public_host(request)
        if host:
            return host
    env_domain = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "").strip()
    if _is_real_public_host(env_domain):
        return env_domain
    last = str(CONFIG.get("last_public_host") or "").strip()
    if _is_real_public_host(last):
        return last
    cfg_host = str(CONFIG.get("host") or "").strip()
    if _is_real_public_host(cfg_host):
        return cfg_host
    return None


def get_public_base(request=None) -> str | None:
    host = get_public_host_strict(request)
    if not host:
        return None
    _, override = _split_base_url(CONFIG.get("public_base_url"))
    scheme = get_scheme() if override else (CONFIG.get("last_public_scheme") or "https")
    return f"{scheme}://{host}"


def get_host(
    request: Request | None = None,
) -> str:
    # اولویت اول: آدرس عمومی صریحی که در تنظیمات پنل ثبت شده (پایدار، مستقل از
    # اینکه درخواست از کجا اومده — پروکسی، آی‌پی داخلی، هلث‌چک و ...).
    _, override_host = _split_base_url(CONFIG.get("public_base_url"))
    if override_host:
        return override_host

    if request is not None:
        forwarded = request.headers.get(
            "x-forwarded-host"
        )

        normal = request.headers.get(
            "host"
        )

        host = (
            forwarded
            or normal
        )

        if host:
            # توجه: دیگه CONFIG["host"] رو اینجا آپدیت نمی‌کنیم؛ این یک متغیر سراسری
            # مشترک بین همه‌ی درخواست‌ها بود و هر درخواست با Host نادرست (هلث‌چک،
            # اسکنر، وبهوک) می‌تونست لینک‌های بعدیِ همه رو خراب کنه.
            return host.split(":")[0].strip()

    railway_domain = os.environ.get(
        "RAILWAY_PUBLIC_DOMAIN"
    )

    if railway_domain:
        return railway_domain

    # بدون درخواست (مثلاً ربات تلگرام): آدرس واقعیِ ذخیره‌شده؛ نه localhost فیک
    return get_public_host_strict() or CONFIG["host"]


def get_scheme() -> str:
    """scheme (http/https) که باید برای ساخت لینک‌های ساب استفاده بشه."""
    scheme, host = _split_base_url(CONFIG.get("public_base_url"))
    if host:
        return scheme
    return "https"


def _tcp_listen_port_snapshot() -> int:
    try:
        import tcp_relay
        return tcp_relay.TCP_LISTEN_PORT
    except Exception:
        return int(os.environ.get("TCP_LISTEN_PORT", "6543"))


def _bot_settings_snapshot() -> dict:
    """وضعیت فعلی ربات تلگرام رو برمی‌گردونه؛ اگه ماژول ربات هنوز ایمپورت نشده
    یا مشکلی داشته باشه، مقدار خالی/امن برمی‌گردونه (این نباید کل پنل رو خراب کنه)."""
    try:
        import telegram_bot
        return telegram_bot.current_config()
    except Exception:
        return {"bot_token": "", "admin_ids": "", "running": False}


# ============================================================
# PASSWORD
# ============================================================

def hash_password(
    password: str,
) -> str:

    payload = (
        password
        + SECRET_KEY
    ).encode("utf-8")

    return hashlib.sha256(
        payload
    ).hexdigest()


DEFAULT_ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin").strip() or "admin"
DEFAULT_ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin")

AUTH = {
    "username": DEFAULT_ADMIN_USERNAME,
    "password_hash":
        hash_password(
            DEFAULT_ADMIN_PASSWORD
        )
}

# ============================================================
# MULTI-ADMIN (sub-admins beyond the owner account)
# ============================================================
# The "owner" account is always backed by AUTH["password_hash"] above
# (fully backward compatible with older single-admin deployments).
# Additional named admin accounts live here and can be managed from
# the "مدیریت ادمین‌ها" tab in the dashboard.

ADMINS: dict = {}

# ============================================================
# ADMIN REGISTRATION REQUESTS ("ثبت‌نام ادمینی" از صفحه لاگین)
# ============================================================
# کاربری که می‌خواهد ادمین شود، فقط نام و آیدی تلگرام خود را از صفحه
# لاگین ارسال می‌کند. درخواست او اینجا به‌صورت pending ذخیره می‌شود تا
# مالک پنل از بخش «مدیریت حساب‌ها» آن را ببیند، تصمیم بگیرد چه دسترسی‌ها
# و چه رمز/نام‌کاربری‌ای به او بدهد، و در صورت تایید حساب ادمین واقعی
# برایش ساخته شود.
ADMIN_REQUESTS: dict = {}
ADMIN_REQUEST_RATE: dict = {}  # ip -> last submit timestamp (ضد اسپم ساده)
ADMIN_REQUEST_COOLDOWN_SECONDS = 60
ADMIN_REQUESTS_LOCK = asyncio.Lock()

ALL_PERMISSIONS = {
    "dashboard": "مشاهده داشبورد",
    "inbounds": "مدیریت اینباند و کلاینت",
    "clients": "ساخت کلاینت (بخش جدا)",
    "subscriptions": "مدیریت سابسکریپشن",
    "categories": "مدیریت دسته‌بندی",
    "reports": "گزارش‌ها",
    "messages": "مرکز پیام و خطا",
    "bot": "مدیریت ربات",
    "admins": "مدیریت ادمین‌ها",
    "settings": "تنظیمات پنل",
    # --- دسترسی‌های جزئی (v2) ---
    "inbound_create": "ساخت اینباند جدید",
    "inbound_edit": "ویرایش اینباند",
    "inbound_delete": "حذف اینباند",
    "client_create": "ساخت کلاینت",
    "client_edit": "ویرایش کلاینت / تغییر وضعیت",
    "client_delete": "حذف کلاینت",
    "client_reset": "ریست مصرف کلاینت",
    "outbound_manage": "مدیریت خروجی / پراکسی",
    "live_connections": "مشاهده اتصال‌های زنده و IP",
}

# کلیدهای جزئی: برای ادمین‌های قدیمی (بدون perm_v2) با داشتن «inbounds» مجاز می‌مانند
GRANULAR_PERMISSIONS = {
    "inbound_create", "inbound_edit", "inbound_delete", "client_create", "client_edit",
    "client_delete", "client_reset", "outbound_manage", "live_connections",
}

import contextvars as _ctxvars
# وقتی مالک هنگام ساخت اینباند گزینه «محدودکردن ادمین به همین اینباند» را بزند، make_link از اینجا می‌خواند
_CREATE_RESTRICT = _ctxvars.ContextVar("vw_create_restrict", default=None)


def has_permission(admin_id: str, key: str) -> bool:
    if admin_id == "owner":
        return True
    a = ADMINS.get(admin_id) or {}
    perms = set(a.get("permissions") or {"dashboard"})
    if key in perms:
        return True
    if key in GRANULAR_PERMISSIONS and not a.get("perm_v2"):
        return "inbounds" in perms          # سازگاری با ادمین‌های قدیمی
    return False


def admin_scope(admin_id: str):
    """None = بدون محدودیت. وگرنه مجموعه‌ی UUID اینباندهایی که ادمین فقط به آن‌ها دسترسی دارد."""
    if admin_id == "owner":
        return None
    a = ADMINS.get(admin_id) or {}
    lst = a.get("allowed_inbounds") or []
    return set(lst) if lst else None


def link_in_scope(scope, uid: str) -> bool:
    if scope is None:
        return True
    if uid in scope:
        return True
    link = LINKS.get(uid) or {}
    return link.get("parent_inbound_id") in scope


def client_visible(link, actor_id: str) -> bool:
    """کلاینت فقط برای سازنده‌اش دیده می‌شود (کلاینت‌های قدیمی بدون created_by = مالک).
    باگ رفع‌شده: مالک همیشه همه‌چیز را می‌بیند — این محدودیت فقط بین ادمین‌ها اعمال می‌شود،
    نه روی مالک (وگرنه مالک اصلاً کلاینت‌های ساخته‌شده توسط ادمین‌ها را نمی‌دید)."""
    if actor_id == "owner":
        return True
    if not link.get("parent_inbound_id"):
        return True
    return (link.get("created_by") or "owner") == actor_id


async def actor_id_of(request: Request) -> str:
    info = await get_session_info(request.cookies.get(SESSION_COOKIE))
    return (info or {}).get("admin_id", "owner")


def scope_replace_uid(old: str, new: str):
    for a in ADMINS.values():
        lst = a.get("allowed_inbounds")
        if lst and old in lst:
            a["allowed_inbounds"] = [new if x == old else x for x in lst]


def scope_remove_uid(uid: str):
    for a in ADMINS.values():
        lst = a.get("allowed_inbounds")
        if lst and uid in lst:
            a["allowed_inbounds"] = [x for x in lst if x != uid]


def _apply_create_restrict(uid: str):
    """در make_link صدا زده می‌شود؛ ادمین انتخاب‌شده را به همین اینباند محدود می‌کند."""
    st = _CREATE_RESTRICT.get()
    if not st:
        return
    a = ADMINS.get(st["admin_id"])
    if not a:
        return
    if st["mode"] == "only" and not st["seen"]:
        a["allowed_inbounds"] = [uid]          # فقط همین یکی (جایگزین محدودیت‌های قبلی)
    else:
        lst = list(a.get("allowed_inbounds") or [])
        if uid not in lst:
            lst.append(uid)
        a["allowed_inbounds"] = lst
    st["seen"] += 1

BOT_TEXTS = {
    "welcome": "🛡 <b>VodiWalker Control Center</b>\n\nاز منوی زیر عملیات موردنظر را انتخاب کنید.",
    "admin_menu": "🛠 <b>مدیریت پنل</b>\n\nساخت اینباند، کلاینت و گروه ساب از همین‌جا در دسترس است.",
    "config_created": "✅ کانفیگ با موفقیت ساخته شد.",
    "config_deleted": "🗑 کانفیگ حذف شد.",
    "config_disabled": "⛔ کانفیگ غیرفعال شد.",
    "config_enabled": "✅ کانفیگ فعال شد.",
}

def get_bot_text(key: str, fallback: str = "") -> str:
    return str(BOT_TEXTS.get(key, fallback))

def permissions_for_admin(admin_id: str) -> set[str]:
    if admin_id == "owner":
        return set(ALL_PERMISSIONS)
    a = ADMINS.get(admin_id) or {}
    return set(a.get("permissions") or {"dashboard"})

async def require_permission(request: Request, permission: str):
    token = request.cookies.get(SESSION_COOKIE)
    info = await get_session_info(token)
    if not info:
        raise HTTPException(status_code=401, detail="unauthorized")
    if permission not in permissions_for_admin(info.get("admin_id", "owner")):
        raise HTTPException(status_code=403, detail="دسترسی این قابلیت برای این ادمین فعال نیست")
    return info


def verify_admin_credentials(username: str | None, password: str):
    """Returns (ok, admin_id, role, display_name)."""
    username = (username or "").strip()
    password = password or ""

    if not username or username.lower() in {"owner", AUTH.get("username", DEFAULT_ADMIN_USERNAME).lower()}:
        if username and username.lower() not in {"owner", AUTH.get("username", DEFAULT_ADMIN_USERNAME).lower()}:
            return False, None, None, None
        if hash_password(password) == AUTH["password_hash"]:
            return True, "owner", "owner", AUTH.get("username", DEFAULT_ADMIN_USERNAME)
        return False, None, None, None

    for admin_id, admin in ADMINS.items():
        if not admin.get("active", True):
            continue
        if admin.get("username", "").lower() == username.lower():
            if hash_password(password) == admin.get("password_hash"):
                return True, admin_id, admin.get("role", "admin"), admin.get("username")
            return False, None, None, None

    return False, None, None, None


# ============================================================
# LOGIN BRUTE-FORCE PROTECTION
# ============================================================
# Maximum failed login attempts per IP inside the rolling window.
LOGIN_MAX_ATTEMPTS = 5
LOGIN_WINDOW_SECONDS = 15 * 60
LOGIN_LOCKOUT_SECONDS = 15 * 60
LOGIN_MIN_PASSWORD_LENGTH = 6

LOGIN_FAILURES = defaultdict(deque)
LOGIN_LOCKED_UNTIL = {}


def _cleanup_login_state(ip: str, now: float | None = None):
    now = now if now is not None else time.time()

    locked_until = LOGIN_LOCKED_UNTIL.get(ip, 0)
    if locked_until and locked_until <= now:
        LOGIN_LOCKED_UNTIL.pop(ip, None)

    failures = LOGIN_FAILURES.get(ip)
    if not failures:
        return

    cutoff = now - LOGIN_WINDOW_SECONDS
    while failures and failures[0] <= cutoff:
        failures.popleft()

    if not failures:
        LOGIN_FAILURES.pop(ip, None)


def login_is_blocked(ip: str):
    now = time.time()
    _cleanup_login_state(ip, now)

    locked_until = LOGIN_LOCKED_UNTIL.get(ip, 0)
    if locked_until > now:
        return True, max(1, int(locked_until - now))

    return False, 0


def register_login_failure(ip: str):
    now = time.time()
    _cleanup_login_state(ip, now)

    failures = LOGIN_FAILURES.setdefault(ip, deque())
    failures.append(now)

    if len(failures) >= LOGIN_MAX_ATTEMPTS:
        LOGIN_LOCKED_UNTIL[ip] = now + LOGIN_LOCKOUT_SECONDS
        failures.clear()
        log_activity(
            "auth",
            f"IP به دلیل تلاش‌های متعدد ورود ناموفق به مدت {LOGIN_LOCKOUT_SECONDS // 60} دقیقه مسدود شد: {ip}",
            "err",
        )
        return True, LOGIN_LOCKOUT_SECONDS

    return False, max(0, LOGIN_MAX_ATTEMPTS - len(failures))


def clear_login_failures(ip: str):
    LOGIN_FAILURES.pop(ip, None)
    LOGIN_LOCKED_UNTIL.pop(ip, None)


# ============================================================
# SESSION
# ============================================================

SESSION_COOKIE = "vodiwalker_session"

SESSION_TTL = (
    60
    * 60
    * 24
    * 365
)


async def create_session(admin_id: str = "owner", role: str = "owner") -> str:

    token = secrets.token_urlsafe(48)

    async with SESSIONS_LOCK:
        SESSIONS[token] = {
            "exp": time.time() + SESSION_TTL,
            "admin_id": admin_id,
            "role": role,
            "permissions": sorted(permissions_for_admin(admin_id)),
        }

    return token


def _session_expiry(entry) -> float:
    if isinstance(entry, dict):
        return entry.get("exp", 0)
    return entry or 0


async def is_valid_session(
    token: str | None,
) -> bool:

    if not token:
        return False

    async with SESSIONS_LOCK:

        entry = SESSIONS.get(token)

        if entry is None:
            return False

        if _session_expiry(entry) < time.time():

            SESSIONS.pop(
                token,
                None,
            )

            return False

        return True


async def get_session_info(token: str | None):
    if not token:
        return None

    async with SESSIONS_LOCK:
        entry = SESSIONS.get(token)

        if entry is None:
            return None

        if _session_expiry(entry) < time.time():
            SESSIONS.pop(token, None)
            return None

        if isinstance(entry, dict):
            return dict(entry)

        return {"exp": entry, "admin_id": "owner", "role": "owner"}


async def require_owner(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    info = await get_session_info(token)

    if not info:
        raise HTTPException(status_code=401, detail="unauthorized")

    if info.get("role") != "owner":
        raise HTTPException(
            status_code=403,
            detail="فقط مالک پنل به این بخش دسترسی دارد",
        )

    return token


async def destroy_session(
    token: str | None,
):
    if not token:
        return

    async with SESSIONS_LOCK:
        SESSIONS.pop(
            token,
            None,
        )


# مقدار برگشتیِ require_auth وقتی درخواست با «توکن نود» (Bearer) احراز شده، نه نشست ادمین.
# get_session_info() برای این مقدار None می‌ده، پس endpointهای حساس (رمز، ادمین‌ها،
# نشست‌ها) که خودشون نشست واقعی می‌خوان با توکن نود هرگز کار نمی‌کنن.
NODE_API_TOKEN_MARK = "__vodiwalker_node_api__"


async def require_auth(
    request: Request,
):
    token = request.cookies.get(
        SESSION_COOKIE
    )

    info = await get_session_info(token)
    if not info:
        if request.headers.get("authorization"):
            try:
                from nodes import authorize_node_request
            except Exception:
                authorize_node_request = None
            if authorize_node_request and authorize_node_request(request):
                return NODE_API_TOKEN_MARK
        raise HTTPException(status_code=401, detail="unauthorized")
    remember_public_host(request)
    if info.get("admin_id") != "owner":
        path = request.url.path
        method = request.method.upper()
        aid = info.get("admin_id", "")
        permission = "dashboard"
        if path.startswith("/api/links") or path.startswith("/api/protocols") or path.startswith("/api/reality") or path.startswith("/api/proxies"):
            permission = "inbounds"
        elif path.startswith("/api/sub") or path.startswith("/sub"):
            permission = "subscriptions"
        elif path.startswith("/api/categories"):
            permission = "categories"
        elif path.startswith("/api/reports"):
            permission = "reports"
        elif path.startswith("/api/errors") or path.startswith("/api/activity"):
            permission = "messages"
        elif path.startswith("/api/settings/bot") or path.startswith("/api/bot"):
            permission = "bot"
        elif path.startswith("/api/settings"):
            permission = "settings"
        elif path.startswith("/api/telemetry") or path.startswith("/api/network"):
            permission = "dashboard"
        if permission not in permissions_for_admin(aid):
            raise HTTPException(status_code=403, detail="دسترسی این قابلیت برای این ادمین فعال نیست")

        scope = admin_scope(aid)
        await _enforce_granular_and_scope(request, aid, scope, path, method)
    else:
        await _prepare_owner_create_restrict(request)
    return token


_SCOPED_STATIC_OK = ("/api/protocols", "/api/telemetry", "/stats", "/api/me")
_LINK_SPECIAL = {"auto", "combo", "outbound"}


def _forbid(msg="این ادمین فقط به اینباند مشخص‌شده دسترسی دارد"):
    raise HTTPException(status_code=403, detail=msg)


async def _safe_json(request: Request):
    try:
        data = await request.json()
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _link_kind_perm(uid: str, inbound_key: str, client_key: str) -> str:
    link = LINKS.get(uid) or {}
    return client_key if link.get("parent_inbound_id") else inbound_key


async def _enforce_granular_and_scope(request: Request, aid: str, scope, path: str, method: str):
    parts = [x for x in path.split("/") if x]          # ['api','links','<uid>','clients',...]
    need = None
    uid = None

    if path.startswith("/api/links"):
        sub = parts[2] if len(parts) > 2 else None
        if sub in _LINK_SPECIAL:
            if method == "POST" and sub in ("auto", "combo"):
                need = "inbound_create"
            elif sub == "outbound":
                need = "outbound_manage"
        elif sub is None:
            if method == "POST":
                need = "inbound_create"
        else:
            uid = sub
            tail = parts[3] if len(parts) > 3 else None
            if tail is None:
                if method == "PATCH":
                    need = _link_kind_perm(uid, "inbound_edit", "client_edit")
                elif method == "DELETE":
                    need = _link_kind_perm(uid, "inbound_delete", "client_delete")
            elif tail == "clients":
                if method == "POST":
                    need = "client_create"
                elif method == "DELETE":
                    need = "client_delete"
            elif tail == "reset-usage":
                need = "client_reset"
            elif tail in ("action", "regenerate"):
                need = _link_kind_perm(uid, "inbound_edit", "client_edit")
    elif path.startswith("/api/proxies"):
        if method != "GET":
            need = "outbound_manage"
    elif path.startswith("/api/connections"):
        need = "live_connections"

    if need and not has_permission(aid, need):
        raise HTTPException(status_code=403, detail="دسترسی این قابلیت برای این ادمین فعال نیست")

    if scope is None:
        return

    # ---- ادمینِ محدود به اینباند مشخص ----
    if path.startswith("/api/links"):
        sub = parts[2] if len(parts) > 2 else None
        if sub is None:
            if method != "GET":
                _forbid("ادمین محدود‌شده نمی‌تواند اینباند جدید بسازد")
            return                                        # GET لیست؛ داخل endpoint فیلتر می‌شود
        if sub in _LINK_SPECIAL:
            if sub == "outbound":
                body = await _safe_json(request)
                for u in (body.get("uuids") or []):
                    if not link_in_scope(scope, str(u)):
                        _forbid()
                return
            _forbid("ادمین محدود‌شده نمی‌تواند اینباند جدید بسازد")
        if not link_in_scope(scope, sub) or sub not in LINKS:
            raise HTTPException(status_code=404, detail="link not found")
        return
    if path.startswith(_SCOPED_STATIC_OK):
        return
    _forbid("این بخش برای ادمین محدود‌شده در دسترس نیست")


async def _prepare_owner_create_restrict(request: Request):
    """مالک هنگام ساخت اینباند می‌تواند restrict_admin_id بفرستد."""
    if request.method.upper() != "POST":
        return
    path = request.url.path
    if path not in ("/api/links", "/api/links/auto", "/api/links/combo"):
        return
    body = await _safe_json(request)
    rid = str(body.get("restrict_admin_id") or "").strip()
    if not rid or rid not in ADMINS:
        return
    mode = "add" if str(body.get("restrict_mode") or "only") == "add" else "only"
    _CREATE_RESTRICT.set({"admin_id": rid, "mode": mode, "seen": 0})


async def actor_scope(request: Request):
    info = await get_session_info(request.cookies.get(SESSION_COOKIE))
    if not info:
        return None
    return admin_scope(info.get("admin_id", "owner"))



def set_auth_cookie(
    response,
    request: Request,
    token: str,
):
    forwarded_proto = (
        request.headers
        .get(
            "x-forwarded-proto",
            "",
        )
        .lower()
    )

    is_https = (
        forwarded_proto == "https"
        or request.url.scheme == "https"
    )

    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        max_age=SESSION_TTL,
        httponly=True,
        samesite="lax",
        path="/",
        secure=is_https,
    )


# ============================================================
# VLESS LINK GENERATION
# ============================================================

def generate_vless_link(
    uuid: str, host: str, remark: str = "VodiWalker",
    protocol: str = DEFAULT_PROTOCOL, fingerprint: str | None = None,
    alpn: str | None = None, port: int | None = None,
):
    protocol = normalize_protocol(protocol)
    fp = (fingerprint or DEFAULT_FINGERPRINT).strip().lower()
    if fp not in FINGERPRINTS: fp = DEFAULT_FINGERPRINT
    port_value = safe_int(port, DEFAULT_PORT, MIN_PORT, MAX_PORT)
    alpn_value = (alpn or DEFAULT_ALPN_BY_PROTOCOL.get(protocol, "http/1.1")).strip()
    label = quote(str(remark or "VodiWalker"), safe="")
    if protocol == "vless-ws":
        q = {"encryption":"none","security":"tls","type":"ws","host":host,"path":f"/ws/{uuid}","sni":host,"fp":fp,"alpn":alpn_value}
        return "vless://" + uuid + "@" + host + ":" + str(port_value) + "?" + "&".join(f"{k}={quote(str(v), safe=',/') }" for k,v in q.items()) + "#" + label
    if protocol == "vless-tcp":
        # VLESS خام روی TCP — این روی پورت HTTP اصلی سرو نمی‌شه، بلکه روی یک پورت TCP
        # مجزا (tcp_relay.py) که آدرس/پورت عمومیش از تنظیمات پنل (Settings) خونده می‌شه
        # تا وقتی روی Railway (یا هر جای دیگه) با TCP Proxy جداگانه دیپلوی شد، خودت
        # می‌تونی آدرس واقعی رو دستی وارد کنی.
        tcp_host = (CONFIG.get("tcp_public_host") or "").strip() or host
        tcp_port = safe_int(CONFIG.get("tcp_public_port"), port_value, MIN_PORT, MAX_PORT)
        q = {"encryption":"none","security":"none","type":"tcp","headerType":"none"}
        return "vless://" + uuid + "@" + tcp_host + ":" + str(tcp_port) + "?" + "&".join(f"{k}={quote(str(v), safe=',/') }" for k,v in q.items()) + "#" + label
    if protocol.startswith("xhttp-"):
        mode = protocol.replace("xhttp-", "")
        q = {"encryption":"none","security":"tls","type":"xhttp","mode":mode,"host":host,"path":f"/xhttp-siz10/{mode}/{uuid}","sni":host,"fp":fp,"alpn":alpn_value}
        return "vless://" + uuid + "@" + host + ":" + str(port_value) + "?" + "&".join(f"{k}={quote(str(v), safe=',/') }" for k,v in q.items()) + "#" + label
    if protocol == "vmess-ws":
        raw = {"v":"2","ps":remark,"add":host,"port":port_value,"id":uuid,"aid":0,"scy":"auto","net":"ws","type":"none","host":host,"path":f"/ws/{uuid}","tls":"tls","sni":host,"fp":fp}
        return "vmess://" + base64.b64encode(json.dumps(raw,separators=(",",":"),ensure_ascii=False).encode()).decode()
    if protocol == "trojan-ws":
        return f"trojan://{uuid}@{host}:{port_value}?security=tls&type=ws&host={quote(host)}&path={quote('/ws/'+uuid)}&sni={quote(host)}#{label}"
    return f"vless://{uuid}@{host}:{port_value}"

# ============================================================
# SUBSCRIPTION REMARK TEMPLATE (configurable, Settings -> Subscription Template)
# ============================================================
def build_config_remark(link: dict, uid: str) -> str:
    """می‌سازه چه متنی به‌عنوان نام کانفیگ (#remark) داخل اپ کاربر دیده بشه.
    پیش‌فرض دقیقاً مثل قبل فقط «نام» است؛ مالک پنل از تب تنظیمات می‌تونه
    نمایش حجم/آی‌دی/نام اینباند رو هم فعال کنه."""
    show_name = bool(CONFIG.get("sub_remark_show_name", True))
    show_volume = bool(CONFIG.get("sub_remark_show_volume", False))
    show_id = bool(CONFIG.get("sub_remark_show_id", False))
    show_inbound = bool(CONFIG.get("sub_remark_show_inbound", False))

    name = str(link.get("label") or "Config").strip() or "Config"
    parts: list[str] = []

    if show_name:
        parts.append(name)

    if show_volume:
        try:
            limit_bytes = int(link.get("limit_bytes") or 0)
        except Exception:
            limit_bytes = 0
        parts.append(fmt_bytes(limit_bytes) if limit_bytes > 0 else "Unlimited")

    if show_id:
        parts.append(str(uid)[:8])

    if show_inbound:
        parent_id = link.get("parent_inbound_id")
        parent = LINKS.get(parent_id) if parent_id else None
        inbound_label = str((parent or {}).get("label") or "").strip()
        if inbound_label:
            parts.append(inbound_label)

    return " | ".join(p for p in parts if p) or name


def _remaining_time_text(expires_at) -> str:
    """متن «زمان باقی‌مانده» به‌شکل خلاصه (روز/ساعت/دقیقه) — برای ردیف تزئینی اطلاعات."""
    if not expires_at:
        return "∞"
    try:
        exp_dt = datetime.fromisoformat(str(expires_at))
        now_dt = datetime.now(exp_dt.tzinfo) if getattr(exp_dt, "tzinfo", None) else datetime.now()
        secs = int((exp_dt - now_dt).total_seconds())
        if secs <= 0:
            return "منقضی"
        days, rem = divmod(secs, 86400)
        hours, rem = divmod(rem, 3600)
        mins = rem // 60
        return f"{days}د {hours}س" if days else (f"{hours}س {mins}د" if hours else f"{mins}د")
    except Exception:
        return str(expires_at)[:16]


def build_info_server_remark(used_bytes: int, limit_bytes: int, expires_at) -> str:
    """متن نمایشیِ «سرور اطلاعاتی» که (در صورت فعال بودن از تنظیمات) به‌عنوان یک ردیف
    ردیف اطلاعاتی که با آدرس و UUID واقعی ساخته می‌شود (کانفیگ واقعی و قابل‌اتصال؛ هیچ آدرس فیکی
    در خروجی نوشته نمی‌شود) و حجم/زمان باقی‌مانده را به کاربر نشان می‌دهد."""
    show_volume = bool(CONFIG.get("sub_info_line_show_volume", True))
    show_expiry = bool(CONFIG.get("sub_info_line_show_expiry", True))
    parts = ["🌐 Vodiwalkerpanel"]
    if show_volume:
        used = int(used_bytes or 0)
        limit = int(limit_bytes or 0)
        remaining = max(0, limit - used) if limit > 0 else 0
        parts.append(
            f"{fmt_bytes(used)}/{fmt_bytes(limit)} (باقی {fmt_bytes(remaining)})"
            if limit > 0 else f"{fmt_bytes(used)}/∞"
        )
    if show_expiry:
        parts.append(_remaining_time_text(expires_at))
    return " | ".join(parts)


def build_manual_uri(
    link: dict,
    uid: str,
    host: str,
    port_override: int | None = None,
) -> str:
    """ساخت لینک کانفیگ برای حالت پروتکل دستی (Manual) — دقیقاً مثل پنل‌های
    3x-ui/Sanaei: پروتکل پایه + شبکه (Network) + امنیت (Security) + فیلدهای
    دستی (آدرس، پورت، مسیر، هاست هدر، SNI، Reality و ...) هر کدام جدا انتخاب
    می‌شن و لینک نهایی از روی آن‌ها ساخته می‌شود."""

    base_protocol = normalize_base_protocol(link.get("base_protocol"))
    network = normalize_network(link.get("network"))
    security = normalize_security(link.get("security"))

    remark = build_config_remark(link, uid)
    label = quote(remark, safe="")

    fp = (link.get("fingerprint") or DEFAULT_FINGERPRINT).strip().lower()
    if fp not in FINGERPRINTS:
        fp = DEFAULT_FINGERPRINT

    port_value = safe_int(
        port_override if port_override is not None else link.get("port"),
        DEFAULT_PORT, MIN_PORT, MAX_PORT,
    )

    address = (str(link.get("address") or "")).strip() or host
    default_alpn = "h2,http/1.1" if network == "xhttp" else "http/1.1"
    alpn_value = (str(link.get("alpn") or default_alpn)).strip()
    xhttp_mode = normalize_xhttp_mode(link.get("xhttp_mode"))
    path = (str(link.get("path") or "")).strip()
    if network == "xhttp":
        # ریلی فقط روی /xhttp-siz10/{mode}/{uuid}/... جواب می‌ده (mode باید packet-up یا
        # stream-up باشه، نه auto). مسیر قدیمیِ /xhttp/{uid} هیچ‌وقت وصل نمی‌شد، پس مثل «خالی» حساب می‌شه.
        if xhttp_mode == "auto":
            xhttp_mode = "packet-up"
        if not path or path == f"/xhttp/{uid}":
            path = f"/xhttp-siz10/{xhttp_mode}/{uid}"
    elif not path:
        path = f"/{network}/{uid}"
    host_header = (str(link.get("host_header") or "")).strip() or address
    sni = (str(link.get("sni") or "")).strip() or address
    flow = (str(link.get("flow") or "")).strip()
    grpc_service = (str(link.get("grpc_service_name") or "")).strip() or uid

    q: dict[str, str] = {}
    if base_protocol == "vless":
        q["encryption"] = "none"

    if security == "tls":
        q["security"] = "tls"
        q["sni"] = sni
        q["fp"] = fp
        q["alpn"] = alpn_value
        if link.get("allow_insecure"):
            q["allowInsecure"] = "1"
    elif security == "reality":
        q["security"] = "reality"
        q["sni"] = sni
        q["fp"] = fp
        q["pbk"] = (str(link.get("reality_public_key") or "")).strip()
        q["sid"] = (str(link.get("reality_short_id") or "")).strip()
        q["spx"] = (str(link.get("reality_spider_x") or "")).strip() or "/"
    else:
        q["security"] = "none"

    if network == "ws":
        q["type"] = "ws"
        q["path"] = path
        q["host"] = host_header
    elif network == "grpc":
        q["type"] = "grpc"
        q["serviceName"] = grpc_service
        q["mode"] = (str(link.get("grpc_mode") or "gun")).strip() or "gun"
    elif network == "xhttp":
        q["type"] = "xhttp"
        q["mode"] = xhttp_mode
        q["path"] = path
        q["host"] = host_header
    else:
        q["type"] = "tcp"
        header_type = (str(link.get("header_type") or "")).strip()
        if header_type:
            q["headerType"] = header_type
        if flow:
            q["flow"] = flow

    if base_protocol == "shadowsocks":
        method = str(link.get("ss_method") or "chacha20-ietf-poly1305").strip()
        password = str(link.get("ss_password") or uid).strip()
        if not method:
            method = "chacha20-ietf-poly1305"
        userinfo = f"{method}:{password}"
        token = base64.urlsafe_b64encode(userinfo.encode()).decode().rstrip("=")
        return f"ss://{token}@{address}:{port_value}#{label}"

    if base_protocol == "vmess":
        raw = {
            "v": "2", "ps": remark, "add": address, "port": port_value, "id": uid,
            "aid": 0, "scy": "auto", "net": network, "type": "none",
            "host": host_header if network in ("ws", "xhttp") else "",
            "path": grpc_service if network == "grpc" else path,
            "tls": security if security != "none" else "",
            "sni": sni, "fp": fp,
        }
        return "vmess://" + base64.b64encode(
            json.dumps(raw, separators=(",", ":"), ensure_ascii=False).encode()
        ).decode()

    scheme = "trojan" if base_protocol == "trojan" else "vless"
    qs = "&".join(f"{k}={quote(str(v), safe=',/')}" for k, v in q.items() if v not in (None, ""))
    return f"{scheme}://{uid}@{address}:{port_value}?{qs}#{label}"


def vless_link_for_link(
    link: dict,
    uid: str,
    host: str,
    port_override: int | None = None,
):
    protocol = normalize_protocol(link.get("protocol", DEFAULT_PROTOCOL))
    if protocol == "manual":
        return build_manual_uri(link, uid, host, port_override=port_override)
    return generate_vless_link(
        uid,
        host,
        remark=str(link.get("label") or "Config"),
        protocol=protocol,
        fingerprint=link.get(
            "fingerprint",
            DEFAULT_FINGERPRINT,
        ),
        alpn=link.get(
            "alpn"
        ),
        port=port_override if port_override is not None else link.get(
            "port",
            DEFAULT_PORT,
        ),
    )


def get_link_info(
    link: dict,
    uid: str,
    host: str,
):
    connected_count = len(unique_ips_for_uuid(uid))
    is_active = is_link_allowed(link)
    limit_b = int(link.get("limit_bytes", 0) or 0)
    used_b = int(link.get("used_bytes", 0) or 0)
    is_expired = is_link_expired(link) or (limit_b > 0 and used_b >= limit_b)
    if not is_active or is_expired:
        status_color = "red"
    elif connected_count > 0:
        status_color = "green"
    else:
        status_color = "gray"
    clean_ips = link.get("clean_ips") or []
    cfg_count = int(link.get("config_count") or 1)
    show_vless = len(clean_ips) <= 1 and cfg_count <= 1
    cat = CATEGORIES.get(str(link.get("category_id") or "0")) or {}
    protocol = normalize_protocol(link.get("protocol"))
    if protocol == "manual":
        display_network = normalize_network(link.get("network"))
        display_security = normalize_security(link.get("security"))
    else:
        display_network, display_security = PROTOCOL_NETWORK_SECURITY.get(
            protocol, ("tcp", "none")
        )
    manual_network = normalize_network(link.get("network"))
    manual_security = normalize_security(link.get("security"))
    manual_mode = normalize_xhttp_mode(link.get("xhttp_mode"))
    manual_live = (
        protocol == "manual"
        and normalize_base_protocol(link.get("base_protocol")) == "vless"
        and (manual_network, manual_security) in MANUAL_LIVE_COMBOS
        and not (manual_network == "xhttp" and manual_mode == "stream-one")
    )
    if protocol == "manual":
        live_status = "live" if manual_live else "link-only"
    elif protocol in LIVE_PROTOCOLS:
        live_status = "live"
    else:
        live_status = "link-only"
    return {
        "uuid": uid,
        "name": link.get("label", ""),
        "label": link.get("label", ""),
        "protocol": link.get("protocol", DEFAULT_PROTOCOL),
        "protocol_display": protocol_display_label(link),
        "base_protocol": normalize_base_protocol(link.get("base_protocol")),
        "network": display_network,
        "security": display_security,
        "manual_live": manual_live,
        "live_status": live_status,
        "live_reason": ("این پروتکل توسط هسته فعلی سرو می‌شود." if live_status == "live" else "فقط لینک ساخته می‌شود؛ برای اجرای واقعی این ترکیب به Xray-core/Inbound خارجی نیاز است."),
        "address": link.get("address", ""),
        "path": link.get("path", ""),
        "host_header": link.get("host_header", ""),
        "sni": link.get("sni", ""),
        "flow": link.get("flow", ""),
        "grpc_service_name": link.get("grpc_service_name", ""),
        "grpc_mode": link.get("grpc_mode", "gun"),
        "xhttp_mode": normalize_xhttp_mode(link.get("xhttp_mode")),
        "header_type": link.get("header_type", ""),
        "allow_insecure": bool(link.get("allow_insecure", False)),
        "reality_public_key": link.get("reality_public_key", ""),
        "reality_short_id": link.get("reality_short_id", ""),
        "reality_spider_x": link.get("reality_spider_x", "/"),
        "active": is_active,
        "used_bytes": used_b,
        "limit_bytes": limit_b,
        "expires_at": link.get("expires_at"),
        "ip_limit": int(link.get("ip_limit", 0) or 0),
        "speed_limit_bytes": int(link.get("speed_limit_bytes", 0) or 0),
        "connection_limit": int(link.get("connection_limit", 0) or 0),
        "fragment": link.get("fragment", "off"),
        "fingerprint": link.get("fingerprint", DEFAULT_FINGERPRINT),
        "alpn": link.get("alpn", ""),
        "port": link.get("port", DEFAULT_PORT),
        "note": link.get("note", ""),
        "clean_ips": clean_ips,
        "alarm_enabled": bool(link.get("alarm_enabled", False)),
        "category_id": str(link.get("category_id") or "0"),
        "category_number": int(cat.get("number", 0)),
        "category_name": str(cat.get("name", "عمومی")),
        "config_count": cfg_count,
        "client_limit": int(link.get("client_limit") or 0),
        "outbound_proxy_id": str(link.get("outbound_proxy_id") or ""),
        "outbound": outbound_info(link.get("outbound_proxy_id")),
        "combo_group_id": link.get("combo_group_id") or "",
        "parent_inbound_id": link.get("parent_inbound_id"),
        "is_client": bool(link.get("parent_inbound_id")),
        "status_color": status_color,
        "connected_ips": connected_count,
        "show_vless": show_vless,
        "vless": vless_link_for_link(link, uid, host) if show_vless else "",
        "vless_full": vless_link_for_link(link, uid, host),
        "sub": f"{get_scheme()}://{host}/sub/{uid}",
        "info": f"{get_scheme()}://{host}/info/{uid}",
        "support": get_support_username(),
        "support_url": get_support_url(),
    }


# ============================================================
# PERSISTENCE
# ============================================================

async def load_state():

    global AUTH

    try:

        DATA_DIR.mkdir(
            parents=True,
            exist_ok=True,
        )

        if not DATA_FILE.exists():
            return

        async with aiofiles.open(
            DATA_FILE,
            "r",
            encoding="utf-8",
        ) as file:
            raw = await file.read()

        data = json.loads(raw)

        LINKS.update(
            data.get(
                "links",
                {},
            )
        )

        SUBS.update(
            data.get(
                "subs",
                {},
            )
        )

        CATEGORIES.update(
            data.get(
                "categories",
                {},
            )
        )

        stored_username = data.get("username")
        if isinstance(stored_username, str) and stored_username.strip():
            AUTH["username"] = stored_username.strip()

        stored_password = data.get(
            "password_hash"
        )

        if stored_password:
            AUTH[
                "password_hash"
            ] = stored_password

        ADMINS.update(
            data.get("admins", {})
        )

        ADMIN_REQUESTS.update(
            data.get("admin_requests", {})
        )

        DAILY_STATS.update(
            data.get("daily_stats", {})
        )

        # بازیابی تنظیمات پنل (آدرس عمومی + مشخصات ربات تلگرام)
        BOT_TEXTS.update(data.get("bot_texts") or {})
        settings_data = data.get("settings") or {}
        if settings_data.get("public_base_url"):
            CONFIG["public_base_url"] = str(settings_data.get("public_base_url") or "").strip()
        if settings_data.get("tcp_public_host"):
            CONFIG["tcp_public_host"] = str(settings_data.get("tcp_public_host") or "").strip()
        if settings_data.get("tcp_public_port"):
            CONFIG["tcp_public_port"] = str(settings_data.get("tcp_public_port") or "").strip()
        CONFIG["bot_auto_start"] = bool(settings_data.get("bot_auto_start", False))
        for _k in PERSISTED_EXTRA_SETTINGS:
            if _k in settings_data:
                CONFIG[_k] = settings_data[_k]
        try:
            import telegram_bot
            telegram_bot.configure(
                token=settings_data.get("bot_token"),
                admin_ids_raw=settings_data.get("bot_admin_ids"),
            )
        except Exception as exc:
            logger.warning("Could not restore bot settings: %s", exc)

        # Compatibility for older records
        for uid, link in LINKS.items():

            link.setdefault(
                "protocol",
                DEFAULT_PROTOCOL,
            )

            link.setdefault(
                "fingerprint",
                DEFAULT_FINGERPRINT,
            )

            link.setdefault(
                "alpn",
                "",
            )

            link.setdefault(
                "port",
                DEFAULT_PORT,
            )

            link.setdefault(
                "ip_limit",
                0,
            )

            link.setdefault(
                "speed_limit_bytes",
                0,
            )

            link.setdefault(
                "connection_limit",
                0,
            )

            link.setdefault(
                "fragment",
                "off",
            )

            link.setdefault(
                "used_bytes",
                0,
            )
            link.setdefault("clean_ips", [])
            link.setdefault("alarm_enabled", False)
            link.setdefault("category_id", "0")
            link.setdefault("config_count", 1)
            link.setdefault("client_limit", 0)
            link.setdefault("usage_history", [])
            link.setdefault("parent_inbound_id", None)

        logger.info(
            "State loaded: %d links / %d subscriptions",
            len(LINKS),
            len(SUBS),
        )

    except Exception as exc:

        logger.exception(
            "Could not load state: %s",
            exc,
        )


async def save_state():

    async with SAVE_LOCK:

        try:

            DATA_DIR.mkdir(
                parents=True,
                exist_ok=True,
            )

            payload = {
                "links":
                    dict(LINKS),

                "subs":
                    dict(SUBS),

                "categories":
                    dict(CATEGORIES),

                "username": AUTH.get("username", DEFAULT_ADMIN_USERNAME),

                "password_hash":
                    AUTH[
                        "password_hash"
                    ],

                "admins":
                    dict(ADMINS),

                "admin_requests":
                    dict(ADMIN_REQUESTS),

                "daily_stats":
                    dict(DAILY_STATS),

                # تنظیمات پنل: آدرس عمومی + مشخصات ربات تلگرام (برای اینکه با ری‌استارت
                # سرویس از دست نرن و نیازی به .env دستی نباشه).
                "bot_texts": BOT_TEXTS,
                "settings": {
                    "public_base_url": CONFIG.get("public_base_url", ""),
                    "tcp_public_host": CONFIG.get("tcp_public_host", ""),
                    "tcp_public_port": CONFIG.get("tcp_public_port", ""),
                    "bot_token": _bot_settings_snapshot().get("bot_token", ""),
                    "bot_admin_ids": _bot_settings_snapshot().get("admin_ids", ""),
                    "bot_auto_start": bool(CONFIG.get("bot_auto_start", False)),
                    **{k: CONFIG[k] for k in PERSISTED_EXTRA_SETTINGS if k in CONFIG},
                },

                "saved_at":
                    datetime.now().isoformat(),
            }

            temp_file = (
                DATA_FILE.with_suffix(
                    ".tmp"
                )
            )

            async with aiofiles.open(
                temp_file,
                "w",
                encoding="utf-8",
            ) as file:

                await file.write(
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        indent=2,
                    )
                )

            temp_file.replace(
                DATA_FILE
            )

        except Exception as exc:

            logger.exception(
                "Could not save state: %s",
                exc,
            )


# ============================================================
# DEFAULT LINK
# ============================================================

_default_link_created = False



async def ensure_default_categories():
    if CATEGORIES:
        return
    CATEGORIES["0"] = {
        "id": "0", "name": "عمومی", "number": 0,
        "limit_bytes": 0, "expires_days": 0, "connection_limit": 0,
        "speed_limit_bytes": 0, "ip_limit": 0, "clean_ips": [],
        "random_name": False, "single_user": False,
        "created_at": datetime.now().isoformat(),
    }
    CATEGORIES["1"] = {
        "id": "1", "name": "VIP", "number": 1,
        "limit_bytes": 0, "expires_days": 0, "connection_limit": 1,
        "speed_limit_bytes": 0, "ip_limit": 1, "clean_ips": [],
        "random_name": False, "single_user": True,
        "created_at": datetime.now().isoformat(),
    }
    asyncio.create_task(save_state())

async def ensure_default_link():

    global _default_link_created

    if _default_link_created:
        return

    async with LINKS_LOCK:

        if not any(
            item.get("is_default")
            for item in LINKS.values()
        ):

            digest = hashlib.sha256(
                (
                    "default"
                    + SECRET_KEY
                ).encode("utf-8")
            ).hexdigest()

            uid = (
                f"{digest[:8]}-"
                f"{digest[8:12]}-"
                f"{digest[12:16]}-"
                f"{digest[16:20]}-"
                f"{digest[20:32]}"
            )

            LINKS[uid] = {
                "label":
                    "لینک پیش‌فرض",

                "limit_bytes":
                    0,

                "used_bytes":
                    0,

                "created_at":
                    datetime.now().isoformat(),

                "active":
                    True,

                "expires_at":
                    None,

                "note":
                    "",

                "is_default":
                    True,

                "sub_id":
                    None,

                "protocol":
                    DEFAULT_PROTOCOL,

                "fingerprint":
                    DEFAULT_FINGERPRINT,

                "alpn":
                    "http/1.1",

                "port":
                    DEFAULT_PORT,

                "ip_limit":
                    0,

                "speed_limit_bytes":
                    DEFAULT_SPEED_LIMIT,

                "connection_limit":
                    0,

                "fragment":
                    "off",
            }

            asyncio.create_task(
                save_state()
            )

    _default_link_created = True


# ============================================================
# LINK MANAGEMENT
# ============================================================

def clean_outbound_proxy_id(value) -> str:
    """'' = مستقیم (خود Railway). غیرخالی باید شناسه‌ی یک پراکسی موجود باشه."""
    pid = str(value or "").strip()
    if not pid:
        return ""
    try:
        from outbound_proxy import get_proxy
    except Exception:
        raise HTTPException(status_code=400, detail="ماژول پراکسی خروجی در دسترس نیست")
    if not get_proxy(pid):
        raise HTTPException(status_code=400, detail="پراکسی انتخاب‌شده پیدا نشد (شاید حذف شده)")
    return pid


def public_remote_links(entries: list) -> list:
    """نسخه‌ی امن یک لیست عضو ریموت برای پنل (بدون خودِ رشته‌ی vless)."""
    try:
        from nodes import get_node
    except Exception:
        get_node = lambda _id: None
    out = []
    for e in entries:
        node = get_node(e.get("node_id") or "")
        out.append({
            "id": f"{e.get('node_id')}:{e.get('uuid')}",
            "node_id": e.get("node_id"),
            "node_name": (node or {}).get("name") or "نود حذف‌شده",
            "node_online": bool(node),
            "node_missing": bool(e.get("node_missing")),
            "uuid": e.get("uuid"),
            "label": e.get("label", ""),
            "protocol_display": e.get("protocol_display", ""),
            "active": bool(e.get("active", True)),
            "last_error": e.get("last_error", ""),
            "last_synced_at": e.get("last_synced_at"),
            "added_at": e.get("added_at"),
        })
    return out


def outbound_info(value):
    """خلاصه‌ی نام/کشور/پرچم پراکسی خروجی برای نمایش در پنل (None = مستقیم)."""
    pid = str(value or "").strip()
    if not pid:
        return None
    try:
        from outbound_proxy import proxy_summary
        return proxy_summary(pid)
    except Exception:
        return None


async def make_link(
    label: str = "لینک جدید",
    limit_bytes: int = 0,
    expires_at: str | None = None,
    note: str = "",
    sub_id: str | None = None,
    protocol: str = DEFAULT_PROTOCOL,
    fingerprint: str = DEFAULT_FINGERPRINT,
    alpn: str = "",
    port: int = DEFAULT_PORT,
    ip_limit: int = 0,
    speed_limit_bytes: int = 0,
    connection_limit: int = 0,
    fragment: str = "off",
    clean_ips=None,
    alarm_enabled: bool = False,
    category_id: str = "0",
    config_count: int = 1,
    manual_fields: dict | None = None,
    outbound_proxy_id: str = "",
):

    protocol = normalize_protocol(protocol)
    manual_fields = manual_fields or {}

    fingerprint = (
        fingerprint
        or DEFAULT_FINGERPRINT
    ).strip().lower()

    if fingerprint not in FINGERPRINTS:
        fingerprint = DEFAULT_FINGERPRINT

    if not (
        MIN_PORT
        <= port
        <= MAX_PORT
    ):
        port = DEFAULT_PORT

    uid = generate_uuid()

    record = {
        "label":
            (sanitize_display_name((label or "").strip()) if (label or "").strip() else random_config_name()),

        "limit_bytes":
            max(
                0,
                int(limit_bytes),
            ),

        "used_bytes":
            0,

        "created_at":
            datetime.now().isoformat(),

        "active":
            True,

        "expires_at":
            expires_at,

        "note":
            (
                note
                or ""
            ).strip()[:500],

        "is_default":
            False,

        "sub_id":
            sub_id,

        # A child client is a real live credential: it owns its own UUID and is
        # therefore accepted by the VLESS/XHTTP relay exactly like the parent.
        "parent_inbound_id": None,

        "protocol":
            protocol,

        "fingerprint":
            fingerprint,

        "alpn":
            (
                alpn
                or ""
            ).strip()[:100],

        "port":
            port,

        "ip_limit":
            max(
                0,
                int(ip_limit),
            ),

        "speed_limit_bytes":
            max(
                0,
                int(speed_limit_bytes),
            ),

        "connection_limit":
            max(
                0,
                int(connection_limit),
            ),

        "fragment":
            (
                fragment
                or "off"
            ).strip().lower(),

        "security_profile": "balanced",
        "multi_login": False,
        "clean_ips": list(clean_ips or []),
        "alarm_enabled": bool(alarm_enabled),
        "category_id": str(category_id or "0"),
        "config_count": max(1, min(40, int(config_count or 1))),
        "client_limit": 0,
        "usage_history": [],
        # مسیر خروجی: خالی = مستقیم از روی Railway. غیرخالی = شناسه‌ی یک پراکسی
        # SOCKS از outbound_proxy.py که ارتباط با مقصد از طریق اون رد می‌شه.
        "outbound_proxy_id": str(outbound_proxy_id or "").strip(),
    }

    if protocol == "manual":
        record.update({
            "base_protocol": normalize_base_protocol(manual_fields.get("base_protocol")),
            "network": normalize_network(manual_fields.get("network")),
            "security": normalize_security(manual_fields.get("security")),
            "address": str(manual_fields.get("address") or "").strip()[:255],
            "path": str(manual_fields.get("path") or "").strip()[:255],
            "host_header": str(manual_fields.get("host_header") or "").strip()[:255],
            "sni": str(manual_fields.get("sni") or "").strip()[:255],
            "flow": str(manual_fields.get("flow") or "").strip()[:64],
            "grpc_service_name": str(manual_fields.get("grpc_service_name") or "").strip()[:128],
            "grpc_mode": str(manual_fields.get("grpc_mode") or "gun").strip()[:32] or "gun",
            "xhttp_mode": normalize_xhttp_mode(manual_fields.get("xhttp_mode")),
            "header_type": str(manual_fields.get("header_type") or "").strip()[:32],
            "allow_insecure": bool(manual_fields.get("allow_insecure", False)),
            "reality_public_key": str(manual_fields.get("reality_public_key") or "").strip()[:128],
            "reality_short_id": str(manual_fields.get("reality_short_id") or "").strip()[:32],
            "reality_spider_x": str(manual_fields.get("reality_spider_x") or "/").strip()[:128] or "/",
            "ss_method": str(manual_fields.get("ss_method") or "chacha20-ietf-poly1305").strip()[:80],
            "ss_password": str(manual_fields.get("ss_password") or uid).strip()[:255],
        })

    record["protocol_label"] = protocol_display_label(record)

    async with LINKS_LOCK:
        LINKS[uid] = record
        if not record.get("parent_inbound_id"):
            _apply_create_restrict(uid)

    bump_daily_stat("new_links")

    if sub_id:

        async with SUBS_LOCK:

            if sub_id in SUBS:

                ids = SUBS[
                    sub_id
                ].setdefault(
                    "link_ids",
                    [],
                )

                if uid not in ids:
                    ids.append(uid)

    await save_state()

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{record['label']}» "
            f"ساخته شد"
        ),
        "ok",
    )

    return uid, record


async def remove_link(
    uid: str,
):

    async with LINKS_LOCK:

        if uid not in LINKS:
            return None

        label = LINKS[
            uid
        ].get(
            "label",
            uid,
        )

        sub_id = LINKS[
            uid
        ].get(
            "sub_id"
        )

        del LINKS[uid]
        scope_remove_uid(uid)

    if sub_id:

        async with SUBS_LOCK:

            if sub_id in SUBS:

                ids = SUBS[
                    sub_id
                ].get(
                    "link_ids",
                    [],
                )

                if uid in ids:
                    ids.remove(uid)

    await save_state()

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{label}» "
            f"حذف شد"
        ),
        "warn",
    )

    return label


async def update_link_fields(
    uid: str,
    *,
    label: str | None = None,
    add_bytes: int | None = None,
    set_limit_bytes: int | None = None,
    extend_days: int | None = None,
    reset_usage: bool = False,
    ip_limit: int | None = None,
    speed_limit_bytes: int | None = None,
):
    """ویرایش امن فیلدهای یک کانفیگ (برای ربات تلگرام): نام، حجم، تمدید، ریست مصرف و ..."""
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if link is None:
            return None
        if label is not None:
            value = str(label).strip()[:60]
            if value:
                link["label"] = value
        if reset_usage:
            link["used_bytes"] = 0
        if set_limit_bytes is not None:
            link["limit_bytes"] = max(0, int(set_limit_bytes))
        if add_bytes:
            link["limit_bytes"] = max(0, int(link.get("limit_bytes") or 0)) + int(add_bytes)
        if extend_days:
            now = datetime.now()
            base = now
            try:
                cur = datetime.fromisoformat(str(link.get("expires_at"))) if link.get("expires_at") else None
                if cur is not None and cur.tzinfo is None and cur > now:
                    base = cur
            except Exception:
                pass
            link["expires_at"] = (base + timedelta(days=int(extend_days))).isoformat()
        if ip_limit is not None:
            link["ip_limit"] = max(0, int(ip_limit))
        if speed_limit_bytes is not None:
            link["speed_limit_bytes"] = max(0, int(speed_limit_bytes))
        record = link
    await save_state()
    return record


async def set_link_active(
    uid: str,
    active: bool,
):

    async with LINKS_LOCK:

        if uid not in LINKS:
            return None

        LINKS[
            uid
        ][
            "active"
        ] = bool(active)

        record = LINKS[uid]

    await save_state()

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{record['label']}» "
            f"{'فعال' if active else 'غیرفعال'} شد"
        ),
        "ok"
        if active
        else "warn",
    )

    return record


# ============================================================
# SUB GROUPS
# ============================================================

async def create_sub_group(
    name: str = "گروه جدید",
    desc: str = "",
    password: str = "",
):

    name = (
        name
        or "گروه جدید"
    ).strip()[:60]

    desc = (
        desc
        or ""
    ).strip()[:200]

    password = (
        password
        or ""
    ).strip()

    sub_id = generate_uuid()

    uuid_key = secrets.token_urlsafe(16)

    record = {
        "name":
            name,

        "desc":
            desc,

        "password_hash":
            (
                hash_password(password)
                if password
                else None
            ),

        "uuid_key":
            uuid_key,

        "created_at":
            datetime.now().isoformat(),

        "link_ids":
            [],

        "remote_links":
            [],
    }

    async with SUBS_LOCK:
        SUBS[sub_id] = record

    await save_state()

    log_activity(
        "sub",
        (
            f"گروه "
            f"«{name}» "
            f"ساخته شد"
        ),
        "ok",
    )

    return (
        sub_id,
        record,
    )


async def set_link_sub(
    uid: str,
    sub_id: str | None,
):

    async with LINKS_LOCK:

        if uid not in LINKS:
            return False

        old_sub = LINKS[
            uid
        ].get(
            "sub_id"
        )

        label = LINKS[
            uid
        ].get(
            "label",
            uid,
        )

    if sub_id is not None:

        async with SUBS_LOCK:

            if sub_id not in SUBS:
                return False

    async with SUBS_LOCK:

        if (
            old_sub
            and old_sub in SUBS
        ):

            ids = SUBS[
                old_sub
            ].get(
                "link_ids",
                [],
            )

            if uid in ids:
                ids.remove(uid)

        if (
            sub_id
            and sub_id in SUBS
        ):

            ids = SUBS[
                sub_id
            ].setdefault(
                "link_ids",
                [],
            )

            if uid not in ids:
                ids.append(uid)

    async with LINKS_LOCK:

        if uid in LINKS:

            LINKS[
                uid
            ][
                "sub_id"
            ] = sub_id

    await save_state()

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{label}» "
            f"{'به گروه اضافه شد' if sub_id else 'از گروه خارج شد'}"
        ),
        "info",
    )

    return True


async def remove_sub_group(
    sub_id: str,
):

    async with SUBS_LOCK:

        if sub_id not in SUBS:
            return None

        name = SUBS[
            sub_id
        ].get(
            "name",
            sub_id,
        )

        del SUBS[sub_id]

    async with LINKS_LOCK:

        for link in LINKS.values():

            if (
                link.get("sub_id")
                == sub_id
            ):
                link["sub_id"] = None

    await save_state()

    log_activity(
        "sub",
        (
            f"گروه "
            f"«{name}» "
            f"حذف شد"
        ),
        "warn",
    )

    return name


async def usage_history_loop():
    """Persist live usage history periodically so the customer graph survives restarts."""
    while True:
        try:
            await asyncio.sleep(60)
            now = now_ir()
            ts = now.replace(second=0, microsecond=0).isoformat()
            changed = False
            async with LINKS_LOCK:
                for link in LINKS.values():
                    history = link.setdefault("usage_history", [])
                    used = int(link.get("used_bytes", 0) or 0)
                    limit = int(link.get("limit_bytes", 0) or 0)
                    if history and str(history[-1].get("ts", ""))[:16] == ts[:16]:
                        if history[-1].get("used") != used or history[-1].get("limit") != limit:
                            history[-1]["used"] = used
                            history[-1]["limit"] = limit
                            changed = True
                    else:
                        history.append({"ts": ts, "used": used, "limit": limit})
                        if len(history) > 144:
                            del history[:-144]
                        changed = True
            if changed:
                await save_state()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("Usage history loop error: %s", exc)


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
async def startup():

    global http_client, USAGE_PERSIST_TASK

    limits = httpx.Limits(
        max_connections=500,
        max_keepalive_connections=100,
    )

    timeout = httpx.Timeout(
        30.0,
        connect=10.0,
    )

    http_client = httpx.AsyncClient(
        limits=limits,
        timeout=timeout,
        follow_redirects=True,
    )

    await load_state()

    USAGE_PERSIST_TASK = asyncio.create_task(usage_history_loop())

    await ensure_default_categories()
    await ensure_default_link()

    log_activity(
        "system",
        (
            f"{APP_NAME} "
            f"v{APP_VERSION} "
            f"راه‌اندازی شد"
        ),
        "ok",
    )

    logger.info(
        "%s v%s started on 0.0.0.0:%s",
        APP_NAME,
        APP_VERSION,
        PORT,
    )

    logger.info(
        "Data directory: %s",
        DATA_DIR,
    )

    try:
        import tcp_relay
        await tcp_relay.start_tcp_relay(app_logger=logger)
    except Exception as exc:
        logger.warning("VLESS-TCP relay startup skipped: %s", exc)


@app.on_event("shutdown")
async def shutdown():

    global USAGE_PERSIST_TASK
    if USAGE_PERSIST_TASK:
        USAGE_PERSIST_TASK.cancel()
        try:
            await USAGE_PERSIST_TASK
        except asyncio.CancelledError:
            pass
        USAGE_PERSIST_TASK = None

    await save_state()

    if http_client:
        await http_client.aclose()

    try:
        import tcp_relay
        await tcp_relay.stop_tcp_relay()
    except Exception:
        pass


# ============================================================
# LANDING
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    # The old public landing/interstitial page has been removed.
    # Visitors go straight to the real login page; authenticated users go to the dashboard.
    if await is_valid_session(request.cookies.get(SESSION_COOKIE)):
        return RedirectResponse("/dashboard")
    return RedirectResponse("/login")



# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
async def health():

    return {
        "status": "ok",
        "service": APP_NAME,
        "version": APP_VERSION,
        "connections": len(connections),
        "uptime": uptime(),
    }


# ============================================================
# LIVE TELEMETRY
# ============================================================

@app.get("/api/telemetry")
async def api_telemetry(_=Depends(require_auth)):
    """Lightweight live server metrics for the dashboard."""
    global _telemetry_prev
    now = time.time()
    vm = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = psutil.disk_usage(str(DATA_DIR))
    cpu = psutil.cpu_percent(interval=None)
    load = None
    try:
        load = [round(x, 2) for x in os.getloadavg()]
    except Exception:
        load = []
    net = psutil.net_io_counters()
    async with _telemetry_lock:
        prev = _telemetry_prev
        dt = max(0.25, now - float(prev.get("ts", now)))
        rx_rate = max(0, net.bytes_recv - int(prev.get("rx", net.bytes_recv))) / dt
        tx_rate = max(0, net.bytes_sent - int(prev.get("tx", net.bytes_sent))) / dt
        _telemetry_prev = {"ts": now, "rx": net.bytes_recv, "tx": net.bytes_sent}
    process = psutil.Process(os.getpid())
    sample = {
        "ts": datetime.now().isoformat(),
        "cpu": _pct(cpu),
        "ram": _pct(vm.percent),
        "swap": _pct(swap.percent),
        "storage": _pct(disk.percent),
        "rx_bps": int(rx_rate),
        "tx_bps": int(tx_rate),
        "connections": len(connections),
    }
    TELEMETRY_HISTORY.append(sample)
    return {
        "ok": True,
        "cpu": _pct(cpu),
        "cpu_cores": psutil.cpu_count(logical=True) or 1,
        "ram": {"percent": _pct(vm.percent), "used": vm.used, "total": vm.total},
        "swap": {"percent": _pct(swap.percent), "used": swap.used, "total": swap.total},
        "storage": {"percent": _pct(disk.percent), "used": disk.used, "total": disk.total},
        "network": {"rx_bps": int(rx_rate), "tx_bps": int(tx_rate), "bytes_recv": int(net.bytes_recv), "bytes_sent": int(net.bytes_sent)},
        "connections": len(connections),
        "traffic_bytes": int(stats.get("total_bytes", 0)),
        "requests": int(stats.get("total_requests", 0)),
        "errors": int(stats.get("total_errors", 0)),
        "uptime": _human_uptime(now - stats.get("start_time", now)),
        "load": load,
        "process": {"rss": process.memory_info().rss, "cpu": _pct(process.cpu_percent(interval=None))},
        "bot_running": bool(_bot_settings_snapshot().get("running")),
        "history": list(TELEMETRY_HISTORY),
    }


# ============================================================
# LOGIN
# ============================================================

from pages import LOGIN_HTML
from pages import ASSET_ICONS_B64, ASSET_VAZIR_B64, ASSET_QR_JS_B64, UI_CSS

_ASSET_ICONS_BYTES = base64.b64decode(ASSET_ICONS_B64)
_ASSET_VAZIR_BYTES = base64.b64decode(ASSET_VAZIR_B64)
_ASSET_QR_BYTES = base64.b64decode(ASSET_QR_JS_B64)
_ASSET_CACHE = {"Cache-Control": "public, max-age=604800, immutable"}


@app.get("/assets/ui.css", include_in_schema=False)
async def asset_ui_css():
    return Response(UI_CSS, media_type="text/css; charset=utf-8", headers=_ASSET_CACHE)


@app.get("/assets/qr.js", include_in_schema=False)
async def asset_qr_js():
    return Response(_ASSET_QR_BYTES, media_type="application/javascript; charset=utf-8", headers=_ASSET_CACHE)


@app.get("/assets/icons.woff2", include_in_schema=False)
async def asset_icons_font():
    return Response(_ASSET_ICONS_BYTES, media_type="font/woff2", headers=_ASSET_CACHE)


@app.get("/assets/vazir.woff2", include_in_schema=False)
async def asset_vazir_font():
    return Response(_ASSET_VAZIR_BYTES, media_type="font/woff2", headers=_ASSET_CACHE)



def login_error_html(
    message: str,
):
    safe_message = escape_html(
        message
    )

    error_block = f'<div class="error" role="alert">{safe_message}</div>'

    if "<!--LOGIN_ERROR-->" in LOGIN_HTML:
        return LOGIN_HTML.replace("<!--LOGIN_ERROR-->", error_block, 1)

    return LOGIN_HTML.replace("</form>", error_block + "</form>", 1)


@app.get(
    "/login",
    response_class=HTMLResponse,
)
async def login_page(
    request: Request,
):

    if await is_valid_session(
        request.cookies.get(
            SESSION_COOKIE
        )
    ):
        return RedirectResponse(
            "/dashboard"
        )

    return HTMLResponse(
        LOGIN_HTML
    )


@app.post("/login")
async def login_form(
    request: Request,
):

    try:

        content_type = (
            request.headers
            .get(
                "content-type",
                "",
            )
            .lower()
        )

        if "application/json" in content_type:

            body = await request.json()

            password = str(
                body.get(
                    "password",
                    "",
                )
            ).strip()

            login_username = str(
                body.get(
                    "username",
                    "",
                )
            ).strip()

        else:

            raw = await request.body()

            parsed = parse_qs(
                raw.decode(
                    "utf-8",
                    errors="ignore",
                )
            )

            password = (
                parsed.get(
                    "password",
                    [""],
                )[0]
                .strip()
            )

            login_username = (
                parsed.get(
                    "username",
                    [""],
                )[0]
                .strip()
            )

    except Exception as exc:

        logger.exception(
            "Login parser error: %s",
            exc,
        )

        return HTMLResponse(
            login_error_html(
                "خطا در پردازش اطلاعات ورود."
            ),
            status_code=400,
        )

    ip = client_ip(request)

    blocked, retry_after = login_is_blocked(ip)
    if blocked:
        minutes = max(1, (retry_after + 59) // 60)
        return HTMLResponse(
            login_error_html(
                f"به دلیل تلاش‌های ناموفق متعدد، ورود موقتاً مسدود شده است. حدود {minutes} دقیقه دیگر دوباره تلاش کنید."
            ),
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )

    if not password:
        register_login_failure(ip)
        return HTMLResponse(
            login_error_html(
                "رمز عبور را وارد کنید."
            ),
            status_code=400,
        )

    ok, admin_id, role, display_name = verify_admin_credentials(login_username, password)

    if not ok:

        locked, value = register_login_failure(ip)
        if locked:
            return HTMLResponse(
                login_error_html(
                    "تعداد تلاش‌های ناموفق بیش از حد مجاز بود. این IP برای ۱۵ دقیقه مسدود شد."
                ),
                status_code=429,
                headers={"Retry-After": str(LOGIN_LOCKOUT_SECONDS)},
            )

        remaining = value
        log_activity(
            "auth",
            (
                f"تلاش ورود ناموفق از {ip}؛ "
                f"{remaining} تلاش باقی مانده"
            ),
            "err",
        )

        return HTMLResponse(
            login_error_html(
                f"رمز عبور اشتباه است. {remaining} تلاش دیگر باقی مانده است."
            ),
            status_code=401,
        )

    clear_login_failures(ip)

    if admin_id != "owner" and admin_id in ADMINS:
        ADMINS[admin_id]["last_login_at"] = datetime.now().isoformat()
        ADMINS[admin_id]["last_login_ip"] = ip
        asyncio.create_task(save_state())

    token = await create_session(admin_id, role)

    response = RedirectResponse(
        "/dashboard?login=1",
        status_code=303,
    )

    set_auth_cookie(
        response,
        request,
        token,
    )

    log_activity(
        "auth",
        (
            f"ورود موفق «{display_name or admin_id}» به پنل "
            f"از {client_ip(request)}"
        ),
        "ok",
    )

    return response


@app.post("/api/login")
async def api_login(
    request: Request,
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="JSON نامعتبر است",
        )

    password = str(
        body.get(
            "password",
            "",
        )
    ).strip()

    login_username = str(
        body.get(
            "username",
            "",
        )
    ).strip()

    ip = client_ip(request)

    blocked, retry_after = login_is_blocked(ip)
    if blocked:
        raise HTTPException(
            status_code=429,
            detail=f"ورود موقتاً مسدود است. حدود {max(1, (retry_after + 59) // 60)} دقیقه دیگر تلاش کنید.",
            headers={"Retry-After": str(retry_after)},
        )

    if not password:
        register_login_failure(ip)
        raise HTTPException(
            status_code=400,
            detail="رمز عبور را وارد کنید",
        )

    ok, admin_id, role, display_name = verify_admin_credentials(login_username, password)

    if not ok:

        locked, value = register_login_failure(ip)
        if locked:
            raise HTTPException(
                status_code=429,
                detail="تعداد تلاش‌های ناموفق بیش از حد مجاز بود. این IP برای ۱۵ دقیقه مسدود شد.",
                headers={"Retry-After": str(LOGIN_LOCKOUT_SECONDS)},
            )

        log_activity(
            "auth",
            (
                f"تلاش ورود ناموفق از {ip}؛ "
                f"{value} تلاش باقی مانده"
            ),
            "err",
        )

        raise HTTPException(
            status_code=401,
            detail=f"رمز عبور اشتباه است؛ {value} تلاش دیگر باقی مانده است",
        )

    clear_login_failures(ip)

    if admin_id != "owner" and admin_id in ADMINS:
        ADMINS[admin_id]["last_login_at"] = datetime.now().isoformat()
        ADMINS[admin_id]["last_login_ip"] = ip
        asyncio.create_task(save_state())

    log_activity(
        "auth",
        f"ورود موفق «{display_name or admin_id}» به پنل از {ip}",
        "ok",
    )

    token = await create_session(admin_id, role)

    response = JSONResponse(
        {
            "ok": True,
            "authenticated": True,
            "admin": {"id": admin_id, "username": display_name or admin_id, "role": role},
        }
    )

    set_auth_cookie(
        response,
        request,
        token,
    )

    return response


# ============================================================
# LOGOUT
# ============================================================

@app.get("/logout")
async def logout_page(
    request: Request,
):

    await destroy_session(
        request.cookies.get(
            SESSION_COOKIE
        )
    )

    response = RedirectResponse(
        "/login"
    )

    response.delete_cookie(
        SESSION_COOKIE,
        path="/",
    )

    return response


@app.post("/api/logout")
async def api_logout(
    request: Request,
):

    await destroy_session(
        request.cookies.get(
            SESSION_COOKIE
        )
    )

    response = JSONResponse(
        {
            "ok": True
        }
    )

    response.delete_cookie(
        SESSION_COOKIE,
        path="/",
    )

    return response


@app.get("/api/me")
async def api_me(
    request: Request,
):

    info = await get_session_info(
        request.cookies.get(SESSION_COOKIE)
    )

    if not info:
        return {"authenticated": False}

    admin_id = info.get("admin_id", "owner")
    role = info.get("role", "owner")

    if admin_id == "owner":
        username = AUTH.get("username", DEFAULT_ADMIN_USERNAME)
    else:
        admin = ADMINS.get(admin_id, {})
        username = admin.get("username", admin_id)

    _a = ADMINS.get(admin_id, {}) if admin_id != "owner" else {}
    return {
        "authenticated": True,
        "admin": {
            "id": admin_id, "username": username, "role": role,
            "permissions": sorted(permissions_for_admin(admin_id)),
            "granted": sorted(k for k in ALL_PERMISSIONS if has_permission(admin_id, k)),
            "allowed_inbounds": list(_a.get("allowed_inbounds") or []),
            "scoped": bool(_a.get("allowed_inbounds")),
        },
    }


# ============================================================
# CHANGE PASSWORD
# ============================================================

@app.get("/api/system/diagnostics")
async def api_system_diagnostics(request: Request, token=Depends(require_auth)):
    """Authenticated live diagnostics used by the Pro dashboard."""
    try:
        import psutil
        proc = psutil.Process(os.getpid())
        vm = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=None)
        mem = proc.memory_info().rss
        net = psutil.net_io_counters()
        disk = psutil.disk_usage("/")
        bot = _bot_settings_snapshot()
        async with LINKS_LOCK:
            links_snapshot = dict(LINKS)
        async with SUBS_LOCK:
            subs_snapshot = dict(SUBS)
        active = sum(1 for x in links_snapshot.values() if is_link_allowed(x))
        clients = sum(1 for x in links_snapshot.values() if x.get("parent_inbound_id"))
        inbounds = len(links_snapshot) - clients
        return {
            "ok": True,
            "time": datetime.now().isoformat(),
            "uptime": _human_uptime(time.time() - stats.get("start_time", time.time())),
            "service": {"status": "healthy", "version": "VodiWalker Pro"},
            "resources": {
                "cpu_percent": round(float(cpu), 1),
                "memory_rss": int(mem),
                "memory_percent": round(float(proc.memory_percent()), 1),
                "system_memory_percent": round(float(vm.percent), 1),
                "disk_percent": round(float(disk.percent), 1),
                "rx_bytes": int(net.bytes_recv),
                "tx_bytes": int(net.bytes_sent),
            },
            "objects": {
                "inbounds": max(0, inbounds),
                "clients": clients,
                "active_links": active,
                "subscriptions": len(subs_snapshot),
                "admins": len(ADMINS) + 1,
                "errors": len(error_logs),
            },
            "bot": {"running": bool(bot.get("running")), "admin_count": len(bot.get("admin_ids", "").split(",")) if bot.get("admin_ids") else 0},
            "security": {"session_count": len(SESSIONS), "username": AUTH.get("username", DEFAULT_ADMIN_USERNAME)},
        }
    except Exception as exc:
        logger.exception("Diagnostics error: %s", exc)
        raise HTTPException(status_code=500, detail="Diagnostics unavailable")


@app.post("/api/security/revoke-other-sessions")
async def api_revoke_other_sessions(request: Request, token=Depends(require_auth)):
    info = await get_session_info(token)
    if not info:
        raise HTTPException(status_code=401, detail="نشست نامعتبر است")
    admin_id = info.get("admin_id", "owner")
    removed = 0
    async with SESSIONS_LOCK:
        stale = [tok for tok, sess in SESSIONS.items() if tok != token and isinstance(sess, dict) and sess.get("admin_id", "owner") == admin_id]
        for tok in stale:
            SESSIONS.pop(tok, None)
            removed += 1
    log_activity("auth", f"نشست‌های قبلی حساب «{AUTH.get('username') if admin_id == 'owner' else ADMINS.get(admin_id, {}).get('username', admin_id)}» لغو شد", "warn")
    return {"ok": True, "revoked": removed}


@app.post("/api/change-password")
async def api_change_password(
    request: Request,
    token=Depends(require_auth),
):
    # نکته مهم (رفع باگ): این endpoint قبلاً همیشه رمز عبور مالک (owner) را
    # چک/جایگزین می‌کرد، حتی وقتی یک ادمین فرعی (sub-admin) وارد شده بود.
    # نتیجه: تغییر رمز برای ادمین‌های فرعی یا با خطای «رمز فعلی اشتباه است»
    # مواجه می‌شد (چون با هش رمز owner مقایسه می‌شد)، یا در بدترین حالت رمز
    # owner را به‌جای رمز خودِ ادمین overwrite می‌کرد. همچنین همه‌ی session های
    # تمام ادمین‌ها پاک می‌شد. اینجا اول مشخص می‌کنیم کدام حساب (owner یا کدام
    # sub-admin) درخواست را زده، سپس دقیقاً همان حساب را چک/آپدیت می‌کنیم و
    # فقط نشست‌های همان حساب باطل می‌شوند، نه بقیه‌ی ادمین‌ها.

    info = await get_session_info(token)
    if not info:
        raise HTTPException(status_code=401, detail="نشست نامعتبر است، دوباره وارد شوید")

    admin_id = info.get("admin_id", "owner")
    is_owner = admin_id == "owner"
    admin_record = None if is_owner else ADMINS.get(admin_id)

    if not is_owner and not admin_record:
        raise HTTPException(status_code=401, detail="حساب کاربری یافت نشد")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="اطلاعات نامعتبر است",
        )

    current_password = str(body.get("current_password", ""))
    current_hash = AUTH["password_hash"] if is_owner else admin_record.get("password_hash", "")

    if hash_password(current_password) != current_hash:
        raise HTTPException(
            status_code=400,
            detail="رمز فعلی اشتباه است",
        )

    new_password = str(body.get("new_password", ""))
    repeat_password = str(body.get("repeat_password", ""))

    if len(new_password) < 8:
        raise HTTPException(
            status_code=400,
            detail="رمز جدید باید حداقل ۸ کاراکتر باشد",
        )

    if new_password == current_password:
        raise HTTPException(
            status_code=400,
            detail="رمز جدید باید با رمز فعلی متفاوت باشد",
        )

    if new_password != repeat_password:
        raise HTTPException(
            status_code=400,
            detail="تکرار رمز عبور یکسان نیست",
        )

    new_hash = hash_password(new_password)

    if is_owner:
        AUTH["password_hash"] = new_hash
    else:
        admin_record["password_hash"] = new_hash

    async with SESSIONS_LOCK:
        # فقط نشست‌های همین حساب باطل می‌شوند (نه همه‌ی ادمین‌ها)، اما نشست
        # فعلی زنده می‌ماند تا کاربر بلافاصله logout نشود.
        stale = [
            tok for tok, sess in SESSIONS.items()
            if sess.get("admin_id", "owner") == admin_id and tok != token
        ]
        for tok in stale:
            SESSIONS.pop(tok, None)

        SESSIONS[token] = {
            "exp": time.time() + SESSION_TTL,
            "admin_id": admin_id,
            "role": info.get("role", "owner" if is_owner else "admin"),
            "permissions": sorted(permissions_for_admin(admin_id)),
        }

    await save_state()

    log_activity(
        "auth",
        "رمز عبور پنل تغییر کرد" if is_owner else f"رمز عبور ادمین «{admin_record.get('username', admin_id)}» تغییر کرد",
        "ok",
    )

    return {
        "ok": True
    }


# ============================================================
# CHANGE USERNAME
# ============================================================

@app.post("/api/change-username")
async def api_change_username(
    request: Request,
    token=Depends(require_auth),
):
    info = await get_session_info(token)
    if not info:
        raise HTTPException(status_code=401, detail="نشست نامعتبر است، دوباره وارد شوید")

    admin_id = info.get("admin_id", "owner")
    is_owner = admin_id == "owner"
    admin_record = None if is_owner else ADMINS.get(admin_id)
    if not is_owner and not admin_record:
        raise HTTPException(status_code=401, detail="حساب کاربری یافت نشد")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات نامعتبر است")

    username = str(body.get("username", "")).strip()
    if not username:
        raise HTTPException(status_code=400, detail="نام کاربری نمی‌تواند خالی باشد")
    if len(username) < 3 or len(username) > 40:
        raise HTTPException(status_code=400, detail="نام کاربری باید بین ۳ تا ۴۰ کاراکتر باشد")
    if any(ch.isspace() for ch in username):
        raise HTTPException(status_code=400, detail="نام کاربری نباید فاصله داشته باشد")
    if username.lower() == "owner":
        raise HTTPException(status_code=400, detail="این نام کاربری رزرو شده است")

    current = AUTH.get("username", DEFAULT_ADMIN_USERNAME) if is_owner else admin_record.get("username", admin_id)
    for aid, admin in ADMINS.items():
        if aid != admin_id and str(admin.get("username", "")).lower() == username.lower():
            raise HTTPException(status_code=409, detail="این نام کاربری قبلاً استفاده شده است")
    if is_owner and username.lower() in {str(a.get("username", "")).lower() for a in ADMINS.values()}:
        raise HTTPException(status_code=409, detail="این نام کاربری قبلاً استفاده شده است")

    if is_owner:
        AUTH["username"] = username
    else:
        admin_record["username"] = username

    async with SESSIONS_LOCK:
        sess = SESSIONS.get(token)
        if sess:
            sess["exp"] = time.time() + SESSION_TTL

    await save_state()
    log_activity("auth", f"نام کاربری «{current}» به «{username}» تغییر کرد", "ok")
    return {"ok": True, "username": username}


# ============================================================
# CREATE LINK
# ============================================================


@app.get("/api/network/railway")
async def railway_network_info(_=Depends(require_auth)):
    """Return Railway networking hints without exposing secrets."""
    return {
        "is_railway": bool(os.environ.get("RAILWAY_PROJECT_ID") or os.environ.get("RAILWAY_ENVIRONMENT_ID")),
        "public_domain": os.environ.get("RAILWAY_PUBLIC_DOMAIN", ""),
        "tcp_proxy_domain": os.environ.get("RAILWAY_TCP_PROXY_DOMAIN", ""),
        "tcp_proxy_port": safe_int(os.environ.get("RAILWAY_TCP_PROXY_PORT", "0"), minimum=0, maximum=65535),
        "tcp_application_port": safe_int(os.environ.get("RAILWAY_TCP_APPLICATION_PORT", "0"), minimum=0, maximum=65535),
        "app_port": safe_int(os.environ.get("PORT", "0"), minimum=0, maximum=65535),
    }


@app.post("/api/network/tcp-ping")
async def tcp_ping(request: Request, _=Depends(require_auth)):
    """Server-side TCP connectivity test for an address/port entered in the builder."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات تست اتصال معتبر نیست.")
    host = str(body.get("host") or body.get("address") or "").strip()
    port = safe_int(body.get("port", 0), minimum=1, maximum=65535)
    timeout = min(max(float(body.get("timeout", 4.0) or 4.0), 0.5), 8.0)
    if not host:
        raise HTTPException(status_code=400, detail="آدرس سرور را وارد کنید.")
    if not port:
        raise HTTPException(status_code=400, detail="پورت باید بین 1 تا 65535 باشد.")
    started = time.perf_counter()
    try:
        infos = await asyncio.get_running_loop().run_in_executor(None, lambda: __import__('socket').getaddrinfo(host, port, type=__import__('socket').SOCK_STREAM))
        resolved = []
        for info in infos:
            addr = info[4][0]
            if addr not in resolved:
                resolved.append(addr)
        reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return {"ok": True, "host": host, "port": port, "latency_ms": round((time.perf_counter()-started)*1000, 1), "resolved": resolved[:6], "message": "اتصال TCP برقرار شد."}
    except asyncio.TimeoutError:
        return {"ok": False, "host": host, "port": port, "latency_ms": round((time.perf_counter()-started)*1000, 1), "message": "Timeout: سرور در زمان تعیین‌شده پاسخ نداد."}
    except Exception as exc:
        return {"ok": False, "host": host, "port": port, "latency_ms": round((time.perf_counter()-started)*1000, 1), "message": f"اتصال ناموفق: {type(exc).__name__}: {str(exc)[:180]}"}


# ═══════════════ Telegram Mini App (روی آدرس خود پنل: /app) ═══════════════
TMA_HTML = r"""<!doctype html><html lang="fa" dir="rtl" translate="no" data-theme="dark"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover,user-scalable=no"><meta name="google" content="notranslate"><title>VodiWalker</title>
<link rel="stylesheet" href="/assets/ui.css"><script src="https://telegram.org/js/telegram-web-app.js"></script><script src="/assets/qr.js"></script>
<style>
:root{--bg:#080b14;--panel:#0f1420;--panel2:#141a2b;--line:#212a40;--line2:#2c3752;--accent:#9b5cff;--accent2:#37d6ff;--accent-d:#7b3ff0;--good:#22c58b;--warn:#f5a524;--bad:#f24955;--text:#eef1fb;--sub:#8b95b3;--sub2:#5c6786}
[data-theme=light]{--bg:#f4f5fa;--panel:#fff;--panel2:#f7f8fb;--line:#e5e7eb;--line2:#d1d5db;--accent:#7c3aed;--accent2:#0ea5c4;--accent-d:#6425d6;--good:#17a673;--warn:#c9820a;--bad:#e0324a;--text:#191c2b;--sub:#666f8a;--sub2:#98a1b8}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}html,body{margin:0}body{background:radial-gradient(60% 26% at 85% -4%,rgba(155,92,255,.22),transparent 70%),radial-gradient(50% 24% at 0 8%,rgba(55,214,255,.10),transparent 70%),var(--bg);color:var(--text);font-family:Vazirmatn,Tahoma,sans-serif;min-height:100dvh;line-height:1.7;padding-bottom:calc(84px + env(safe-area-inset-bottom,0))}
.top{position:sticky;top:0;z-index:10;background:var(--panel);padding:calc(8px + env(safe-area-inset-top,0)) 12px 10px;border-bottom:1px solid var(--line)}.top:after{content:"";position:absolute;inset:auto 0 -1px;height:1px;background:linear-gradient(90deg,transparent,rgba(155,92,255,.6),rgba(55,214,255,.6),transparent)}
.hd{display:flex;align-items:center;gap:10px}.lg{width:36px;height:36px;border-radius:10px;display:grid;place-items:center;font-size:19px;color:#fff;background:linear-gradient(135deg,var(--accent),var(--accent2));box-shadow:0 0 0 2px rgba(155,92,255,.3)}.hd b{font-size:15px;flex:1}.hd small{display:block;font-size:10px;color:var(--sub);font-weight:500}
.ib{width:36px;height:36px;border-radius:10px;border:1px solid var(--line);background:var(--panel2);color:var(--text);display:grid;place-items:center;font-size:17px;padding:0}
.sr{position:relative;margin-top:10px}.sr i{position:absolute;top:50%;transform:translateY(-50%);inset-inline-start:12px;color:var(--sub2);font-size:17px}.sr input{padding-inline-start:38px;margin:0}
.w{padding:12px}.card{position:relative;overflow:hidden;background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:14px;margin-bottom:10px}.card:before{content:"";position:absolute;top:0;left:16px;right:16px;height:2px;border-radius:0 0 3px 3px;background:linear-gradient(90deg,var(--accent),var(--accent2));opacity:.55}
h2{font-size:13px;margin:16px 2px 8px;display:flex;align-items:center;gap:7px;color:var(--sub);font-weight:800}h2 i{color:var(--accent)}
.hero b{display:block;font-size:26px;font-weight:800;direction:ltr;text-align:right;background:linear-gradient(90deg,var(--accent),var(--accent2));-webkit-background-clip:text;background-clip:text;color:transparent}.hero small{color:var(--sub);font-size:11px}
.sg{display:grid;grid-template-columns:1fr 1fr;gap:10px}.sc{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:13px 14px}.sc .t{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}.sc .ic{width:32px;height:32px;border-radius:9px;display:grid;place-items:center;font-size:16px}.sc b{font-size:21px;font-weight:800;direction:ltr;display:block}.sc small{font-size:11px;color:var(--sub)}
button,.btn{font:600 12.5px Vazirmatn,Tahoma,sans-serif;display:inline-flex;align-items:center;justify-content:center;gap:7px;padding:10px 14px;border-radius:9px;border:1px solid var(--line);background:var(--panel2);color:var(--text);cursor:pointer;transition:background .14s,border-color .14s}button:active{transform:scale(.97)}.pr{background:var(--accent);border-color:var(--accent);color:#fff}.dg{color:var(--bad);border-color:rgba(242,73,85,.3)}
input,select{width:100%;background:var(--panel2);border:1px solid var(--line);color:var(--text);border-radius:9px;padding:11px;font:16px Vazirmatn,sans-serif;margin-bottom:8px;outline:0}input:focus{border-color:var(--accent)}.rw{display:flex;gap:8px;flex-wrap:wrap}.rw>*{flex:1}.q6{display:grid;grid-template-columns:1fr 1fr;gap:8px}
.it{position:relative;display:flex;align-items:center;gap:11px;padding:12px;background:var(--panel);border:1px solid var(--line);border-radius:14px;margin-bottom:8px;cursor:pointer}.it:hover{border-color:var(--line2)}.av{position:relative;width:40px;height:40px;flex:none;border-radius:11px;display:grid;place-items:center;font-size:19px;color:var(--accent);background:rgba(155,92,255,.12);border:1px solid rgba(155,92,255,.3)}.av:after{content:"";position:absolute;bottom:-3px;inset-inline-end:-3px;width:11px;height:11px;border-radius:50%;border:2px solid var(--panel);background:var(--sub2)}.av.on:after{background:var(--good)}.av.off:after{background:var(--bad)}
.n{flex:1;min-width:0}.n b{display:block;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;direction:ltr;text-align:right;unicode-bidi:plaintext}.n small{color:var(--sub);font-size:10.5px}.bar{height:5px;border-radius:9px;background:var(--line);overflow:hidden;margin-top:6px}.bar i{display:block;height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2))}
.bg{display:inline-flex;align-items:center;gap:5px;padding:2px 9px;border-radius:99px;font-size:10.5px;font-weight:700}.gr{background:rgba(34,197,139,.14);color:var(--good)}.rd{background:rgba(242,73,85,.14);color:var(--bad)}.or{background:rgba(245,165,36,.14);color:var(--warn)}.gy{background:rgba(135,146,168,.14);color:var(--sub)}
.fl{display:flex;gap:6px;overflow-x:auto;margin-bottom:10px;scrollbar-width:none}.fl::-webkit-scrollbar{display:none}.ch{flex:none;border-radius:99px;padding:6px 13px}.ch.on{background:linear-gradient(120deg,var(--accent-d),var(--accent));border-color:transparent;color:#fff}
nav{position:fixed;inset:auto 0 0;display:flex;background:var(--panel);border-top:1px solid var(--line);padding:6px 8px calc(6px + env(safe-area-inset-bottom,0));z-index:9}nav a{flex:1;text-align:center;font-size:10.5px;color:var(--sub);padding:6px 0;border-radius:10px;font-weight:700}nav a i{display:block;font-size:21px}nav a.on{color:var(--accent);background:rgba(155,92,255,.12)}
.sh{position:fixed;inset:0;background:rgba(0,0,0,.6);display:none;align-items:flex-end;z-index:20}.sh.on{display:flex}.sh>div{width:100%;max-height:90dvh;overflow:auto;background:var(--panel);border-radius:20px 20px 0 0;padding:10px 14px calc(18px + env(safe-area-inset-bottom,0));border-top:1px solid var(--line2)}.sh>div:before{content:"";display:block;width:40px;height:4px;border-radius:9px;background:var(--line2);margin:0 auto 12px}
.q{width:190px;height:190px;background:#fff;padding:8px;border-radius:12px;margin:8px auto;display:block}.u{font:11px ui-monospace,monospace;direction:ltr;word-break:break-all;background:var(--panel2);border:1px dashed var(--line2);padding:9px;border-radius:10px;color:var(--accent2);margin:6px 0}
#tt{position:fixed;top:14px;left:50%;transform:translateX(-50%);background:var(--panel);border:1px solid var(--line2);padding:9px 16px;border-radius:99px;font-size:12px;z-index:50;display:none;box-shadow:0 14px 34px rgba(0,0,0,.4)}.em{text-align:center;color:var(--sub);padding:30px 10px;font-size:12.5px}

/* ══════ Mini App · Pro UI layer (design system v2) ══════ */
:root{--r-lg:18px;--r-md:14px;--r-sm:10px;--shadow:0 12px 32px -14px rgba(0,0,0,.55);--ease:cubic-bezier(.22,.8,.24,1);--accent-rgb:155,92,255}
html{-webkit-text-size-adjust:100%;scroll-behavior:smooth}
body{font-feature-settings:"tnum" 1;padding-bottom:calc(86px + env(safe-area-inset-bottom,0));-webkit-font-smoothing:antialiased}
button,.it,.ch,nav a,.ib,.pr{-webkit-tap-highlight-color:transparent;touch-action:manipulation;transition:transform .16s var(--ease),background-color .18s,border-color .18s,box-shadow .2s,filter .16s}
button:active,.it:active,.ch:active,.ib:active,.pr:active{transform:scale(.965)}
button:focus,button:focus-visible,.it:focus,.ch:focus,a:focus,input:focus,select:focus{outline:none}
button:focus-visible,.it:focus-visible{box-shadow:0 0 0 3px rgba(var(--accent-rgb),.35)}
button:hover:not(.pr):not(.ch),.it:hover{border-color:var(--accent)}
input:focus,select:focus{border-color:var(--accent)!important;box-shadow:0 0 0 3px rgba(var(--accent-rgb),.2)!important}
.pr,button.pr{background:linear-gradient(135deg,var(--accent),var(--accent-d))!important;border-color:transparent!important;color:#fff!important;box-shadow:0 10px 22px -12px var(--accent);font-weight:700}
.pr:active{filter:brightness(.92)}

/* header */
.top{backdrop-filter:blur(18px) saturate(1.4);-webkit-backdrop-filter:blur(18px) saturate(1.4);background:color-mix(in srgb,var(--panel) 82%,transparent);border-bottom:1px solid var(--line);padding-bottom:12px}
.hd b{display:flex;flex-direction:column;flex:1;line-height:1.25;font-size:15px;letter-spacing:.2px}
.hd b small{font-size:10.5px;color:var(--sub);font-weight:600;margin-top:1px}
.lg{width:40px;height:40px;border-radius:13px;box-shadow:0 8px 20px -8px var(--accent),inset 0 1px 0 rgba(255,255,255,.35)}
.ib{width:38px;height:38px;border-radius:12px}
.sr input{border-radius:13px;background:var(--panel2);height:42px;font-size:14px}

/* hero + stat tiles */
.card{border-radius:var(--r-lg);box-shadow:var(--shadow);border-color:var(--line)}
.hero{padding:20px 18px;background:radial-gradient(120% 140% at 100% 0,rgba(var(--accent-rgb),.28),transparent 60%),radial-gradient(90% 120% at 0 100%,rgba(55,214,255,.12),transparent 60%),var(--panel)}
.hero small{letter-spacing:.3px;opacity:.85}
.hero b{font-size:34px;letter-spacing:-.5px;margin:6px 0 8px}
.sg{gap:10px}
.sc{border-radius:var(--r-md);padding:14px;position:relative;overflow:hidden;box-shadow:var(--shadow)}
.sc:after{content:"";position:absolute;inset:auto -20px -24px auto;width:70px;height:70px;border-radius:50%;background:radial-gradient(circle,rgba(var(--accent-rgb),.14),transparent 70%)}
.sc .ic{width:34px;height:34px;border-radius:11px;font-size:18px}
.sc b{font-size:21px;display:block;margin-top:8px;font-weight:800;letter-spacing:-.2px}
.sc small{color:var(--sub);font-size:11px}
h2{font-size:12.5px;text-transform:none;letter-spacing:.2px;margin:20px 4px 10px}

/* list rows */
.it{border-radius:var(--r-md);padding:13px;box-shadow:var(--shadow);overflow:hidden}
.it:before{content:"";position:absolute;inset-block:12px;inset-inline-start:0;width:3px;border-radius:0 3px 3px 0;background:var(--line2)}
.it:has(.av.on):before{background:var(--good)}
.it:has(.av.off):before{background:var(--bad)}
.av{width:42px;height:42px;border-radius:13px}
.n b{font-size:13.5px}
.bg{font-weight:700;letter-spacing:.1px}

/* filter chips */
.fl{padding:2px 1px 4px;margin-inline:-2px}
.ch{border-radius:99px;padding:8px 15px;font-weight:700;font-size:12px}
.ch.on{background:linear-gradient(120deg,var(--accent),var(--accent-d))!important;border-color:transparent!important;color:#fff!important;box-shadow:0 8px 18px -10px var(--accent)}

/* floating pill nav */
nav{inset:auto 12px calc(10px + env(safe-area-inset-bottom,0));border:1px solid var(--line2);border-radius:22px;padding:6px;gap:2px;background:color-mix(in srgb,var(--panel) 86%,transparent);backdrop-filter:blur(20px) saturate(1.5);-webkit-backdrop-filter:blur(20px) saturate(1.5);box-shadow:0 18px 40px -14px rgba(0,0,0,.65)}
nav a{border-radius:16px;padding:8px 2px 7px;display:flex;flex-direction:column;align-items:center;gap:3px;color:var(--sub);font-weight:700}
nav a i{font-size:20px}
nav a.on{color:#fff;background:linear-gradient(135deg,var(--accent),var(--accent-d));box-shadow:0 10px 20px -10px var(--accent)}

/* bottom sheet + toast */
.sh{backdrop-filter:blur(6px);-webkit-backdrop-filter:blur(6px);background:rgba(0,0,0,.55)}
.sh.on>div{animation:sheetUp .28s var(--ease)}
.sh>div{border-radius:26px 26px 0 0;box-shadow:0 -20px 50px -20px rgba(0,0,0,.6)}
@keyframes sheetUp{from{transform:translateY(40px);opacity:0}to{transform:none;opacity:1}}
#tt{border-radius:99px;border-color:rgba(var(--accent-rgb),.5);box-shadow:0 14px 34px -10px rgba(0,0,0,.6),0 0 0 3px rgba(var(--accent-rgb),.12);font-weight:700;top:calc(12px + env(safe-area-inset-top,0))}
.u{border-radius:12px}
.q{box-shadow:0 12px 30px -12px rgba(0,0,0,.5)}

/* loading skeleton + entrance */
.sk{height:78px;border-radius:var(--r-md);margin-bottom:10px;background:linear-gradient(100deg,var(--panel) 30%,var(--panel2) 50%,var(--panel) 70%);background-size:220% 100%;animation:sk 1.3s linear infinite;border:1px solid var(--line)}
.sk.h{height:128px;border-radius:var(--r-lg)}
@keyframes sk{from{background-position:120% 0}to{background-position:-100% 0}}
#v>*{animation:rise .32s var(--ease) both}
#v>*:nth-child(2){animation-delay:.04s}#v>*:nth-child(3){animation-delay:.08s}#v>*:nth-child(4){animation-delay:.12s}
@keyframes rise{from{transform:translateY(8px);opacity:0}to{transform:none;opacity:1}}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style></head><body>
<div class="top"><div class="hd"><div class="lg"><i class="ti ti-shield-check"></i></div><b>VodiWalker<small>Control Center · مینی‌اپ</small></b><button class="ib" onclick="th()"><i class="ti ti-moon"></i></button><button class="ib" onclick="load(1)"><i class="ti ti-refresh"></i></button></div>
<div class="sr"><i class="ti ti-search"></i><input id="gs" placeholder="جستجو در کانفیگ‌ها، گروه‌ها و تنظیمات…" oninput="S.q=this.value;S.q?draw():draw()" autocomplete="off"></div></div>
<div class="w" id="v"></div><nav id="nv"></nav><div class="sh" id="sh"><div id="sb"></div></div><div id="tt"></div>
<script>
var W=window.Telegram&&Telegram.WebApp,S={tab:'h',L:[],G:[],q:'',f:'all'},$=function(i){return document.getElementById(i)};
if(W){W.ready();W.expand();try{if(W.colorScheme=='light')document.documentElement.dataset.theme='light'}catch(x){}}
function th(){var r=document.documentElement;r.dataset.theme=r.dataset.theme=='dark'?'light':'dark'}
function e(s){return String(s==null?'':s).replace(/[&<>"']/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
function tt(m){var x=$('tt');x.textContent=m;x.style.display='block';clearTimeout(tt.h);tt.h=setTimeout(function(){x.style.display='none'},1800);if(W&&W.HapticFeedback)W.HapticFeedback.impactOccurred('light')}
function api(a,b){return fetch('/api/tma/'+a,{method:'POST',headers:{'Content-Type':'application/json','X-TG-Init':W?W.initData:''},body:JSON.stringify(b||{})}).then(function(r){return r.json().then(function(j){if(!r.ok)throw Error(j.detail||'خطا');return j})})}
function cp(v){try{navigator.clipboard.writeText(v).then(function(){tt('کپی شد ✓')})}catch(x){var a=document.createElement('textarea');a.value=v;document.body.appendChild(a);a.select();document.execCommand('copy');a.remove();tt('کپی شد ✓')}}
function fb(n){n=+n||0;var u=['B','KB','MB','GB','TB'],i=0;while(n>=1024&&i<4){n/=1024;i++}return(n>=10?n.toFixed(1):n.toFixed(2))+' '+u[i]}
function qr(v){try{var q=qrcode(0,'M');q.addData(v);q.make();return'data:image/svg+xml;charset=utf-8,'+encodeURIComponent(q.createSvgTag(5,4))}catch(x){return''}}
function sheet(h){$('sb').innerHTML=h;$('sh').classList.add('on')}function closeS(){$('sh').classList.remove('on')}$('sh').onclick=function(x){if(x.target===this)closeS()};
function nav(){var T=[['h','ti-layout-dashboard','داشبورد'],['c','ti-stack-2','اینباندها'],['g','ti-folders','گروه‌ها'],['r','ti-chart-bar','گزارش'],['m','ti-settings','تنظیمات']];$('nv').innerHTML=T.map(function(t){return'<a href="#" class="'+(S.tab==t[0]?'on':'')+'" onclick="go(\''+t[0]+'\');return false"><i class="ti '+t[1]+'"></i>'+t[2]+'</a>'}).join('')}
function go(t){S.tab=t;S.q='';$('gs').value='';nav();draw()}
function load(m){return api('state').then(function(d){S.D=d;S.L=d.links.filter(function(x){return!x.parent});S.C=d.links.filter(function(x){return x.parent});S.G=d.groups;draw();if(m)tt('بروزرسانی شد ✓')}).catch(function(x){$('v').innerHTML='<div class="card"><h2 style="margin:0 0 8px"><i class="ti ti-lock"></i> دسترسی ندارید</h2><small style="color:var(--sub)">'+e(x.message)+'<br>این مینی‌اپ فقط برای ادمین‌های ربات است.</small></div>'})}
function st(l){return!l.active?['off','غیرفعال','gy']:(l.limit&&l.used>=l.limit)||l.days===0?['off','تمام‌شده','rd']:l.online?['on','آنلاین','gr']:['on','آماده','gy']}
function li(l){var p=l.limit?Math.min(100,Math.round(l.used/l.limit*100)):0,s=st(l);return'<div class="it" onclick="op(\''+l.id+'\')"><div class="av '+s[0]+'"><i class="ti ti-router"></i></div><div class="n"><b>'+e(l.label)+'</b><small><span class="bg '+s[2]+'">'+s[1]+'</span> '+fb(l.used)+' / '+(l.limit?fb(l.limit):'∞')+' · '+(l.days===null?'∞':l.days+' روز')+(l.online?' · 🟢'+l.online:'')+'</small><div class="bar"><i style="width:'+(l.limit?p:100)+'%;opacity:'+(l.limit?1:.3)+'"></i></div></div><i class="ti ti-chevron-left" style="color:var(--sub2)"></i></div>'}
function sc(i,c,v,l){return'<div class="sc"><div class="t"><div class="ic" style="background:'+c+'22;color:'+c+'"><i class="ti '+i+'"></i></div></div><b>'+v+'</b><small>'+l+'</small></div>'}
function fl(l){var f=S.f;return f=='all'||(f=='on'&&l.active&&!(l.limit&&l.used>=l.limit))||(f=='off'&&!l.active)||(f=='net'&&l.online)||(f=='near'&&l.active&&((l.days!==null&&l.days<=3)||(l.limit&&l.used>=l.limit*.85)))}
function draw(){var D=S.D;if(!D)return;var h='',q=S.q.trim().toLowerCase();
if(q){var a=S.L.filter(function(l){return(l.label+' '+l.id).toLowerCase().indexOf(q)>-1}),g=S.G.filter(function(x){return x.name.toLowerCase().indexOf(q)>-1});h='<h2><i class="ti ti-search"></i> نتایج «'+e(S.q)+'»</h2>'+(a.length?a.map(li).join(''):'')+(g.length?'<h2><i class="ti ti-folders"></i> گروه‌ها</h2>'+g.map(gc).join(''):'')+((a.length||g.length)?'':'<div class="em">چیزی پیدا نشد 🔍</div>');$('v').innerHTML=h;return}
if(S.tab=='h'){h='<div class="card hero"><small>مجموع کل ترافیک</small><b>'+fb(D.traffic)+'</b><small><span class="bg gr">● '+D.online+' آنلاین</span></small></div><div class="sg">'+sc('ti-network','#9b5cff',D.total,'کل اینباندها')+sc('ti-circle-check','#22c58b',D.active,'فعال')+sc('ti-clock-exclamation','#f5a524',D.near,'نزدیک انقضا')+sc('ti-folders','#37d6ff',S.G.length,'گروه ساب')+'</div><h2><i class="ti ti-bolt"></i> ساخت سریع (واقعی)</h2><div class="q6">'+[[10,30],[30,30],[50,30],[100,30],[200,60],[0,30]].map(function(p){return'<button class="pr" onclick="qc('+p[0]+','+p[1]+')">'+(p[0]?p[0]+' گیگ':'♾ نامحدود')+' · '+p[1]+' روز</button>'}).join('')+'</div><h2><i class="ti ti-adjustments"></i> ساخت دلخواه</h2><div class="card"><input id="cn" placeholder="اسم (Vodiwalker → Vodiwalker|Tofan🚀)"><div class="rw"><input id="cg" type="number" placeholder="حجم GB (0=∞)"><input id="cd" type="number" placeholder="روز (0=∞)"></div><button class="pr" style="width:100%" onclick="qc(+$(\'cg\').value||0,+$(\'cd\').value||0,$(\'cn\').value)"><i class="ti ti-plus"></i> ساخت اینباند</button></div><button style="width:100%" onclick="adv()"><i class="ti ti-adjustments"></i> ساخت پیشرفته (پروتکل، پورت، محدودیت IP و اتصال)</button>'}
else if(S.tab=='c'){var F=[['all','همه'],['on','فعال'],['net','آنلاین'],['near','نزدیک انقضا'],['off','غیرفعال']];h='<div class="fl">'+F.map(function(x){return'<button class="ch '+(S.f==x[0]?'on':'')+'" onclick="S.f=\''+x[0]+'\';draw()">'+x[1]+'</button>'}).join('')+'</div>'+(S.L.filter(fl).map(li).join('')||'<div class="em">موردی نیست</div>')}
else if(S.tab=='g'){h='<button class="pr" style="width:100%;margin-bottom:10px" onclick="gsel()"><i class="ti ti-folder-plus"></i> ساخت گروه ساب جدید</button>'+(S.G.map(gc).join('')||'<div class="em">هنوز گروهی ندارید</div>')}
else if(S.tab=='r'){$('v').innerHTML=rp();lact();return}
else{h='<div class="card"><b><i class="ti ti-server"></i> وضعیت سرور</b><div class="sg" style="margin-top:10px">'+sc('ti-cpu','#9b5cff',D.srv.cpu+'%','CPU')+sc('ti-device-ram','#37d6ff',D.srv.ram+'%','RAM')+sc('ti-database','#f5a524',D.srv.disk+'%','دیسک')+sc('ti-world','#22c58b','<span style="font-size:12px">'+e(D.host)+'</span>','آدرس پنل')+'</div></div><div class="card"><b><i class="ti ti-brand-telegram"></i> پشتیبانی اشتراک</b><input id="su" dir="ltr" style="margin-top:10px" placeholder="@YourSupport" value="'+e(D.support)+'"><button class="pr" style="width:100%" onclick="ss()">ذخیره آیدی پشتیبان</button></div>'}
$('v').innerHTML=h}
function gc(g){return'<div class="card"><b>'+e(g.name)+'</b> <span class="bg gy">'+g.count+' کانفیگ</span><div class="u">'+e(g.url)+'</div><div class="rw"><button class="pr" onclick="cp(\''+e(g.url)+'\')"><i class="ti ti-copy"></i> کپی ساب</button><button onclick="sq(\''+e(g.url)+'\')"><i class="ti ti-qrcode"></i> QR</button></div><div class="rw" style="margin-top:8px"><button onclick="gsel(\''+g.id+'\')"><i class="ti ti-edit"></i> مدیریت کانفیگ‌ها</button><button class="dg" onclick="gd(\''+g.id+'\')"><i class="ti ti-trash"></i> حذف</button></div></div>'}
function qc(g,d,n){api('create',{gb:g,days:d,label:n||''}).then(function(r){tt('ساخته شد ✓');load().then(function(){op(r.item.id)})}).catch(function(x){tt(x.message)})}
function op(id){var l=S.L.filter(function(x){return x.id==id})[0];if(!l)return;var s=st(l);sheet('<div class="hd"><div class="av '+s[0]+'"><i class="ti ti-router"></i></div><b style="font-size:14px;direction:ltr;text-align:right">'+e(l.label)+'</b><span class="bg '+s[2]+'">'+s[1]+'</span></div><div class="u">'+e(l.link||'—')+'</div><img class="q" src="'+qr(l.link||'')+'"><div class="rw"><button class="pr" onclick="cp(\''+e(l.link)+'\')"><i class="ti ti-copy"></i> کپی لینک</button><button onclick="cp(\''+e(l.sub)+'\')"><i class="ti ti-link"></i> کپی ساب</button></div><h2><i class="ti ti-pencil"></i> ویرایش سریع</h2><div class="q6"><button onclick="ed(\''+id+'\',\'gb\',10)">+10 GB</button><button onclick="ed(\''+id+'\',\'gb\',50)">+50 GB</button><button onclick="ed(\''+id+'\',\'d\',30)">+30 روز</button><button onclick="ed(\''+id+'\',\'d\',90)">+90 روز</button><button onclick="ed(\''+id+'\',\'reset\')"><i class="ti ti-refresh"></i> ریست مصرف</button><button onclick="ed(\''+id+'\',\'rname\')"><i class="ti ti-dice-5"></i> اسم خفن</button><button onclick="ed(\''+id+'\',\'toggle\')">'+(l.active?'⏸ غیرفعال':'▶ فعال')+'</button><button class="dg" onclick="if(confirm(\'حذف شود؟\'))ed(\''+id+'\',\'del\')"><i class="ti ti-trash"></i> حذف</button></div>'+cl(l))}
function ed(id,k,v){api('edit',{id:id,op:k,v:v}).then(function(){tt('انجام شد ✓');closeS();load().then(function(){if(k!='del')op(id)})}).catch(function(x){tt(x.message)})}
function sq(u){sheet('<img class="q" src="'+qr(u)+'"><div class="u">'+e(u)+'</div><button class="pr" style="width:100%" onclick="cp(\''+e(u)+'\')">کپی</button>')}
function mg(){api('group',{name:$('gn').value}).then(function(){tt('گروه ساخته شد ✓');load()}).catch(function(x){tt(x.message)})}
function ss(){api('support',{v:$('su').value}).then(function(){tt('ذخیره شد ✓');load()}).catch(function(x){tt(x.message)})}

function ER(x){tt(x.message)}
function rp(){var t=S.L.slice().sort(function(a,b){return b.used-a.used}).slice(0,8),mx=(t[0]&&t[0].used)||1;return'<h2><i class="ti ti-flame"></i> پرمصرف‌ترین‌ها</h2><div class="card">'+(t.map(function(l){return'<div style="margin-bottom:10px"><div style="display:flex;justify-content:space-between"><small style="direction:ltr">'+e(l.label)+'</small><small>'+fb(l.used)+'</small></div><div class="bar"><i style="width:'+Math.round(l.used/mx*100)+'%"></i></div></div>'}).join('')||'<div class="em">داده‌ای نیست</div>')+'</div><h2><i class="ti ti-activity"></i> فعالیت‌های اخیر</h2><div class="card" id="ac"><div class="em">در حال بارگذاری…</div></div>'}
function lact(){api('activity').then(function(r){$('ac').innerHTML=r.logs.map(function(x){return'<div style="padding:8px 0;border-bottom:1px solid var(--line);font-size:11.5px"><span class="bg '+(x.level=='error'?'rd':x.level=='warning'?'or':'gy')+'">'+e(x.kind)+'</span> '+e(x.message)+'<br><small style="color:var(--sub2)">'+e(String(x.time).slice(5,16).replace('T',' '))+'</small></div>'}).join('')||'<div class="em">فعالیتی ثبت نشده</div>'}).catch(function(){})}
function adv(){sheet('<h2 style="margin-top:0"><i class="ti ti-adjustments"></i> ساخت پیشرفته</h2><input id="an" placeholder="اسم"><select id="ap">'+S.D.protocols.map(function(p){return'<option>'+e(p)+'</option>'}).join('')+'</select><div class="rw"><input id="ag" type="number" placeholder="حجم GB (0=∞)"><input id="ad" type="number" placeholder="روز (0=∞)"></div><div class="rw"><input id="ao" type="number" placeholder="پورت (443)"><input id="ai" type="number" placeholder="محدودیت IP (0=∞)" value="0"></div><div class="rw"><input id="ak" type="number" placeholder="اتصال هم‌زمان (0=∞)"><input id="ae" placeholder="یادداشت"></div><button class="pr" style="width:100%" onclick="av()">ساخت</button>')}
function av(){api('create',{label:$('an').value,protocol:$('ap').value,gb:+$('ag').value||0,days:+$('ad').value||0,port:+$('ao').value||0,ip:+$('ai').value||0,conn:+$('ak').value||0,note:$('ae').value}).then(function(r){closeS();tt('ساخته شد ✓');load().then(function(){op(r.item.id)})}).catch(ER)}
function cl(l){var c=S.C.filter(function(x){return x.parent==l.id});return'<h2><i class="ti ti-users"></i> کلاینت‌ها ('+c.length+')</h2>'+c.map(function(x){return'<div class="it" style="cursor:default"><div class="n"><b>'+e(x.label)+'</b><small>'+fb(x.used)+' / '+(x.limit?fb(x.limit):'∞')+'</small></div><button onclick="cp(\''+e(x.link)+'\')"><i class="ti ti-copy"></i></button><button class="dg" onclick="if(confirm(\'حذف؟\'))ed(\''+x.id+'\',\'del\')"><i class="ti ti-trash"></i></button></div>'}).join('')+'<div class="card"><input id="cn_" placeholder="نام کلاینت جدید"><div class="rw"><input id="cg_" type="number" placeholder="GB"><input id="cd_" type="number" placeholder="روز"></div><button class="pr" style="width:100%" onclick="ac_(\''+l.id+'\')"><i class="ti ti-user-plus"></i> افزودن کلاینت</button></div>'}
function ac_(id){api('client_add',{id:id,label:$('cn_').value,gb:+$('cg_').value||0,days:+$('cd_').value||0}).then(function(){tt('کلاینت ساخته شد ✓');load().then(function(){op(id)})}).catch(ER)}
function gsel(gid){var g=gid?S.G.filter(function(x){return x.id==gid})[0]:null,sel=g?g.ids:[];sheet('<h2 style="margin-top:0"><i class="ti ti-folders"></i> '+(g?'مدیریت «'+e(g.name)+'»':'گروه ساب جدید')+'</h2>'+(g?'':'<input id="gn" placeholder="نام گروه"><input id="gp" placeholder="رمز (اختیاری)">')+'<div style="max-height:46dvh;overflow:auto">'+S.L.map(function(l){return'<label class="it"><input type="checkbox" class="gk" value="'+l.id+'" '+(sel.indexOf(l.id)>-1?'checked':'')+' style="width:20px;margin:0"><div class="n"><b>'+e(l.label)+'</b></div></label>'}).join('')+'</div><button class="pr" style="width:100%;margin-top:8px" onclick="gs_(\''+(gid||'')+'\')">ذخیره</button>')}
function gs_(gid){var ids=[].slice.call(document.querySelectorAll('.gk:checked')).map(function(x){return x.value});api(gid?'group_set':'group_create',{gid:gid,name:$('gn')?$('gn').value:'',password:$('gp')?$('gp').value:'',ids:ids}).then(function(){closeS();tt('ذخیره شد ✓');load()}).catch(ER)}
function gd(id){if(confirm('گروه حذف شود؟'))api('group_del',{gid:id}).then(function(){tt('حذف شد');load()}).catch(ER)}
$('v').innerHTML='<div class="sk h"></div><div class="sk"></div><div class="sk"></div>';try{if(W){W.setHeaderColor(getComputedStyle(document.body).backgroundColor);W.setBackgroundColor(getComputedStyle(document.body).backgroundColor)}}catch(x){}
nav();load();setInterval(function(){if(!$('sh').classList.contains('on')&&document.activeElement.tagName!='INPUT')load()},30000);
</script></body></html>
"""


def _tma_admin(request: Request) -> int:
    """اعتبارسنجی initData تلگرام (HMAC با توکن ربات) + فقط ادمین‌های ربات."""
    import hmac
    from urllib.parse import parse_qsl
    import telegram_bot as _tb
    cfg = _tb.current_config()
    token = cfg.get("bot_token") or ""
    raw = request.headers.get("x-tg-init", "")
    try:
        pairs = dict(parse_qsl(raw, keep_blank_values=True))
        got = pairs.pop("hash", "")
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
        ok = bool(token) and hmac.compare_digest(hmac.new(secret, check.encode(), hashlib.sha256).hexdigest(), got)
        if not ok or time.time() - int(pairs.get("auth_date", "0")) > 86400:
            raise ValueError
        uid = int(json.loads(pairs["user"])["id"])
    except Exception:
        raise HTTPException(status_code=401, detail="احراز هویت تلگرام نامعتبر است")
    if uid not in {int(x) for x in str(cfg.get("admin_ids", "")).split(",") if x.strip().isdigit()}:
        raise HTTPException(status_code=403, detail="شما ادمین ربات نیستید")
    return uid


def _tma_item(request: Request, uid: str, l: dict, host: str) -> dict:
    exp = l.get("expires_at")
    try:
        days = max(0, int((datetime.fromisoformat(str(exp)) - datetime.now()).total_seconds() // 86400)) if exp else None
    except Exception:
        days = None
    return {
        "id": uid, "label": l.get("label", "?"), "active": bool(l.get("active", True)),
        "used": int(l.get("used_bytes") or 0), "limit": int(l.get("limit_bytes") or 0), "days": days,
        "online": len(unique_ips_for_uuid(uid)),
        "link": vless_link_for_link(l, uid, host), "sub": f"{get_scheme()}://{host}/sub/{uid}",
        "parent": l.get("parent_inbound_id"), "port": l.get("port"), "protocol": l.get("protocol"),
    }


@app.get("/app", response_class=HTMLResponse, include_in_schema=False)
@app.get("/miniapp", response_class=HTMLResponse, include_in_schema=False)
async def tma_page():
    return HTMLResponse(TMA_HTML, headers={"Cache-Control": "no-store"})


@app.post("/api/tma/{action}", include_in_schema=False)
async def tma_api(action: str, request: Request):
    _tma_admin(request)
    try:
        b = await request.json()
    except Exception:
        b = {}
    host = get_host(request)
    if action == "create":
        gb, days = float(b.get("gb") or 0), int(b.get("days") or 0)
        label = decorate_label(sanitize_display_name(str(b.get("label")))) if str(b.get("label") or "").strip() else auto_display_name()
        proto = str(b.get("protocol") or "")
        if proto not in LIVE_PROTOCOLS:
            proto = "vless-ws" if "vless-ws" in LIVE_PROTOCOLS else DEFAULT_PROTOCOL
        uid, link = await make_link(
            label=label, limit_bytes=int(parse_size_to_bytes(gb, "GB")) if gb > 0 else 0,
            expires_at=(datetime.now() + timedelta(days=days)).isoformat() if days > 0 else None,
            protocol=proto, port=max(1, min(65535, int(b.get("port") or DEFAULT_PORT))),
            ip_limit=max(0, int(b.get("ip") or 0)), connection_limit=max(0, int(b.get("conn") or 0)),
            note=str(b.get("note") or "")[:200],
        )
        return {"ok": True, "item": _tma_item(request, uid, link, host)}
    if action == "edit":
        uid, op, v = str(b.get("id")), b.get("op"), b.get("v")
        if uid not in LINKS:
            raise HTTPException(status_code=404, detail="کانفیگ پیدا نشد")
        if op == "del":
            await remove_link(uid)
        elif op == "toggle":
            await set_link_active(uid, not LINKS[uid].get("active", True))
        elif op == "gb":
            if not int(LINKS[uid].get("limit_bytes") or 0):
                raise HTTPException(status_code=400, detail="حجم این کانفیگ نامحدود است")
            await update_link_fields(uid, add_bytes=int(parse_size_to_bytes(float(v), "GB")))
        elif op == "d":
            await update_link_fields(uid, extend_days=int(v))
        elif op == "reset":
            await update_link_fields(uid, reset_usage=True)
        elif op == "rname":
            await update_link_fields(uid, label=auto_display_name())
        return {"ok": True}
    if action == "client_add":
        gb = float(b.get("gb") or 0)
        lab = str(b.get("label") or "").strip()
        res = await add_client_to_inbound(
            str(b.get("id")), label=decorate_label(sanitize_display_name(lab)) if lab else None,
            limit_bytes=int(parse_size_to_bytes(gb, "GB")) if gb > 0 else None, expires_days=int(b.get("days") or 0),
        )
        if not res:
            raise HTTPException(status_code=400, detail="ساخت کلاینت ممکن نیست")
        return {"ok": True}
    if action in ("group", "group_create"):
        name = sanitize_display_name(str(b.get("name") or "")) or "VodiWalker"
        sid, _s = await create_sub_group(name=name, password=str(b.get("password") or ""))
        ids = b.get("ids") or [u for u, l in LINKS.items() if l.get("active", True) and not l.get("parent_inbound_id")]
        for u in ids:
            await set_link_sub(str(u), sid)
        return {"ok": True}
    if action == "group_set":
        gid = str(b.get("gid"))
        if gid not in SUBS:
            raise HTTPException(status_code=404, detail="گروه پیدا نشد")
        want = {str(x) for x in (b.get("ids") or [])}
        for u in list(SUBS[gid].get("link_ids") or []):
            if u not in want:
                await set_link_sub(u, None)
        for u in want:
            await set_link_sub(u, gid)
        return {"ok": True}
    if action == "group_del":
        if await remove_sub_group(str(b.get("gid"))) is None:
            raise HTTPException(status_code=404, detail="گروه پیدا نشد")
        return {"ok": True}
    if action == "activity":
        return {"logs": list(activity_logs)[-80:][::-1]}
    if action == "support":
        v = _clean_tg_username(b.get("v"))
        if v and not _TG_USER_RE.match(v):
            raise HTTPException(status_code=400, detail="آیدی تلگرام معتبر نیست")
        CONFIG["support_username"] = v
        await save_state()
        return {"ok": True}
    if action != "state":
        raise HTTPException(status_code=404, detail="unknown")
    import psutil
    items = sorted((_tma_item(request, u, l, host) for u, l in LINKS.items()), key=lambda x: x["label"])
    base = get_public_base(request) or f"{get_scheme()}://{host}"
    return {
        "links": items, "total": len(items), "active": sum(1 for i in items if i["active"]),
        "near": sum(1 for i in items if i["active"] and ((i["days"] is not None and i["days"] <= 3) or (i["limit"] and i["used"] >= i["limit"] * .85))),
        "traffic": sum(i["used"] for i in items), "online": sum(i["online"] for i in items),
        "protocols": sorted(LIVE_PROTOCOLS),
        "groups": [{"id": sid_, "ids": list(s.get("link_ids") or []), "name": s.get("name", "?"), "count": len(s.get("link_ids") or []), "url": f"{base}/p/{s.get('uuid_key', '')}"} for sid_, s in SUBS.items()],
        "srv": {"cpu": psutil.cpu_percent(), "ram": psutil.virtual_memory().percent, "disk": psutil.disk_usage("/").percent},
        "host": host, "support": get_support_username(),
    }



@app.get("/api/name-suggestions")
async def api_name_suggestions(request: Request, _=Depends(require_auth)):
    base = request.query_params.get("base", "")
    count = safe_int(request.query_params.get("n"), default=12, minimum=4, maximum=24)
    return {
        "ok": True,
        "enabled": bool(CONFIG.get("name_style_enabled", True)),
        "suggestions": name_suggestions(base, count),
    }


@app.post("/api/links")
async def create_link_api(
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()

        if not isinstance(body, dict):
            raise ValueError(
                "body is not object"
            )

    except Exception as exc:

        logger.exception(
            "Create link JSON error: %s",
            exc,
        )

        raise HTTPException(
            status_code=400,
            detail="اطلاعات ارسال‌شده معتبر نیست.",
        )

    limit_value = safe_float(
        body.get(
            "limit_value",
            0,
        )
    )

    limit_unit = str(
        body.get(
            "limit_unit",
            "GB",
        )
        or "GB"
    ).upper()

    limit_bytes = (
        0
        if limit_value <= 0
        else parse_size_to_bytes(
            limit_value,
            limit_unit,
        )
    )

    expires_days = safe_int(
        body.get(
            "expires_days",
            0,
        ),
        minimum=0,
    )

    expires_at = (
        (
            datetime.now()
            + timedelta(
                days=expires_days
            )
        ).isoformat()
        if expires_days > 0
        else None
    )

    port = safe_int(
        body.get(
            "port",
            DEFAULT_PORT,
        ),
        default=DEFAULT_PORT,
        minimum=MIN_PORT,
        maximum=MAX_PORT,
    )

    ip_limit = safe_int(
        body.get(
            "ip_limit",
            0,
        ),
        minimum=0,
    )

    speed_value = safe_float(
        body.get(
            "speed_limit_value",
            0,
        )
    )

    speed_unit = str(
        body.get(
            "speed_limit_unit",
            "MBIT",
        )
        or "MBIT"
    ).upper()

    speed_bytes = (
        0
        if speed_value <= 0
        else parse_speed_to_bytes(
            speed_value,
            speed_unit,
        )
    )

    connection_limit = safe_int(
        body.get(
            "connection_limit",
            0,
        ),
        minimum=0,
    )

    protocol = str(
        body.get(
            "protocol",
            DEFAULT_PROTOCOL,
        )
        or DEFAULT_PROTOCOL
    ).strip().lower()

    if protocol != "manual" and protocol not in PROTOCOLS:
        protocol = DEFAULT_PROTOCOL

    manual_fields = body.get("manual") or {}
    if not isinstance(manual_fields, dict):
        manual_fields = {}

    fingerprint = str(
        body.get(
            "fingerprint",
            DEFAULT_FINGERPRINT,
        )
        or DEFAULT_FINGERPRINT
    ).strip().lower()

    if fingerprint not in FINGERPRINTS:
        fingerprint = DEFAULT_FINGERPRINT

    fragment = str(
        body.get(
            "fragment",
            "off",
        )
        or "off"
    ).strip().lower()

    allowed_fragments = {
        "off",
        "safe",
        "balanced",
        "aggressive",
    }

    if fragment not in allowed_fragments:
        fragment = "off"

    raw_clean = body.get("clean_ips") or body.get("clean_ip") or ""
    if isinstance(raw_clean, list):
        clean_ips = [str(x).strip() for x in raw_clean if str(x).strip()]
    else:
        clean_ips = [x.strip() for x in str(raw_clean).replace(",", "\n").splitlines() if x.strip()]
    alarm_enabled = bool(body.get("alarm_enabled", False))
    category_id = str(body.get("category_id") or "0")
    if category_id not in CATEGORIES:
        category_id = "0"
    config_count = safe_int(body.get("config_count", 1), minimum=1, maximum=40)
    client_limit = safe_int(body.get("client_limit", 0), minimum=0, maximum=1000)
    requested_expires_at = str(body.get("expires_at") or "").strip()
    if requested_expires_at:
        try:
            dt = datetime.fromisoformat(requested_expires_at.replace("Z", "+00:00"))
            expires_at = dt.replace(tzinfo=None).isoformat()
        except Exception:
            raise HTTPException(status_code=400, detail="زمان انقضا معتبر نیست")
    cat = CATEGORIES.get(category_id) or {}
    if cat.get("limit_bytes") and limit_bytes <= 0:
        limit_bytes = int(cat["limit_bytes"])
    if cat.get("expires_days") and expires_days <= 0:
        expires_days = int(cat["expires_days"])
        expires_at = (datetime.now() + timedelta(days=expires_days)).isoformat() if expires_days > 0 else None
    if cat.get("connection_limit") and connection_limit <= 0:
        connection_limit = int(cat["connection_limit"])
    if cat.get("speed_limit_bytes") and speed_bytes <= 0:
        speed_bytes = int(cat["speed_limit_bytes"])
    if cat.get("ip_limit") and ip_limit <= 0:
        ip_limit = int(cat["ip_limit"])
    if cat.get("clean_ips") and not clean_ips:
        clean_ips = list(cat["clean_ips"])
    if cat.get("single_user"):
        if ip_limit == 0: ip_limit = 1
        if connection_limit == 0: connection_limit = 1
    label_val = body.get("label", "")
    if cat.get("random_name") or not str(label_val).strip():
        label_val = auto_display_name()
    else:
        label_val = decorate_label(sanitize_display_name(str(label_val)))

    uid, link = await make_link(
        label=label_val,
        limit_bytes=limit_bytes,
        expires_at=expires_at,
        note=body.get(
            "note",
            "",
        ),
        sub_id=body.get(
            "sub_id"
        ),
        protocol=protocol,
        fingerprint=fingerprint,
        alpn=body.get(
            "alpn",
            DEFAULT_ALPN_BY_PROTOCOL.get(
                protocol,
                "http/1.1",
            ),
        ),
        port=port,
        ip_limit=ip_limit,
        speed_limit_bytes=speed_bytes,
        connection_limit=connection_limit,
        fragment=fragment,
        clean_ips=clean_ips,
        alarm_enabled=alarm_enabled,
        category_id=category_id,
        config_count=config_count,
        manual_fields=manual_fields,
        outbound_proxy_id=clean_outbound_proxy_id(body.get("outbound_proxy_id")),
    )

    async with LINKS_LOCK:
        LINKS[uid]["client_limit"] = client_limit
    await save_state()

    host = get_host(request)

    result = {
        **get_link_info(
            link,
            uid,
            host,
        ),
        "ok": True,
    }

    return result


# ============================================================
# WS + XHTTP COMBO SUBSCRIPTION  ·  OUTBOUND (EXIT) CONTROL
# ============================================================
#
# «کامبو» = یک گروه ساب (یک لینک اشتراک) که برای هر «خروجی» انتخاب‌شده دقیقاً
# دو اینباند دارد: یک VLESS-WS و یک VLESS-XHTTP. خروجی یعنی مسیر خروج ترافیک:
#   ""        -> مستقیم از خود Railway
#   "<id>"    -> از یک پراکسی SOCKS5 (outbound_proxy.py)
# با یک خروجی: اشتراک دقیقاً ۲ خط دارد (۱ WS + ۱ XHTTP).
# با N خروجی : اشتراک ۲×N خط دارد و اسم هر خط با کد کشورش تفکیک می‌شود.

COMBO_MEMBERS = (("vless-ws", "ws"), ("xhttp-packet-up", "xhttp"))
MAX_COMBO_EXITS = 20
MAX_BULK_LINKS = 500

# اموجی‌های «خفن» که به‌ترتیب برای هر خروجی/کانفیگ جدید استفاده می‌شن (چرخشی).
CONFIG_EMOJI_POOL = [
    "🚀", "⚡", "🔥", "🛡", "🌪", "💎", "🦾", "🌊", "🎯", "🧿",
    "🐉", "🦅", "🌟", "🛰", "🧊", "🌀", "🪐", "🦁", "🔱", "💠",
    "🧨", "🌈", "🦈", "🐺", "🔮", "🛸", "🧬", "🕹", "🎇", "🌋",
]


def sanitize_display_name(name: str, fallback: str = "VodiWalker") -> str:
    """برخلاف sanitize_config_name (که فقط ASCII نگه می‌داره و برای اسم داخلیِ
    گروه/اسلاگ استفاده می‌شه)، این تابع اسمی که کاربر برای نمایش در کلاینت
    انتخاب کرده (فارسی/انگلیسی/هر چیزی، به‌همراه فاصله) رو دست‌نخورده نگه
    می‌داره و فقط کاراکترهای کنترلی/خط‌جدید رو حذف و طولش رو محدود می‌کنه."""
    cleaned = "".join(ch for ch in str(name or "") if ch.isprintable()).strip()
    return cleaned[:40] if cleaned else fallback


def emoji_for_index(i: int) -> str:
    return CONFIG_EMOJI_POOL[i % len(CONFIG_EMOJI_POOL)]


# ---------------- اسم‌های خفن برای کانفیگ‌ها (دستی و خودکار) ----------------
COOL_WORDS = [
    "Tofan", "Barq", "Shahab", "Simorgh", "Parvaz", "Aftab", "Setareh", "Atash", "Sayeh", "Oghab",
    "Palang", "Rostam", "Sohrab", "Kaveh", "Arash", "Zagros", "Alborz", "Damavand", "Ghoghnoos", "Azhdaha",
    "Storm", "Thunder", "Phantom", "Ghost", "Nova", "Falcon", "Viper", "Titan", "Orbit", "Comet",
    "Pulse", "Turbo", "Rocket", "Blaze", "Shadow", "Vortex", "Nebula", "Zenith", "Apex", "Matrix",
    "Cyber", "Neon", "Sonic", "Hyper", "Warp", "Quantum", "Nitro", "Spark", "Wolf", "Dragon", "Phoenix", "Raven",
]
COOL_EMOJIS = [
    "🚀", "⚡", "🔥", "🌪️", "🦅", "🐉", "🛡️", "💎", "🌌", "⭐", "✨", "🧿", "🏹", "⚔️", "👑", "💜",
    "🌐", "🛰️", "🦁", "🐺", "🦋", "🌙", "☄️", "🎯", "💫", "🔮", "🧬", "🏴‍☠️", "🌋", "🪐", "🛸", "💠",
    "🔱", "🧊", "🌀", "🎇",
]
COOL_STYLES = [
    "{base}|{word}{emoji}",
    "{emoji} {base} | {word}",
    "{base} ✦ {word} {emoji}",
    "{base}·{word}{emoji}{emoji2}",
    "【{base}】{word}{emoji}",
    "{base} ⟪{word}⟫ {emoji}",
    "{emoji}{base}-{word}",
    "{base} ▸ {word} {emoji}",
]


def is_name_styled(name: str) -> bool:
    """اگر کاربر خودش اسم را تزئین کرده باشد (| یا اموجی/نماد)، دیگر دست نمی‌زنیم."""
    text = str(name or "")
    return "|" in text or any(ord(ch) >= 0x2190 for ch in text)


def style_config_name(base: str, index: int | None = None, style: int | None = None) -> str:
    """Vodiwalker → Vodiwalker|Tofan🚀 ؛ با index هر خروجی اسم/اموجی متفاوتی می‌گیرد."""
    base = "".join(ch for ch in str(base or "") if ch.isprintable()).strip()[:32] or "VodiWalker"
    rnd = secrets.SystemRandom()
    if index is None:
        word, emoji = rnd.choice(COOL_WORDS), rnd.choice(COOL_EMOJIS)
        emoji2 = rnd.choice(COOL_EMOJIS)
    else:
        word = COOL_WORDS[index % len(COOL_WORDS)]
        emoji = COOL_EMOJIS[(index * 5 + 1) % len(COOL_EMOJIS)]
        emoji2 = COOL_EMOJIS[(index * 5 + 9) % len(COOL_EMOJIS)]
    tpl = COOL_STYLES[(style or 0) % len(COOL_STYLES)]
    return tpl.format(base=base, word=word, emoji=emoji, emoji2=emoji2)[:60]


def decorate_label(base: str, index: int | None = None) -> str:
    """اعمال استایل خودکار روی اسم (در صورت فعال بودن از تنظیمات و تزئین‌نشده بودن اسم)."""
    if CONFIG.get("name_style_enabled", True) and not is_name_styled(base):
        return style_config_name(base, index=index)
    return base


def auto_display_name() -> str:
    """اسم خودکار خفن برای وقتی که کاربر چیزی ننوشته."""
    if CONFIG.get("name_style_enabled", True):
        return style_config_name("VodiWalker")
    return auto_config_name()


def name_suggestions(base: str, count: int = 12) -> list[str]:
    base = sanitize_display_name(base, "VodiWalker")
    rnd = secrets.SystemRandom()
    words = rnd.sample(COOL_WORDS, min(count, len(COOL_WORDS)))
    out: list[str] = []
    for i, word in enumerate(words):
        tpl = COOL_STYLES[0] if i < 4 else COOL_STYLES[i % len(COOL_STYLES)]
        out.append(tpl.format(base=base[:32], word=word, emoji=rnd.choice(COOL_EMOJIS), emoji2=rnd.choice(COOL_EMOJIS))[:60])
    return out


def normalize_exit_ids(body: dict) -> list[str]:
    """لیست یکتا و اعتبارسنجی‌شده‌ی خروجی‌ها از بدنه‌ی درخواست.
    هم `outbound_proxy_ids: [...]` و هم `outbound_proxy_id: "..."` قبول می‌شود."""
    raw = body.get("outbound_proxy_ids")
    if raw is None:
        raw = [body.get("outbound_proxy_id") or ""]
    elif not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="لیست خروجی‌ها معتبر نیست")
    exits: list[str] = []
    for item in raw:
        pid = clean_outbound_proxy_id(item)
        if pid not in exits:
            exits.append(pid)
    if not exits:
        exits = [""]
    if len(exits) > MAX_COMBO_EXITS:
        raise HTTPException(status_code=400, detail=f"حداکثر {MAX_COMBO_EXITS} خروجی همزمان مجاز است")
    return exits


def _exit_tag(info: dict | None, used: dict) -> str:
    """پسوند کوتاه ASCII برای اسم کانفیگ: de / us / dir (مستقیم)؛ تکراری‌ها de2, de3 ..."""
    if not info:
        tag = "dir"
    else:
        cc = "".join(ch for ch in str(info.get("country_code") or "").lower() if ch.isascii() and ch.isalnum())
        tag = cc[:3] or "px"
    used[tag] = used.get(tag, 0) + 1
    return tag if used[tag] == 1 else f"{tag}{used[tag]}"


async def create_combo_subscription(
    request: Request,
    *,
    exits: list[str],
    pairs_per_exit: int = 1,
    port: int,
    label: str = "",
    group_name: str = "",
    limit_bytes: int = 0,
    expires_at: str | None = None,
    ip_limit: int = 0,
    connection_limit: int = 0,
    speed_limit_bytes: int = 0,
    fingerprint: str = DEFAULT_FINGERPRINT,
    fragment: str = "off",
    note: str = "",
    category_id: str = "0",
    client_limit: int = 0,
    config_count: int = 1,
    clean_ips=None,
    alarm_enabled: bool = False,
    security_profile: str = "balanced",
) -> dict:
    """یک گروه ساب می‌سازد و برای هر خروجی یک WS + یک XHTTP داخلش می‌گذارد."""
    host = get_host(request)
    display_name = sanitize_display_name(label) if str(label or "").strip() else "VodiWalker"
    base = sanitize_config_name(label) if str(label or "").strip() else auto_config_name()
    infos = {pid: outbound_info(pid) for pid in exits}
    _emoji_counter = 0

    def _flag_country(info):
        return f"{info.get('flag') or ''} {info.get('country') or info.get('name') or ''}".strip()

    desc = "WS + XHTTP · خروجی: " + " ، ".join(_flag_country(infos[p]) if infos[p] else "مستقیم" for p in exits)
    sub_id, sub = await create_sub_group(name=(group_name or base)[:60], desc=desc[:200])

    pairs_per_exit = max(1, min(40, int(pairs_per_exit or 1)))
    used_tags: dict = {}
    multi = len(exits) > 1 or pairs_per_exit > 1
    rows: list[dict] = []
    items: list[dict] = []

    for pid in exits:
        for _pair_i in range(pairs_per_exit):
            # هر بار که تگ برای همون pid دوباره خواسته بشه، _exit_tag خودش شماره‌گذاری
            # می‌کنه (de, de2, de3, ...) — دقیقاً همون چیزی که برای «تعداد کل کانفیگ» لازمه.
            tag = _exit_tag(infos[pid], used_tags) if multi else ""
            row = {"outbound_proxy_id": pid, "outbound": infos[pid], "tag": tag}
            # هر خروجی/جفت یک اموجی خفنِ مخصوص خودش می‌گیره؛ اسم نمایشی برای همه‌شون
            # همون اسمیه که کاربر انتخاب کرده (WS و XHTTP یک خروجی هم اموجی مشترک دارن).
            pair_emoji = emoji_for_index(_emoji_counter)
            _emoji_counter += 1
            display_label = (
                decorate_label(display_name, _emoji_counter - 1)
                if CONFIG.get("name_style_enabled", True) and not is_name_styled(display_name)
                else f"{pair_emoji} {display_name}"
            )
            for protocol, suffix in COMBO_MEMBERS:
                uid, link = await make_link(
                    label=display_label,
                    limit_bytes=limit_bytes,
                    expires_at=expires_at,
                    note=note,
                    sub_id=sub_id,
                    protocol=protocol,
                    fingerprint=fingerprint,
                    alpn=DEFAULT_ALPN_BY_PROTOCOL.get(protocol, ""),
                    port=port,
                    ip_limit=ip_limit,
                    speed_limit_bytes=speed_limit_bytes,
                    connection_limit=connection_limit,
                    fragment=fragment,
                    clean_ips=clean_ips,
                    alarm_enabled=alarm_enabled,
                    category_id=category_id,
                    config_count=config_count,
                    outbound_proxy_id=pid,
                )
                async with LINKS_LOCK:
                    LINKS[uid]["combo_group_id"] = sub_id
                    # combo_tag یعنی «شریک جفت»: ws و xhttپ همین تگ، تا موقع ساخت
                    # کلاینت بشه جفتشون رو پیدا کرد و توی یک ساب گذاشت (نه دوتا جدا).
                    LINKS[uid]["combo_tag"] = tag
                    LINKS[uid]["combo_base_label"] = display_name
                    LINKS[uid]["client_limit"] = client_limit
                    LINKS[uid]["security_profile"] = security_profile
                row[suffix] = uid
                items.append(get_link_info(LINKS[uid], uid, host))
            rows.append(row)

    await save_state()
    sub_url = f"{get_scheme()}://{host}/sub-group/{sub['uuid_key']}"
    public_url = f"{get_scheme()}://{host}/p/{sub['uuid_key']}"
    where = "، ".join(infos[p]["name"] if infos[p] else "مستقیم" for p in exits)
    log_activity("link", f"اشتراک WS+XHTTP روی پورت {port} ساخته شد (خروجی: {where})", "ok")
    return {
        "ok": True, "combo": True,
        "combo_group_id": sub_id, "sub_id": sub_id,
        "sub_url": sub_url, "public_url": public_url, "sub_name": sub.get("name", base),
        "exits": rows,
        "outbound": infos[exits[0]] if len(exits) == 1 else None,   # سازگاری با UI قدیمی
        "items": items,
    }


async def add_combo_exits(sub_id: str, host: str, new_exits: list[str]) -> dict:
    """به یک اشتراک WS+XHTTP از قبل موجود، بدون ساخت اشتراک جدید، خروجی‌های تازه
    اضافه می‌کنه — تنظیمات (پورت، فینگرپرینت، محدودیت‌ها و ...) دقیقاً از روی یکی
    از جفت‌های موجود همون گروه کپی می‌شه، فقط outbound_proxy_id عوض می‌شه."""
    async with LINKS_LOCK:
        existing_pairs = [(uid, dict(l)) for uid, l in LINKS.items() if l.get("combo_group_id") == sub_id]
    if not existing_pairs:
        raise ValueError("این گروه یک اشتراک WS+XHTTP نیست (هیچ جفتی در آن پیدا نشد)")
    template = existing_pairs[0][1]
    already = {l.get("outbound_proxy_id", "") for _uid, l in existing_pairs}

    to_add, seen = [], set()
    for raw in new_exits:
        pid = clean_outbound_proxy_id(raw)
        if pid in already or pid in seen:
            continue
        seen.add(pid)
        to_add.append(pid)
    if not to_add:
        return {"added": 0, "items": [], "skipped_existing": list(new_exits)}

    infos = {pid: outbound_info(pid) for pid in to_add}

    # از تگ‌های موجود میان همین گروه، شمارنده‌ی هر پیشوند (de, us, dir, ...) رو
    # دوباره می‌سازیم تا خروجی‌های جدید با شماره‌ی درست ادامه پیدا کنن (de -> de2)
    used_tags: dict = {}
    for tag in {l.get("combo_tag", "") for _uid, l in existing_pairs if l.get("combo_tag")}:
        m = re.match(r"^([a-zA-Z]+)(\d*)$", tag)
        if not m:
            continue
        base, num = m.group(1), m.group(2)
        used_tags[base] = max(used_tags.get(base, 0), int(num) if num else 1)
    multi = True  # با بیش از یک خروجی در گروه، تگ‌گذاری همیشه لازمه

    rows, items = [], []
    _existing_exit_count = len(existing_pairs) // max(1, len(COMBO_MEMBERS))
    _combo_display_name = (
        template.get("combo_base_label")
        or sanitize_display_name(template.get("label") or "")
        or "VodiWalker"
    )
    for _add_i, pid in enumerate(to_add):
        tag = _exit_tag(infos[pid], used_tags)
        row = {"outbound_proxy_id": pid, "outbound": infos[pid], "tag": tag}
        pair_emoji = emoji_for_index(_existing_exit_count + _add_i)
        display_label = (
            decorate_label(_combo_display_name, _existing_exit_count + _add_i)
            if CONFIG.get("name_style_enabled", True) and not is_name_styled(_combo_display_name)
            else f"{pair_emoji} {_combo_display_name}"
        )
        for protocol, suffix in COMBO_MEMBERS:
            uid, link = await make_link(
                label=display_label,
                limit_bytes=template.get("limit_bytes", 0),
                expires_at=template.get("expires_at"),
                note=template.get("note", ""),
                sub_id=sub_id,
                protocol=protocol,
                fingerprint=template.get("fingerprint", DEFAULT_FINGERPRINT),
                alpn=DEFAULT_ALPN_BY_PROTOCOL.get(protocol, ""),
                port=int(template.get("port") or DEFAULT_PORT),
                ip_limit=template.get("ip_limit", 0),
                speed_limit_bytes=template.get("speed_limit_bytes", 0),
                connection_limit=template.get("connection_limit", 0),
                fragment=template.get("fragment", "off"),
                clean_ips=list(template.get("clean_ips") or []),
                alarm_enabled=bool(template.get("alarm_enabled", False)),
                category_id=template.get("category_id", "0"),
                config_count=template.get("config_count", 1),
                outbound_proxy_id=pid,
            )
            async with LINKS_LOCK:
                LINKS[uid]["combo_group_id"] = sub_id
                LINKS[uid]["combo_tag"] = tag
                LINKS[uid]["client_limit"] = template.get("client_limit", 0)
                LINKS[uid]["security_profile"] = template.get("security_profile", "balanced")
            row[suffix] = uid
            items.append(get_link_info(LINKS[uid], uid, host))
        rows.append(row)

    await save_state()
    where = "، ".join((infos[p]["name"] if infos[p] else "مستقیم") for p in to_add)
    async with SUBS_LOCK:
        sub_name = (SUBS.get(sub_id) or {}).get("name", "")
    log_activity("link", f"{len(to_add)} خروجی تازه (WS+XHTTP) به اشتراک «{sub_name}» اضافه شد: {where}", "ok")
    return {"added": len(to_add), "exits": rows, "items": items}


@app.post("/api/subs/{sub_id}/combo-exits")
async def api_add_combo_exits(sub_id: str, request: Request, _=Depends(require_auth)):
    """چک‌باکسی: هر چقدر پروکسی (به‌علاوه‌ی «مستقیم» اگه بخوای) روی این اشتراک بزن —
    برای هرکدوم که هنوز نداره، یک جفت WS+XHTTP تازه با همون تنظیمات ساخته می‌شه."""
    async with SUBS_LOCK:
        if sub_id not in SUBS:
            raise HTTPException(status_code=404, detail="گروه ساب پیدا نشد")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر است")
    exits = body.get("outbound_proxy_ids") if isinstance(body, dict) else None
    if not isinstance(exits, list) or not exits:
        raise HTTPException(status_code=400, detail="حداقل یک خروجی (پروکسی یا مستقیم) انتخاب کن")
    if len(exits) > MAX_COMBO_EXITS:
        raise HTTPException(status_code=400, detail=f"حداکثر {MAX_COMBO_EXITS} خروجی در هر بار")
    host = get_host(request)
    try:
        result = await add_combo_exits(sub_id, host, exits)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"ok": True, **result}


@app.post("/api/links/auto")
async def create_auto_link(
    request: Request,
    _=Depends(require_auth),
):
    try:
        body = await request.json()
    except Exception:
        body = {}
    if not isinstance(body, dict):
        body = {}
    host = get_host(request)
    profile = str(body.get("profile", "balanced")).strip().lower()
    profiles = {
        # ساخت خودکار: محدودیت آی‌پی همیشه نامحدود (0) است
        "normal": {"ip":0,"conn":0,"speed":0,"fp":"chrome","fragment":"off"},
        "balanced": {"ip":0,"conn":4,"speed":0,"fp":"chrome","fragment":"safe"},
        "gaming": {"ip":0,"conn":2,"speed":0,"fp":"chrome","fragment":"safe"},
        "maximum": {"ip":0,"conn":0,"speed":0,"fp":"randomized","fragment":"safe"},
    }
    cfg = dict(profiles.get(profile, profiles["balanced"]))
    cfg["ip"] = 0  # حتی اگر پروفایل/درخواست چیز دیگری بگوید، ساخت خودکار = آی‌پی نامحدود
    port = safe_int(body.get("port", 443), minimum=MIN_PORT, maximum=MAX_PORT)
    exits = normalize_exit_ids(body)
    pairs_per_exit = max(1, (safe_int(body.get("pairs_count", 2), minimum=2, maximum=80) + 1) // 2)
    note = f"Auto generated by VodiWalker | profile={profile}"
    combo = bool(body.get("combo")) or str(body.get("protocol", "")).strip().lower() in ("combo", "ws+xhttp", "combo-ws-xhttp")

    if combo:
        result = await create_combo_subscription(
            request, exits=exits, pairs_per_exit=pairs_per_exit, port=port, note=note, fingerprint=cfg["fp"], fragment=cfg["fragment"],
            ip_limit=cfg["ip"], connection_limit=cfg["conn"], speed_limit_bytes=cfg["speed"], security_profile=profile,
        )
        result["profile"] = profile
        return result

    protocol = normalize_protocol(body.get("protocol", DEFAULT_PROTOCOL))
    uid, link = await make_link(
        label=auto_display_name(), limit_bytes=0, expires_at=None, note=note, protocol=protocol,
        fingerprint=cfg["fp"], alpn=DEFAULT_ALPN_BY_PROTOCOL.get(protocol, ""), port=port,
        ip_limit=cfg["ip"], speed_limit_bytes=cfg["speed"], connection_limit=cfg["conn"],
        fragment=cfg["fragment"], outbound_proxy_id=exits[0],
    )
    link["security_profile"] = profile
    log_activity("link", f"کانفیگ خودکار «{link['label']}» با {PROTOCOL_LABELS.get(protocol, protocol)} ساخته شد", "ok")
    return {**get_link_info(link, uid, host), "ok": True, "profile": profile}


@app.post("/api/links/combo")
async def create_combo_api(
    request: Request,
    _=Depends(require_auth),
):
    """«اینباند جدید» پیش‌فرض: یک اشتراک با ۱ WS + ۱ XHTTP برای هر خروجی انتخاب‌شده."""
    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="اطلاعات ارسال‌شده معتبر نیست.")

    exits = normalize_exit_ids(body)

    limit_value = safe_float(body.get("limit_value", 0))
    limit_unit = str(body.get("limit_unit", "GB") or "GB").upper()
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    expires_days = safe_int(body.get("expires_days", 0), minimum=0)
    speed_value = safe_float(body.get("speed_limit_value", 0))
    speed_unit = str(body.get("speed_limit_unit", "MBIT") or "MBIT").upper()
    speed_bytes = 0 if speed_value <= 0 else parse_speed_to_bytes(speed_value, speed_unit)
    ip_limit = safe_int(body.get("ip_limit", 0), minimum=0)
    connection_limit = safe_int(body.get("connection_limit", 0), minimum=0)
    port = safe_int(body.get("port", DEFAULT_PORT), default=DEFAULT_PORT, minimum=MIN_PORT, maximum=MAX_PORT)
    config_count = safe_int(body.get("config_count", 1), minimum=1, maximum=40)
    client_limit = safe_int(body.get("client_limit", 0), minimum=0, maximum=1000)

    fingerprint = str(body.get("fingerprint", DEFAULT_FINGERPRINT) or DEFAULT_FINGERPRINT).strip().lower()
    if fingerprint not in FINGERPRINTS:
        fingerprint = DEFAULT_FINGERPRINT
    fragment = str(body.get("fragment", "off") or "off").strip().lower()
    if fragment not in {"off", "safe", "balanced", "aggressive"}:
        fragment = "off"

    category_id = str(body.get("category_id") or "0")
    if category_id not in CATEGORIES:
        category_id = "0"
    cat = CATEGORIES.get(category_id) or {}
    clean_ips = list(cat.get("clean_ips") or [])
    if cat.get("limit_bytes") and limit_bytes <= 0:
        limit_bytes = int(cat["limit_bytes"])
    if cat.get("expires_days") and expires_days <= 0:
        expires_days = int(cat["expires_days"])
    if cat.get("connection_limit") and connection_limit <= 0:
        connection_limit = int(cat["connection_limit"])
    if cat.get("speed_limit_bytes") and speed_bytes <= 0:
        speed_bytes = int(cat["speed_limit_bytes"])
    if cat.get("ip_limit") and ip_limit <= 0:
        ip_limit = int(cat["ip_limit"])
    if cat.get("single_user"):
        ip_limit = ip_limit or 1
        connection_limit = connection_limit or 1
    expires_at = (datetime.now() + timedelta(days=expires_days)).isoformat() if expires_days > 0 else None

    pairs_per_exit = max(1, (safe_int(body.get("pairs_count", 2), minimum=2, maximum=80) + 1) // 2)
    label = "" if cat.get("random_name") else str(body.get("label") or "").strip()
    return await create_combo_subscription(
        request, exits=exits, pairs_per_exit=pairs_per_exit, port=port, label=label, group_name=label,
        limit_bytes=limit_bytes, expires_at=expires_at, ip_limit=ip_limit,
        connection_limit=connection_limit, speed_limit_bytes=speed_bytes,
        fingerprint=fingerprint, fragment=fragment, note=str(body.get("note") or "")[:500],
        category_id=category_id, client_limit=client_limit, config_count=config_count,
        clean_ips=clean_ips, alarm_enabled=bool(body.get("alarm_enabled", False)),
    )


@app.post("/api/links/outbound")
async def set_links_outbound(
    request: Request,
    _=Depends(require_auth),
):
    """اعمال (یا تغییر) خروجی روی هر تعداد اینباند. کلاینت‌های هر اینباند هم
    پیش‌فرض همراهش عوض می‌شوند، چون ریلی خروجی را از UUID خودِ کلاینت می‌خواند.
    body: {uuids: [...], outbound_proxy_id: "" | "<id>", include_clients: true}"""
    try:
        body = await request.json()
    except Exception:
        body = None
    if not isinstance(body, dict) or not isinstance(body.get("uuids"), list) or not body["uuids"]:
        raise HTTPException(status_code=400, detail="حداقل یک اینباند انتخاب کنید.")
    if len(body["uuids"]) > MAX_BULK_LINKS:
        raise HTTPException(status_code=400, detail=f"حداکثر {MAX_BULK_LINKS} مورد در هر بار")
    pid = clean_outbound_proxy_id(body.get("outbound_proxy_id"))
    include_clients = bool(body.get("include_clients", True))
    targets = [str(u) for u in dict.fromkeys(body["uuids"])]

    updated, clients, missing = 0, 0, []
    async with LINKS_LOCK:
        for uid in targets:
            link = LINKS.get(uid)
            if link is None:
                missing.append(uid)
                continue
            link["outbound_proxy_id"] = pid
            updated += 1
            if include_clients:
                for child in LINKS.values():
                    if child.get("parent_inbound_id") == uid:
                        child["outbound_proxy_id"] = pid
                        clients += 1
    await save_state()
    info = outbound_info(pid)
    log_activity("link", f"خروجی {updated} اینباند و {clients} کلاینت روی «{info['name'] if info else 'مستقیم'}» تنظیم شد", "ok")
    return {"ok": True, "updated": updated, "clients": clients, "missing": missing, "outbound": info}


# ============================================================
# INBOUND CLIENT MANAGER
# ============================================================

async def add_client_to_inbound(uid: str, label: str = None, limit_bytes: int = None, expires_days: int = 0,
                                  ip_limit: int = None, speed_limit_bytes: int = None, connection_limit: int = None,
                                  note: str = None, outbound_proxy_id: str | None = None, created_by: str = "owner"):
    """Core logic to create a real client (child link) under an inbound. Shared by the
    HTTP API and the Telegram bot so both stay in sync."""
    async with LINKS_LOCK:
        parent = LINKS.get(uid)
        if not parent:
            raise ValueError("اینباند پیدا نشد")
        source = dict(parent)
        existing_clients = sum(1 for x in LINKS.values() if x.get("parent_inbound_id") == uid)
        client_limit = int(source.get("client_limit") or 0)
        if client_limit and existing_clients >= client_limit:
            raise ValueError(f"ظرفیت اینباند تکمیل است ({client_limit} کاربر)")
    final_label = str(label or f"Client · {existing_clients+1}").strip()[:120]
    final_limit_bytes = safe_int(limit_bytes if limit_bytes is not None else source.get("limit_bytes", 0), minimum=0)
    expires_at = (datetime.now() + timedelta(days=expires_days)).isoformat() if expires_days else source.get("expires_at")
    child_uid, child = await make_link(
        label=final_label,
        limit_bytes=final_limit_bytes,
        expires_at=expires_at,
        note=str(note or source.get("note") or "")[:500],
        # اشتراک ترکیبی «ساخت سریع» (یک WS + یک XHTTP) باید همیشه دقیقاً دو خط بمونه؛
        # پس کلاینتِ اینباندهای اون گروه وارد اون گروه نمی‌شه.
        sub_id=(None if (source.get("combo_group_id") and source.get("combo_group_id") == source.get("sub_id")) else source.get("sub_id")),
        protocol=source.get("protocol", DEFAULT_PROTOCOL),
        fingerprint=source.get("fingerprint", DEFAULT_FINGERPRINT),
        alpn=source.get("alpn", ""),
        port=int(source.get("port", DEFAULT_PORT) or DEFAULT_PORT),
        ip_limit=safe_int(ip_limit if ip_limit is not None else source.get("ip_limit", 0), minimum=0),
        speed_limit_bytes=safe_int(speed_limit_bytes if speed_limit_bytes is not None else source.get("speed_limit_bytes", 0), minimum=0),
        connection_limit=safe_int(connection_limit if connection_limit is not None else source.get("connection_limit", 0), minimum=0),
        fragment=source.get("fragment", "off"),
        clean_ips=source.get("clean_ips", []),
        alarm_enabled=bool(source.get("alarm_enabled", False)),
        category_id=str(source.get("category_id") or "0"),
        config_count=1,
        manual_fields={k: source.get(k) for k in ("base_protocol","network","security","address","path","host_header","sni","flow","grpc_service_name","grpc_mode","xhttp_mode","header_type","allow_insecure","reality_public_key","reality_short_id","reality_spider_x","ss_method","ss_password")},
        # None = مثل اینباند والد؛ "" = مستقیم؛ غیرخالی = همون پراکسی
        outbound_proxy_id=(str(source.get("outbound_proxy_id") or "") if outbound_proxy_id is None else str(outbound_proxy_id).strip()),
    )
    async with LINKS_LOCK:
        LINKS[child_uid]["parent_inbound_id"] = uid
        LINKS[child_uid]["created_by"] = created_by or "owner"
        LINKS[child_uid]["is_default"] = False
        LINKS[child_uid]["protocol_label"] = protocol_display_label(LINKS[child_uid])
    await save_state()
    log_activity("client", f"کلاینت جدید برای «{source.get('label','اینباند')}» ساخته شد", "ok")
    return child_uid, LINKS[child_uid]


async def remove_inbound_client(uid: str, client_id: str):
    async with LINKS_LOCK:
        child = LINKS.get(client_id)
        if not child or child.get("parent_inbound_id") != uid:
            raise ValueError("کلاینت پیدا نشد")
        LINKS.pop(client_id, None)
    await save_state()
    log_activity("client", f"کلاینت {client_id[:8]}… حذف شد", "warn")


@app.get("/api/links/{uid}/clients")
async def list_inbound_clients(uid: str, request: Request, _=Depends(require_auth)):
    async with LINKS_LOCK:
        parent = LINKS.get(uid)
        if not parent:
            raise HTTPException(status_code=404, detail="اینباند پیدا نشد")
        _aid = await actor_id_of(request)
        children = [(cid, dict(link)) for cid, link in LINKS.items() if link.get("parent_inbound_id") == uid and client_visible(link, _aid)]
    host = get_host(request)
    return {"ok": True, "inbound": get_link_info(parent, uid, host), "clients": [get_link_info(x, cid, host) for cid, x in children]}

def find_combo_sibling(uid: str) -> str | None:
    """اگه uid یکی از دو عضو یک جفت WS+XHTTP باشه (همون combo_tag، همون گروه، پروتکل متفاوت)،
    UUID شریکش رو برمی‌گردونه؛ وگرنه None (یعنی اینباند تکی و معمولیه)."""
    parent = LINKS.get(uid)
    if not parent or not parent.get("combo_group_id"):
        return None
    group_id, tag, protocol = parent.get("combo_group_id"), parent.get("combo_tag", ""), parent.get("protocol")
    for other_uid, other in LINKS.items():
        if other_uid != uid and other.get("combo_group_id") == group_id and other.get("combo_tag", "") == tag and other.get("protocol") != protocol:
            return other_uid
    return None


@app.post("/api/links/{uid}/clients")
async def create_inbound_client(uid: str, request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        body = {}
    async with LINKS_LOCK:
        if uid not in LINKS:
            raise HTTPException(status_code=404, detail="اینباند پیدا نشد")
        sibling_uid = find_combo_sibling(uid)
    if not link_in_scope(await actor_scope(request), uid):
        raise HTTPException(status_code=403, detail="به این اینباند دسترسی ندارید")
    kwargs = dict(
        created_by=await actor_id_of(request),
        label=body.get("label"),
        limit_bytes=body.get("limit_bytes"),
        expires_days=safe_int(body.get("expires_days", 0), minimum=0),
        ip_limit=body.get("ip_limit"),
        speed_limit_bytes=body.get("speed_limit_bytes"),
        connection_limit=body.get("connection_limit"),
        note=body.get("note"),
        outbound_proxy_id=(clean_outbound_proxy_id(body.get("outbound_proxy_id")) if "outbound_proxy_id" in body else None),
    )
    host = get_host(request)
    try:
        if sibling_uid:
            # اینباند مقصد بخشی از یک جفت WS+XHTTP است: به‌جای یک کلاینت تکی، برای
            # هر دو عضو کلاینت می‌سازیم و هر دو را در یک ساب‌گروه مخصوص همین کاربر
            # می‌گذاریم — یعنی مشتری یک لینک اشتراک می‌گیرد که هم WS و هم XHTTP دارد.
            base_label = str(body.get("label") or "Client").strip()[:80] or "Client"
            sub_id, sub = await create_sub_group(name=f"{base_label}"[:60], desc="کلاینت WS + XHTTP")
            created_ids = []
            for member_uid in (uid, sibling_uid):
                child_uid, _child = await add_client_to_inbound(member_uid, **kwargs)
                await set_link_sub(child_uid, sub_id)
                created_ids.append(child_uid)
            await save_state()
            items = [get_link_info(LINKS[c], c, host) for c in created_ids]
            return {
                "ok": True, "combo": True,
                "clients": items, "client": items[0],
                "sub_url": f"{get_scheme()}://{host}/sub-group/{sub['uuid_key']}",
                "public_url": f"{get_scheme()}://{host}/p/{sub['uuid_key']}",
            }
        child_uid, _child = await add_client_to_inbound(uid, **kwargs)
    except ValueError as exc:
        code = 409 if "ظرفیت" in str(exc) else 404
        raise HTTPException(status_code=code, detail=str(exc))
    return {"ok": True, "client": get_link_info(LINKS[child_uid], child_uid, host)}

@app.delete("/api/links/{uid}/clients/{client_id}")
async def delete_inbound_client(uid: str, client_id: str, request: Request, _=Depends(require_auth)):
    _aid = await actor_id_of(request)
    _c = LINKS.get(client_id)
    if _c is not None and not client_visible(_c, _aid):
        raise HTTPException(status_code=404, detail="کلاینت پیدا نشد")
    try:
        await remove_inbound_client(uid, client_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"ok": True, "deleted": client_id}

# ============================================================
# LIST LINKS
# ============================================================

@app.get("/api/protocols")
async def api_protocols(request: Request, _=Depends(require_auth)):
    return {
        "protocols": [
            {
                "id": p,
                "label": PROTOCOL_LABELS.get(p, p),
                "functional": p in LIVE_PROTOCOLS,
                "live_status": "live" if p in LIVE_PROTOCOLS else "link-only",
            }
            for p in PROTOCOLS
        ],
        "default": DEFAULT_PROTOCOL,
        "manual": {
            "base_protocols": [
                {"id": p, "label": MANUAL_BASE_PROTOCOL_LABELS.get(p, p)}
                for p in MANUAL_BASE_PROTOCOLS
            ],
            "networks": [
                {"id": n, "label": NETWORK_LABELS.get(n, n)}
                for n in NETWORKS
            ],
            "securities": [
                {"id": s, "label": SECURITY_LABELS.get(s, s)}
                for s in SECURITIES
            ],
            "xhttp_modes": list(XHTTP_MODES),
            "shadowsocks_methods": list(SHADOWSOCKS_METHODS),
            "fingerprints": list(FINGERPRINTS),
            "live_combos": [["vless", n, s] for n, s in MANUAL_LIVE_COMBOS],
        },
    }


@app.get("/api/reality-keypair")
async def api_reality_keypair(_=Depends(require_auth)):
    """تولید یک جفت‌کلید X25519 و Short ID تصادفی برای Reality — دقیقاً با همان
    فرمتی که Xray-core و کلاینت‌ها (v2rayN، NekoBox، Streisand، ...) انتظار دارند
    (base64url بدون padding، ۳۲ بایت خام)."""
    try:
        from cryptography.hazmat.primitives.asymmetric import x25519
        from cryptography.hazmat.primitives import serialization

        private_key = x25519.X25519PrivateKey.generate()
        public_key = private_key.public_key()

        priv_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        pub_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

        b64 = lambda b: base64.urlsafe_b64encode(b).decode().rstrip("=")

        return {
            "ok": True,
            "private_key": b64(priv_bytes),
            "public_key": b64(pub_bytes),
            "short_id": secrets.token_hex(4),
        }
    except Exception as exc:
        logger.exception("Reality keypair generation failed: %s", exc)
        raise HTTPException(status_code=500, detail="تولید کلید Reality ممکن نشد. کتابخانه‌ی cryptography نصب است؟")


@app.get("/api/links")
async def list_links(
    request: Request,
    _=Depends(require_auth),
):

    host = get_host(request)
    _scope = await actor_scope(request)

    async with LINKS_LOCK:
        snapshot = dict(LINKS)
    if _scope is not None:
        snapshot = {u: l for u, l in snapshot.items() if u in _scope or l.get("parent_inbound_id") in _scope}
    _aid = await actor_id_of(request)
    snapshot = {u: l for u, l in snapshot.items() if client_visible(l, _aid)}

    result = []

    for uid, link in snapshot.items():

        info = get_link_info(
            link,
            uid,
            host,
        )

        info["client_count"] = sum(1 for x in snapshot.values() if x.get("parent_inbound_id") == uid)
        result.append(
            {
                **info,

                "created_at":
                    link.get(
                        "created_at"
                    ),

                "expired":
                    is_link_expired(
                        link
                    ),

                "sub_url":
                    f"{get_scheme()}://{host}/sub/{uid}",

                "info_url":
                    f"{get_scheme()}://{host}/info/{uid}",

                "connected_ips":
                    len(
                        unique_ips_for_uuid(
                            uid
                        )
                    ),
            }
        )

    result.sort(
        key=lambda item:
            item.get(
                "created_at",
                "",
            ),
        reverse=True,
    )

    return {
        "links": result
    }


# ============================================================
# LINK INFO API
# ============================================================

@app.get("/api/links/{uid}/info")
async def link_info_api(
    uid: str,
    request: Request,
    _=Depends(require_auth),
):

    async with LINKS_LOCK:

        link = LINKS.get(uid)

        if not link:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        snapshot = dict(link)

    host = get_host(request)

    return {
        "ok": True,
        **get_link_info(
            snapshot,
            uid,
            host,
        ),
    }


# ============================================================
# UPDATE LINK
# ============================================================

@app.patch("/api/links/{uid}")
async def update_link(
    uid: str,
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="اطلاعات نامعتبر است",
        )

    if not isinstance(body, dict):
        raise HTTPException(
            status_code=400,
            detail="اطلاعات نامعتبر است",
        )

    async with LINKS_LOCK:

        if uid not in LINKS:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        link = LINKS[uid]

        old_sub = link.get(
            "sub_id"
        )

        label = link.get(
            "label",
            uid,
        )

        if "active" in body:
            link["active"] = bool(
                body["active"]
            )

        if "label" in body:

            value = str(
                body["label"]
            ).strip()

            if value:
                link["label"] = value[:60]

        if "note" in body:

            link["note"] = str(
                body.get(
                    "note",
                    "",
                )
            )[:500]

        if "reset_usage" in body:

            if body.get(
                "reset_usage"
            ):
                link[
                    "used_bytes"
                ] = 0

        if "limit_value" in body:

            value = safe_float(
                body.get(
                    "limit_value",
                    0,
                )
            )

            unit = str(
                body.get(
                    "limit_unit",
                    "GB",
                )
                or "GB"
            )

            link[
                "limit_bytes"
            ] = (
                0
                if value <= 0
                else parse_size_to_bytes(
                    value,
                    unit,
                )
            )

        if "expires_at" in body and str(body.get("expires_at") or "").strip():
            try:
                dt = datetime.fromisoformat(str(body.get("expires_at")).replace("Z", "+00:00"))
                link["expires_at"] = dt.replace(tzinfo=None).isoformat()
            except Exception:
                raise HTTPException(status_code=400, detail="زمان انقضا معتبر نیست")
        elif "expires_at" in body and not str(body.get("expires_at") or "").strip() and "expires_days" not in body:
            link["expires_at"] = None

        if "expires_days" in body:

            days = safe_int(
                body.get(
                    "expires_days",
                    0,
                ),
                minimum=0,
            )

            link[
                "expires_at"
            ] = (
                (
                    datetime.now()
                    + timedelta(
                        days=days
                    )
                ).isoformat()
                if days > 0
                else None
            )

        if "fingerprint" in body:

            fingerprint = str(
                body.get(
                    "fingerprint",
                    DEFAULT_FINGERPRINT,
                )
            ).strip().lower()

            link[
                "fingerprint"
            ] = (
                fingerprint
                if fingerprint in FINGERPRINTS
                else DEFAULT_FINGERPRINT
            )

        if "alpn" in body:

            link["alpn"] = str(
                body.get(
                    "alpn",
                    "",
                )
            )[:100]

        if "port" in body:

            p = safe_int(
                body.get(
                    "port",
                    DEFAULT_PORT,
                ),
                default=DEFAULT_PORT,
                minimum=MIN_PORT,
                maximum=MAX_PORT,
            )

            link["port"] = p

        if "ip_limit" in body:

            link["ip_limit"] = safe_int(
                body.get(
                    "ip_limit",
                    0,
                ),
                minimum=0,
            )

        if "connection_limit" in body:

            link[
                "connection_limit"
            ] = safe_int(
                body.get(
                    "connection_limit",
                    0,
                ),
                minimum=0,
            )

        if "client_limit" in body:
            link["client_limit"] = safe_int(body.get("client_limit", 0), minimum=0, maximum=1000)

        if "config_count" in body:
            link["config_count"] = safe_int(body.get("config_count", 1), minimum=1, maximum=40)

        if "speed_limit_value" in body:

            speed_value = safe_float(
                body.get(
                    "speed_limit_value",
                    0,
                )
            )

            speed_unit = str(
                body.get(
                    "speed_limit_unit",
                    "MBIT",
                )
                or "MBIT"
            )

            link[
                "speed_limit_bytes"
            ] = (
                0
                if speed_value <= 0
                else parse_speed_to_bytes(
                    speed_value,
                    speed_unit,
                )
            )

        if "protocol" in body:

            protocol = str(
                body.get(
                    "protocol",
                    DEFAULT_PROTOCOL,
                )
            ).strip().lower()

            link["protocol"] = (
                protocol
                if protocol == "manual" or protocol in PROTOCOLS
                else DEFAULT_PROTOCOL
            )
            if link["protocol"] != "manual":
                link["protocol_label"] = protocol_display_label(link)

        if link.get("protocol") == "manual" and isinstance(body.get("manual"), dict):
            manual_fields = body["manual"]
            link["base_protocol"] = normalize_base_protocol(manual_fields.get("base_protocol", link.get("base_protocol")))
            link["network"] = normalize_network(manual_fields.get("network", link.get("network")))
            link["security"] = normalize_security(manual_fields.get("security", link.get("security")))
            if "address" in manual_fields:
                link["address"] = str(manual_fields.get("address") or "").strip()[:255]
            if "path" in manual_fields:
                link["path"] = str(manual_fields.get("path") or "").strip()[:255]
            if "host_header" in manual_fields:
                link["host_header"] = str(manual_fields.get("host_header") or "").strip()[:255]
            if "sni" in manual_fields:
                link["sni"] = str(manual_fields.get("sni") or "").strip()[:255]
            if "flow" in manual_fields:
                link["flow"] = str(manual_fields.get("flow") or "").strip()[:64]
            if "grpc_service_name" in manual_fields:
                link["grpc_service_name"] = str(manual_fields.get("grpc_service_name") or "").strip()[:128]
            if "grpc_mode" in manual_fields:
                link["grpc_mode"] = str(manual_fields.get("grpc_mode") or "gun").strip()[:32] or "gun"
            if "xhttp_mode" in manual_fields:
                link["xhttp_mode"] = normalize_xhttp_mode(manual_fields.get("xhttp_mode"))
            if "header_type" in manual_fields:
                link["header_type"] = str(manual_fields.get("header_type") or "").strip()[:32]
            if "allow_insecure" in manual_fields:
                link["allow_insecure"] = bool(manual_fields.get("allow_insecure"))
            if "reality_public_key" in manual_fields:
                link["reality_public_key"] = str(manual_fields.get("reality_public_key") or "").strip()[:128]
            if "reality_short_id" in manual_fields:
                link["reality_short_id"] = str(manual_fields.get("reality_short_id") or "").strip()[:32]
            if "reality_spider_x" in manual_fields:
                link["reality_spider_x"] = str(manual_fields.get("reality_spider_x") or "/").strip()[:128] or "/"
            if "ss_method" in manual_fields:
                link["ss_method"] = str(manual_fields.get("ss_method") or "chacha20-ietf-poly1305").strip()[:80]
            if "ss_password" in manual_fields:
                link["ss_password"] = str(manual_fields.get("ss_password") or "").strip()[:255]
            link["protocol_label"] = protocol_display_label(link)

        if "outbound_proxy_id" in body:
            link["outbound_proxy_id"] = clean_outbound_proxy_id(body.get("outbound_proxy_id"))
            # کلاینت‌های این اینباند باید همان خروجی را داشته باشند (ریلی از UUID کلاینت می‌خواند)
            for _child in LINKS.values():
                if _child.get("parent_inbound_id") == uid:
                    _child["outbound_proxy_id"] = link["outbound_proxy_id"]

        if "fragment" in body:

            fragment = str(
                body.get(
                    "fragment",
                    "off",
                )
                or "off"
            ).strip().lower()

            if fragment not in {
                "off",
                "safe",
                "balanced",
                "aggressive",
            }:
                fragment = "off"

            link["fragment"] = fragment

        if "sub_id" in body:

            link[
                "sub_id"
            ] = (
                body.get(
                    "sub_id"
                )
                or None
            )

        new_sub = body.get(
            "sub_id",
            "UNCHANGED",
        )

    if new_sub != "UNCHANGED":

        async with SUBS_LOCK:

            if (
                old_sub
                and old_sub in SUBS
            ):

                ids = SUBS[
                    old_sub
                ].get(
                    "link_ids",
                    [],
                )

                if uid in ids:
                    ids.remove(uid)

            if (
                new_sub
                and new_sub in SUBS
            ):

                ids = SUBS[
                    new_sub
                ].setdefault(
                    "link_ids",
                    [],
                )

                if uid not in ids:
                    ids.append(uid)

    await save_state()

    log_activity(
        "link",
        (
            f"کانفیگ "
            f"«{label}» "
            f"ویرایش شد"
        ),
        "info",
    )

    return {
        "ok": True
    }


# ============================================================
# RESET USAGE
# ============================================================

@app.post(
    "/api/links/{uid}/reset-usage"
)
async def reset_link_usage(
    uid: str,
    _=Depends(require_auth),
):

    async with LINKS_LOCK:

        link = LINKS.get(uid)

        if not link:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        link["used_bytes"] = 0

        label = link.get(
            "label",
            uid,
        )

    await save_state()

    log_activity(
        "link",
        (
            f"مصرف کانفیگ "
            f"«{label}» ریست شد"
        ),
        "info",
    )

    return {
        "ok": True,
        "uuid": uid,
        "used_bytes": 0,
    }


# ============================================================
# REGENERATE / SWAP LINK (تعویض لینک — UUID جدید، همان تنظیمات)
# ============================================================
# لینک قدیمی بلافاصله از کار می‌افتد (چون UUID عوض شده) و یک UUID جدید با
# همان تنظیمات (حجم، انقضا، دسته، پروتکل، محدودیت‌ها و ...) جایگزینش می‌شود.
# برای کلاینت‌های فرزند یک اینباند هم پشتیبانی می‌شود.

@app.post("/api/links/{uid}/regenerate")
async def regenerate_link(
    uid: str,
    request: Request,
    _=Depends(require_auth),
):
    async with LINKS_LOCK:
        old_link = LINKS.get(uid)
        if not old_link:
            raise HTTPException(status_code=404, detail="link not found")

        new_uid = generate_uuid()
        while new_uid in LINKS:
            new_uid = generate_uuid()

        new_link = dict(old_link)
        # مصرف قبلی حفظ می‌شود (این فقط تعویض کلید/لینک است، نه ریست حجم)
        LINKS[new_uid] = new_link
        del LINKS[uid]
        scope_replace_uid(uid, new_uid)

        parent_id = old_link.get("parent_inbound_id")
        sub_id = old_link.get("sub_id")
        label = old_link.get("label", uid)

        # اگر این اینباند بود، فرزندانش را به UUID جدید مادر وصل کن
        updated_children = 0
        for child in LINKS.values():
            if child.get("parent_inbound_id") == uid:
                child["parent_inbound_id"] = new_uid
                updated_children += 1

    if sub_id:
        async with SUBS_LOCK:
            sub = SUBS.get(sub_id)
            if sub:
                ids = sub.get("link_ids", [])
                if uid in ids:
                    ids[ids.index(uid)] = new_uid

    await save_state()

    log_activity(
        "link",
        f"لینک «{label}» تعویض شد (UUID جدید صادر شد)",
        "warn",
    )

    host = get_host(request)
    async with LINKS_LOCK:
        refreshed = LINKS.get(new_uid)

    return {
        **(get_link_info(refreshed, new_uid, host) if refreshed else {}),
        "ok": True,
        "old_uuid": uid,
        "uuid": new_uid,
        "updated_children": updated_children,
    }


# ============================================================
# LINK ACTION
# ============================================================

@app.post(
    "/api/links/{uid}/action"
)
async def link_action(
    uid: str,
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="JSON نامعتبر است",
        )

    action = str(
        body.get(
            "action",
            "",
        )
    ).strip().lower()

    if action == "reset":

        await reset_link_usage(
            uid,
            _
        )

        return {
            "ok": True,
            "action": "reset",
        }

    if action == "enable":

        result = await set_link_active(
            uid,
            True,
        )

        if result is None:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        return {
            "ok": True,
            "action": "enable",
        }

    if action == "disable":

        result = await set_link_active(
            uid,
            False,
        )

        if result is None:
            raise HTTPException(
                status_code=404,
                detail="link not found",
            )

        return {
            "ok": True,
            "action": "disable",
        }

    raise HTTPException(
        status_code=400,
        detail="unknown action",
    )


# ============================================================
# DELETE LINK
# ============================================================

@app.delete("/api/links/{uid}")
async def delete_link(
    uid: str,
    _=Depends(require_auth),
):

    label = await remove_link(uid)

    if label is None:
        raise HTTPException(
            status_code=404,
            detail="link not found",
        )

    return {
        "ok": True,
        "deleted": uid,
    }




def subscription_metadata_headers(used_bytes: int, limit_bytes: int, expires_at, host: str, info_url: str, title: str):
    """Standard subscription headers understood by v2rayNG/v2rayN/Hiddify and similar clients."""
    used_bytes = max(0, int(used_bytes or 0))
    limit_bytes = max(0, int(limit_bytes or 0))

    expire_unix = 0
    if expires_at:
        try:
            dt = datetime.fromisoformat(str(expires_at))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=IRAN_TZ) if IRAN_TZ else dt
            expire_unix = max(0, int(dt.timestamp()))
        except Exception:
            expire_unix = 0

    userinfo = f"upload=0; download={used_bytes}; total={limit_bytes}; expire={expire_unix}"

    return {
        "profile-title": quote(title, safe=""),
        "profile-web-page-url": info_url,
        "support-url": get_support_url(),
        "profile-update-interval": "12",
        "subscription-userinfo": userinfo,
        "content-disposition": 'inline; filename="subscription.txt"',
    }

# ============================================================
# ONE SUBSCRIPTION LINK FOR BOTH APPS AND BROWSERS
# ============================================================
# VPN clients (v2rayNG, v2rayN, Hiddify, Clash, sing-box, ...) send a
# non-browser User-Agent and never ask for text/html, so they keep getting
# the raw base64 config feed below exactly as before. A normal visit from a
# desktop/mobile browser is redirected to the rich HTML portal instead — the
# customer only ever needs to hand out a single /sub/{uuid} link, whether
# it's pasted into an app or opened by hand to check usage.
_SUB_CLIENT_UA_HINTS = (
    "v2ray", "v2rayng", "v2rayn", "hiddify", "clash", "sing-box", "sing_box",
    "shadowrocket", "streisand", "nekobox", "nekoray", "karing", "matsuri",
    "kitsunebi", "quantumult", "surge", "loon", "stash", "husi", "foxray",
    "v2box", "happ", "flclash", "mihomo", "openclash", "passwall",
    "npvtunnel", "netch", "qv2ray", "leaf", "outline", "throne", "exclave",
    "okhttp", "curl", "wget", "python", "go-http", "libcurl",
)
_SUB_BROWSER_UA_HINTS = ("mozilla", "chrome", "safari", "firefox", "edg/", "opr/", "webkit", "gecko")

_SUB_APP_ONLY_HEADERS = ("x-hwid", "x-device-os", "x-ver-os", "x-device-model", "hwid", "x-app-version")
_SUB_NO_STORE = {
    "Cache-Control": "no-store, max-age=0",
    "Vary": "User-Agent, Accept, Sec-Fetch-Mode, Sec-Fetch-Dest",
}

def _subscription_wants_browser_view(request: Request) -> bool:
    """True فقط وقتی یک انسان واقعاً لینک را در مرورگر باز کرده باشد.

    ‏اپ‌های VPN هرگز هدرهای Sec-Fetch-* نمی‌فرستند، ولی هر مرورگر مدرن روی باز کردن
    یک صفحه (navigate) آن‌ها را می‌فرستد؛ پس این تشخیص برخلاف حدس‌زدن با User-Agent
    اپ را هیچ‌وقت به صفحه‌ی HTML نمی‌برد. برای حالت‌های خاص:
      ?raw=1  ->  همیشه کانفیگ خام (دکمه‌ی «دریافت کانفیگ»)
      ?web=1  ->  همیشه صفحه‌ی گرافیکی
    """
    qp = request.query_params
    if str(qp.get("raw", "")).lower() in ("1", "true", "yes"):
        return False
    if str(qp.get("web", "")).lower() in ("1", "true", "yes"):
        return True
    h = request.headers
    ua = (h.get("user-agent") or "").lower()
    if not ua or any(x in ua for x in _SUB_CLIENT_UA_HINTS):
        return False
    if any(h.get(k) for k in _SUB_APP_ONLY_HEADERS):
        return False
    accept = (h.get("accept") or "").lower()
    if "text/html" not in accept:
        return False
    mode = (h.get("sec-fetch-mode") or "").lower()
    dest = (h.get("sec-fetch-dest") or "").lower()
    if mode or dest:
        return mode == "navigate" and dest in ("document", "")
    # مرورگرهای خیلی قدیمی بدون Sec-Fetch: فقط اگر کاملاً شبیه مرورگر باشند
    return ua.startswith("mozilla/") and "application/xhtml+xml" in accept

# ============================================================
# SINGLE SUB
# ============================================================

@app.get("/sub/{uuid}")
async def subscription_single(
    uuid: str,
    request: Request,
):

    async with LINKS_LOCK:
        link = LINKS.get(uuid)

    if not is_link_allowed(link):
        raise HTTPException(
            status_code=404,
            detail="not found or inactive",
        )

    if _subscription_wants_browser_view(request):
        return RedirectResponse(url=f"/subscription/{uuid}", status_code=307, headers=_SUB_NO_STORE)

    host = get_host(request)
    clean_ips = link.get("clean_ips") or []
    used = int(link.get("used_bytes", 0) or 0)
    limit = int(link.get("limit_bytes", 0) or 0)
    expires_at = link.get("expires_at")
    stats_remark = build_info_server_remark(used, limit, expires_at)
    lines = []
    if bool(CONFIG.get("sub_info_line_enabled", True)):
        # ردیف اطلاعات (حجم/زمان باقی‌مانده): یک کانفیگ واقعی و قابل‌اتصال با آدرس واقعی پنل
        lines.append(vless_link_for_link({**link, "label": stats_remark}, uuid, (clean_ips[0] if clean_ips else host)))
    used_names = set()
    cfg_count = max(1, min(40, int(link.get("config_count") or 1)))
    if clean_ips:
        hosts = list(clean_ips)
        while len(hosts) < cfg_count:
            hosts.extend(clean_ips)
        hosts = hosts[:cfg_count]
        for cip in hosts:
            name = random_config_name(used_names)
            used_names.add(name)
            lines.append(vless_link_for_link({**link, "label": name}, uuid, cip))
    else:
        for i in range(cfg_count):
            name = random_config_name(used_names)
            used_names.add(name)
            lines.append(vless_link_for_link({**link, "label": name}, uuid, host))
    content = base64.b64encode("\n".join(lines).encode()).decode()
    profile_title = stats_remark
    headers = subscription_metadata_headers(
        used,
        limit,
        link.get("expires_at"),
        host,
        f"{get_scheme()}://{host}/info/{uuid}",
        profile_title,
    )
    headers.update(_SUB_NO_STORE)

    return Response(
        content=content,
        media_type="text/plain; charset=utf-8",
        headers=headers,
    )

# ============================================================
# LIVE SUBSCRIPTION TELEMETRY
# ============================================================

@app.get("/api/subscription/{uuid}")
async def subscription_telemetry(uuid: str):
    async with LINKS_LOCK:
        link = LINKS.get(uuid)
        if not link or not is_link_allowed(link):
            raise HTTPException(status_code=404, detail="subscription not found or inactive")
        used = int(link.get("used_bytes", 0) or 0)
        limit = int(link.get("limit_bytes", 0) or 0)
        history = list(link.get("usage_history") or [])[-144:]
        if not history:
            history = [{"ts": now_ir().replace(second=0, microsecond=0).isoformat(), "used": used, "limit": limit}]
        active_ips = sorted({str(x.get("ip") or "").strip() for x in connections.values() if x.get("uuid") == uuid and str(x.get("ip") or "").strip()})
        active_sessions = sum(1 for x in connections.values() if x.get("uuid") == uuid)
        return {
            "ok": True, "uuid": uuid, "active": bool(link.get("active", True)),
            "traffic_used": used, "traffic_limit": limit,
            "traffic_remaining": max(0, limit - used) if limit else None,
            "traffic_percent": min(100, round((used / limit) * 100, 1)) if limit else 0,
            "active_connections": len(active_ips), "active_sessions": active_sessions,
            "active_ips": active_ips,
            "connection_limit": int(link.get("connection_limit", 0) or 0),
            "ip_limit": int(link.get("ip_limit", 0) or 0),
            "updated_at": now_ir().isoformat(), "usage_history": history,
        }

# ============================================================
# SMART SUBSCRIPTION PORTAL
# ============================================================

@app.get("/subscription/{uuid}", response_class=HTMLResponse)
async def subscription_portal(uuid: str, request: Request):
    '''Premium customer subscription portal. All figures come from live backend state.'''
    async with LINKS_LOCK:
        link = LINKS.get(uuid)
        if link:
            link = dict(link)
    if not is_link_allowed(link):
        raise HTTPException(status_code=404, detail="subscription not found or inactive")

    host = get_host(request)
    raw_url = f"{get_scheme()}://{host}/sub/{uuid}"
    info_url = f"{get_scheme()}://{host}/info/{uuid}"
    label = str(link.get("label") or "VodiWalker Subscription")
    protocol = protocol_display_label(link)
    used = int(link.get("used_bytes", 0) or 0)
    limit = int(link.get("limit_bytes", 0) or 0)
    pct = min(100, round((used / limit) * 100, 1)) if limit else 0
    remaining = max(0, limit - used) if limit else None
    expires = str(link.get("expires_at") or "نامحدود")
    conn_limit = int(link.get("connection_limit", 0) or 0)
    ip_limit = int(link.get("ip_limit", 0) or 0)
    active = bool(link.get("active", True))
    active_ips = {str(x.get("ip") or "").strip() for x in connections.values()
                  if x.get("uuid") == uuid and str(x.get("ip") or "").strip()}
    active_people = len(active_ips)
    active_sessions = sum(1 for x in connections.values() if x.get("uuid") == uuid)
    qr = quote(raw_url, safe="")
    initial = (label.strip()[:1] or "V").upper()

    html = r'''<!doctype html><html lang="fa" dir="rtl"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070a12"><title>__LABEL__ · VodiWalker</title>
<link rel="stylesheet" href="/assets/ui.css"><script src="/assets/qr.js"></script>
<style>
:root{--bg:#070a12;--bg2:#0a0e19;--card:#0c111b;--card2:#101725;--line:rgba(255,255,255,.08);--text:#f8fafc;--muted:#8792a6;--soft:#59657a;--a:#8b5cf6;--a2:#6366f1;--c:#22d3ee;--g:#22c55e;--g2:#16a34a;--w:#f59e0b;--r:#ef4444;--shadow:0 24px 80px rgba(0,0,0,.35);--grid:rgba(255,255,255,.055);--url:#080c14;--radius:26px}
body[data-theme="light"]{--bg:#f4f7fb;--bg2:#eef2f8;--card:#ffffff;--card2:#f7f9fc;--line:rgba(15,23,42,.10);--text:#0f172a;--muted:#526176;--soft:#748197;--shadow:0 20px 60px rgba(15,23,42,.10);--grid:rgba(15,23,42,.08);--url:#eef2f7}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;min-height:100vh;background:
    radial-gradient(circle at 12% 0%,rgba(139,92,246,.20),transparent 32%),
    radial-gradient(circle at 100% 18%,rgba(34,211,238,.12),transparent 30%),
    radial-gradient(circle at 30% 100%,rgba(34,197,94,.08),transparent 28%),
    var(--bg);
  color:var(--text);font-family:Vazirmatn,Tahoma,sans-serif;overflow-x:hidden;transition:background .25s,color .25s}
a{text-decoration:none;color:inherit}
button{font-family:inherit;cursor:pointer}
.wrap{width:min(760px,calc(100% - 28px));margin:auto;padding:22px 0 118px}

/* TOP BAR */
.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;gap:10px}
.brand{display:flex;gap:11px;align-items:center;min-width:0}
.logo{width:44px;height:44px;flex:none;border-radius:15px;display:grid;place-items:center;background:linear-gradient(135deg,#1c1533,#101b2d);border:1px solid rgba(139,92,246,.4);font-weight:900;font-size:19px;box-shadow:0 0 22px rgba(139,92,246,.22)}
.brand b{display:block;font-size:14px}
.brand small{display:block;color:var(--soft);font-size:9px;margin-top:2px;letter-spacing:.06em}
.top-actions{display:flex;align-items:center;gap:8px}
.theme-btn,.lang-btn{border:1px solid var(--line);background:var(--card);color:var(--text);border-radius:12px;padding:9px 11px;display:flex;align-items:center;gap:6px;font-size:9px;font-weight:800;transition:.2s}
.theme-btn:hover,.lang-btn:hover{transform:translateY(-1px);border-color:rgba(139,92,246,.35)}
.theme-btn i{font-size:15px;color:var(--a)}
.live{display:flex;align-items:center;gap:7px;color:var(--g);font-size:9px;font-weight:800;padding:9px 12px;border:1px solid rgba(34,197,94,.2);background:rgba(34,197,94,.08);border-radius:999px;white-space:nowrap}
.live.off{color:var(--r);border-color:rgba(239,68,68,.22);background:rgba(239,68,68,.08)}
.dot{width:7px;height:7px;border-radius:50%;background:currentColor;box-shadow:0 0 12px currentColor;animation:pulse 1.8s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}

/* IDENTITY CARD */
.identity{position:relative;overflow:hidden;border:1px solid var(--line);background:linear-gradient(150deg,rgba(20,16,34,.98),rgba(10,12,20,.98));border-radius:var(--radius);padding:24px;box-shadow:var(--shadow);margin-bottom:14px;display:grid;grid-template-columns:76px 1fr auto;gap:16px;align-items:center}
body[data-theme="light"] .identity{background:linear-gradient(150deg,#ffffff,#f7f9fc)}
.identity:after{content:"";position:absolute;width:320px;height:320px;left:-140px;top:-200px;background:radial-gradient(circle,rgba(139,92,246,.24),transparent 68%);pointer-events:none}
.avatar{position:relative;width:76px;height:76px;border-radius:50%;display:grid;place-items:center;font:900 30px Arial,sans-serif;color:#fff;background:radial-gradient(circle at 38% 32%,#3a2a63,#12101c);border:2px solid rgba(139,92,246,.55);box-shadow:0 0 26px rgba(139,92,246,.35)}
.identity h1{margin:0 0 6px;font-size:clamp(18px,4vw,23px);letter-spacing:-.02em;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.identity .chips{display:flex;flex-wrap:wrap;gap:6px}
.chip{padding:6px 10px;border:1px solid var(--line);background:rgba(255,255,255,.03);border-radius:9px;color:var(--muted);font-size:9px}
.chip b{color:var(--text)}
.chip.status-on{color:var(--g);border-color:rgba(34,197,94,.25);background:rgba(34,197,94,.08)}
.chip.status-off{color:var(--r);border-color:rgba(239,68,68,.25);background:rgba(239,68,68,.08)}
.jump-btn{justify-self:end;align-self:center;border:1px solid rgba(139,92,246,.4);background:linear-gradient(135deg,var(--a),var(--a2));color:#fff;border-radius:14px;padding:12px 16px;font-weight:800;font-size:11px;display:flex;align-items:center;gap:6px;white-space:nowrap}

/* GRID */
.grid{display:grid;grid-template-columns:1.15fr .85fr;gap:14px}
.card{border:1px solid var(--line);background:rgba(12,17,27,.96);border-radius:24px;box-shadow:var(--shadow);overflow:hidden}
body[data-theme="light"] .card{background:var(--card)}
.head{padding:17px 19px;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;gap:12px;align-items:center}
.head b{font-size:13px}
.head small{display:block;color:var(--soft);font-size:8px;margin-top:3px}
.body{padding:19px}

.usage{display:grid;grid-template-columns:190px 1fr;gap:18px;align-items:center}
.gauge{width:172px;height:172px;margin:auto;border-radius:50%;background:conic-gradient(var(--a) calc(var(--pct)*1%),rgba(255,255,255,.07) 0);position:relative;display:grid;place-items:center;box-shadow:0 0 55px rgba(139,92,246,.12)}
.gauge:before{content:"";position:absolute;inset:13px;border-radius:50%;background:var(--card);border:1px solid var(--line)}
body[data-theme="light"] .gauge:before{background:var(--card)}
.gauge-center{position:relative;text-align:center}
.gauge-center b{font-size:32px;letter-spacing:-.06em}
.gauge-center small{display:block;color:var(--soft);font-size:9px;margin-top:2px}
.metrics{display:grid;grid-template-columns:1fr 1fr;gap:9px}
.metric{padding:13px;border:1px solid var(--line);border-radius:15px;background:var(--card2)}
.metric small{display:block;color:var(--soft);font-size:8px}
.metric b{display:block;margin-top:6px;font-size:15px;direction:ltr;text-align:right}
.metric .ok{color:var(--g)}
.metric .cyan{color:var(--c)}
.meter{margin-top:12px;height:9px;border-radius:99px;background:rgba(255,255,255,.07);overflow:hidden}
.meter i{display:block;height:100%;width:calc(var(--pct)*1%);border-radius:inherit;background:linear-gradient(90deg,var(--a),var(--c));transition:width .5s ease}
.actions{display:flex;gap:8px;margin-top:14px;flex-wrap:wrap}
.btn{flex:1;min-width:120px;border:1px solid var(--line);border-radius:12px;padding:12px 13px;background:var(--card2);color:var(--text);font-family:inherit;font-weight:800;font-size:10.5px;text-align:center;display:flex;align-items:center;justify-content:center;gap:6px;transition:.15s}
.btn:hover{border-color:rgba(139,92,246,.4)}
.btn.primary{background:linear-gradient(135deg,var(--a),var(--a2));border-color:transparent;color:#fff}
.url{direction:ltr;text-align:left;word-break:break-all;padding:13px;border:1px dashed var(--line);border-radius:12px;background:var(--url);color:#9ca9bd;font:9.5px/1.6 monospace}

.livebox{display:flex;align-items:center;justify-content:space-between;padding:14px;border:1px solid rgba(34,197,94,.18);background:rgba(34,197,94,.055);border-radius:15px;margin-bottom:10px}
.livebox b{font-size:24px;color:var(--g)}
.livebox small{display:block;color:var(--soft);font-size:8px}
.session{color:var(--muted);font-size:9px}
.status{display:inline-flex;padding:6px 9px;border-radius:9px;background:rgba(34,197,94,.10);color:var(--g);font-size:8px;font-weight:800}
.status.off{background:rgba(239,68,68,.1);color:var(--r)}

.expire-card{display:grid;grid-template-columns:56px 1fr auto;gap:14px;align-items:center;padding:19px}
.expire-icon{width:56px;height:56px;border-radius:16px;display:grid;place-items:center;font-size:24px;background:radial-gradient(circle at 35% 35%,rgba(245,158,11,.28),rgba(20,16,10,.2));border:1px solid rgba(245,158,11,.3);color:var(--w)}
.expire-info span{display:block;color:var(--soft);font-size:9px}
.expire-info strong{display:block;margin-top:5px;font-size:16px;direction:ltr;text-align:right}
.shield-mini{width:46px;height:46px;border-radius:50%;display:grid;place-items:center;font-size:20px;color:var(--g);background:radial-gradient(circle,rgba(34,197,94,.18),transparent 70%);border:1px solid rgba(34,197,94,.3)}

.chart{height:180px;position:relative}
.chart svg{width:100%;height:100%;overflow:visible}
.chart .line{fill:none;stroke:var(--c);stroke-width:3;stroke-linecap:round;stroke-linejoin:round;filter:drop-shadow(0 4px 8px rgba(34,211,238,.18))}
.chart .area{fill:url(#area)}
.chart .gridline{stroke:var(--grid);stroke-width:1}
.chart .point{fill:var(--card);stroke:var(--c);stroke-width:2}
.chart text{fill:var(--soft);font-size:8px}
.chart .last{fill:var(--c);stroke:var(--card);stroke-width:3}

.qr-wrap{display:none;place-items:center;margin-bottom:13px}
.qr-wrap.show{display:grid}
.qr-wrap img{width:170px;height:170px;padding:8px;background:#fff;border-radius:16px}

/* APP QUICK CONNECT */
.apps{display:grid;gap:10px;margin-top:4px}
.app-row{display:grid;grid-template-columns:52px 1fr auto;gap:12px;align-items:center;padding:13px 14px;border-radius:18px;background:var(--card2);border:1px solid var(--line)}
.app-icon{width:52px;height:52px;border-radius:15px;display:grid;place-items:center;font-size:23px;background:radial-gradient(circle at 35% 35%,rgba(139,92,246,.3),rgba(20,16,34,.2));border:1px solid rgba(139,92,246,.3)}
.app-name{font-weight:800;font-size:13px}
.app-tag{display:inline-block;margin-top:3px;padding:2px 7px;border-radius:7px;background:rgba(34,197,94,.12);color:var(--g);font-size:8px;font-weight:800}
.app-tag.ios{background:rgba(245,158,11,.14);color:var(--w)}
.app-go{border:1px solid rgba(139,92,246,.35);background:rgba(139,92,246,.1);color:var(--a);border-radius:11px;padding:9px 13px;font-size:10px;font-weight:800;white-space:nowrap}

.note{padding:12px;border-radius:13px;background:rgba(255,255,255,.025);color:var(--muted);font-size:9px;line-height:2;margin-top:10px}
.footer{text-align:center;color:var(--soft);font-size:8px;margin-top:18px}

/* BOTTOM NAV (mobile) */
.bottom-nav{display:none}
@media(max-width:760px){
  .bottom-nav{
    display:grid;position:fixed;z-index:30;left:50%;bottom:12px;transform:translateX(-50%);
    width:min(420px,calc(100% - 24px));grid-template-columns:repeat(3,1fr);align-items:center;
    padding:7px;border-radius:26px;background:rgba(12,17,27,.94);border:1px solid var(--line);
    box-shadow:0 15px 35px rgba(0,0,0,.45);backdrop-filter:blur(18px)
  }
  body[data-theme="light"] .bottom-nav{background:rgba(255,255,255,.94)}
  .bottom-nav button{height:52px;border:0;background:transparent;color:var(--text);font-size:19px;border-radius:19px;display:grid;place-items:center;gap:2px}
  .bottom-nav button small{font-size:8px;font-weight:800;color:var(--soft)}
  .bottom-nav button.active{background:linear-gradient(135deg,rgba(139,92,246,.22),rgba(34,211,238,.14));color:var(--a)}
  .bottom-nav button.active small{color:var(--a)}
}
.toast{position:fixed;top:16px;left:50%;z-index:100;transform:translate(-50%,-120px);padding:12px 18px;border-radius:14px;background:rgba(34,197,94,.14);color:var(--g);border:1px solid rgba(34,197,94,.35);transition:.3s;font-size:11px;font-weight:800;backdrop-filter:blur(10px)}
.toast.show{transform:translate(-50%,0)}

@media(max-width:800px){.grid,.usage{grid-template-columns:1fr}.gauge{width:170px;height:170px}.metrics{grid-template-columns:1fr 1fr}.identity{padding:20px}}
@media(max-width:500px){.metrics{grid-template-columns:1fr}.wrap{width:min(100% - 18px,1120px);padding-top:14px}.identity{border-radius:21px;grid-template-columns:60px 1fr;row-gap:12px}.identity h1{font-size:17px}.jump-btn{grid-column:1/-1}.card{border-radius:20px}.expire-card{grid-template-columns:44px 1fr}.shield-mini{display:none}}

/* ===== VW PRO: glow control + performance layer ===== */
:root{--glow:1;--acc:139,92,246;--a:rgb(var(--acc))}
body{background:
 radial-gradient(circle at 12% 0%,rgba(var(--acc),calc(.20*var(--glow))),transparent 32%),
 radial-gradient(circle at 100% 18%,rgba(34,211,238,calc(.12*var(--glow))),transparent 30%),
 radial-gradient(circle at 30% 100%,rgba(34,197,94,calc(.08*var(--glow))),transparent 28%),var(--bg)}
.identity:after{background:radial-gradient(circle,rgba(var(--acc),calc(.24*var(--glow))),transparent 68%)}
.avatar{border-color:rgba(var(--acc),.55);box-shadow:0 0 calc(26px*var(--glow)) rgba(var(--acc),calc(.35*var(--glow)))}
.logo{border-color:rgba(var(--acc),.45);box-shadow:0 0 calc(24px*var(--glow)) rgba(var(--acc),calc(.28*var(--glow)))}
.dot{box-shadow:0 0 calc(12px*var(--glow)) currentColor}
.bottom-nav button.active{background:linear-gradient(135deg,rgba(var(--acc),.22),rgba(34,211,238,.14))}
.bottom-nav{-webkit-backdrop-filter:none!important;backdrop-filter:none!important}
.theme-btn:hover,.lang-btn:hover{transform:none}
.card,.identity{contain:layout style}
.card{content-visibility:auto;contain-intrinsic-size:auto 240px}
html.paused *{animation-play-state:paused!important}
html.fx-off *,html.fx-off *:before,html.fx-off *:after{animation:none!important;transition:none!important}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
.fx-panel{position:fixed;z-index:60;top:68px;inset-inline-end:14px;width:min(290px,calc(100% - 28px));padding:16px;border-radius:18px;
 background:var(--card);border:1px solid var(--line);box-shadow:var(--shadow);opacity:0;visibility:hidden;transform:translateY(-6px);transition:opacity .18s,transform .18s,visibility .18s}
.fx-panel.open{opacity:1;visibility:visible;transform:none}
.fx-panel h4{margin:0 0 12px;font-size:12px;display:flex;justify-content:space-between;align-items:center}
.fx-panel h4 b{color:var(--a);font-size:12px;direction:ltr}
.fx-panel label{display:block;font-size:10px;color:var(--muted);margin:12px 0 7px;font-weight:800}
.fx-range{-webkit-appearance:none;appearance:none;width:100%;height:6px;border-radius:99px;outline:0;direction:ltr;
 background:linear-gradient(90deg,var(--a) var(--p,66%),var(--line) var(--p,66%))}
.fx-range::-webkit-slider-thumb{-webkit-appearance:none;width:20px;height:20px;border-radius:50%;background:#fff;border:3px solid var(--a);box-shadow:0 2px 8px rgba(0,0,0,.35);cursor:pointer}
.fx-range::-moz-range-thumb{width:16px;height:16px;border-radius:50%;background:#fff;border:3px solid var(--a);cursor:pointer}
.fx-sw{display:flex;gap:8px;flex-wrap:wrap}
.fx-sw button{width:28px;height:28px;border-radius:50%;border:2px solid transparent;padding:0;outline:0}
.fx-sw button.on{border-color:var(--text);box-shadow:0 0 0 2px var(--card) inset}
.fx-row{display:flex;justify-content:space-between;align-items:center;font-size:11px;margin-top:14px;font-weight:700}
.fx-tg{width:40px;height:22px;border-radius:99px;border:1px solid var(--line);background:var(--line);position:relative;padding:0;transition:.2s}
.fx-tg:after{content:"";position:absolute;top:2px;inset-inline-start:2px;width:16px;height:16px;border-radius:50%;background:#fff;transition:.2s}
.fx-tg.on{background:var(--a);border-color:transparent}.fx-tg.on:after{inset-inline-start:20px}
.fx-reset{margin-top:14px;width:100%;padding:9px;border-radius:11px;border:1px solid var(--line);background:transparent;color:var(--muted);font-size:10px;font-weight:800}
</style></head><body><script>(function(){var d=document.documentElement;try{var g=localStorage.getItem('vw_sub_glow');if(g!==null)d.style.setProperty('--glow',Math.max(0,Math.min(1.5,g/100)));var a=localStorage.getItem('vw_sub_acc');if(a)d.style.setProperty('--acc',a);if(localStorage.getItem('vw_sub_fx')==='0')d.classList.add('fx-off')}catch(e){}})();</script><div class="fx-panel" id="fxPanel" onclick="event.stopPropagation()">
  <h4><span>تنظیمات نور و ظاهر</span><b id="fxVal">100%</b></h4>
  <label>شدت نور (Glow)</label>
  <input class="fx-range" id="fxRange" type="range" min="0" max="150" step="5" value="100" oninput="fxGlow(this.value)" onchange="fxSave()">
  <label>رنگ تاکیدی</label>
  <div class="fx-sw" id="fxSw"></div>
  <div class="fx-row"><span>انیمیشن‌ها</span><button class="fx-tg on" id="fxAnim" type="button" onclick="fxAnimToggle()" aria-label="انیمیشن"></button></div>
  <button class="fx-reset" type="button" onclick="fxReset()">بازنشانی</button>
</div>
<main class="wrap">

<header class="top">
  <div class="brand"><div class="logo">V</div><div><b>VodiWalker</b><small>SUBSCRIPTION CENTER</small></div></div>
  <div class="top-actions">
    <button class="theme-btn" id="fxBtn" type="button" onclick="fxToggle(event)" aria-label="تنظیم نور و ظاهر"><i class="ti ti-adjustments-horizontal"></i><span>نور</span></button>
    <button class="theme-btn" id="themeBtn" type="button" onclick="toggleTheme()" aria-label="تغییر حالت نمایش"><i class="ti ti-sun-moon"></i><span id="themeLabel">روشن</span></button>
    <div class="live" id="liveBadge"><i class="dot"></i><span id="liveState">سرویس آنلاین</span></div>
  </div>
</header>

<section class="identity">
  <div class="avatar">__INITIAL__</div>
  <div>
    <h1>__LABEL__</h1>
    <div class="chips">
      <span class="chip">پروتکل <b>__PROTOCOL__</b></span>
      <span class="chip" id="statusChip">وضعیت <b id="heroStatus">__STATUS__</b></span>
      <span class="chip">انقضا <b id="expires">__EXPIRES__</b></span>
    </div>
  </div>
  <a class="jump-btn" href="#configs"><i class="ti ti-apps"></i> کانفیگ‌ها</a>
</section>

<section class="grid">
  <div class="card">
    <div class="head"><div><b>مصرف اشتراک</b><small>نمایش مصرف واقعی ثبت‌شده روی سرویس</small></div><span id="updated" style="color:var(--soft);font-size:8px">—</span></div>
    <div class="body">
      <div class="usage">
        <div class="gauge" id="gauge" style="--pct:__PCT__"><div class="gauge-center"><b id="pct">__PCT__%</b><small>مصرف شده</small></div></div>
        <div>
          <div class="metrics">
            <div class="metric"><small>مصرف شده</small><b class="cyan" id="used">__USED__</b></div>
            <div class="metric"><small>باقی‌مانده</small><b class="ok" id="remaining">__REMAINING__</b></div>
            <div class="metric"><small>سقف اشتراک</small><b id="limit">__LIMIT__</b></div>
            <div class="metric"><small>درصد مصرف</small><b id="summaryPct">__PCT__%</b></div>
          </div>
          <div class="meter"><i id="meter"></i></div>
          <div class="note">عدد مصرف از شمارنده واقعی سرویس خوانده می‌شود؛ با هر بار افزایش ترافیک، مقدار و نمودار به‌روزرسانی می‌شوند.</div>
        </div>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="head"><div><b>اتصال‌های فعال</b><small>کاربران آنلاین همین لحظه</small></div><span class="status" id="statusBadge">فعال</span></div>
    <div class="body">
      <div class="livebox">
        <div><b id="liveConnections">__ACTIVE_CONN__</b><small>دستگاه / IP یکتا</small></div>
        <div style="text-align:left"><span class="session" id="sessions">__ACTIVE_SESSIONS__ session</span><br><span class="session" id="connectionLimit">__CONN_LIMIT__</span></div>
      </div>
      <div class="metric"><small>محدودیت IP</small><b id="ipLimit">__IP_LIMIT__</b></div>
      <div class="note">برای جلوگیری از نمایش عدد غیرواقعی، یک IP فقط یک کاربر فعال محسوب می‌شود؛ Sessionهای فنی جداگانه نمایش داده می‌شود.</div>
      <div class="actions">
        <button class="btn primary" onclick="copyLink()"><i class="ti ti-copy"></i> کپی لینک اشتراک</button>
        <a class="btn" href="__INFO_URL__"><i class="ti ti-info-circle"></i> اطلاعات سرویس</a>
      </div>
    </div>
  </div>
</section>

<section class="card" style="margin-top:14px">
  <div class="head"><div><b>روند مصرف</b><small>تغییرات ثبت‌شده مصرف اشتراک</small></div><span id="chartState" style="color:var(--soft);font-size:8px">در حال همگام‌سازی</span></div>
  <div class="body"><div class="chart" id="chart"><svg viewBox="0 0 900 190" preserveAspectRatio="none"><defs><linearGradient id="area" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#22d3ee" stop-opacity=".24"/><stop offset="1" stop-color="#22d3ee" stop-opacity="0"/></linearGradient></defs><g id="gridLines"></g><path id="areaPath" class="area"></path><path id="linePath" class="line"></path><g id="chartPoints"></g><text x="895" y="184" text-anchor="end">زمان</text></svg></div></div>
</section>

<section class="card" style="margin-top:14px">
  <div class="expire-card">
    <div class="expire-icon"><i class="ti ti-calendar-due"></i></div>
    <div class="expire-info"><span>انقضای اشتراک</span><strong id="expireStrong">__EXPIRES__</strong></div>
    <div class="shield-mini"><i class="ti ti-shield-check"></i></div>
  </div>
</section>

<section class="card" style="margin-top:14px"><div class="body" style="display:flex;align-items:center;gap:12px;justify-content:space-between;flex-wrap:wrap"><div><b>💬 نیاز به کمک داری؟</b><small style="display:block;color:var(--muted);margin-top:2px">پیام مستقیم به پشتیبان <bdi dir="ltr">__SUPPORT__</bdi></small></div><a class="btn primary" href="__SUPPORT_URL__" target="_blank" rel="noopener"><i class="ti ti-brand-telegram"></i> پشتیبانی تلگرام</a></div></section>
<section class="card" style="margin-top:14px" id="linkCard">
  <div class="head"><div><b>لینک اصلی اشتراک</b><small>برای وارد کردن در کلاینت سازگار</small></div>
    <button class="btn" style="flex:none;padding:8px 12px" onclick="toggleQr()"><i class="ti ti-qrcode"></i> QR</button>
  </div>
  <div class="body">
    <div class="qr-wrap" id="qrWrap"><img id="qrMainImg" alt="QR" width="220" height="220"></div>
    <div class="url" id="subUrl">__RAW__</div>
    <div class="actions">
      <button class="btn primary" onclick="copyLink()"><i class="ti ti-copy"></i> کپی لینک</button>
      <a class="btn" href="__RAW_URL__?raw=1" target="_blank" rel="noopener"><i class="ti ti-external-link"></i> باز کردن لینک</a>
    </div>
  </div>
</section>

<section class="card" style="margin-top:14px" id="configs">
  <div class="head"><div><b>اتصال سریع</b><small>باز کردن مستقیم در برنامه کلاینت</small></div></div>
  <div class="body">
    <div class="apps">
      <div class="app-row">
        <div class="app-icon"><i class="ti ti-brand-android"></i></div>
        <div><div class="app-name">v2rayNG</div><span class="app-tag">اندروید</span></div>
        <button class="app-go" onclick="quickConnect('v2rayng://install-config?url='+encodeURIComponent(SUB_URL))">اتصال</button>
      </div>
      <div class="app-row">
        <div class="app-icon"><i class="ti ti-shield-bolt"></i></div>
        <div><div class="app-name">Hiddify</div><span class="app-tag">اندروید / iOS / ویندوز</span></div>
        <button class="app-go" onclick="quickConnect('hiddify://import/'+encodeURIComponent(SUB_URL))">اتصال</button>
      </div>
      <div class="app-row">
        <div class="app-icon"><i class="ti ti-brand-apple"></i></div>
        <div><div class="app-name">Streisand</div><span class="app-tag ios">iOS</span></div>
        <button class="app-go" onclick="quickConnect('streisand://import/'+encodeURIComponent(SUB_URL))">اتصال</button>
      </div>
      <div class="app-row">
        <div class="app-icon"><i class="ti ti-device-laptop"></i></div>
        <div><div class="app-name">NekoBox</div><span class="app-tag">دسکتاپ</span></div>
        <button class="app-go" onclick="copyLink()">کپی لینک</button>
      </div>
    </div>
    <div class="note">در صورتی که برنامه به‌صورت خودکار باز نشد، برنامه را نصب کرده و لینک کپی‌شده را به‌صورت دستی وارد کنید.</div>
  </div>
</section>

<div class="footer">VodiWalker · وضعیت و مصرف به‌صورت زنده از سرویس خوانده می‌شود</div>
</main>

<nav class="bottom-nav">
  <button class="active" onclick="copyLink()"><i class="ti ti-link"></i><small>کپی لینک</small></button>
  <button onclick="document.getElementById('configs').scrollIntoView({behavior:'smooth'})"><i class="ti ti-apps"></i><small>کانفیگ‌ها</small></button>
  <button onclick="toggleTheme()"><i class="ti ti-sun-moon"></i><small>تم</small></button>
</nav>

<div class="toast" id="toast">کپی شد ✓</div>

<script>
const SUB_URL=__RAW_JS__;
function fmt(n){n=Number(n)||0;if(!n)return'0 B';const u=['B','KB','MB','GB','TB'];let i=0;while(n>=1024&&i<u.length-1){n/=1024;i++}return(n>=100?Math.round(n):n>=10?n.toFixed(1):n.toFixed(2))+' '+u[i]}

function showToast(text){
  const toast=document.getElementById('toast');
  toast.textContent=text;
  toast.classList.add('show');
  clearTimeout(window.toastTimer);
  window.toastTimer=setTimeout(()=>toast.classList.remove('show'),2200);
}

async function copyLink(){
  try{
    await navigator.clipboard.writeText(SUB_URL);
    showToast('لینک اشتراک کپی شد ✓');
  }catch(e){
    try{
      const ta=document.createElement('textarea');
      ta.value=SUB_URL;ta.style.position='fixed';ta.style.opacity='0';
      document.body.appendChild(ta);ta.select();document.execCommand('copy');ta.remove();
      showToast('لینک اشتراک کپی شد ✓');
    }catch(e2){ prompt('لینک اشتراک:',SUB_URL); }
  }
}

function quickConnect(deepLink){
  copyLink();
  window.location.href=deepLink;
}

function toggleQr(){
  const w=document.getElementById('qrWrap');w.classList.toggle('show');
  const im=document.getElementById('qrMainImg');
  if(w.classList.contains('show')&&!im.getAttribute('src')&&window.qrcode){
    try{const q=qrcode(0,'M');q.addData(document.getElementById('subUrl').textContent.trim());q.make();
      im.src='data:image/svg+xml;charset=utf-8,'+encodeURIComponent(q.createSvgTag(5,4));}catch(e){}
  }
}

function applyTheme(){
  const t=localStorage.getItem('vw_sub_theme')||'dark';
  document.body.dataset.theme=t;
  const light=t==='light';
  document.getElementById('themeLabel').textContent=light?'تیره':'روشن';
  document.querySelector('#themeBtn i').className=light?'ti ti-moon':'ti ti-sun-moon';
}
function toggleTheme(){
  const next=(document.body.dataset.theme||'dark')==='dark'?'light':'dark';
  localStorage.setItem('vw_sub_theme',next);
  applyTheme();
}
applyTheme();

/* ===== glow / accent / motion controls ===== */
var FX_ACC={'#8b5cf6':'139,92,246','#3b82f6':'59,130,246','#22d3ee':'34,211,238','#22c55e':'34,197,94','#ec4899':'236,72,153','#f59e0b':'245,158,11'};
var fxRaf=0;
function fxLS(k,v){try{if(v===null)localStorage.removeItem(k);else localStorage.setItem(k,v)}catch(e){}}
function fxGet(k,d){try{var v=localStorage.getItem(k);return v===null?d:v}catch(e){return d}}
function fxGlow(v){
  v=+v;var r=document.getElementById('fxRange');
  cancelAnimationFrame(fxRaf);
  fxRaf=requestAnimationFrame(function(){
    document.documentElement.style.setProperty('--glow',v/100);
    document.getElementById('fxVal').textContent=v+'%';
    r.style.setProperty('--p',(v/150*100)+'%');
  });
}
function fxSave(){fxLS('vw_sub_glow',document.getElementById('fxRange').value)}
function fxAcc(rgb){
  document.documentElement.style.setProperty('--acc',rgb);fxLS('vw_sub_acc',rgb);
  document.querySelectorAll('#fxSw button').forEach(function(b){b.classList.toggle('on',FX_ACC[b.dataset.c]===rgb)});
}
function fxAnimSet(on){
  document.documentElement.classList.toggle('fx-off',!on);
  document.getElementById('fxAnim').classList.toggle('on',on);
  fxLS('vw_sub_fx',on?'1':'0');
}
function fxAnimToggle(){fxAnimSet(document.documentElement.classList.contains('fx-off'))}
function fxToggle(e){if(e)e.stopPropagation();document.getElementById('fxPanel').classList.toggle('open')}
function fxReset(){
  ['vw_sub_glow','vw_sub_acc','vw_sub_fx'].forEach(function(k){fxLS(k,null)});
  var r=document.getElementById('fxRange');r.value=100;fxGlow(100);fxAcc('139,92,246');fxAnimSet(true);
}
(function fxInit(){
  var sw=document.getElementById('fxSw');
  Object.keys(FX_ACC).forEach(function(c){
    var b=document.createElement('button');b.type='button';b.dataset.c=c;b.style.background=c;
    b.setAttribute('aria-label',c);b.onclick=function(){fxAcc(FX_ACC[c])};sw.appendChild(b);
  });
  var g=+fxGet('vw_sub_glow',100);document.getElementById('fxRange').value=g;fxGlow(g);
  fxAcc(fxGet('vw_sub_acc','139,92,246'));
  document.getElementById('fxAnim').classList.toggle('on',fxGet('vw_sub_fx','1')!=='0');
  document.addEventListener('click',function(){document.getElementById('fxPanel').classList.remove('open')});
  document.addEventListener('visibilitychange',function(){document.documentElement.classList.toggle('paused',document.hidden)});
})();

function drawChart(history,limit){
  const line=document.getElementById('linePath'),area=document.getElementById('areaPath'),grid=document.getElementById('gridLines'),points=document.getElementById('chartPoints');
  const clean=Array.isArray(history)?history.filter(x=>Number.isFinite(Number(x.used))).slice(-144):[];
  if(clean.length<2){
    line.setAttribute('d','M 0 158 L 900 158');area.setAttribute('d','M 0 158 L 900 158 L 900 170 L 0 170 Z');grid.innerHTML='';points.innerHTML='';document.getElementById('chartState').textContent='در حال جمع‌آوری داده واقعی';return;
  }
  const vals=clean.map(x=>Number(x.used)||0),max=Math.max(Number(limit)||0,...vals,1),min=0;
  const top=14,bottom=162,height=bottom-top;
  grid.innerHTML=[0,.25,.5,.75,1].map(r=>{const y=bottom-height*r;return `<line class="gridline" x1="0" y1="${y}" x2="900" y2="${y}"></line><text x="0" y="${y-4}">${fmt(vals.length?max*r:0)}</text>`}).join('');
  const pts=vals.map((v,i)=>{const x=i*(900/Math.max(1,vals.length-1));const y=bottom-((v-min)/(max-min))*height;return[x,y]});
  const d=pts.map((p,i)=>(i?'L':'M')+' '+p[0].toFixed(1)+' '+p[1].toFixed(1)).join(' ');
  line.setAttribute('d',d);area.setAttribute('d',d+' L '+pts[pts.length-1][0].toFixed(1)+' '+bottom+' L 0 '+bottom+' Z');
  points.innerHTML=pts.map((p,i)=>{const h=clean[i]?.ts?new Date(clean[i].ts).toLocaleString('fa-IR',{hour:'2-digit',minute:'2-digit'}):'';return `<circle class="point ${i===pts.length-1?'last':''}" cx="${p[0].toFixed(1)}" cy="${p[1].toFixed(1)}" r="${i===pts.length-1?5:2.2}"><title>${h} · ${fmt(vals[i])}</title></circle>`}).join('');
  document.getElementById('chartState').textContent=`${clean.length} نقطه واقعی · آخرین مقدار ${fmt(vals[vals.length-1])}`;
}

async function refresh(){
  try{
    const r=await fetch('/api/subscription/__UUID__',{cache:'no-store'});
    if(!r.ok)return;
    const d=await r.json();
    const lim=Number(d.traffic_limit||0),used=Number(d.traffic_used||0),p=lim?Math.min(100,Math.round(used/lim*1000)/10):0;
    document.getElementById('gauge').style.setProperty('--pct',p);
    document.getElementById('pct').textContent=p+'%';
    document.getElementById('summaryPct').textContent=p+'%';
    document.getElementById('used').textContent=fmt(used);
    document.getElementById('limit').textContent=lim?fmt(lim):'نامحدود';
    document.getElementById('remaining').textContent=lim?fmt(Math.max(0,lim-used)):'نامحدود';
    document.getElementById('meter').style.width=p+'%';
    const active=Number(d.active_connections||0);
    document.getElementById('liveConnections').textContent=active;
    document.getElementById('sessions').textContent=Number(d.active_sessions||0)+' session';
    document.getElementById('connectionLimit').textContent=Number(d.connection_limit||0)?'حداکثر '+d.connection_limit+' اتصال':'بدون محدودیت اتصال';
    document.getElementById('ipLimit').textContent=Number(d.ip_limit||0)?'حداکثر '+d.ip_limit+' IP':'بدون محدودیت';
    const statusText=d.active?'فعال':'غیرفعال';
    document.getElementById('heroStatus').textContent=statusText;
    document.getElementById('liveState').textContent=d.active?'سرویس آنلاین':'سرویس غیرفعال';
    document.getElementById('liveBadge').classList.toggle('off',!d.active);
    document.getElementById('statusBadge').textContent=statusText;
    document.getElementById('statusBadge').classList.toggle('off',!d.active);
    document.getElementById('statusChip').classList.toggle('status-on',!!d.active);
    document.getElementById('statusChip').classList.toggle('status-off',!d.active);
    const now=new Date().toLocaleTimeString('fa-IR',{hour:'2-digit',minute:'2-digit',second:'2-digit'});
    document.getElementById('updated').textContent=now;
    drawChart(d.usage_history,lim);
  }catch(e){
    document.getElementById('chartState').textContent='همگام‌سازی ناموفق';
  }
}
refresh();setInterval(()=>{if(!document.hidden)refresh()},10000);
</script></body></html>'''
    replacements={
      '__LABEL__':escape_html(label),'__PROTOCOL__':escape_html(protocol),'__EXPIRES__':escape_html(expires[:19]),
      '__STATUS__':'فعال' if active else 'غیرفعال','__PCT__':str(pct),'__USED__':escape_html(fmt_bytes(used)),
      '__REMAINING__':escape_html(fmt_bytes(remaining) if remaining is not None else 'نامحدود'),
      '__LIMIT__':escape_html(fmt_bytes(limit) if limit else 'نامحدود'),'__ACTIVE_CONN__':str(active_people),
      '__ACTIVE_SESSIONS__':str(active_sessions),'__CONN_LIMIT__':('حداکثر '+str(conn_limit)+' اتصال') if conn_limit else 'بدون محدودیت اتصال',
      '__IP_LIMIT__':('حداکثر '+str(ip_limit)+' IP') if ip_limit else 'بدون محدودیت','__RAW__':escape_html(raw_url),
      '__RAW_URL__':escape_html(raw_url),'__INFO_URL__':escape_html(info_url),'__QR__':qr,'__UUID__':escape_html(uuid),
      '__RAW_JS__':repr(raw_url),'__INITIAL__':escape_html(initial),
      '__SUPPORT__':escape_html(get_support_username()),'__SUPPORT_URL__':escape_html(get_support_url()),
    }
    for k,v in replacements.items(): html=html.replace(k,v)
    return HTMLResponse(html)

@app.get("/sub-all")
async def subscription_all(
    request: Request,
    _=Depends(require_auth),
):

    host = get_host(request)

    async with LINKS_LOCK:

        lines = [
            vless_link_for_link(
                link,
                uid,
                host,
            )

            for uid, link
            in LINKS.items()

            if is_link_allowed(link)
        ]

    content = (
        base64
        .b64encode(
            "\n".join(
                lines
            ).encode()
        )
        .decode()
    )

    return Response(
        content=content,
        media_type="text/plain",
    )


# ============================================================
# INFO PAGE
# ============================================================

@app.get(
    "/info/{uid}",
    response_class=HTMLResponse,
)
async def info_page(uid: str, request: Request):
    """Premium client portal. Keeps the stable /info/{uid} route but replaces the legacy card layout."""
    async with LINKS_LOCK:
        link = LINKS.get(uid)
        if not link:
            return HTMLResponse("<html lang=\"fa\" dir=\"rtl\"><body style=\"margin:0;background:#070a10;color:#fff;font-family:sans-serif;padding:40px\"><h2>سرویس پیدا نشد</h2></body></html>", status_code=404)
        snapshot = dict(link)

    host = get_host(request)
    vless_url = vless_link_for_link(snapshot, uid, host)
    sub_url = f"{get_scheme()}://{host}/sub/{uid}"
    label = str(snapshot.get("label") or "VodiWalker")
    protocol = protocol_display_label(snapshot)
    used = int(snapshot.get("used_bytes", 0) or 0)
    limit = int(snapshot.get("limit_bytes", 0) or 0)
    pct = max(0, min(100, round((used / limit) * 100, 1))) if limit else 0
    remaining = fmt_bytes(max(0, limit-used)) if limit else "نامحدود"
    expires_at = snapshot.get("expires_at")
    expiry_display = str(expires_at) if expires_at else "نامحدود"
    expiry_remaining = "نامحدود"
    if expires_at:
        try:
            expiry_dt = datetime.fromisoformat(str(expires_at))
            now_dt = datetime.now(expiry_dt.tzinfo) if expiry_dt.tzinfo else datetime.now()
            seconds = int((expiry_dt - now_dt).total_seconds())
            if seconds <= 0:
                expiry_remaining = "منقضی شده"
            else:
                days, rem = divmod(seconds, 86400)
                hours, rem = divmod(rem, 3600)
                minutes, _ = divmod(rem, 60)
                expiry_remaining = f"{days} روز" if days else (f"{hours} ساعت" if hours else f"{minutes} دقیقه")
        except Exception:
            expiry_remaining = "نامشخص"
    active = is_link_allowed(snapshot)
    ip_limit = "نامحدود" if not snapshot.get("ip_limit", 0) else str(snapshot.get("ip_limit"))
    conn_limit = "نامحدود" if not snapshot.get("connection_limit", 0) else str(snapshot.get("connection_limit"))
    speed_limit = "نامحدود" if not snapshot.get("speed_limit_bytes", 0) else fmt_bytes(snapshot.get("speed_limit_bytes", 0)) + "/s"
    ips = len(unique_ips_for_uuid(uid))

    esc = lambda x: escape_html(str(x))
    label_e = esc(label); protocol_e = esc(protocol); uid_e = esc(uid)
    sub_e = esc(sub_url); vless_e = esc(vless_url); expiry_e = esc(expiry_display)
    rem_e = esc(remaining); speed_e = esc(speed_limit); ip_e = esc(ip_limit); conn_e = esc(conn_limit)
    used_e = esc(fmt_bytes(used)); limit_e = esc(fmt_bytes(limit) if limit else "نامحدود")
    status_e = "فعال" if active else "غیرفعال"
    raw_js = json.dumps(raw_url if 'raw_url' in locals() else sub_url)
    vless_js = json.dumps(vless_url)
    sub_js = json.dumps(sub_url)

    html = """<!doctype html>
<html lang="fa" dir="rtl">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#070a12"><meta name="color-scheme" content="dark"><title>__LABEL__ · VodiWalker</title>
<link rel="stylesheet" href="/assets/ui.css">
<script src="/assets/qr.js"></script>
<style>
:root{--bg:#060812;--panel:#0d1220;--panel2:#111827;--line:rgba(255,255,255,.08);--muted:#8b97ad;--text:#f5f7fb;--accent:#7c5cff;--cyan:#3dd8ff;--good:#2dd4a0;--warn:#f5b942;--danger:#ff6175}
*{box-sizing:border-box}html,body{margin:0;min-height:100%;font-family:Vazirmatn,Inter,sans-serif;background:var(--bg);color:var(--text)}body{overflow-x:hidden;background:radial-gradient(900px 420px at 85% -10%,rgba(124,92,255,.18),transparent 60%),radial-gradient(700px 380px at 5% 25%,rgba(61,216,255,.08),transparent 62%),linear-gradient(180deg,#070a12,#05070d)}
body:before{content:"";position:fixed;inset:0;pointer-events:none;opacity:.28;background-image:linear-gradient(rgba(255,255,255,.035) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.025) 1px,transparent 1px);background-size:48px 48px;mask-image:linear-gradient(#000,transparent 90%)}
.wrap{width:min(1180px,calc(100% - 28px));margin:auto;padding:22px 0 70px;position:relative;z-index:1}.top{display:flex;justify-content:space-between;align-items:center;gap:14px;margin-bottom:14px}.brand{display:flex;align-items:center;gap:11px}.mark{width:42px;height:42px;border-radius:14px;display:grid;place-items:center;background:linear-gradient(145deg,#1b1730,#111c2c);border:1px solid rgba(124,92,255,.35);box-shadow:0 10px 35px rgba(0,0,0,.3);font-size:18px}.brand b{display:block;font-size:15px}.brand small{display:block;color:#66738a;font-size:9px;letter-spacing:.14em;margin-top:3px}.top-actions{display:flex;gap:8px;flex-wrap:wrap}.btn{border:1px solid var(--line);background:rgba(255,255,255,.035);color:#dce3ef;border-radius:11px;padding:10px 13px;font:800 11px inherit;cursor:pointer;text-decoration:none;display:inline-flex;align-items:center;justify-content:center;gap:7px}.btn.primary{border-color:rgba(124,92,255,.45);background:linear-gradient(135deg,#7c5cff,#5d72ff);color:#fff;box-shadow:0 12px 32px rgba(92,91,255,.2)}.btn.good{color:#7bf0c6;border-color:rgba(45,212,160,.25);background:rgba(45,212,160,.07)}
.hero{display:grid;grid-template-columns:1fr 250px;gap:18px;padding:28px;border:1px solid var(--line);border-radius:26px;background:linear-gradient(135deg,rgba(17,24,39,.92),rgba(8,12,21,.88));box-shadow:0 30px 100px rgba(0,0,0,.25);overflow:hidden;position:relative}.hero:after{content:"";position:absolute;width:360px;height:360px;left:-140px;top:-220px;border-radius:50%;background:radial-gradient(circle,rgba(124,92,255,.22),transparent 68%)}.hero-main{position:relative;z-index:1}.eyebrow{font-size:9px;letter-spacing:.18em;color:#8290a8;font-weight:900;text-transform:uppercase}.hero h1{font-size:clamp(28px,5vw,50px);line-height:1.08;letter-spacing:-.045em;margin:10px 0 8px}.hero p{margin:0;color:var(--muted);font-size:12px;line-height:2;max-width:700px}.chips{display:flex;flex-wrap:wrap;gap:7px;margin-top:15px}.chip{border:1px solid var(--line);background:rgba(255,255,255,.035);padding:7px 9px;border-radius:10px;color:#b9c4d5;font-size:9.5px}.chip b{color:#fff}.hero-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:17px}.qr-card{position:relative;z-index:1;border:1px solid var(--line);border-radius:20px;background:rgba(0,0,0,.18);padding:14px;text-align:center}.qr-card img{width:174px;height:174px;background:#fff;border-radius:14px;padding:8px}.qr-card small{display:block;color:#69768c;font-size:9px;margin-top:8px}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:12px 0}.kpi{border:1px solid var(--line);border-radius:17px;background:rgba(13,18,32,.82);padding:16px}.kpi .cap{font-size:9px;color:#6e7b90}.kpi .num{font-size:19px;font-weight:900;margin-top:6px}.kpi.good .num{color:#5ee7ba}.kpi.warn .num{color:#ffd067}.kpi.blue .num{color:#72cfff}.kpi.purple .num{color:#b8a7ff}
.grid{display:grid;grid-template-columns:1.35fr .65fr;gap:12px}.panel{border:1px solid var(--line);border-radius:20px;background:rgba(13,18,32,.84);overflow:hidden}.head{padding:16px 18px;border-bottom:1px solid var(--line);display:flex;justify-content:space-between;align-items:center;gap:10px}.head b{font-size:12px}.head small{display:block;color:#6e7b90;font-size:9px;margin-top:3px}.body{padding:18px}.usage-top{display:flex;align-items:center;gap:18px}.ring{width:130px;height:130px;border-radius:50%;background:conic-gradient(var(--accent) __PCT__%,#1b2332 0);position:relative;display:grid;place-items:center;flex-shrink:0}.ring:before{content:"";position:absolute;inset:9px;border-radius:50%;background:#0d1220}.ring>div{position:relative;text-align:center}.ring strong{font-size:22px}.ring small{display:block;color:#6d7890;font-size:8px;margin-top:2px}.usage-val{font-size:26px;font-weight:950;letter-spacing:-.04em}.usage-val span{font-size:11px;color:#69768c;font-weight:600}.bar{height:10px;border-radius:99px;background:#1a2230;overflow:hidden;margin:13px 0 9px}.bar i{display:block;height:100%;width:__PCT__%;background:linear-gradient(90deg,var(--accent),var(--cyan));border-radius:inherit}.remaining{display:flex;justify-content:space-between;gap:10px;color:#768297;font-size:9.5px;flex-wrap:wrap}.trend{margin-top:15px;border:1px solid var(--line);background:rgba(0,0,0,.12);border-radius:14px;padding:10px}.trend svg{width:100%;height:80px}.facts{display:grid;grid-template-columns:1fr 1fr;gap:9px}.fact{padding:13px;border:1px solid var(--line);border-radius:14px;background:rgba(255,255,255,.018)}.fact small{display:block;color:#6d7890;font-size:8.5px}.fact b{display:block;margin-top:6px;font-size:11px;word-break:break-word}.linkbox{margin-top:12px;padding:13px;border:1px solid var(--line);border-radius:14px;background:#080c15;direction:ltr;text-align:left;color:#b8c7ff;font:10px/1.8 ui-monospace,SFMono-Regular,Consolas,monospace;word-break:break-all}.actions{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:9px}.wide{grid-column:1/-1}.tech{display:grid;grid-template-columns:repeat(4,1fr);gap:9px}.tech .fact{min-height:76px}.apps{display:grid;grid-template-columns:repeat(3,1fr);gap:9px}.app{padding:13px;border:1px solid var(--line);border-radius:14px;background:rgba(255,255,255,.018);text-decoration:none}.app b{font-size:11px}.app small{display:block;color:#6d7890;font-size:8.5px;margin-top:4px}.footer{text-align:center;color:#566174;font-size:9px;padding:22px 0}.toast{position:fixed;bottom:22px;left:50%;transform:translate(-50%,18px);opacity:0;pointer-events:none;background:#111827;border:1px solid var(--line);border-radius:12px;padding:10px 14px;font-size:10px;transition:.2s;z-index:20}.toast.show{opacity:1;transform:translate(-50%,0)}
@media(max-width:900px){.hero{grid-template-columns:1fr}.qr-card{max-width:240px}.grid{grid-template-columns:1fr}.kpis{grid-template-columns:repeat(2,1fr)}.tech{grid-template-columns:repeat(2,1fr)}}@media(max-width:540px){.wrap{width:calc(100% - 18px);padding-top:12px}.hero{padding:20px;border-radius:21px}.kpis{grid-template-columns:1fr 1fr}.usage-top{align-items:flex-start}.ring{width:100px;height:100px}.usage-val{font-size:21px}.facts{grid-template-columns:1fr}.tech,.apps{grid-template-columns:1fr}.actions{grid-template-columns:1fr}.top{align-items:flex-start}.top-actions{justify-content:flex-end}.hero h1{font-size:32px}}
</style></head><body>
<main class="wrap">
<div class="top"><div class="brand"><div class="mark">✦</div><div><b>VodiWalker</b><small>SECURE CLIENT PORTAL</small></div></div><div class="top-actions"><button class="btn" onclick="toggleTheme()">◐ پوسته</button><button class="btn" onclick="openQr()">▦ QR</button><span class="btn good">● __STATUS__</span></div></div>
<section class="hero"><div class="hero-main"><div class="eyebrow">Private Access Workspace</div><h1>__LABEL__</h1><p>مرکز حرفه‌ای مدیریت دسترسی شما؛ وضعیت مصرف، اعتبار سرویس، لینک اشتراک و مشخصات اتصال در یک فضای سریع و تمیز.</p><div class="chips"><span class="chip">پروتکل <b>__PROTOCOL__</b></span><span class="chip">شناسه <b>__UID_SHORT__</b></span><span class="chip">انقضا <b>__EXPIRY__</b></span></div><div class="hero-actions"><button class="btn primary" onclick="copy(SUB)">کپی Subscription</button><button class="btn" onclick="copy(VLESS)">کپی کانفیگ</button><a class="btn" href="__SUB_URL__?raw=1">دریافت Subscription</a></div></div><div class="qr-card"><img id="qrImg" alt="QR"><small>اسکن برای اتصال سریع</small></div></section>
<section class="kpis"><div class="kpi good"><div class="cap">مصرف‌شده</div><div class="num">__USED__</div></div><div class="kpi warn"><div class="cap">باقی‌مانده</div><div class="num">__REMAINING__</div></div><div class="kpi blue"><div class="cap">IP فعال</div><div class="num">__IPS__</div></div><div class="kpi purple"><div class="cap">زمان باقی‌مانده</div><div class="num">__EXPIRY_REMAINING__</div></div></section>
<section class="grid"><div class="panel"><div class="head"><div><b>مصرف و سلامت سرویس</b><small>Real-time service overview</small></div><span style="color:#68e6b7;font-size:9px">● LIVE</span></div><div class="body"><div class="usage-top"><div class="ring"><div><strong>__PCT__%</strong><small>مصرف</small></div></div><div style="flex:1;min-width:0"><div class="usage-val">__USED__ <span>/ __LIMIT__</span></div><div class="bar"><i></i></div><div class="remaining"><span>باقی‌مانده: <b style="color:#dce3ef">__REMAINING__</b></span><span>انقضا: <b style="color:#dce3ef">__EXPIRY__</b></span></div></div></div><div class="trend"><small style="color:#6d7890;font-size:8.5px">روند مصرف</small><svg viewBox="0 0 700 90" preserveAspectRatio="none"><polyline points="0,78 80,68 150,72 230,48 310,55 390,34 470,43 550,24 700,18" fill="none" stroke="#6f83ff" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/><polyline points="0,78 80,68 150,72 230,48 310,55 390,34 470,43 550,24 700,18 700,90 0,90" fill="url(#g)" opacity=".22"/><defs><linearGradient id="g" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="#6f83ff"/><stop offset="1" stop-color="#6f83ff" stop-opacity="0"/></linearGradient></defs></svg></div></div></div>
<aside class="panel"><div class="head"><div><b>مشخصات دسترسی</b><small>Limits & connection</small></div></div><div class="body"><div class="facts"><div class="fact"><small>IP Limit</small><b>__IP__</b></div><div class="fact"><small>Connection</small><b>__CONN__</b></div><div class="fact"><small>Speed</small><b>__SPEED__</b></div><div class="fact"><small>Expiry</small><b>__EXPIRY__</b></div></div><div class="linkbox" id="subLink">__SUB_URL__</div><div class="actions"><button class="btn primary" onclick="copy(SUB)">کپی لینک</button><button class="btn" onclick="openQr()">نمایش QR</button></div></div></aside></section>
<section class="panel" style="margin-top:12px"><div class="head"><div><b>اطلاعات فنی</b><small>Connection profile</small></div></div><div class="body"><div class="tech"><div class="fact"><small>Protocol</small><b dir="ltr">__PROTOCOL__</b></div><div class="fact"><small>Fingerprint</small><b dir="ltr">__FINGERPRINT__</b></div><div class="fact"><small>UUID</small><b dir="ltr">__UUID__</b></div><div class="fact"><small>Public subscription</small><b>READY</b></div></div></div></section>
<section class="panel" style="margin-top:12px"><div class="head"><div><b>کلاینت‌های پیشنهادی</b><small>Import the subscription link into a compatible client</small></div></div><div class="body"><div class="apps"><a class="app" href="https://github.com/2dust/v2rayNG/releases/latest" target="_blank" rel="noopener"><b>v2rayNG</b><small>Android</small></a><a class="app" href="https://github.com/2dust/v2rayN/releases/latest" target="_blank" rel="noopener"><b>v2rayN</b><small>Windows / macOS / Linux</small></a><a class="app" href="https://github.com/hiddify/hiddify-app/releases/latest" target="_blank" rel="noopener"><b>Hiddify</b><small>Android / Desktop</small></a></div></div></section>
<div class="footer">VodiWalker Secure Client Portal · اطلاعات اتصال فقط برای صاحب این لینک</div>
</main><div id="toast" class="toast"></div>
<div id="qrModal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.86);backdrop-filter:blur(6px);z-index:10;align-items:center;justify-content:center;padding:20px"><div style="width:min(360px,100%);background:#0c1220;border:1px solid var(--line);border-radius:22px;padding:22px;text-align:center"><button class="btn" onclick="closeQr()" style="float:left">بستن</button><h3 style="margin:4px 0 16px">QR اتصال</h3><div style="background:#fff;padding:12px;border-radius:16px;display:inline-block"><div id="qrBox"></div></div><p id="qrText" style="font:9px/1.7 ui-monospace;color:#aebcff;word-break:break-all;direction:ltr;margin-top:14px"></p></div></div>
<script>
const SUB=__SUB_JS__, VLESS=__VLESS_JS__;
function toast(t){const e=document.getElementById('toast');e.textContent=t;e.classList.add('show');setTimeout(()=>e.classList.remove('show'),1600)}
async function copy(v){try{await navigator.clipboard.writeText(v);toast('کپی شد ✓')}catch(e){const x=document.createElement('textarea');x.value=v;document.body.appendChild(x);x.select();document.execCommand('copy');x.remove();toast('کپی شد ✓')}}
function toggleTheme(){document.body.classList.toggle('light');localStorage.setItem('vw_portal_theme',document.body.classList.contains('light')?'light':'dark')}
(function(){if(localStorage.getItem('vw_portal_theme')==='light'){document.body.classList.add('light');document.documentElement.style.setProperty('--bg','#eef1f7');document.documentElement.style.setProperty('--panel','#fff');document.documentElement.style.setProperty('--panel2','#f5f7fb');document.documentElement.style.setProperty('--text','#151827');document.documentElement.style.setProperty('--muted','#667085')}})();
function qrFor(v){try{const q=qrcode(0,'M');q.addData(v);q.make();document.getElementById('qrImg').src='data:image/svg+xml;charset=utf-8,'+encodeURIComponent(q.createSvgTag(4,4));document.getElementById('qrBox').innerHTML=q.createSvgTag(5,4);document.getElementById('qrText').textContent=v}catch(e){}}
function openQr(){document.getElementById('qrModal').style.display='flex'}function closeQr(){document.getElementById('qrModal').style.display='none'}qrFor(VLESS);
</script><a href="__SUPPORT_URL__" target="_blank" rel="noopener" aria-label="support" style="position:fixed;inset-inline-start:14px;bottom:calc(14px + env(safe-area-inset-bottom,0px));z-index:40;display:flex;align-items:center;gap:8px;padding:11px 16px;border-radius:999px;background:linear-gradient(120deg,#0ea5e9,#22d3ee);color:#03121c;font:800 12px Vazirmatn,Tahoma,sans-serif;text-decoration:none;box-shadow:0 14px 30px -12px rgba(34,211,238,.8)"><i class="ti ti-brand-telegram" style="font-size:18px"></i> پشتیبانی <bdi dir="ltr">__SUPPORT__</bdi></a>
</body></html>"""
    repl = {
        "__LABEL__": label_e, "__PROTOCOL__": protocol_e, "__UID_SHORT__": esc(uid[:18]+'…'),
        "__EXPIRY__": expiry_e, "__STATUS__": status_e, "__USED__": used_e, "__REMAINING__": rem_e,
        "__IPS__": str(ips), "__EXPIRY_REMAINING__": esc(expiry_remaining), "__LIMIT__": limit_e,
        "__IP__": ip_e, "__CONN__": conn_e, "__SPEED__": speed_e, "__FINGERPRINT__": esc(snapshot.get("fingerprint", "chrome")),
        "__UUID__": uid_e, "__SUB_URL__": sub_e, "__VLESS_URL__": vless_e, "__PCT__": str(pct),
        "__SUB_JS__": sub_js, "__VLESS_JS__": vless_js,
        "__SUPPORT__": esc(get_support_username()), "__SUPPORT_URL__": esc(get_support_url()),
    }
    for k,v in repl.items(): html = html.replace(k,v)
    return HTMLResponse(html)

# ============================================================
# SUB GROUP API
# ============================================================

@app.post("/api/subs")
async def create_sub_api(
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="JSON نامعتبر است",
        )

    sub_id, sub = await create_sub_group(
        name=body.get(
            "name",
            "گروه جدید",
        ),
        desc=body.get(
            "desc",
            "",
        ),
        password=body.get(
            "password",
            "",
        ),
    )

    host = get_host(request)

    return {
        "sub_id":
            sub_id,

        **sub,

        "password_hash":
            None,

        "public_url":
            (
                f"{get_scheme()}://{host}"
                f"/p/{sub['uuid_key']}"
            ),

        "sub_url":
            (
                f"{get_scheme()}://{host}"
                f"/sub-group/{sub['uuid_key']}"
            ),
    }


@app.get("/api/subs")
async def list_subs_api(
    request: Request,
    _=Depends(require_auth),
):

    host = get_host(request)

    async with SUBS_LOCK:
        snapshot_subs = dict(SUBS)

    async with LINKS_LOCK:
        snapshot_links = dict(LINKS)

    result = []

    for sid, sub in snapshot_subs.items():

        link_ids = sub.get(
            "link_ids",
            [],
        )

        active_count = sum(
            1
            for lid in link_ids
            if is_link_allowed(
                snapshot_links.get(
                    lid
                )
            )
        )

        total_used = sum(
            snapshot_links[
                lid
            ].get(
                "used_bytes",
                0,
            )

            for lid in link_ids

            if lid in snapshot_links
        )

        result.append(
            {
                "sub_id":
                    sid,

                **sub,

                "password_hash":
                    None,

                "has_password":
                    sub.get(
                        "password_hash"
                    ) is not None,

                "links_count":
                    len(link_ids) + len(sub.get("remote_links", [])),

                "local_count":
                    len(link_ids),

                "remote_count":
                    len(sub.get("remote_links", [])),

                "active_count":
                    active_count,

                "total_used_bytes":
                    total_used,

                "total_used_fmt":
                    fmt_bytes(
                        total_used
                    ),

                "public_url":
                    (
                        f"{get_scheme()}://{host}"
                        f"/p/{sub['uuid_key']}"
                    ),

                "sub_url":
                    (
                        f"{get_scheme()}://{host}"
                        f"/sub-group/{sub['uuid_key']}"
                    ),

                "remote_links":
                    public_remote_links(sub.get("remote_links", [])),
            }
        )

    result.sort(
        key=lambda item:
            item.get(
                "created_at",
                "",
            ),
        reverse=True,
    )

    return {
        "subs": result
    }


@app.patch("/api/subs/{sub_id}")
async def update_sub_api(
    sub_id: str,
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="JSON نامعتبر است",
        )

    async with SUBS_LOCK:

        if sub_id not in SUBS:
            raise HTTPException(
                status_code=404,
                detail="sub not found",
            )

        sub = SUBS[sub_id]

        if "name" in body:
            sub["name"] = str(
                body["name"]
            )[:60]

        if "desc" in body:
            sub["desc"] = str(
                body["desc"]
            )[:200]

        if "password" in body:

            password = str(
                body.get(
                    "password",
                    "",
                )
            ).strip()

            sub["password_hash"] = (
                hash_password(password)
                if password
                else None
            )

        if "link_ids" in body:

            sub["link_ids"] = list(
                body["link_ids"]
            )

    await save_state()

    return {
        "ok": True
    }


@app.delete("/api/subs/{sub_id}")
async def delete_sub_api(
    sub_id: str,
    _=Depends(require_auth),
):

    name = await remove_sub_group(
        sub_id
    )

    if name is None:
        raise HTTPException(
            status_code=404,
            detail="sub not found",
        )

    return {
        "ok": True,
        "deleted": sub_id,
    }


@app.post("/api/subs/{sub_id}/links")
async def assign_link_to_sub(
    sub_id: str,
    request: Request,
    _=Depends(require_auth),
):

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(
            status_code=400,
            detail="JSON نامعتبر است",
        )

    link_id = str(
        body.get(
            "link_id",
            "",
        )
    )

    action = str(
        body.get(
            "action",
            "add",
        )
    )

    if action == "add":

        success = await set_link_sub(
            link_id,
            sub_id,
        )

    else:

        success = await set_link_sub(
            link_id,
            None,
        )

    if not success:
        raise HTTPException(
            status_code=404,
            detail="link or sub not found",
        )

    return {
        "ok": True
    }


# ============================================================
# NODE LINKS IN A SUBSCRIPTION  ·  دقیقاً مثل «Nodes» در پنل سنایی:
# یک اینباند در این پنل + یک اینباند روی یک نودِ دیگر، هر دو در یک اشتراک
# ============================================================

MAX_REMOTE_LINKS_PER_SUB = 200


@app.get("/api/nodes/{node_id}/inbounds")
async def api_node_inbounds(node_id: str, _=Depends(require_owner)):
    """لیست اینباندهای واقعی (نه کلاینت‌ها) روی یک نود، برای انتخاب و اتصال به یک ساب‌گروه اینجا."""
    try:
        from nodes import call_node_json, NodeCallError, get_node
    except Exception:
        raise HTTPException(status_code=503, detail="ماژول نودها در دسترس نیست")
    node = get_node(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="نود پیدا نشد")
    try:
        data = await call_node_json(node_id, "GET", "/api/links")
    except NodeCallError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc))
    items = [l for l in (data.get("links") or []) if not l.get("is_client")]
    items.sort(key=lambda l: l.get("created_at") or "", reverse=True)
    return {
        "ok": True,
        "node": {"id": node_id, "name": node.get("name", "")},
        "inbounds": [
            {
                "uuid": l.get("uuid"),
                "label": l.get("label", ""),
                "protocol_display": l.get("protocol_display", ""),
                "network": l.get("network", ""),
                "port": l.get("port"),
                "active": bool(l.get("active", True)),
                "outbound": l.get("outbound"),
                "client_count": l.get("client_count", 0),
            }
            for l in items
        ],
    }


@app.post("/api/subs/{sub_id}/remote-links")
async def add_remote_links_to_sub(sub_id: str, request: Request, _=Depends(require_owner)):
    try:
        from nodes import call_node_json, NodeCallError, get_node
    except Exception:
        raise HTTPException(status_code=503, detail="ماژول نودها در دسترس نیست")
    async with SUBS_LOCK:
        sub = SUBS.get(sub_id)
        if not sub:
            raise HTTPException(status_code=404, detail="گروه ساب پیدا نشد")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر است")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="اطلاعات معتبر نیست")
    node_id = str(body.get("node_id") or "").strip()
    uuids = body.get("uuids")
    if uuids is None:
        uuids = [body.get("uuid")]
    if not isinstance(uuids, list) or not uuids:
        raise HTTPException(status_code=400, detail="حداقل یک اینباند از نود انتخاب کن")
    node = get_node(node_id)
    if not node:
        raise HTTPException(status_code=404, detail="نود پیدا نشد")

    async with SUBS_LOCK:
        entries = sub.setdefault("remote_links", [])
        existing_keys = {(e["node_id"], e["uuid"]) for e in entries}

    added, failed = [], []
    for raw_uid in dict.fromkeys(str(u) for u in uuids):
        if (node_id, raw_uid) in existing_keys:
            continue
        if len(existing_keys) + len(added) >= MAX_REMOTE_LINKS_PER_SUB:
            failed.append({"uuid": raw_uid, "error": f"حداکثر {MAX_REMOTE_LINKS_PER_SUB} عضو ریموت در هر ساب"})
            continue
        try:
            data = await call_node_json(node_id, "GET", f"/api/links/{raw_uid}/info", timeout=10.0)
        except NodeCallError as exc:
            failed.append({"uuid": raw_uid, "error": str(exc)})
            continue
        entry = {
            "node_id": node_id,
            "uuid": raw_uid,
            "label": str(data.get("label") or ""),
            "protocol_display": str(data.get("protocol_display") or ""),
            "vless": str(data.get("vless_full") or data.get("vless") or ""),
            "active": bool(data.get("active", True)),
            "used_bytes": int(data.get("used_bytes") or 0),
            "limit_bytes": int(data.get("limit_bytes") or 0),
            "expires_at": data.get("expires_at"),
            "added_at": datetime.now().isoformat(),
            "last_synced_at": datetime.now().isoformat(),
            "last_error": "",
        }
        added.append(entry)

    if added:
        async with SUBS_LOCK:
            sub.setdefault("remote_links", []).extend(added)
        await save_state()
        log_activity("sub", f"{len(added)} اینباند از نود «{node['name']}» به گروه «{sub.get('name','')}» اضافه شد", "ok")

    return {"ok": True, "added": len(added), "failed": failed, "remote_links": public_remote_links(sub.get("remote_links", []))}


@app.delete("/api/subs/{sub_id}/remote-links/{ref_id}")
async def remove_remote_link_from_sub(sub_id: str, ref_id: str, _=Depends(require_owner)):
    async with SUBS_LOCK:
        sub = SUBS.get(sub_id)
        if not sub:
            raise HTTPException(status_code=404, detail="گروه ساب پیدا نشد")
        node_id, _, uid = ref_id.partition(":")
        before = len(sub.get("remote_links", []))
        sub["remote_links"] = [e for e in sub.get("remote_links", []) if not (e["node_id"] == node_id and e["uuid"] == uid)]
        removed = before - len(sub["remote_links"])
    if removed:
        await save_state()
        log_activity("sub", f"یک عضو ریموت از گروه «{sub.get('name','')}» حذف شد", "warn")
    return {"ok": True, "removed": bool(removed)}


@app.post("/api/subs/{sub_id}/remote-links/refresh")
async def refresh_remote_links_api(sub_id: str, _=Depends(require_owner)):
    async with SUBS_LOCK:
        sub = SUBS.get(sub_id)
        if not sub:
            raise HTTPException(status_code=404, detail="گروه ساب پیدا نشد")
    await refresh_remote_links_if_stale(sub, sub_id=sub_id, force=True)
    return {"ok": True, "remote_links": public_remote_links(sub.get("remote_links", []))}


# ============================================================
# GROUP SUB
# ============================================================

REMOTE_LINK_TTL = 300.0  # ثانیه؛ کش لینک‌های نود تا این مدت بدون تماس با نود سرو می‌شود


async def refresh_remote_link_entry(entry: dict) -> dict:
    """کش یک عضو ریموت (لینک روی یک نود) را با یک تماس به همان نود تازه می‌کند.
    اگر نود جواب ندهد، آخرین نسخه‌ی کش‌شده دست‌نخورده می‌ماند (فقط last_error ست می‌شود)."""
    try:
        from nodes import call_node_json, NodeCallError, get_node
    except Exception:
        entry["last_error"] = "ماژول نودها در دسترس نیست"
        return entry
    if not get_node(entry["node_id"]):
        # نود از رجیستری پنل اصلی حذف شده؛ دیگه سعی نمی‌کنیم بهش وصل بشیم و
        # این عضو رو توی خروجی اشتراک نمی‌ذاریم (تا کانفیگِ یتیم بی‌صدا سرو نشه)
        entry["node_missing"] = True
        entry["last_error"] = "این نود از پنل اصلی حذف شده است"
        return entry
    entry.pop("node_missing", None)
    try:
        data = await call_node_json(entry["node_id"], "GET", f"/api/links/{entry['uuid']}/info", timeout=6.0)
        entry.update(
            label=str(data.get("label") or entry.get("label") or ""),
            protocol_display=str(data.get("protocol_display") or entry.get("protocol_display") or ""),
            vless=str(data.get("vless_full") or data.get("vless") or ""),
            active=bool(data.get("active", True)),
            used_bytes=int(data.get("used_bytes") or 0),
            limit_bytes=int(data.get("limit_bytes") or 0),
            expires_at=data.get("expires_at"),
            last_synced_at=datetime.now().isoformat(),
            last_error="",
        )
    except NodeCallError as exc:
        entry["last_error"] = str(exc)[:200]
    except Exception as exc:
        entry["last_error"] = f"{type(exc).__name__}: {str(exc)[:150]}"
    return entry


async def mark_node_removed_in_subs(node_id: str):
    """وقتی یک نود از رجیستری پنل اصلی حذف می‌شه، بلافاصله (نه فقط بعد از TTL کش)
    عضوهای ریموتی که به همون نود اشاره می‌کنن رو «یتیم» علامت می‌زنیم تا از سرو شدن
    یک کانفیگ بدون نظارت جلوگیری بشه. صدا زده می‌شه از nodes.py، هنگام حذف نود."""
    changed = False
    async with SUBS_LOCK:
        for sub in SUBS.values():
            for entry in sub.get("remote_links", []):
                if entry.get("node_id") == node_id and not entry.get("node_missing"):
                    entry["node_missing"] = True
                    entry["last_error"] = "این نود از پنل اصلی حذف شده است"
                    changed = True
    if changed:
        await save_state()


async def refresh_remote_links_if_stale(sub: dict, sub_id: str | None = None, force: bool = False) -> list[dict]:
    entries = sub.get("remote_links", [])
    if not entries:
        return []
    now = time.time()

    def _is_stale(e):
        ts = e.get("last_synced_at")
        if not ts:
            return True
        try:
            return (now - datetime.fromisoformat(ts).timestamp()) > REMOTE_LINK_TTL
        except Exception:
            return True

    stale = entries if force else [e for e in entries if _is_stale(e)]
    if stale:
        await asyncio.gather(*(refresh_remote_link_entry(e) for e in stale), return_exceptions=True)
        if sub_id:
            await save_state()
    return entries


@app.get("/sub-group/{uuid_key}")
async def sub_group_subscription(
    uuid_key: str,
    request: Request,
):

    async with SUBS_LOCK:

        sub = next(
            (
                item
                for item
                in SUBS.values()
                if item.get(
                    "uuid_key"
                ) == uuid_key
            ),
            None,
        )

    if not sub:
        raise HTTPException(
            status_code=404,
            detail="not found",
        )

    if _subscription_wants_browser_view(request):
        _qs = "&".join(
            f"{quote(str(k), safe='')}={quote(str(v), safe='')}"
            for k, v in request.query_params.multi_items()
            if k not in ("web", "raw")
        )
        return RedirectResponse(
            url=f"/p/{uuid_key}" + (f"?{_qs}" if _qs else ""),
            status_code=307,
            headers=_SUB_NO_STORE,
        )

    if sub.get(
        "password_hash"
    ):

        password = (
            request.query_params.get(
                "pw",
                "",
            )
        )

        if (
            hash_password(password)
            != sub["password_hash"]
        ):

            raise HTTPException(
                status_code=403,
                detail="wrong password",
            )

    host = get_host(request)

    template_link = None
    template_id = None
    total_used = 0
    total_limit = 0
    expiries = []

    async with LINKS_LOCK:

        lines = []

        for link_id in sub.get(
            "link_ids",
            [],
        ):

            link = LINKS.get(
                link_id
            )

            if (
                link
                and is_link_allowed(
                    link
                )
            ):

                if template_link is None:
                    template_link = link
                    template_id = link_id
                total_used += int(link.get("used_bytes", 0) or 0)
                total_limit += int(link.get("limit_bytes", 0) or 0)
                if link.get("expires_at"):
                    expiries.append(str(link.get("expires_at")))

                lines.append(
                    vless_link_for_link(
                        link,
                        link_id,
                        host,
                    )
                )

    # اعضای «ریموت» = لینک‌هایی که روی یک نود دیگر ساخته شده‌اند و به این ساب‌گروه
    # وصل شده‌اند (دقیقاً همان ایده‌ی «Nodes» در پنل سنایی: یک اینباند روی پنل اصلی +
    # یک اینباند روی پنل دیگر، هر دو داخل یک اشتراک).
    remote_entries = await refresh_remote_links_if_stale(sub, sub_id=next((k for k, v in SUBS.items() if v is sub), None))
    for entry in remote_entries:
        if not entry.get("active", True):
            continue
        total_used += int(entry.get("used_bytes", 0) or 0)
        total_limit += int(entry.get("limit_bytes", 0) or 0)
        if entry.get("expires_at"):
            expiries.append(str(entry.get("expires_at")))
        if not entry.get("node_missing") and entry.get("vless"):
            lines.append(entry["vless"])

    # For a group subscription, expose aggregate usage/expiry in standard headers.
    group_limit = total_limit if total_limit > 0 else 0
    group_expiry = None
    if expiries:
        try:
            group_expiry = min(
                expiries,
                key=lambda x: datetime.fromisoformat(x)
            )
        except Exception:
            group_expiry = expiries[0]

    # ردیف اطلاعاتی (با آدرس و UUID واقعی، بدون هیچ آدرس فیک) — اگر حداقل یک
    # کانفیگ واقعی در این گروه باشد، به‌عنوان اولین ردیفِ لیست سرورها اضافه می‌شود تا
    # کاربر همان لحظه که اپش را باز می‌کند، حجم/زمان باقی‌مانده‌اش را ببیند.
    group_info_remark = build_info_server_remark(total_used, group_limit, group_expiry)
    if template_link is not None and bool(CONFIG.get("sub_info_line_enabled", True)):
        lines.insert(0, vless_link_for_link({**template_link, "label": group_info_remark}, template_id, host))

    content = (
        base64
        .b64encode(
            "\n".join(
                lines
            ).encode()
        )
        .decode()
    )

    _chan = _clean_tg_username(CONFIG.get("channel_username")) or CHANNEL_USERNAME
    group_title = f"{group_info_remark} | {sub['name']} | @{_chan}"
    headers = subscription_metadata_headers(
        total_used,
        group_limit,
        group_expiry,
        host,
        f"{get_scheme()}://{host}/p/{uuid_key}",
        group_title,
    )
    headers.update(_SUB_NO_STORE)

    return Response(
        content=content,
        media_type="text/plain; charset=utf-8",
        headers=headers,
    )


# ============================================================
# PUBLIC GROUP
# ============================================================

PUBLIC_SUB_HTML = r"""<!doctype html>
<html lang="fa" dir="rtl" translate="no"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="google" content="notranslate"><meta name="theme-color" content="#070a14"><title>VodiWalker · اشتراک</title>
<link rel="stylesheet" href="/assets/ui.css">
<style>
:root{--bg:#060813;--card:#0d1120;--card2:#121831;--line:rgba(148,130,255,.16);--line2:rgba(167,139,250,.38);--text:#f4f3ff;--mut:#9a98bd;--soft:#6a6890;--pri:#8b5cf6;--pri2:#6366f1;--cy:#22d3ee;--ok:#34d399;--warn:#fbbf24;--bad:#fb7185}
@media(prefers-color-scheme:light){:root:not([data-theme=dark]){--bg:#f3f2fb;--card:#fff;--card2:#f6f5ff;--line:rgba(99,80,200,.14);--line2:rgba(99,80,200,.32);--text:#191635;--mut:#5d5a83;--soft:#8582aa}}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{margin:0}body{min-height:100dvh;background:radial-gradient(60% 34% at 50% -4%,rgba(124,80,240,.30),transparent 70%),radial-gradient(50% 30% at 100% 30%,rgba(34,211,238,.09),transparent 70%),var(--bg);color:var(--text);font-family:'Vazirmatn',Tahoma,sans-serif;line-height:1.7}
.wrap{width:min(720px,calc(100% - 24px));margin:0 auto;padding:calc(14px + env(safe-area-inset-top,0px)) 0 calc(96px + env(safe-area-inset-bottom,0px))}
.top{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:14px}
.brand{display:flex;align-items:center;gap:10px;font-weight:900;font-size:16px}
.mark{width:42px;height:42px;border-radius:14px;display:grid;place-items:center;font-size:22px;background:linear-gradient(145deg,rgba(139,92,246,.35),rgba(34,211,238,.14));border:1px solid var(--line2)}
.brand small{display:block;font-size:10px;color:var(--soft);font-weight:600;letter-spacing:.14em;font-family:Inter,sans-serif}
.chip{display:inline-flex;align-items:center;gap:6px;padding:6px 12px;border-radius:999px;font-size:11px;font-weight:800;border:1px solid}
.chip.ok{color:var(--ok);border-color:rgba(52,211,153,.35);background:rgba(52,211,153,.08)}.chip.bad{color:var(--bad);border-color:rgba(251,113,133,.35);background:rgba(251,113,133,.08)}.chip.warn{color:var(--warn);border-color:rgba(251,191,36,.35);background:rgba(251,191,36,.08)}
.chip:before{content:"";width:7px;height:7px;border-radius:50%;background:currentColor;box-shadow:0 0 10px currentColor}
.hero{position:relative;overflow:hidden;border:1px solid var(--line2);border-radius:26px;padding:20px;background:linear-gradient(160deg,var(--card2),var(--card));box-shadow:0 30px 80px -40px rgba(124,80,240,.6)}
.hero:before{content:"";position:absolute;inset:-40% 30% auto -20%;height:220px;background:radial-gradient(closest-side,rgba(139,92,246,.35),transparent);pointer-events:none}
.eyebrow{font-size:10px;letter-spacing:.16em;color:var(--soft);font-weight:800;font-family:Inter,sans-serif}
.hero h1{position:relative;margin:4px 0 2px;font-size:clamp(22px,6vw,30px);font-weight:900;word-break:break-word}
.hero p{position:relative;margin:0;color:var(--mut);font-size:12.5px}
.ringrow{position:relative;display:flex;align-items:center;gap:18px;margin-top:18px}
.ring{position:relative;width:132px;height:132px;flex:none}
.ring svg{width:100%;height:100%;transform:rotate(-90deg)}
.ring .bg{stroke:rgba(148,130,255,.16)}.ring .fg{stroke:url(#g);stroke-linecap:round;transition:stroke-dashoffset .9s cubic-bezier(.2,.8,.2,1)}
.ring .mid{position:absolute;inset:0;display:grid;place-content:center;text-align:center}
.ring .mid b{font-size:26px;font-weight:900;font-family:Inter,sans-serif;direction:ltr}.ring .mid small{font-size:10px;color:var(--mut)}
.kv{flex:1;display:grid;gap:8px;min-width:0}
.kv div{display:flex;justify-content:space-between;gap:8px;align-items:center;padding:9px 12px;border-radius:13px;background:rgba(148,130,255,.07);border:1px solid var(--line);font-size:12px}
.kv span{color:var(--mut)}.kv b{font-weight:800;direction:ltr;unicode-bidi:isolate;white-space:nowrap;font-family:Inter,'Vazirmatn',sans-serif}
.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-top:12px}
.stat{padding:12px 8px;text-align:center;border-radius:16px;border:1px solid var(--line);background:var(--card)}
.stat i{font-size:20px;color:var(--pri)}.stat b{display:block;font-size:16px;font-weight:900;margin-top:2px;direction:ltr;font-family:Inter,'Vazirmatn',sans-serif}.stat small{font-size:10.5px;color:var(--mut)}
.sec{margin-top:14px;border:1px solid var(--line);border-radius:22px;background:var(--card);overflow:hidden}
.sec-h{display:flex;align-items:center;justify-content:space-between;gap:8px;padding:14px 16px;border-bottom:1px solid var(--line);font-weight:900;font-size:14px}
.sec-h small{display:block;color:var(--soft);font-size:10.5px;font-weight:500}.sec-h .n{font-size:11px;color:var(--mut);font-weight:700}
.sec-b{padding:14px 16px}
.url{padding:12px;border-radius:14px;background:rgba(0,0,0,.25);border:1px dashed var(--line2);direction:ltr;text-align:left;word-break:break-all;color:#c4b5fd;font:11.5px/1.7 ui-monospace,Consolas,monospace}
@media(prefers-color-scheme:light){.url{background:#f1efff;color:#5b3fd0}}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;border:1px solid transparent;cursor:pointer;text-decoration:none;color:#fff;font-family:inherit;font-weight:800;font-size:13px;padding:12px 14px;border-radius:14px;background:linear-gradient(120deg,var(--pri2),var(--pri) 55%,#a855f7);box-shadow:0 12px 28px -14px rgba(124,80,240,.9);transition:transform .12s,filter .12s}
.btn:hover{filter:brightness(1.1)}.btn:active{transform:scale(.97)}
.btn.alt{background:var(--card2);color:var(--text);border-color:var(--line2);box-shadow:none}.btn.tg{background:linear-gradient(120deg,#0ea5e9,#22d3ee);color:#03121c;box-shadow:0 12px 28px -14px rgba(34,211,238,.8)}
.btn.block{width:100%}.row{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px}
.apps{display:flex;flex-wrap:wrap;gap:7px;margin-top:10px}.apps a{font-size:11.5px;padding:8px 12px;border-radius:999px;border:1px solid var(--line2);background:rgba(139,92,246,.10);color:var(--text);text-decoration:none;font-weight:700}
.apps a:hover{background:rgba(139,92,246,.24)}
.qrbox{display:none;margin-top:12px;text-align:center}.qrbox.show{display:block}.qrbox img{width:200px;height:200px;background:#fff;padding:10px;border-radius:18px}
.cfg{padding:14px;border:1px solid var(--line);border-radius:18px;background:var(--card2);margin-bottom:10px}.cfg:last-child{margin-bottom:0}
.cfg-top{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.cfg-name{font-weight:900;font-size:14px;word-break:break-word;direction:ltr;text-align:right;unicode-bidi:plaintext}
.proto{display:inline-block;margin-top:4px;padding:2px 9px;border-radius:999px;background:rgba(139,92,246,.14);color:#c4b5fd;font-size:10px;font-weight:800;font-family:Inter,sans-serif}
.bar{height:7px;border-radius:99px;background:rgba(148,130,255,.14);margin:12px 0 8px;overflow:hidden}.bar i{display:block;height:100%;border-radius:99px;background:linear-gradient(90deg,var(--pri2),var(--cy))}
.bar.hi i{background:linear-gradient(90deg,var(--warn),var(--bad))}
.meta{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;font-size:11px}.meta div{padding:7px 8px;border-radius:11px;background:rgba(148,130,255,.07);border:1px solid var(--line)}
.meta small{display:block;color:var(--soft);font-size:9.5px}.meta b{display:block;font-weight:800;direction:ltr;unicode-bidi:isolate;font-family:Inter,'Vazirmatn',sans-serif;font-size:11px}
.empty{padding:30px 10px;text-align:center;color:var(--soft);font-size:12.5px}
.support{margin-top:14px;padding:18px;border-radius:24px;border:1px solid rgba(34,211,238,.32);background:linear-gradient(150deg,rgba(34,211,238,.10),rgba(139,92,246,.10))}
.support h3{margin:0 0 4px;font-size:15px;font-weight:900}.support p{margin:0 0 12px;color:var(--mut);font-size:12px}
.foot{margin-top:18px;text-align:center;color:var(--soft);font-size:10.5px}
.dock{position:fixed;inset:auto 0 0 0;display:flex;justify-content:center;padding:10px 12px calc(10px + env(safe-area-inset-bottom,0px));background:linear-gradient(transparent,var(--bg) 40%);pointer-events:none;z-index:20}
.dock a{pointer-events:auto;width:min(720px,100%)}
.locked{max-width:460px;margin:12vh auto 0}.field{display:flex;gap:8px;margin-top:12px}.field input{flex:1;min-width:0;background:var(--card2);border:1px solid var(--line2);color:var(--text);padding:12px;border-radius:12px;direction:ltr;font-family:inherit}
.toast{position:fixed;left:50%;bottom:86px;transform:translate(-50%,16px);opacity:0;background:#1b1436;color:#fff;border:1px solid var(--line2);padding:10px 16px;border-radius:14px;font-size:12px;font-weight:700;transition:.2s;z-index:50;pointer-events:none}.toast.show{opacity:1;transform:translate(-50%,0)}
.skel{height:180px;border-radius:26px;background:linear-gradient(90deg,var(--card),var(--card2),var(--card));background-size:200% 100%;animation:sk 1.2s infinite}@keyframes sk{to{background-position:-200% 0}}
@media(max-width:430px){.ringrow{flex-direction:column;align-items:stretch}.ring{margin:0 auto}.meta{grid-template-columns:1fr 1fr}.meta div:last-child{grid-column:1/-1}}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style></head><body>
<main class="wrap">
  <div class="top"><div class="brand"><div class="mark">🛡️</div><div>VodiWalker<small>SUBSCRIPTION</small></div></div><span class="chip ok" id="stateChip">آماده</span></div>
  <div id="app"><div class="skel"></div></div>
  <div class="foot">VodiWalker · اتصال به یک اینترنت بهتر 💜</div>
</main>
<div class="toast" id="toast"></div>
<script src="/assets/qr.js"></script>
<script>
var KEY=location.pathname.split('/').pop(),QS=location.search||'';
function $(i){return document.getElementById(i)}
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,function(c){return{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
function toast(t){var e=$('toast');e.textContent=t;e.classList.add('show');clearTimeout(toast._t);toast._t=setTimeout(function(){e.classList.remove('show')},1700)}
function copyText(v){
  function fallback(){var a=document.createElement('textarea');a.value=v;a.style.position='fixed';a.style.opacity='0';document.body.appendChild(a);a.select();try{document.execCommand('copy');toast('کپی شد ✓')}catch(e){prompt('کپی کنید:',v)}a.remove()}
  if(navigator.clipboard&&window.isSecureContext){navigator.clipboard.writeText(v).then(function(){toast('کپی شد ✓')},fallback)}else fallback()
}
function fmt(n){n=Number(n)||0;if(!n)return'0 B';var u=['B','KB','MB','GB','TB'],i=0;while(n>=1024&&i<u.length-1){n/=1024;i++}return(n>=100?Math.round(n):n>=10?n.toFixed(1):n.toFixed(2))+' '+u[i]}
function daysLeft(iso){if(!iso)return null;var t=new Date(String(iso).replace(' ','T')).getTime();if(isNaN(t))return null;return Math.ceil((t-Date.now())/864e5)}
function qrSvg(v){try{var q=qrcode(0,'M');q.addData(v);q.make();return'data:image/svg+xml;charset=utf-8,'+encodeURIComponent(q.createSvgTag(5,4))}catch(e){return''}}
function b64(s){try{return btoa(unescape(encodeURIComponent(s)))}catch(e){return''}}
function unlock(ev){ev.preventDefault();location.search='?pw='+encodeURIComponent($('pw').value)}
function toggleQr(){var b=$('qrbox');b.classList.toggle('show');if(b.classList.contains('show')&&!b.dataset.done){b.dataset.done=1;$('qrimg').src=qrSvg(window.SUBURL)}}
function render(d){
  var app=$('app');
  if(d.locked){
    app.innerHTML='<section class="sec locked"><div class="sec-b"><div class="eyebrow">PROTECTED</div><h2 style="margin:4px 0 6px">🔒 '+esc(d.name||'اشتراک')+'</h2><p style="color:var(--mut);font-size:12.5px;margin:0">این اشتراک با رمز محافظت می‌شود. رمز را وارد کنید تا اطلاعات و لینک‌ها نمایش داده شوند.</p><form class="field" id="pwForm"><input id="pw" type="password" placeholder="رمز اشتراک" autocomplete="off"><button class="btn">ورود</button></form></div></section>';
    $('pwForm').addEventListener('submit',unlock);$('stateChip').className='chip warn';$('stateChip').textContent='قفل';return
  }
  var links=d.links||[],url=d.sub_url||'';window.SUBURL=url;
  var used=Number(d.total_used_bytes||0),limit=Number(d.total_limit_bytes||0);
  var pct=limit>0?Math.min(100,Math.round(used/limit*1000)/10):0;
  var rem=limit>0?Math.max(0,limit-used):null;
  var dl=daysLeft(d.expires_at),activeN=links.filter(function(x){return x.active}).length;
  var expired=(dl!==null&&dl<=0)||(limit>0&&used>=limit)||(links.length>0&&activeN===0);
  var chip=$('stateChip');chip.className='chip '+(expired?'bad':(dl!==null&&dl<=3?'warn':'ok'));chip.textContent=expired?'غیرفعال / تمام‌شده':(dl!==null&&dl<=3?'رو به پایان':'فعال');
  var C=2*Math.PI*52,off=C*(1-pct/100);
  var name=encodeURIComponent(d.name||'VodiWalker'),enc=encodeURIComponent(url);
  var apps=[['Hiddify','hiddify://import/'+url+'#'+name],['v2rayNG','v2rayng://install-sub?url='+enc+'&name='+name],['Streisand','streisand://import/'+url],['Shadowrocket','shadowrocket://add/sub://'+b64(url)],['sing-box','sing-box://import-remote-profile?url='+enc+'#'+name]];
  var cfgs=links.length?links.map(function(l){
    var lp=Number(l.limit_bytes)>0?Math.min(100,Math.round(Number(l.used_bytes||0)/Number(l.limit_bytes)*100)):0;
    return '<article class="cfg"><div class="cfg-top"><div><div class="cfg-name">'+esc(l.label||'Config')+'</div><span class="proto">'+esc(String(l.protocol||'vless').toUpperCase())+'</span></div><span class="chip '+(l.active?'ok':'bad')+'">'+(l.active?'فعال':'غیرفعال')+'</span></div>'
    +'<div class="bar'+(lp>85?' hi':'')+'"><i style="width:'+(Number(l.limit_bytes)>0?lp:100)+'%"></i></div>'
    +'<div class="meta"><div><small>مصرف</small><b>'+esc(l.used_fmt||'0 B')+' / '+esc(l.limit_fmt||'∞')+'</b></div><div><small>اتصال آنلاین</small><b>'+Number(l.connections||0)+' / '+(Number(l.connection_limit||0)||'∞')+'</b></div><div><small>انقضا</small><b>'+esc(String(l.expires_at||'نامحدود').slice(0,10))+'</b></div></div>'
    +'<div class="row"><button class="btn alt" data-copy="'+esc(l.vless_link||'')+'">📋 کپی کانفیگ</button><button class="btn alt" data-copy="'+esc(l.sub_url||'')+'">🔗 کپی ساب</button></div></article>'
  }).join(''):'<div class="empty">هنوز کانفیگی برای این اشتراک ثبت نشده است.</div>';
  app.innerHTML=
   '<section class="hero"><div class="eyebrow">SUBSCRIPTION CENTER</div><h1>'+esc(d.name||'اشتراک')+'</h1><p>'+esc(d.desc||'همه‌ی کانفیگ‌ها، مصرف و زمان باقی‌مانده‌ات در یک صفحه ✨')+'</p>'
   +'<div class="ringrow"><div class="ring"><svg viewBox="0 0 120 120"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#8b5cf6"/><stop offset="1" stop-color="#22d3ee"/></linearGradient></defs><circle class="bg" cx="60" cy="60" r="52" fill="none" stroke-width="10"/><circle class="fg" cx="60" cy="60" r="52" fill="none" stroke-width="10" stroke-dasharray="'+C.toFixed(1)+'" stroke-dashoffset="'+C.toFixed(1)+'" id="ringFg"/></svg><div class="mid"><b>'+(limit>0?pct+'%':'∞')+'</b><small>'+(limit>0?'مصرف‌شده':'نامحدود')+'</small></div></div>'
   +'<div class="kv"><div><span>باقی‌مانده</span><b>'+(rem===null?'نامحدود ♾️':fmt(rem))+'</b></div><div><span>مصرف‌شده</span><b>'+fmt(used)+'</b></div><div><span>حجم کل</span><b>'+(limit>0?fmt(limit):'نامحدود')+'</b></div></div></div></section>'
   +'<div class="stats"><div class="stat"><i class="ti ti-calendar-time"></i><b>'+(dl===null?'∞':(dl<=0?'0':dl))+'</b><small>روز باقی‌مانده</small></div><div class="stat"><i class="ti ti-stack-2"></i><b>'+activeN+'/'+links.length+'</b><small>کانفیگ فعال</small></div><div class="stat"><i class="ti ti-plug-connected"></i><b>'+Number(d.active_connections||0)+'</b><small>اتصال آنلاین</small></div></div>'
   +'<section class="sec"><div class="sec-h"><div>لینک اشتراک<small>این لینک را در اپ خود وارد کن</small></div><span class="n">🔗</span></div><div class="sec-b"><div class="url">'+esc(url)+'</div>'
   +'<div class="row"><button class="btn" data-copy="'+esc(url)+'">📋 کپی لینک اشتراک</button><button class="btn alt" id="qrBtn">▦ نمایش QR</button></div>'
   +'<div class="qrbox" id="qrbox"><img id="qrimg" alt="QR"></div>'
   +'<div style="margin-top:14px;font-size:12px;color:var(--mut);font-weight:700">⚡ افزودن با یک کلیک به اپ:</div><div class="apps">'+apps.map(function(a){return'<a href="'+esc(a[1])+'">'+a[0]+'</a>'}).join('')+'</div></div></section>'
   +'<section class="sec"><div class="sec-h"><div>کانفیگ‌های اشتراک<small>وضعیت و مصرف هر مسیر</small></div><span class="n">'+links.length+' مورد</span></div><div class="sec-b">'+cfgs+'</div></section>'
   +'<section class="support"><h3>💬 نیاز به کمک داری؟</h3><p>برای راهنمایی، تمدید یا رفع مشکل مستقیم به پشتیبانی پیام بده.</p><a class="btn tg block" target="_blank" rel="noopener" href="'+esc(d.support_url||'#')+'">✈️ پیام به پشتیبان <bdi dir="ltr">'+esc(d.support||'')+'</bdi></a>'
   +(d.channel_url?'<a class="btn alt block" style="margin-top:8px" target="_blank" rel="noopener" href="'+esc(d.channel_url)+'">📢 عضویت در کانال اطلاع‌رسانی</a>':'')+'</section>';
  document.querySelectorAll('[data-copy]').forEach(function(b){b.addEventListener('click',function(){copyText(b.getAttribute('data-copy'))})});
  $('qrBtn').addEventListener('click',toggleQr);
  requestAnimationFrame(function(){requestAnimationFrame(function(){var f=$('ringFg');if(f)f.style.strokeDashoffset=off.toFixed(1)})});
  var dock=document.querySelector('.dock');if(dock)dock.remove();
  if(d.support_url){var dk=document.createElement('div');dk.className='dock';dk.innerHTML='<a class="btn tg" target="_blank" rel="noopener" href="'+esc(d.support_url)+'">✈️ پشتیبانی تلگرام <bdi dir="ltr">'+esc(d.support||'')+'</bdi></a>';document.body.appendChild(dk)}
}
function load(first){
  fetch('/api/public/sub/'+encodeURIComponent(KEY)+QS,{cache:'no-store'}).then(function(r){return r.json().then(function(j){if(!r.ok)throw Error(j.detail||'خطا');return j})}).then(function(d){
    if(!first&&!d.locked){var y=window.scrollY;render(d);window.scrollTo(0,y)}else render(d)
  }).catch(function(){
    if(first)$('app').innerHTML='<section class="sec locked"><div class="sec-b" style="text-align:center"><div style="font-size:40px">🔍</div><h2 style="margin:6px 0">اشتراک پیدا نشد</h2><p style="color:var(--mut);font-size:12.5px;margin:0">لینک منقضی شده، حذف شده یا در دسترس نیست. برای کمک با پشتیبانی تماس بگیر.</p></div></section>'
  })
}
load(true);setInterval(function(){if(!document.hidden&&!(document.activeElement&&document.activeElement.tagName==='INPUT'))load(false)},30000);
</script></body></html>
"""



@app.get(
    "/p/{uuid_key}",
    response_class=HTMLResponse,
)
async def public_sub_page(
    uuid_key: str,
):

    async with SUBS_LOCK:

        exists = any(
            item.get(
                "uuid_key"
            ) == uuid_key
            for item in SUBS.values()
        )

    if not exists:

        return HTMLResponse(
            """
            <h2
            style="
            font-family:sans-serif;
            padding:40px;
            "
            >
            گروه پیدا نشد
            </h2>
            """,
            status_code=404,
        )

    return HTMLResponse(
        PUBLIC_SUB_HTML
    )


@app.get("/api/public/sub/{uuid_key}")
async def public_sub_data(
    uuid_key: str,
    request: Request,
):

    async with SUBS_LOCK:

        entry = next(
            (
                (
                    sid,
                    item,
                )

                for sid, item
                in SUBS.items()

                if item.get(
                    "uuid_key"
                ) == uuid_key
            ),
            None,
        )

    if not entry:
        raise HTTPException(
            status_code=404,
            detail="not found",
        )

    _, sub = entry

    has_password = (
        sub.get(
            "password_hash"
        ) is not None
    )

    if has_password:

        password = (
            request
            .query_params
            .get(
                "pw",
                "",
            )
        )

        if (
            hash_password(password)
            != sub[
                "password_hash"
            ]
        ):

            return JSONResponse(
                {
                    "locked": True,
                    "name":
                        sub["name"],
                }
            )

    host = get_host(request)

    async with LINKS_LOCK:
        snapshot = dict(LINKS)

    links_out = []

    active_ip_set = set()
    active_session_count = 0

    for link_id in sub.get(
        "link_ids",
        [],
    ):

        link = snapshot.get(
            link_id
        )

        if not link:
            continue

        allowed = is_link_allowed(
            link
        )

        link_ips = {
            str(item.get("ip") or "").strip()
            for item in connections.values()
            if item.get("uuid") == link_id and str(item.get("ip") or "").strip()
        }
        connection_count = len(link_ips)
        active_session_count += sum(1 for item in connections.values() if item.get("uuid") == link_id)
        active_ip_set.update(link_ips)

        links_out.append(
            {
                "uuid":
                    link_id,

                "label":
                    link.get(
                        "label"
                    ),

                "active":
                    allowed,

                "protocol":
                    link.get(
                        "protocol",
                        DEFAULT_PROTOCOL,
                    ),

                "used_bytes":
                    link.get(
                        "used_bytes",
                        0,
                    ),

                "used_fmt":
                    fmt_bytes(
                        link.get(
                            "used_bytes",
                            0,
                        )
                    ),

                "limit_bytes":
                    link.get(
                        "limit_bytes",
                        0,
                    ),

                "limit_fmt":
                    (
                        "∞"
                        if not link.get(
                            "limit_bytes",
                            0,
                        )
                        else fmt_bytes(
                            link[
                                "limit_bytes"
                            ]
                        )
                    ),

                "expires_at":
                    link.get(
                        "expires_at"
                    ),

                "vless_link":
                    vless_link_for_link(
                        link,
                        link_id,
                        host,
                    ),

                "sub_url":
                    (
                        f"{get_scheme()}://{host}"
                        f"/sub/{link_id}"
                    ),

                "info_url":
                    (
                        f"{get_scheme()}://{host}"
                        f"/info/{link_id}"
                    ),

                "connections":
                    connection_count,

                "ip_limit":
                    link.get(
                        "ip_limit",
                        0,
                    ),

                "speed_limit_bytes":
                    link.get(
                        "speed_limit_bytes",
                        0,
                    ),

                "connection_limit":
                    link.get(
                        "connection_limit",
                        0,
                    ),
            }
        )

    total_used = sum(
        item["used_bytes"]
        for item in links_out
    )

    # حجم کل: اگر حتی یک کانفیگ نامحدود باشد، کل اشتراک نامحدود حساب می‌شود
    _limits = [int(item.get("limit_bytes") or 0) for item in links_out]
    _sub_total_limit = sum(_limits) if _limits and all(x > 0 for x in _limits) else 0
    _exps = [str(item.get("expires_at") or "") for item in links_out]
    _sub_expires_at = max(_exps) if _exps and all(_exps) else None

    return {
        "locked": False,

        "name":
            sub["name"],

        "desc":
            sub.get(
                "desc",
                "",
            ),

        "sub_url":
            (
                f"{get_scheme()}://{host}"
                f"/sub-group/{uuid_key}"
            ),

        "active_connections":
            len(active_ip_set),

        "active_sessions":
            active_session_count,

        "active_ips":
            sorted(active_ip_set),

        "total_used_fmt":
            fmt_bytes(
                total_used
            ),

        "support":
            get_support_username(),

        "support_url":
            get_support_url(),

        "channel_url":
            get_channel_url() if _clean_tg_username(CONFIG.get("channel_username")) else "",

        "total_used_bytes":
            total_used,

        "total_limit_bytes":
            _sub_total_limit,

        "expires_at":
            _sub_expires_at,

        "links":
            links_out,
    }




@app.post("/api/mix-sub")
async def mix_subscription(request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر")
    ids = body.get("link_ids") or []
    if not isinstance(ids, list) or len(ids) < 2:
        raise HTTPException(status_code=400, detail="حداقل ۲ کانفیگ انتخاب کنید")
    if len(ids) > 40:
        raise HTTPException(status_code=400, detail="حداکثر ۴۰ کانفیگ")
    host = get_host(request)
    lines = []
    used_names = set()
    total_used = 0
    total_limit = 0
    labels = []
    async with LINKS_LOCK:
        for lid in ids:
            link = LINKS.get(lid)
            if not link or not is_link_allowed(link):
                continue
            labels.append(str(link.get("label") or lid[:8]))
            total_used += int(link.get("used_bytes", 0) or 0)
            total_limit += int(link.get("limit_bytes", 0) or 0)
            name = random_config_name(used_names)
            used_names.add(name)
            lines.append(vless_link_for_link({**link, "label": name}, lid, host))
    if not lines:
        raise HTTPException(status_code=400, detail="هیچ کانفیگ معتبری انتخاب نشده")
    # stats first line
    vol = f"{fmt_bytes(total_used)}/{fmt_bytes(total_limit)}" if total_limit > 0 else f"{fmt_bytes(total_used)}/∞"
    mix_label = "Mix-" + random_config_name()[:6]
    stats = f"{mix_label} | {vol} | {len(lines)} configs"
    first = generate_vless_link(ids[0], "127.0.0.1", remark=stats, protocol="vless-ws")
    content = base64.b64encode(("\n".join([first] + lines)).encode()).decode()
    # store as a sub group for reuse
    sub_id, sub = await create_sub_group(name=mix_label, desc="مخلوط‌سازی کانفیگ‌ها")
    async with SUBS_LOCK:
        if sub_id in SUBS:
            SUBS[sub_id]["link_ids"] = list(ids)
    await save_state()
    return {
        "ok": True,
        "sub_url": f"{get_scheme()}://{host}/sub-group/{sub['uuid_key']}",
        "name": mix_label,
        "count": len(lines),
        "content_preview": stats,
    }


@app.get("/api/categories")
async def list_categories(_=Depends(require_auth)):
    items = [{**cat, "id": cid} for cid, cat in CATEGORIES.items()]
    items.sort(key=lambda x: int(x.get("number", 0)))
    return {"categories": items}

@app.post("/api/categories")
async def create_category(request: Request, _=Depends(require_auth)):
    if len(CATEGORIES) >= 10:
        raise HTTPException(status_code=400, detail="حداکثر ۱۰ دسته‌بندی")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر")
    name = str(body.get("name") or "دسته جدید").strip()[:40]
    used = {int(x.get("number", 0)) for x in CATEGORIES.values()}
    num = 0
    while num in used:
        num += 1
    cid = str(num)
    limit_value = safe_float(body.get("limit_value", 0))
    limit_unit = str(body.get("limit_unit") or "GB").upper()
    limit_bytes = 0 if limit_value <= 0 else parse_size_to_bytes(limit_value, limit_unit)
    speed_value = safe_float(body.get("speed_limit_value", 0))
    speed_bytes = 0 if speed_value <= 0 else parse_speed_to_bytes(speed_value, "MBIT")
    raw_clean = body.get("clean_ips") or ""
    if isinstance(raw_clean, list):
        clean_ips = [str(x).strip() for x in raw_clean if str(x).strip()]
    else:
        clean_ips = [x.strip() for x in str(raw_clean).replace(",", "\n").splitlines() if x.strip()]
    record = {
        "id": cid, "name": name, "number": num,
        "limit_bytes": limit_bytes,
        "expires_days": safe_int(body.get("expires_days", 0), minimum=0),
        "connection_limit": safe_int(body.get("connection_limit", 0), minimum=0),
        "speed_limit_bytes": speed_bytes,
        "ip_limit": safe_int(body.get("ip_limit", 0), minimum=0),
        "clean_ips": clean_ips,
        "random_name": bool(body.get("random_name", False)),
        "single_user": bool(body.get("single_user", False)),
        "created_at": datetime.now().isoformat(),
    }
    CATEGORIES[cid] = record
    await save_state()
    return {"ok": True, **record}


@app.patch("/api/categories/{cid}")
async def update_category(cid: str, request: Request, _=Depends(require_auth)):
    if cid not in CATEGORIES:
        raise HTTPException(status_code=404, detail="یافت نشد")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON نامعتبر")
    cat = CATEGORIES[cid]
    if "name" in body:
        cat["name"] = str(body.get("name") or cat["name"]).strip()[:40]
    if "limit_value" in body:
        lv = safe_float(body.get("limit_value", 0))
        unit = str(body.get("limit_unit") or "GB").upper()
        cat["limit_bytes"] = 0 if lv <= 0 else parse_size_to_bytes(lv, unit)
    if "expires_days" in body:
        cat["expires_days"] = safe_int(body.get("expires_days", 0), minimum=0)
    if "connection_limit" in body:
        cat["connection_limit"] = safe_int(body.get("connection_limit", 0), minimum=0)
    if "speed_limit_value" in body:
        sv = safe_float(body.get("speed_limit_value", 0))
        cat["speed_limit_bytes"] = 0 if sv <= 0 else parse_speed_to_bytes(sv, "MBIT")
    if "ip_limit" in body:
        cat["ip_limit"] = safe_int(body.get("ip_limit", 0), minimum=0)
    if "clean_ips" in body:
        raw = body.get("clean_ips") or ""
        if isinstance(raw, list):
            cat["clean_ips"] = [str(x).strip() for x in raw if str(x).strip()]
        else:
            cat["clean_ips"] = [x.strip() for x in str(raw).replace(",", "\n").splitlines() if x.strip()]
    if "random_name" in body:
        cat["random_name"] = bool(body.get("random_name"))
    if "single_user" in body:
        cat["single_user"] = bool(body.get("single_user"))
    await save_state()
    return {"ok": True, **cat}

@app.delete("/api/categories/{cid}")
async def delete_category(cid: str, _=Depends(require_auth)):
    if cid in ("0", "1"):
        raise HTTPException(status_code=400, detail="پیش‌فرض قابل حذف نیست")
    if cid not in CATEGORIES:
        raise HTTPException(status_code=404, detail="یافت نشد")
    del CATEGORIES[cid]
    for link in LINKS.values():
        if str(link.get("category_id")) == cid:
            link["category_id"] = "0"
    await save_state()
    return {"ok": True}

# ============================================================
# STATS
# ============================================================

@app.get("/stats")
async def get_stats(
    request: Request,
    _=Depends(require_auth),
):

    async with LINKS_LOCK:
        snapshot = dict(LINKS)
    _scope = await actor_scope(request)
    _aid = await actor_id_of(request)
    snapshot = {u: l for u, l in snapshot.items() if client_visible(l, _aid)}
    if _scope is not None:
        snapshot = {u: l for u, l in snapshot.items() if u in _scope or l.get("parent_inbound_id") in _scope}
        used = sum(int(l.get("used_bytes", 0) or 0) for l in snapshot.values())
        return {
            "service": APP_NAME, "version": APP_VERSION, "scoped": True,
            "active_connections": sum(1 for c in connections.values() if c.get("uuid") in snapshot),
            "total_traffic_mb": round(used / (1024 ** 2), 2), "total_traffic_bytes": used,
            "total_requests": 0, "total_errors": 0, "uptime": uptime(),
            "timestamp": datetime.now().isoformat(), "hourly": {}, "recent_errors": [],
            "links_count": len(snapshot),
            "active_links": sum(1 for l in snapshot.values() if is_link_allowed(l)),
            "expired_links": sum(1 for l in snapshot.values() if is_link_expired(l)),
            "subs_count": 0,
        }

    return {
        "service":
            APP_NAME,

        "version":
            APP_VERSION,

        "active_connections":
            len(connections),

        "total_traffic_mb":
            round(
                stats[
                    "total_bytes"
                ]
                / (
                    1024 ** 2
                ),
                2,
            ),

        "total_traffic_bytes":
            stats[
                "total_bytes"
            ],

        "total_requests":
            stats[
                "total_requests"
            ],

        "total_errors":
            stats[
                "total_errors"
            ],

        "uptime":
            uptime(),

        "timestamp":
            datetime.now().isoformat(),

        "hourly":
            dict(
                hourly_traffic
            ),

        "recent_errors":
            list(
                error_logs
            )[-10:],

        "links_count":
            len(snapshot),

        "active_links":
            sum(
                1
                for link
                in snapshot.values()
                if is_link_allowed(
                    link
                )
            ),

        "expired_links":
            sum(
                1
                for link
                in snapshot.values()
                if is_link_expired(
                    link
                )
            ),

        "subs_count":
            len(SUBS),
    }


@app.get("/api/errors")
async def get_errors(
    _=Depends(require_auth),
):
    rows = list(error_logs)[-100:]
    warnings = sum(1 for x in rows if x.get("level") == "warn")
    client_errors = sum(1 for x in rows if x.get("source") == "client")
    return {
        "ok": True,
        "errors": rows,
        "total_errors": len(rows),
        "warnings": warnings,
        "client_errors": client_errors,
        "healthy": not any(x.get("level", "err") == "err" for x in rows[-20:]),
    }


@app.post("/api/errors/client")
async def report_client_error(request: Request, _=Depends(require_auth)):
    try:
        body = await request.json()
    except Exception:
        body = {}
    message = str(body.get("message") or "Unknown browser error").strip()[:1200]
    path = str(body.get("path") or request.url.path).strip()[:500]
    stack = str(body.get("stack") or "").strip()[:4000]
    details = str(body.get("details") or "").strip()[:1500]
    error_logs.append({
        "error": message,
        "path": path,
        "method": "CLIENT",
        "source": "client",
        "level": "err",
        "stack": stack,
        "details": details,
        "time": datetime.now().isoformat(),
    })
    stats["total_errors"] += 1
    logger.error("Client error: %s | %s", path, message)
    return {"ok": True}


@app.post("/api/errors/clear")
async def clear_errors(_=Depends(require_owner)):
    count = len(error_logs)
    error_logs.clear()
    stats["total_errors"] = 0
    log_activity("system", f"مرکز پیام پاک شد؛ {count} خطا حذف شد", "warn" if count else "info")
    return {"ok": True, "cleared": count}


@app.get("/api/activity")
async def get_activity(
    _=Depends(require_auth),
):

    return {
        "logs":
            list(
                activity_logs
            )[-150:]
    }


# ============================================================
# CONNECTIONS
# ============================================================

@app.get("/api/connections")
async def get_connections(
    _=Depends(require_auth),
):

    async with LINKS_LOCK:
        snapshot = dict(LINKS)

    grouped = {}

    for connection in connections.values():

        ip = connection.get(
            "ip",
            "نامشخص",
        )

        link = snapshot.get(
            connection.get(
                "uuid"
            )
        )

        label = (
            link.get(
                "label"
            )
            if link
            else "نامشخص"
        )

        group = grouped.get(ip)

        if group is None:

            group = {
                "ip":
                    ip,

                "sessions":
                    0,

                "bytes":
                    0,

                "labels":
                    set(),

                "transports":
                    set(),

                "first_connected_at":
                    connection.get(
                        "connected_at"
                    ),

                "last_connected_at":
                    connection.get(
                        "connected_at"
                    ),
            }

            grouped[ip] = group

        group["sessions"] += 1

        group["bytes"] += int(
            connection.get(
                "bytes",
                0,
            )
            or 0
        )

        group["labels"].add(
            label
        )

        group["transports"].add(
            connection.get(
                "transport",
                DEFAULT_PROTOCOL,
            )
        )

    result = []

    for group in grouped.values():

        result.append(
            {
                "ip":
                    group["ip"],

                "sessions":
                    group["sessions"],

                "labels":
                    sorted(
                        group["labels"]
                    ),

                "label":
                    (
                        " · ".join(
                            sorted(
                                group["labels"]
                            )
                        )
                        if group["labels"]
                        else "نامشخص"
                    ),

                "transports":
                    sorted(
                        group["transports"]
                    ),

                "bytes":
                    group["bytes"],

                "bytes_fmt":
                    fmt_bytes(
                        group["bytes"]
                    ),

                "connected_at":
                    group[
                        "first_connected_at"
                    ],

                "last_connected_at":
                    group[
                        "last_connected_at"
                    ],
            }
        )

    result.sort(
        key=lambda item:
            item.get(
                "last_connected_at"
            )
            or "",
        reverse=True,
    )

    return {
        "connections":
            result,

        "count":
            len(result),

        "raw_count":
            len(connections),
    }


# ============================================================
# OPTIONAL EXISTING PROJECT MODULES
# ============================================================

# ============================================================
# IMPORTANT:
# DO NOT REPLACE THIS VLESS CORE.
# ============================================================

try:

    from relay_vless import (
        RELAY_BUF,
        parse_vless_header,
        check_and_use,
        relay_ws_to_tcp,
        relay_tcp_to_ws,
        websocket_tunnel,
    )

    app.add_api_websocket_route(
        "/ws/{uuid}",
        websocket_tunnel,
    )

    logger.info(
        "VLESS relay loaded."
    )

except Exception as exc:

    logger.warning(
        "VLESS relay module unavailable: %s",
        exc,
    )


# ============================================================
# XHTTP
# ============================================================

try:

    from xhttp_siz10 import (
        router as xhttp_router
    )

    app.include_router(
        xhttp_router
    )

    logger.info(
        "XHTTP module loaded."
    )

except Exception as exc:

    logger.warning(
        "XHTTP module unavailable: %s",
        exc,
    )


# ============================================================
# OUTBOUND PROXY (SOCKS)
# ============================================================

try:

    from outbound_proxy import (
        router as outbound_proxy_router
    )

    app.include_router(
        outbound_proxy_router
    )

    logger.info(
        "Outbound proxy module loaded."
    )

except Exception as exc:

    logger.warning(
        "Outbound proxy module unavailable: %s",
        exc,
    )


# نودها: توکن API این پنل + اتصال به پنل‌های دیگر (nodes.py)
try:

    from nodes import router as nodes_router

    app.include_router(nodes_router)

    logger.info("Nodes module loaded.")

except Exception as exc:

    logger.warning(
        "Nodes module unavailable: %s",
        exc,
    )


# ============================================================
# TELEGRAM
# ============================================================

try:

    from telegram_bot import (
        start_bot as _tg_start_bot,
        stop_bot as _tg_stop_bot,
    )

except Exception:

    async def _tg_start_bot():
        return None

    async def _tg_stop_bot():
        return None


@app.on_event("startup")
async def start_optional_telegram():

    try:

        await _tg_start_bot()

        logger.info(
            "Telegram module initialized."
        )

    except Exception as exc:

        logger.warning(
            "Telegram bot disabled/error: %s",
            exc,
        )


# ============================================================
# HTTP PROXY
# ============================================================

_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-encoding",
    "content-length",
}


@app.api_route(
    "/proxy/{target_url:path}",
    methods=[
        "GET",
        "POST",
        "PUT",
        "DELETE",
        "PATCH",
        "HEAD",
        "OPTIONS",
    ],
)
async def http_proxy(
    target_url: str,
    request: Request,
):

    if not target_url.startswith("http"):
        target_url = (
            "https://"
            + target_url
        )

    if http_client is None:
        raise HTTPException(
            status_code=503,
            detail="HTTP client not ready",
        )

    try:

        body = await request.body()

        headers = {
            key: value
            for key, value
            in request.headers.items()
            if (
                key.lower()
                not in _HOP
            )
            and (
                key.lower()
                != "host"
            )
        }

        response = await http_client.request(
            method=request.method,
            url=target_url,
            headers=headers,
            content=body,
        )

        stats["total_bytes"] += len(
            response.content
        )

        bump_daily_stat("traffic_bytes", len(response.content))

        stats["total_requests"] += 1

        hourly_traffic[
            now_ir().strftime(
                "%H:00"
            )
        ] += len(
            response.content
        )

        output_headers = {
            key: value
            for key, value
            in response.headers.items()
            if key.lower() not in _HOP
        }

        return Response(
            content=response.content,
            status_code=response.status_code,
            headers=output_headers,
        )

    except Exception as exc:

        stats["total_errors"] += 1

        error_logs.append(
            {
                "error":
                    str(exc),

                "url":
                    target_url,

                "time":
                    datetime.now().isoformat(),
            }
        )

        logger.exception(
            "Proxy error: %s",
            target_url,
        )

        raise HTTPException(
            status_code=502,
            detail=(
                "Proxy error: "
                f"{exc}"
            ),
        )


# ============================================================
# DASHBOARD
# ============================================================

from pages import DASHBOARD_HTML


@app.get(
    "/dashboard",
    response_class=HTMLResponse,
)
async def dashboard(
    request: Request,
):

    if not await is_valid_session(
        request.cookies.get(
            SESSION_COOKIE
        )
    ):
        return RedirectResponse(
            "/login"
        )

    await ensure_default_categories()
    await ensure_default_link()

    return HTMLResponse(
        DASHBOARD_HTML
    )


# ============================================================
# TEST
# ============================================================

@app.get(
    "/test-ws",
    response_class=HTMLResponse,
)
async def test_ws():

    return HTMLResponse(
        """
        <script>
        location.href='/dashboard'
        </script>
        """
    )


# ============================================================
# ADMIN MANAGEMENT (multi-admin / sub-admins)
# ============================================================

def _admin_public(admin_id: str, admin: dict) -> dict:
    return {
        "id": admin_id,
        "username": admin.get("username", admin_id),
        "role": admin.get("role", "admin"),
        "permissions": sorted(admin.get("permissions") or {"dashboard"}),
        "active": admin.get("active", True),
        "created_at": admin.get("created_at"),
        "last_login_at": admin.get("last_login_at"),
        "last_login_ip": admin.get("last_login_ip"),
        "credit_stars": admin.get("credit_stars", 0),
        "full_name": admin.get("full_name", ""),
        "telegram_id": admin.get("telegram_id", ""),
        "allowed_inbounds": list(admin.get("allowed_inbounds") or []),
        "perm_v2": bool(admin.get("perm_v2")),
    }


def _clean_allowed_inbounds(raw):
    if not isinstance(raw, list):
        return []
    out = []
    for u in raw:
        u = str(u)
        l = LINKS.get(u)
        if l and not l.get("parent_inbound_id") and u not in out:
            out.append(u)
    return out


@app.get("/api/admins")
async def api_list_admins(token=Depends(require_owner)):
    owner_entry = {
        "id": "owner",
        "username": AUTH.get("username", DEFAULT_ADMIN_USERNAME),
        "role": "owner",
        "active": True,
        "created_at": None,
        "last_login_at": None,
        "last_login_ip": None,
    }
    admins = [owner_entry] + [
        _admin_public(aid, a) for aid, a in ADMINS.items()
    ]
    return {"ok": True, "admins": admins}


@app.post("/api/admins")
async def api_create_admin(request: Request, token=Depends(require_owner)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات نامعتبر است")

    username = str(body.get("username", "")).strip()
    password = str(body.get("password", "")).strip()

    if not username or username.lower() == "owner":
        raise HTTPException(status_code=400, detail="نام کاربری نامعتبر است")

    if len(password) < LOGIN_MIN_PASSWORD_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"رمز عبور باید حداقل {LOGIN_MIN_PASSWORD_LENGTH} کاراکتر باشد",
        )

    if username.lower() == AUTH.get("username", DEFAULT_ADMIN_USERNAME).lower():
        raise HTTPException(status_code=409, detail="این نام کاربری قبلاً استفاده شده است")
    for a in ADMINS.values():
        if a.get("username", "").lower() == username.lower():
            raise HTTPException(status_code=409, detail="این نام کاربری قبلاً استفاده شده است")

    admin_id = secrets.token_hex(6)

    ADMINS[admin_id] = {
        "username": username,
        "password_hash": hash_password(password),
        "role": "admin",
        "permissions": [p for p in (body.get("permissions") or ["dashboard", "inbounds", "subscriptions"]) if p in ALL_PERMISSIONS],
        "perm_v2": True,
        "allowed_inbounds": _clean_allowed_inbounds(body.get("allowed_inbounds")),
        "active": True,
        "created_at": datetime.now().isoformat(),
        "last_login_at": None,
        "last_login_ip": None,
    }

    await save_state()

    log_activity("auth", f"ادمین جدید «{username}» ایجاد شد", "ok")

    return {"ok": True, "admin": _admin_public(admin_id, ADMINS[admin_id])}


@app.patch("/api/admins/{admin_id}")
async def api_update_admin(admin_id: str, request: Request, token=Depends(require_owner)):
    admin = ADMINS.get(admin_id)
    if not admin:
        raise HTTPException(status_code=404, detail="ادمین یافت نشد")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات نامعتبر است")

    if "username" in body:
        new_username = str(body["username"]).strip()
        if not new_username or new_username.lower() == "owner":
            raise HTTPException(status_code=400, detail="نام کاربری نامعتبر است")
        for aid, a in ADMINS.items():
            if aid != admin_id and a.get("username", "").lower() == new_username.lower():
                raise HTTPException(status_code=409, detail="این نام کاربری قبلاً استفاده شده است")
        admin["username"] = new_username

    password_changed = False
    if "password" in body and str(body["password"]).strip():
        new_password = str(body["password"]).strip()
        if len(new_password) < LOGIN_MIN_PASSWORD_LENGTH:
            raise HTTPException(
                status_code=400,
                detail=f"رمز عبور باید حداقل {LOGIN_MIN_PASSWORD_LENGTH} کاراکتر باشد",
            )
        admin["password_hash"] = hash_password(new_password)
        password_changed = True

    if "permissions" in body:
        raw_permissions = body.get("permissions") or []
        if not isinstance(raw_permissions, list):
            raise HTTPException(status_code=400, detail="لیست دسترسی‌ها نامعتبر است")
        admin["permissions"] = [p for p in raw_permissions if p in ALL_PERMISSIONS]
        admin["perm_v2"] = True

    if "allowed_inbounds" in body:
        admin["allowed_inbounds"] = _clean_allowed_inbounds(body.get("allowed_inbounds"))

    if "active" in body:
        admin["active"] = bool(body["active"])

    # Password changes must invalidate existing sessions for that account.
    # Otherwise an old stolen/remembered session would remain usable after a
    # credential reset. Deactivation also revokes every session.
    if password_changed or not admin.get("active", True):
        async with SESSIONS_LOCK:
            for tok in [t for t, info in SESSIONS.items()
                        if isinstance(info, dict) and info.get("admin_id") == admin_id]:
                SESSIONS.pop(tok, None)

    await save_state()

    log_activity("auth", f"اطلاعات ادمین «{admin.get('username')}» ویرایش شد", "ok")

    return {"ok": True, "admin": _admin_public(admin_id, admin)}


@app.delete("/api/admins/{admin_id}")
async def api_delete_admin(admin_id: str, token=Depends(require_owner)):
    admin = ADMINS.pop(admin_id, None)
    if not admin:
        raise HTTPException(status_code=404, detail="ادمین یافت نشد")

    async with SESSIONS_LOCK:
        for tok in [t for t, info in SESSIONS.items() if isinstance(info, dict) and info.get("admin_id") == admin_id]:
            SESSIONS.pop(tok, None)

    await save_state()

    log_activity("auth", f"ادمین «{admin.get('username')}» حذف شد", "warn")

    return {"ok": True}


# ============================================================
# ADMIN REGISTRATION REQUESTS ("ثبت‌نام ادمینی" روی صفحه لاگین)
# ============================================================

def _admin_request_public(req_id: str, req: dict) -> dict:
    return {
        "id": req_id,
        "full_name": req.get("full_name", ""),
        "telegram_id": req.get("telegram_id", ""),
        "note": req.get("note", ""),
        "status": req.get("status", "pending"),
        "created_at": req.get("created_at"),
        "decided_at": req.get("decided_at"),
        "admin_id": req.get("admin_id"),
        "ip": req.get("ip"),
    }


@app.post("/api/admin-requests")
async def api_submit_admin_request(request: Request):
    """صفحه لاگین این را صدا می‌زند؛ نیازی به احراز هویت ندارد."""

    ip = client_ip(request)

    now = time.time()
    if len(ADMIN_REQUEST_RATE) > 2000:
        for _ip in [k for k, v in ADMIN_REQUEST_RATE.items() if now - v > ADMIN_REQUEST_COOLDOWN_SECONDS]:
            ADMIN_REQUEST_RATE.pop(_ip, None)
    last = ADMIN_REQUEST_RATE.get(ip, 0)
    if now - last < ADMIN_REQUEST_COOLDOWN_SECONDS:
        raise HTTPException(
            status_code=429,
            detail="کمی صبر کنید و دوباره تلاش کنید",
        )

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات نامعتبر است")

    full_name = str(body.get("full_name", "")).strip()
    telegram_id = str(body.get("telegram_id", "")).strip().lstrip("@")
    note = str(body.get("note", "")).strip()[:500]

    if not full_name or len(full_name) < 3:
        raise HTTPException(status_code=400, detail="نام و نام خانوادگی را کامل وارد کنید")
    if not re.fullmatch(r"[A-Za-z0-9_]{3,64}", telegram_id):
        raise HTTPException(status_code=400, detail="آیدی تلگرام معتبر وارد کنید")

    if sum(1 for r in ADMIN_REQUESTS.values() if r.get("status") == "pending") >= 200:
        raise HTTPException(status_code=429, detail="ظرفیت درخواست‌ها پر است؛ بعداً تلاش کنید")

    ADMIN_REQUEST_RATE[ip] = now

    async with ADMIN_REQUESTS_LOCK:
        req_id = secrets.token_hex(6)
        ADMIN_REQUESTS[req_id] = {
            "full_name": full_name[:120],
            "telegram_id": telegram_id[:120],
            "note": note,
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "decided_at": None,
            "admin_id": None,
            "ip": ip,
        }

    await save_state()

    log_activity(
        "auth",
        f"درخواست ثبت‌نام ادمین جدید از «{full_name}» (@{telegram_id})",
        "info",
    )

    return {"ok": True, "id": req_id}


@app.get("/api/admin-requests")
async def api_list_admin_requests(token=Depends(require_owner)):
    pending = sum(1 for r in ADMIN_REQUESTS.values() if r.get("status") == "pending")
    requests_list = sorted(
        (_admin_request_public(rid, r) for rid, r in ADMIN_REQUESTS.items()),
        key=lambda r: r.get("created_at") or "",
        reverse=True,
    )
    return {"ok": True, "requests": requests_list, "pending": pending}


@app.post("/api/admin-requests/{req_id}/approve")
async def api_approve_admin_request(req_id: str, request: Request, token=Depends(require_owner)):
    """مالک اینجا تصمیم می‌گیرد چه نام‌کاربری/رمز/دسترسی/شارژی به درخواست‌کننده بدهد
    و همان لحظه حساب ادمین واقعی برایش ساخته می‌شود."""

    req = ADMIN_REQUESTS.get(req_id)
    if not req:
        raise HTTPException(status_code=404, detail="درخواست یافت نشد")
    if req.get("status") != "pending":
        raise HTTPException(status_code=409, detail="این درخواست قبلاً بررسی شده است")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات نامعتبر است")

    username = str(body.get("username", "")).strip()
    password = str(body.get("password", "")).strip()

    if not username or username.lower() == "owner":
        raise HTTPException(status_code=400, detail="نام کاربری نامعتبر است")
    if len(password) < LOGIN_MIN_PASSWORD_LENGTH:
        raise HTTPException(
            status_code=400,
            detail=f"رمز عبور باید حداقل {LOGIN_MIN_PASSWORD_LENGTH} کاراکتر باشد",
        )
    if username.lower() == AUTH.get("username", DEFAULT_ADMIN_USERNAME).lower():
        raise HTTPException(status_code=409, detail="این نام کاربری قبلاً استفاده شده است")
    for a in ADMINS.values():
        if a.get("username", "").lower() == username.lower():
            raise HTTPException(status_code=409, detail="این نام کاربری قبلاً استفاده شده است")

    credit_stars = safe_int(body.get("credit_stars"), default=0, minimum=0)

    admin_id = secrets.token_hex(6)
    ADMINS[admin_id] = {
        "username": username,
        "password_hash": hash_password(password),
        "role": "admin",
        "permissions": list(body.get("permissions") or {"dashboard", "inbounds", "subscriptions"}),
        "active": True,
        "created_at": datetime.now().isoformat(),
        "last_login_at": None,
        "last_login_ip": None,
        "credit_stars": credit_stars,
        "full_name": req.get("full_name", ""),
        "telegram_id": req.get("telegram_id", ""),
        "perm_v2": True,
        "allowed_inbounds": _clean_allowed_inbounds(body.get("allowed_inbounds")),
    }

    req["status"] = "approved"
    req["decided_at"] = datetime.now().isoformat()
    req["admin_id"] = admin_id

    await save_state()

    log_activity(
        "auth",
        f"درخواست «{req.get('full_name')}» تایید و حساب ادمین «{username}» ساخته شد",
        "ok",
    )

    delivery_message = (
        f"سلام {req.get('full_name','')} عزیز 👋\n\n"
        f"حساب ادمین شما در VodiWalker فعال شد.\n\n"
        f"نام کاربری: {username}\n"
        f"رمز عبور: {password}\n\n"
        f"از طریق صفحه ورود پنل وارد شوید و رمز خود را در اولین فرصت تغییر دهید."
    )

    return {
        "ok": True,
        "admin": _admin_public(admin_id, ADMINS[admin_id]),
        "telegram_id": req.get("telegram_id", ""),
        "delivery_message": delivery_message,
    }


@app.post("/api/admin-requests/{req_id}/reject")
async def api_reject_admin_request(req_id: str, request: Request, token=Depends(require_owner)):
    req = ADMIN_REQUESTS.get(req_id)
    if not req:
        raise HTTPException(status_code=404, detail="درخواست یافت نشد")
    if req.get("status") != "pending":
        raise HTTPException(status_code=409, detail="این درخواست قبلاً بررسی شده است")

    try:
        body = await request.json()
    except Exception:
        body = {}

    reason = str((body or {}).get("reason", "")).strip()[:300]

    req["status"] = "rejected"
    req["decided_at"] = datetime.now().isoformat()
    req["note"] = reason or req.get("note", "")

    await save_state()

    log_activity("auth", f"درخواست ادمینی «{req.get('full_name')}» رد شد", "warn")

    return {"ok": True}


@app.delete("/api/admin-requests/{req_id}")
async def api_delete_admin_request(req_id: str, token=Depends(require_owner)):
    if req_id not in ADMIN_REQUESTS:
        raise HTTPException(status_code=404, detail="درخواست یافت نشد")
    ADMIN_REQUESTS.pop(req_id, None)
    await save_state()
    return {"ok": True}


# ============================================================
# BOT CONTROL CENTER
@app.get("/api/bot/texts")
async def api_bot_texts(token=Depends(require_owner)):
    return {"ok": True, "texts": BOT_TEXTS}

@app.post("/api/bot/texts")
async def api_bot_texts_save(request: Request, token=Depends(require_owner)):
    if BOT_TEXTS_LOCKED:
        raise HTTPException(status_code=403, detail=BOT_TEXTS_LOCKED_MSG)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات نامعتبر است")
    texts = body.get("texts") if isinstance(body, dict) else None
    if not isinstance(texts, dict):
        raise HTTPException(status_code=400, detail="ساختار متن‌ها نامعتبر است")
    for key in list(BOT_TEXTS):
        if key in texts:
            BOT_TEXTS[key] = str(texts[key])[:4000]
    await save_state()
    log_activity("bot", "متن‌های ربات از پنل بروزرسانی شد", "ok")
    return {"ok": True, "texts": BOT_TEXTS}

# PANEL SETTINGS (آدرس عمومی پنل + مدیریت ربات تلگرام از داخل پنل)
# ============================================================

@app.get("/api/settings")
async def api_get_settings(request: Request, token=Depends(require_owner)):
    bot_cfg = _bot_settings_snapshot()
    override_scheme, override_host = _split_base_url(CONFIG.get("public_base_url"))
    return {
        "ok": True,
        "public_base_url": CONFIG.get("public_base_url", ""),
        "effective_host": get_host(request),
        "effective_scheme": get_scheme(),
        "tcp_public_host": CONFIG.get("tcp_public_host", ""),
        "tcp_public_port": CONFIG.get("tcp_public_port", ""),
        "tcp_listen_port": _tcp_listen_port_snapshot(),
        "bot_token": bot_cfg.get("bot_token", ""),
        "bot_admin_ids": bot_cfg.get("admin_ids", ""),
        "bot_running": bot_cfg.get("running", False),
        "bot_texts_locked": BOT_TEXTS_LOCKED,
        "bot_auto_start": bool(CONFIG.get("bot_auto_start", False)),
        "admin_username": AUTH.get("username", DEFAULT_ADMIN_USERNAME),
        "sub_remark_show_name": bool(CONFIG.get("sub_remark_show_name", True)),
        "sub_remark_show_volume": bool(CONFIG.get("sub_remark_show_volume", False)),
        "sub_remark_show_id": bool(CONFIG.get("sub_remark_show_id", False)),
        "sub_remark_show_inbound": bool(CONFIG.get("sub_remark_show_inbound", False)),
        "sub_info_line_enabled": bool(CONFIG.get("sub_info_line_enabled", True)),
        "sub_info_line_show_volume": bool(CONFIG.get("sub_info_line_show_volume", True)),
        "sub_info_line_show_expiry": bool(CONFIG.get("sub_info_line_show_expiry", True)),
        "support_username": _clean_tg_username(CONFIG.get("support_username")),
        "channel_username": _clean_tg_username(CONFIG.get("channel_username")),
        "name_style_enabled": bool(CONFIG.get("name_style_enabled", True)),
    }


@app.post("/api/settings")
async def api_update_settings(request: Request, token=Depends(require_owner)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات نامعتبر است")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="اطلاعات نامعتبر است")

    bot_settings_changed = False

    if "public_base_url" in body:
        raw = str(body.get("public_base_url") or "").strip()
        # اعتبارسنجی سبک: اگه چیزی وارد شده، باید حداقل یک هاست معتبر ازش دربیاد
        if raw:
            _, parsed_host = _split_base_url(raw)
            if not parsed_host:
                raise HTTPException(status_code=400, detail="آدرس عمومی نامعتبر است (مثال درست: https://panel.example.com)")
        CONFIG["public_base_url"] = raw

    if "bot_auto_start" in body:
        CONFIG["bot_auto_start"] = bool(body.get("bot_auto_start"))

    if "tcp_public_host" in body:
        CONFIG["tcp_public_host"] = str(body.get("tcp_public_host") or "").strip()

    if "tcp_public_port" in body:
        raw_port = str(body.get("tcp_public_port") or "").strip()
        if raw_port and not raw_port.isdigit():
            raise HTTPException(status_code=400, detail="پورت عمومی TCP باید عدد باشد")
        CONFIG["tcp_public_port"] = raw_port

    if "support_username" in body:
        value = _clean_tg_username(body.get("support_username"))
        if value and not _TG_USER_RE.match(value):
            raise HTTPException(status_code=400, detail="آیدی تلگرام معتبر نیست (۵ تا ۳۲ کاراکتر: حروف انگلیسی، عدد و _ ؛ مثال: @MySupport)")
        CONFIG["support_username"] = value

    if "channel_username" in body:
        value = _clean_tg_username(body.get("channel_username"))
        if value and not _TG_USER_RE.match(value):
            raise HTTPException(status_code=400, detail="آیدی کانال معتبر نیست (مثال: @MyChannel)")
        CONFIG["channel_username"] = value

    for flag in (
        "name_style_enabled",
        "sub_remark_show_name", "sub_remark_show_volume", "sub_remark_show_id", "sub_remark_show_inbound",
        "sub_info_line_enabled", "sub_info_line_show_volume", "sub_info_line_show_expiry",
    ):
        if flag in body:
            CONFIG[flag] = bool(body.get(flag))

    try:
        import telegram_bot

        if "bot_token" in body or "bot_admin_ids" in body:
            new_token = body.get("bot_token")
            new_admin_ids = body.get("bot_admin_ids")
            telegram_bot.configure(
                token=(str(new_token).strip() if new_token is not None else None),
                admin_ids_raw=(str(new_admin_ids).strip() if new_admin_ids is not None else None),
            )
            bot_settings_changed = True
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning("Bot configure error: %s", exc)

    await save_state()

    # اگه ربات از قبل روشن بوده و توکن/آیدی‌ها عوض شده، برای اعمال شدنِ واقعی
    # باید دوباره راه‌اندازی بشه (وگرنه با کانکشن قدیمی به توکن قبلی وصل می‌مونه)
    restarted = False
    try:
        import telegram_bot
        if bot_settings_changed and telegram_bot.is_running():
            await telegram_bot.restart_bot()
            restarted = True
    except Exception as exc:
        logger.warning("Bot restart error: %s", exc)

    log_activity("system", "تنظیمات پنل (آدرس عمومی/ربات) به‌روزرسانی شد", "ok")

    return {"ok": True, "bot_restarted": restarted, **_bot_settings_snapshot()}


@app.post("/api/settings/bot/start")
async def api_bot_start(token=Depends(require_owner)):
    try:
        import telegram_bot
        await telegram_bot.start_bot()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"خطا در روشن کردن ربات: {exc}")
    log_activity("system", "ربات تلگرام از داخل پنل روشن شد", "ok")
    return {"ok": True, **_bot_settings_snapshot()}


@app.post("/api/settings/bot/stop")
async def api_bot_stop(token=Depends(require_owner)):
    try:
        import telegram_bot
        await telegram_bot.stop_bot()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"خطا در خاموش کردن ربات: {exc}")
    log_activity("system", "ربات تلگرام از داخل پنل خاموش شد", "warn")
    return {"ok": True, **_bot_settings_snapshot()}


# ============================================================
# ADVANCED REPORTING
# ============================================================

@app.get("/api/reports/summary")
async def api_reports_summary(request: Request, token=Depends(require_auth)):
    days = safe_int(request.query_params.get("days"), default=14, minimum=1, maximum=180)

    today = datetime.now(IRAN_TZ) if IRAN_TZ else datetime.now()
    date_keys = [
        (today - timedelta(days=offset)).strftime("%Y-%m-%d")
        for offset in range(days - 1, -1, -1)
    ]

    series = []
    for key in date_keys:
        bucket = DAILY_STATS.get(key, {})
        series.append({
            "date": key,
            "traffic_mb": round(bucket.get("traffic_bytes", 0) / (1024 ** 2), 2),
            "new_links": bucket.get("new_links", 0),
        })

    now_ts = time.time()
    active_links = 0
    expired_links = 0
    unlimited_links = 0
    protocol_counts = defaultdict(int)
    top_links = []

    for uid, link in LINKS.items():
        protocol_counts[protocol_display_label(link)] += 1

        expires_at = link.get("expires_at")
        is_expired = False
        if expires_at:
            try:
                is_expired = datetime.fromisoformat(expires_at).timestamp() < now_ts
            except Exception:
                is_expired = False

        if is_expired:
            expired_links += 1
        else:
            active_links += 1

        if not link.get("limit_bytes"):
            unlimited_links += 1

        top_links.append({
            "uid": uid,
            "label": link.get("label", ""),
            "used_bytes": link.get("used_bytes", 0),
            "limit_bytes": link.get("limit_bytes", 0),
            "protocol": link.get("protocol", DEFAULT_PROTOCOL),
        })

    top_links.sort(key=lambda x: x["used_bytes"], reverse=True)

    return {
        "ok": True,
        "series": series,
        "totals": {
            "links": len(LINKS),
            "active_links": active_links,
            "expired_links": expired_links,
            "unlimited_links": unlimited_links,
            "subs": len(SUBS),
            "admins": len(ADMINS) + 1,
        },
        "protocol_distribution": [
            {"protocol": proto, "count": count} for proto, count in protocol_counts.items()
        ],
        "top_links": top_links[:10],
    }


@app.get("/api/reports/export.csv")
async def api_reports_export_csv(token=Depends(require_auth)):
    lines = ["uid,label,protocol,used_bytes,limit_bytes,expires_at,created_at"]

    for uid, link in LINKS.items():
        row = [
            uid,
            str(link.get("label", "")).replace(",", " "),
            link.get("protocol", DEFAULT_PROTOCOL),
            str(link.get("used_bytes", 0)),
            str(link.get("limit_bytes", 0)),
            str(link.get("expires_at", "") or ""),
            str(link.get("created_at", "") or ""),
        ]
        lines.append(",".join(row))

    csv_content = "\n".join(lines)

    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=vodiwalker-links-report.csv"},
    )


# ============================================================
# GLOBAL ERROR HANDLER
# ============================================================

@app.exception_handler(Exception)
async def global_exception_handler(
    request: Request,
    exc: Exception,
):

    stats[
        "total_errors"
    ] += 1

    error_logs.append(
        {
            "error": str(exc) or "internal server error",
            "path": str(request.url.path),
            "method": request.method,
            "source": "server",
            "level": "err",
            "time": datetime.now().isoformat(),
        }
    )

    logger.exception(
        "Unhandled exception: %s %s",
        request.method,
        request.url,
    )

    # API requests
    if (
        request.url.path.startswith(
            "/api/"
        )
        or request.url.path == "/stats"
    ):

        return JSONResponse(
            {
                "ok": False,
                "error":
                    str(exc)
                or "internal server error",
            },
            status_code=500,
        )

    return HTMLResponse(
        """
        <html lang="fa" dir="rtl">
        <body style="
            background:#07070a;
            color:#fff;
            font-family:sans-serif;
            padding:40px;
        ">
            <h2>
            خطای داخلی VodiWalker
            </h2>

            <p>
            لطفاً لاگ Railway را بررسی کنید.
            </p>
        </body>
        </html>
        """,
        status_code=500,
    )


# ============================================================
# MAIN
# ============================================================

try:
    import shop  # noqa: E402,F401 — فروشگاه: مینی‌اپ مشتری/ادمین، تست رایگان، Stars
except Exception as _shop_err:
    logger.warning(f"shop module failed to load: {_shop_err}")


if __name__ == "__main__":

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        workers=1,
    )
