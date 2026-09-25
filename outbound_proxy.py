# outbound_proxy.py
# ══════════════════════════════════════════════════════════════════════════════
# «پراکسی IP (SOCKS)» — به هر کانفیگ می‌شه گفت ترافیک خروجی‌اش (اتصال به مقصد)
# به‌جای مستقیم از روی Railway، از یک SOCKS5 در یک کشور دیگه رد بشه. این ماژول:
#   ۱) لیست پراکسی‌ها رو نگه می‌داره (فایل JSON، مثل بقیه‌ی استورهای پروژه)
#   ۲) تست واقعی می‌کنه: یک تونل SOCKS5 واقعی باز می‌کنه، پینگ «از داخل پراکسی»
#      رو اندازه می‌گیره و IP/کشور/پرچم «خروجی» (نه آدرس ورودی پراکسی) رو برمی‌گردونه
#   ۳) open_target_connection(...) — تابع مشترکی که relay_vless.py / tcp_relay.py /
#      xhttp_siz10.py به‌جاش asyncio.open_connection مستقیم صدا می‌زدن استفاده می‌کنن.
#
# رفتار وقتی پراکسی از کار افتاده:
#   پیش‌فرض: اتصال «رد» می‌شه (fail-closed) تا ترافیک بی‌صدا از IP خود Railway
#   خارج نشه. اگه ترجیح می‌دی به حالت مستقیم برگرده: OUTBOUND_FALLBACK_DIRECT=1
# ══════════════════════════════════════════════════════════════════════════════
import asyncio
import re
import json
import os
import secrets
import time
from datetime import datetime
from urllib.parse import unquote, urlparse

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request

from main import (
    DATA_DIR,
    LINKS,
    LINKS_LOCK,
    logger,
    log_activity,
    require_auth,
    safe_int,
    save_state,
    get_session_info,
    SESSION_COOKIE,
    NODE_API_TOKEN_MARK,
)

PROXIES_FILE = DATA_DIR / "vodiwalker_proxies.json"
PROXIES_LOCK = asyncio.Lock()

PROXIES: dict = {}

GEOIP_TIMEOUT = 6.0
TCP_PING_TIMEOUT = 5.0
PROBE_TIMEOUT = 10.0
PROBE_HOST = "ip-api.com"   # فقط HTTP ساده؛ از داخل تونل SOCKS صدا زده می‌شه تا IP/کشور «خروجی» معلوم بشه

MAX_PROXIES = 300           # سقف کل پراکسی‌های ذخیره‌شده در این پنل
SCAN_MAX_CANDIDATES = 60    # حداکثر تعداد کاندید در هر اسکن (برای اینکه درخواست خیلی طول نکشه)
SCAN_MAX_TEXT_BYTES = 500_000
SCAN_CONCURRENCY = 20       # چند تا هم‌زمان تست بشه
SCAN_TCP_TIMEOUT = 3.0      # timeoutهای کوتاه‌تر مخصوص اسکن (تا کل عملیات معطل یکی دو تا کند نمونه)
SCAN_PROBE_TIMEOUT = 6.0
SCAN_URL_MAX_BYTES = 2_000_000

FALLBACK_DIRECT = os.environ.get("OUTBOUND_FALLBACK_DIRECT", "").strip().lower() in ("1", "true", "yes", "on")


def _now_iso() -> str:
    return datetime.now().isoformat()


# کدهای دو حرفی کشور -> ایموجی پرچم (بدون نیاز به هیچ فایل/فونت اضافه)
def _flag_from_country_code(cc: str) -> str:
    cc = (cc or "").strip().upper()
    if len(cc) != 2 or not cc.isalpha():
        return "🏳️"
    base = 0x1F1E6
    return "".join(chr(base + (ord(c) - ord("A"))) for c in cc)


def _load_sync():
    try:
        if PROXIES_FILE.exists():
            data = json.loads(PROXIES_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                PROXIES.clear()
                PROXIES.update(data)
    except Exception as exc:
        logger.warning(f"proxy store load failed: {exc}")


def _save_sync():
    PROXIES_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = PROXIES_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(PROXIES, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(PROXIES_FILE)


def load_proxies():
    _load_sync()


async def save_proxies():
    async with PROXIES_LOCK:
        await asyncio.to_thread(_save_sync)


load_proxies()


def _public(p: dict) -> dict:
    """نسخه‌ی امن برای API/پنل: پسورد هیچ‌وقت به مرورگر برنمی‌گرده."""
    d = dict(p)
    d["has_auth"] = bool(d.get("username"))
    d.pop("password", None)
    return d


def list_proxies() -> list:
    return sorted(PROXIES.values(), key=lambda p: p.get("created_at", ""))


def get_proxy(proxy_id: str):
    return PROXIES.get(proxy_id)


def proxy_summary(proxy_id: str) -> dict | None:
    """خلاصه‌ی کوتاه برای نمایش روی کارت اینباند (نام/کشور/پرچم)."""
    p = PROXIES.get(proxy_id or "")
    if not p:
        return None
    return {
        "id": p.get("id"),
        "name": p.get("name", ""),
        "country": p.get("country", ""),
        "country_code": (p.get("country_code") or "").lower(),
        "flag": p.get("flag", ""),
        "ping_ms": p.get("ping_ms"),
        "test_ok": bool(p.get("test_ok")),
    }


def proxy_usage_counts() -> dict:
    counts: dict = {}
    for link in LINKS.values():
        pid = link.get("outbound_proxy_id") or ""
        if pid:
            counts[pid] = counts.get(pid, 0) + 1
    return counts


def _parse_host_input(host: str, port, username: str, password: str):
    """کاربرها معمولاً کل آدرس رو پیست می‌کنن؛ این فرمت‌ها رو می‌فهمیم:
       socks5://user:pass@host:port   |   host:port   |   host:port:user:pass"""
    host = (host or "").strip()
    if "://" in host:
        u = urlparse(host)
        host = u.hostname or ""
        port = u.port or port
        if u.username:
            username = unquote(u.username)
        if u.password:
            password = unquote(u.password)
    elif host.count(":") == 1:
        h, p = host.split(":")
        if p.strip().isdigit():
            host, port = h.strip(), int(p)
    elif host.count(":") == 3 and "[" not in host:
        h, p, u, pw = host.split(":")
        if p.strip().isdigit():
            host, port, username, password = h.strip(), int(p), u, pw
    return host, port, username, password


async def upsert_proxy(proxy_id: str | None, data: dict) -> dict:
    pid = proxy_id or secrets.token_urlsafe(8)
    existing = PROXIES.get(pid, {})
    host = str(data.get("host") or existing.get("host") or "").strip()[:255]
    port = max(1, min(65535, int(data.get("port") or existing.get("port") or 1080)))
    changed_endpoint = bool(existing) and (host != existing.get("host") or port != existing.get("port"))
    record = {
        "id": pid,
        "name": str(data.get("name") or existing.get("name") or "پراکسی جدید").strip()[:80],
        "host": host,
        "port": port,
        "username": str(data.get("username") if data.get("username") is not None else existing.get("username", "") or "").strip()[:120],
        "password": str(data.get("password") if data.get("password") is not None else existing.get("password", "") or "").strip()[:200],
        "created_at": existing.get("created_at") or _now_iso(),
        # نتیجه‌ی آخرین تست (اگه آدرس عوض شده باشه نتیجه‌ی قدیمی بی‌اعتباره):
        "ping_ms": None if changed_endpoint else existing.get("ping_ms"),
        "tcp_ms": None if changed_endpoint else existing.get("tcp_ms"),
        "exit_ip": "" if changed_endpoint else existing.get("exit_ip", ""),
        "country": "" if changed_endpoint else existing.get("country", ""),
        "country_code": "" if changed_endpoint else existing.get("country_code", ""),
        "flag": "" if changed_endpoint else existing.get("flag", ""),
        "tested_at": None if changed_endpoint else existing.get("tested_at"),
        "test_ok": False if changed_endpoint else existing.get("test_ok", False),
        "test_message": "" if changed_endpoint else existing.get("test_message", ""),
    }
    PROXIES[pid] = record
    await save_proxies()
    return record


async def delete_proxy(proxy_id: str) -> int:
    """پراکسی رو حذف می‌کنه و از همه‌ی کانفیگ‌هایی که ازش استفاده می‌کردن جدا می‌کنه
    (برمی‌گردن به مستقیم). تعداد کانفیگ‌های تغییرکرده رو برمی‌گردونه."""
    PROXIES.pop(proxy_id, None)
    await save_proxies()
    changed = 0
    async with LINKS_LOCK:
        for link in LINKS.values():
            if link.get("outbound_proxy_id") == proxy_id:
                link["outbound_proxy_id"] = ""
                changed += 1
    if changed:
        await save_state()
    return changed


# ══════════════════════════════════════════════════════════════════════════════
# تست واقعی
# ══════════════════════════════════════════════════════════════════════════════

def _make_socks_proxy(proxy: dict):
    """نکته: Proxy.from_url(url, username=..., password=...) در python-socks خطای
    «multiple values for argument 'username'» می‌ده؛ پس مستقیم با create می‌سازیم.
    rdns=True یعنی DNS مقصد روی خود پراکسی resolve بشه (نه روی Railway)."""
    from python_socks import ProxyType
    from python_socks.async_.asyncio import Proxy

    return Proxy.create(
        proxy_type=ProxyType.SOCKS5,
        host=proxy["host"],
        port=int(proxy["port"]),
        username=(proxy.get("username") or None),
        password=(proxy.get("password") or None),
        rdns=True,
    )


async def _tcp_ping(host: str, port: int, timeout: float = TCP_PING_TIMEOUT):
    started = time.perf_counter()
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return True, round((time.perf_counter() - started) * 1000, 1), ""
    except asyncio.TimeoutError:
        return False, None, "Timeout: پراکسی در زمان تعیین‌شده پاسخ نداد."
    except Exception as exc:
        return False, None, f"اتصال ناموفق: {type(exc).__name__}: {str(exc)[:180]}"


async def _read_http_json(reader) -> dict | None:
    buf = b""
    while len(buf) < 16384:
        chunk = await reader.read(4096)
        if not chunk:
            break
        buf += chunk
    text = buf.decode("utf-8", errors="ignore")
    a, b = text.find("{"), text.rfind("}")
    if a == -1 or b <= a:
        return None
    return json.loads(text[a:b + 1])


async def _probe_via_socks(proxy: dict, timeout: float = PROBE_TIMEOUT) -> dict:
    """یک تونل SOCKS5 واقعی به ip-api.com:80 باز می‌کنه.
    connect_ms = زمان دست‌دادن SOCKS5 + اتصال به مقصد «از داخل پراکسی» (پینگ واقعی).
    geo = IP/کشور «خروجی» (چیزی که سایت‌ها می‌بینن)؛ اگه نشد None."""
    px = _make_socks_proxy(proxy)
    started = time.perf_counter()
    sock = await asyncio.wait_for(px.connect(dest_host=PROBE_HOST, dest_port=80), timeout=timeout)
    connect_ms = round((time.perf_counter() - started) * 1000, 1)

    geo = None
    writer = None
    try:
        reader, writer = await asyncio.open_connection(sock=sock)
        req = (
            f"GET /json/?fields=status,country,countryCode,query HTTP/1.1\r\n"
            f"Host: {PROBE_HOST}\r\nUser-Agent: VodiWalker\r\nConnection: close\r\n\r\n"
        ).encode()
        writer.write(req)
        await writer.drain()
        data = await asyncio.wait_for(_read_http_json(reader), timeout=timeout)
        if data and data.get("status") == "success":
            geo = data
    except Exception as exc:
        logger.warning(f"exit geo probe failed: {exc}")
    finally:
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        else:
            try:
                sock.close()
            except Exception:
                pass
    return {"connect_ms": connect_ms, "geo": geo}


async def _geo_lookup_host(host: str) -> dict | None:
    """پشتیبان: اگه از داخل تونل کشور معلوم نشد، کشور «آدرس ورودی» پراکسی رو می‌گیریم."""
    async with httpx.AsyncClient(timeout=GEOIP_TIMEOUT) as client:
        try:
            r = await client.get(f"http://ip-api.com/json/{host}?fields=status,country,countryCode,query")
            j = r.json()
            if j.get("status") == "success":
                return j
        except Exception as exc:
            logger.warning(f"geoip(ip-api) failed for {host}: {exc}")
        try:
            r = await client.get(f"https://ipwho.is/{host}")
            j = r.json()
            if j.get("success"):
                return {"country": j.get("country", ""), "countryCode": j.get("country_code", ""), "query": j.get("ip", "")}
        except Exception as exc:
            logger.warning(f"geoip(ipwho) failed for {host}: {exc}")
    return None


def parse_proxy_line(line: str):
    """یک خط ورودی رو به (host, port, username, password) تبدیل می‌کنه.
    فرمت‌های قبول‌شده: host:port | host:port:user:pass | socks5://user:pass@host:port | host تنها (پورت پیش‌فرض 1080)"""
    line = line.strip().strip(",;")
    if not line or line.startswith("#") or line.startswith("//") or any(ch.isspace() for ch in line):
        return None
    host, port, username, password = "", 1080, "", ""
    if "://" in line:
        u = urlparse(line)
        if u.scheme.lower() not in ("socks5", "socks5h", "socks4", "socks", ""):
            return None
        host = u.hostname or ""
        port = u.port or port
        if u.username:
            username = unquote(u.username)
        if u.password:
            password = unquote(u.password)
    else:
        parts = line.split(":")
        if len(parts) == 1:
            host = parts[0]
        elif len(parts) == 2:
            host, p = parts
            if not p.strip().isdigit():
                return None
            port = int(p)
        elif len(parts) == 4:
            host, p, username, password = parts
            if not p.strip().isdigit():
                return None
            port = int(p)
        else:
            return None
    host = host.strip()
    if not host or not (1 <= port <= 65535):
        return None
    if not re.match(r"^[A-Za-z0-9](?:[A-Za-z0-9\.\-]{0,253}[A-Za-z0-9])?$", host):
        return None
    return host, port, username.strip(), password.strip()


# ══════════════════════════════════════════════════════════════════════════════
# اسکنر پروکسی — لیستی از کاندیدها رو (پیست‌شده یا از یک URL) موازی تست می‌کنه:
# TCP واقعی + دست‌دادن SOCKS5 واقعی + کشور/پرچم IP خروجی. هیچ پراکسی‌ای از خودِ
# اینترنت جمع‌آوری نمی‌شه — کاندیدها همیشه از خود ادمین می‌آیند (پیست یا لینک
# لیستِ سرویس‌دهنده‌ی خودشون)، چون پراکسی‌های «رایگان عمومی» معمولاً یا از قبل
# فیلترن یا واسطه‌ی ناشناسی دارن که امنیت ترافیک کاربرهای پنل رو به خطر می‌ندازه.
# ══════════════════════════════════════════════════════════════════════════════

async def scan_one_candidate(host: str, port: int, username: str, password: str) -> dict:
    tcp_ok, tcp_ms, message = await _tcp_ping(host, port, timeout=SCAN_TCP_TIMEOUT)
    ok, ping_ms, geo = False, None, None
    if tcp_ok:
        try:
            probe = await _probe_via_socks({"host": host, "port": port, "username": username, "password": password}, timeout=SCAN_PROBE_TIMEOUT)
            ok, ping_ms, geo = True, probe["connect_ms"], probe["geo"]
            message = "اتصال SOCKS5 موفق"
        except asyncio.TimeoutError:
            message = "تونل SOCKS5 در زمان تعیین‌شده باز نشد"
        except Exception as exc:
            hint = " (نام کاربری/رمز را چک کن)" if "auth" in str(exc).lower() or "password" in str(exc).lower() else ""
            message = f"دست‌دادن SOCKS5 ناموفق{hint}: {type(exc).__name__}"
    country, country_code, flag, exit_ip = "", "", "", ""
    if geo:
        country = geo.get("country", "") or ""
        country_code = geo.get("countryCode", "") or ""
        exit_ip = geo.get("query", "") or ""
        flag = _flag_from_country_code(country_code) if country_code else "🏳️"
    return {
        "host": host, "port": port, "username": username, "password": password,
        "ok": ok, "ping_ms": ping_ms, "tcp_ms": tcp_ms,
        "country": country, "country_code": country_code, "flag": flag,
        "exit_ip": exit_ip, "message": message,
    }


async def test_proxy(proxy_id: str) -> dict:
    """۱) پینگ TCP خام به پورت پراکسی  ۲) تونل واقعی SOCKS5 + پینگ از داخل پراکسی
    ۳) کشور/پرچم IP خروجی. نتیجه در استور ذخیره می‌شه."""
    proxy = PROXIES.get(proxy_id)
    if not proxy:
        raise ValueError("پراکسی پیدا نشد")
    host, port = proxy["host"], int(proxy["port"])

    tcp_ok, tcp_ms, message = await _tcp_ping(host, port)
    ok, ping_ms, geo = False, None, None
    exit_ip = ""

    if tcp_ok:
        try:
            probe = await _probe_via_socks(proxy)
            ok, ping_ms, geo = True, probe["connect_ms"], probe["geo"]
            message = "تونل SOCKS5 برقرار شد"
        except asyncio.TimeoutError:
            message = "SOCKS5: پراکسی جواب داد ولی تونل در زمان تعیین‌شده باز نشد."
        except Exception as exc:
            hint = " (نام کاربری/رمز را چک کن)" if "auth" in str(exc).lower() or "password" in str(exc).lower() else ""
            message = f"دست‌دادن SOCKS5 ناموفق{hint}: {type(exc).__name__}: {str(exc)[:160]}"

    geo_from_exit = bool(geo)
    if not geo:
        geo = await _geo_lookup_host(host)

    country = proxy.get("country", "") if not geo else (geo.get("country", "") or "")
    country_code = proxy.get("country_code", "") if not geo else (geo.get("countryCode", "") or "")
    if geo:
        exit_ip = geo.get("query", "") if geo_from_exit else ""
    flag = _flag_from_country_code(country_code) if country_code else proxy.get("flag", "")
    if ok and geo_from_exit and exit_ip:
        message = f"تونل SOCKS5 برقرار شد — IP خروجی: {exit_ip}"
    elif ok and not geo_from_exit:
        message += " (کشور از روی آدرس پراکسی حدس زده شد)"

    proxy.update({
        "ping_ms": ping_ms if ok else None,
        "tcp_ms": tcp_ms,
        "exit_ip": exit_ip,
        "country": country,
        "country_code": country_code,
        "flag": flag,
        "tested_at": _now_iso(),
        "test_ok": ok,
        "test_message": message,
    })
    PROXIES[proxy_id] = proxy
    await save_proxies()
    shown = f"{ping_ms}ms" if ok else "قطع"
    log_activity("network", f"تست پراکسی «{proxy['name']}» — {'موفق' if ok else 'ناموفق'} ({shown})", "ok" if ok else "warn")
    return proxy


# ══════════════════════════════════════════════════════════════════════════════
# اتصال واقعی relay ها
# ══════════════════════════════════════════════════════════════════════════════

async def _connect_via_socks(proxy: dict, address: str, port: int):
    """یک اتصال TCP از طریق SOCKS5 پراکسی به مقصد (address, port) باز می‌کنه و
    reader/writer استاندارد asyncio برمی‌گردونه — دقیقاً همون شکلی که
    asyncio.open_connection مستقیم برمی‌گردوند، پس بقیه‌ی کد relay نیازی به تغییر نداره."""
    px = _make_socks_proxy(proxy)
    sock = await px.connect(dest_host=address, dest_port=port)
    return await asyncio.open_connection(sock=sock)


async def open_target_connection(link: dict | None, address: str, port: int, timeout: float = 10.0):
    """جایگزین مستقیم asyncio.open_connection برای همه‌ی relayها. اگه کانفیگ یک
    outbound_proxy_id معتبر داشته باشه، اتصال از طریق اون SOCKS5 پراکسی باز می‌شه؛
    وگرنه (پیش‌فرض) دقیقاً همون رفتار قبلی — اتصال مستقیم از روی Railway."""
    proxy_id = (link or {}).get("outbound_proxy_id") or ""
    proxy = PROXIES.get(proxy_id) if proxy_id else None
    if proxy_id and (not proxy or not proxy.get("host")):
        logger.warning(f"outbound proxy id={proxy_id} not found; using direct")
        proxy = None
    if not proxy:
        return await asyncio.wait_for(asyncio.open_connection(address, port), timeout=timeout)
    try:
        return await asyncio.wait_for(_connect_via_socks(proxy, address, port), timeout=timeout)
    except Exception as exc:
        if FALLBACK_DIRECT:
            logger.warning(f"outbound proxy «{proxy.get('name')}» failed ({exc!r}); falling back to direct")
            return await asyncio.wait_for(asyncio.open_connection(address, port), timeout=timeout)
        raise ConnectionError(f"outbound proxy «{proxy.get('name')}» failed: {type(exc).__name__}: {exc}") from exc


# ══════════════════════════════════════════════════════════════════════════════
# API — مدیریت پراکسی‌ها (تب تنظیمات پنل)
# ══════════════════════════════════════════════════════════════════════════════
router = APIRouter()


async def require_owner(request: Request, token=Depends(require_auth)):
    if token == NODE_API_TOKEN_MARK:   # پنل اصلی که با توکن نود این پنل را مدیریت می‌کند
        return token
    info = await get_session_info(request.cookies.get(SESSION_COOKIE))
    if not info or info.get("admin_id") != "owner":
        raise HTTPException(status_code=403, detail="فقط مالک پنل می‌تواند پراکسی‌ها را مدیریت کند.")
    return token


@router.get("/api/proxies")
async def api_list_proxies(_=Depends(require_auth)):
    counts = proxy_usage_counts()
    out = []
    for p in list_proxies():
        d = _public(p)
        d["in_use"] = counts.get(p["id"], 0)
        out.append(d)
    return {"ok": True, "proxies": out, "fallback_direct": FALLBACK_DIRECT}


@router.post("/api/proxies")
async def api_upsert_proxy(request: Request, _=Depends(require_owner)):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="اطلاعات پراکسی معتبر نیست.")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="اطلاعات پراکسی معتبر نیست.")
    host, port, username, password = _parse_host_input(
        str(body.get("host") or ""),
        body.get("port", 1080),
        str(body.get("username") or ""),
        str(body.get("password") or ""),
    )
    if not host:
        raise HTTPException(status_code=400, detail="آدرس (Host) پراکسی را وارد کنید.")
    proxy_id = str(body.get("id") or "").strip() or None
    if proxy_id and proxy_id not in PROXIES:
        raise HTTPException(status_code=404, detail="پراکسی پیدا نشد")
    if not proxy_id and len(PROXIES) >= MAX_PROXIES:
        raise HTTPException(status_code=400, detail=f"حداکثر {MAX_PROXIES} پراکسی مجاز است")
    record = await upsert_proxy(proxy_id, {
        "name": body.get("name"),
        "host": host,
        "port": safe_int(port, minimum=1, maximum=65535),
        "username": username,
        "password": password,
    })
    log_activity("network", f"پراکسی «{record['name']}» ذخیره شد", "ok")
    return {"ok": True, "proxy": _public(record)}


@router.delete("/api/proxies/{proxy_id}")
async def api_delete_proxy(proxy_id: str, _=Depends(require_owner)):
    if proxy_id not in PROXIES:
        raise HTTPException(status_code=404, detail="پراکسی پیدا نشد")
    name = PROXIES[proxy_id].get("name", proxy_id)
    detached = await delete_proxy(proxy_id)
    log_activity("network", f"پراکسی «{name}» حذف شد ({detached} کانفیگ به مستقیم برگشت)", "warn")
    return {"ok": True, "detached": detached}


@router.post("/api/proxies/{proxy_id}/test")
async def api_test_proxy(proxy_id: str, _=Depends(require_owner)):
    if proxy_id not in PROXIES:
        raise HTTPException(status_code=404, detail="پراکسی پیدا نشد")
    try:
        result = await test_proxy(proxy_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"ok": True, "proxy": _public(result)}


@router.post("/api/proxies/scan")
async def api_scan_proxies(request: Request, _=Depends(require_owner)):
    """یک لیست کاندید (پیست‌شده یا از یک URL) رو موازی تست می‌کنه: TCP واقعی +
    دست‌دادن SOCKS5 واقعی + کشور/پرچم IP خروجی. چیزی ذخیره نمی‌شه؛ برای ذخیره از
    /api/proxies/bulk با همون نتیجه‌ها استفاده کن."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="ورودی معتبر نیست")
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="ورودی معتبر نیست")

    source = str(body.get("source") or "text").strip().lower()
    raw_text = str(body.get("text") or "")[:SCAN_MAX_TEXT_BYTES]

    if source == "url":
        url = str(body.get("url") or "").strip()
        if not url.lower().startswith(("http://", "https://")):
            raise HTTPException(status_code=400, detail="آدرس لیست باید با http:// یا https:// شروع بشه")
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                r = await client.get(url)
                r.raise_for_status()
                raw_text = r.text[:SCAN_URL_MAX_BYTES]
        except httpx.HTTPStatusError as exc:
            raise HTTPException(status_code=400, detail=f"سرور لیست پاسخ {exc.response.status_code} داد")
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"دریافت لیست ناموفق بود: {type(exc).__name__}: {str(exc)[:150]}")

    candidates, seen = [], set()
    for line in raw_text.replace("\r", "\n").replace(",", "\n").split("\n"):
        parsed = parse_proxy_line(line)
        if not parsed:
            continue
        key = (parsed[0], parsed[1])
        if key in seen:
            continue
        seen.add(key)
        candidates.append(parsed)
        if len(candidates) >= SCAN_MAX_CANDIDATES:
            break
    if not candidates:
        raise HTTPException(status_code=400, detail="هیچ پروکسی معتبری در ورودی پیدا نشد (فرمت: host:port یا host:port:user:pass)")

    sem = asyncio.Semaphore(SCAN_CONCURRENCY)

    async def _bounded(host, port, username, password):
        async with sem:
            return await scan_one_candidate(host, port, username, password)

    results = await asyncio.gather(*(_bounded(*c) for c in candidates))
    results = sorted(results, key=lambda r: (not r["ok"], r["ping_ms"] if r["ping_ms"] is not None else 999999))
    working = sum(1 for r in results if r["ok"])
    log_activity("network", f"اسکن پروکسی: {working} از {len(results)} مورد سالم بود", "ok" if working else "warn")
    return {"ok": True, "scanned": len(results), "working": working, "truncated": len(candidates) >= SCAN_MAX_CANDIDATES, "results": results}


@router.post("/api/proxies/bulk")
async def api_bulk_add_proxies(request: Request, _=Depends(require_owner)):
    """چند پراکسی رو یک‌جا ذخیره می‌کنه — برای «افزودن انتخاب‌شده‌ها» بعد از اسکن.
    اگه آیتم همراه نتیجه‌ی تست (ok/ping_ms/...) اومده باشه، همون کش می‌شه (نیازی
    به تست دوباره نیست)."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="ورودی معتبر نیست")
    items = (body or {}).get("items")
    if not isinstance(items, list) or not items:
        raise HTTPException(status_code=400, detail="حداقل یک پراکسی انتخاب کن")
    if len(items) > 50:
        raise HTTPException(status_code=400, detail="حداکثر ۵۰ مورد در هر بار")
    if len(PROXIES) >= MAX_PROXIES:
        raise HTTPException(status_code=400, detail=f"حداکثر {MAX_PROXIES} پراکسی مجاز است")

    existing = {(p["host"], p["port"]) for p in PROXIES.values()}
    added, skipped = [], 0
    for it in items:
        if not isinstance(it, dict) or len(PROXIES) >= MAX_PROXIES:
            continue
        host = str(it.get("host") or "").strip()
        try:
            port = max(1, min(65535, int(it.get("port") or 1080)))
        except (TypeError, ValueError):
            continue
        if not host or (host, port) in existing:
            skipped += 1
            continue
        record = await upsert_proxy(None, {
            "name": str(it.get("name") or it.get("country") or host)[:80],
            "host": host, "port": port,
            "username": it.get("username", ""), "password": it.get("password", ""),
        })
        if it.get("ok"):
            record.update({
                "ping_ms": it.get("ping_ms"), "tcp_ms": it.get("tcp_ms"),
                "country": str(it.get("country") or ""), "country_code": str(it.get("country_code") or ""),
                "flag": str(it.get("flag") or ""), "exit_ip": str(it.get("exit_ip") or ""),
                "tested_at": _now_iso(), "test_ok": True, "test_message": str(it.get("message") or "از اسکن"),
            })
            PROXIES[record["id"]] = record
        existing.add((host, port))
        added.append(record["id"])

    if added:
        await save_proxies()
        msg = f"{len(added)} پراکسی از اسکن اضافه شد" + (f" ({skipped} مورد تکراری رد شد)" if skipped else "")
        log_activity("network", msg, "ok")
    return {"ok": True, "added": len(added), "skipped": skipped, "proxies": [_public(PROXIES[i]) for i in added]}
