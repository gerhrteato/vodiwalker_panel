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


async def _tcp_ping(host: str, port: int):
    started = time.perf_counter()
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=TCP_PING_TIMEOUT)
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


async def _probe_via_socks(proxy: dict) -> dict:
    """یک تونل SOCKS5 واقعی به ip-api.com:80 باز می‌کنه.
    connect_ms = زمان دست‌دادن SOCKS5 + اتصال به مقصد «از داخل پراکسی» (پینگ واقعی).
    geo = IP/کشور «خروجی» (چیزی که سایت‌ها می‌بینن)؛ اگه نشد None."""
    px = _make_socks_proxy(proxy)
    started = time.perf_counter()
    sock = await asyncio.wait_for(px.connect(dest_host=PROBE_HOST, dest_port=80), timeout=PROBE_TIMEOUT)
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
        data = await asyncio.wait_for(_read_http_json(reader), timeout=PROBE_TIMEOUT)
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
