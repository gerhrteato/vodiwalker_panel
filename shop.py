# VodiWalker Shop v2 — مینی‌اپ مشتری + مینی‌اپ ادمین (فقط تست رایگان، بدون فروش)
import asyncio, hashlib, hmac, json, re, secrets, time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl

from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse

from main import (app, DATA_DIR, LINKS, LINKS_LOCK, make_link, remove_link, get_host, get_scheme, get_public_base,
                  vless_link_for_link, parse_size_to_bytes, save_state, bump_daily_stat, get_support_username,
                  DEFAULT_PROTOCOL, LIVE_PROTOCOLS, logger)

HERE = Path(__file__).parent
FILE = DATA_DIR / "vodiwalker_shop.json"
LOCK = asyncio.Lock()
DEFAULTS = {
    "brand": "VodiWalker",
    "theme": {"accent": "#9b5cff", "accent2": "#37d6ff", "mode": "dark", "glass": 60, "radius": 18},
    "trial": {"enabled": True, "gb": 1, "days": 1, "speed_mbps": 0, "ip_limit": 1, "per_user": 1, "channel": ""},
    "texts": {"welcome": "👋 به VodiWalker خوش اومدی!\nاینترنت سریع و پایدار با تحویل آنی.",
              "trial_ok": "🎁 تست رایگان شما فعال شد!",
              "announce": "", "guide": "۱) از تب «سرویس‌ها» لینک ساب رو کپی کن.\n۲) در v2rayNG / Streisand / Hiddify گزینه‌ی افزودن از کلیپ‌بورد رو بزن.\n۳) متصل شو و لذت ببر."},
    "announce_on": False, "maintenance": False,
    "buttons": [], "users": {}, "trials": {}, "bonus": {}, "admins": [],
}
CFG: dict = {}


def _load():
    CFG.clear(); CFG.update(json.loads(json.dumps(DEFAULTS)))
    try:
        for k, v in json.loads(FILE.read_text("utf-8")).items():
            if isinstance(v, dict) and isinstance(CFG.get(k), dict): CFG[k].update(v)
            else: CFG[k] = v
    except Exception:
        pass


def _save_sync():
    tmp = FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(CFG, ensure_ascii=False, indent=1), "utf-8"); tmp.replace(FILE)


async def save():
    async with LOCK:
        await asyncio.to_thread(_save_sync)

_load()


def _tb():
    import telegram_bot
    return telegram_bot


# ── احراز هویت initData ──
def tg_user(request: Request) -> dict:
    token = _tb().BOT_TOKEN
    try:
        pairs = dict(parse_qsl(request.headers.get("x-tg-init", ""), keep_blank_values=True))
        got = pairs.pop("hash", "")
        check = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
        secret = hmac.new(b"WebAppData", token.encode(), hashlib.sha256).digest()
        if not token or not hmac.compare_digest(hmac.new(secret, check.encode(), hashlib.sha256).hexdigest(), got) \
                or time.time() - int(pairs.get("auth_date", "0")) > 86400:
            raise ValueError
        u = json.loads(pairs["user"]); u["id"] = int(u["id"]); return u
    except Exception:
        raise HTTPException(401, "احراز هویت تلگرام نامعتبر است")


def is_owner(uid) -> bool:
    return int(uid) in _tb().ADMIN_IDS


def is_admin(uid) -> bool:
    return is_owner(uid) or int(uid) in {int(x) for x in CFG["admins"]}


def touch_user(u: dict) -> bool:
    new = str(u["id"]) not in CFG["users"]
    r = CFG["users"].setdefault(str(u["id"]), {"first": datetime.now().isoformat(), "banned": False})
    r.update(name=(u.get("first_name") or "")[:40], username=u.get("username") or "", last=datetime.now().isoformat())
    return new


def item(uid, l, host):
    exp, days = l.get("expires_at"), None
    try: days = max(0, int((datetime.fromisoformat(str(exp)) - datetime.now()).total_seconds() // 86400)) if exp else None
    except Exception: pass
    return {"id": uid, "label": l.get("label", "?"), "active": bool(l.get("active", True)),
            "used": int(l.get("used_bytes") or 0), "limit": int(l.get("limit_bytes") or 0), "days": days,
            "link": vless_link_for_link(l, uid, host), "sub": f"{get_scheme()}://{host}/sub/{uid}",
            "trial": str(l.get("note", "")).endswith(":trial")}


def mine(tid):
    return [(u, l) for u, l in LINKS.items() if str(l.get("note", "")).split(":")[:2] == ["tg", str(tid)]]


def trial_quota(tid) -> int:
    t = CFG["trial"]
    return 0 if not t["per_user"] else int(t["per_user"]) + int(CFG["bonus"].get(str(tid), 0))


def trial_used(tid) -> bool:
    q = trial_quota(tid)
    return bool(q) and len(CFG["trials"].get(str(tid), [])) >= q


# ── تست رایگان ──
async def make_trial(u: dict, force=False, gb=None, days=None) -> dict:
    t, tid = CFG["trial"], u["id"]
    if not force:
        if CFG["maintenance"]: raise HTTPException(503, "سرویس موقتاً در حال بروزرسانی است")
        if not t["enabled"]: raise HTTPException(400, "تست رایگان فعلاً غیرفعال است")
        if CFG["users"].get(str(tid), {}).get("banned"): raise HTTPException(403, "دسترسی شما مسدود است")
        if trial_used(tid): raise HTTPException(400, "شما قبلاً از تست رایگان استفاده کرده‌اید")
        ch = str(t.get("channel") or "").strip()
        if ch:
            r = await _tb()._call("getChatMember", chat_id=ch if ch.startswith("@") else "@" + ch, user_id=tid)
            if ((r or {}).get("result") or {}).get("status") not in ("member", "administrator", "creator"):
                raise HTTPException(400, f"ابتدا در کانال {ch} عضو شوید و دوباره تلاش کنید")
    uid, link = await make_link(
        label=f"Test-{u.get('username') or tid}",
        limit_bytes=int(parse_size_to_bytes(float(gb or t["gb"]), "GB")),
        expires_at=(datetime.now() + timedelta(days=float(days or t["days"]))).isoformat(),
        protocol="vless-ws" if "vless-ws" in LIVE_PROTOCOLS else DEFAULT_PROTOCOL,
        ip_limit=int(t["ip_limit"]), speed_limit_bytes=int(float(t["speed_mbps"]) * 1024 * 1024 / 8),
        note=f"tg:{tid}:trial")
    CFG["trials"].setdefault(str(tid), []).append(uid); await save(); bump_daily_stat("new_links")
    return item(uid, link, get_host())


# ── ربات: فقط منوی تست رایگان ──
def _base(path):
    b = get_public_base()
    return f"{b}{path}" if b and b.startswith("https://") else None


def admin_rows():
    a, s = _base("/tma/admin"), _base("/tma/shop")
    return ([[{"text": "🛠 مینی‌اپ ادمین", "web_app": {"url": a}}, {"text": "🚀 مینی‌اپ مشتری", "web_app": {"url": s}}]] if a else []) + \
           [[{"text": "👤 نمای مشتری", "callback_data": "c:menu"}]]


def customer_kb(admin=False):
    rows, s = [], _base("/tma/shop")
    if s: rows.append([{"text": "🚀 باز کردن VodiWalker", "web_app": {"url": s}}])
    r = []
    if CFG["trial"]["enabled"]: r.append({"text": "🎁 تست رایگان", "callback_data": "c:trial"})
    r.append({"text": "📦 سرویس‌های من", "callback_data": "c:my"})
    rows.append(r)
    for b in sorted(CFG["buttons"], key=lambda b: b.get("order", 0)):
        if not b.get("enabled", True) or b.get("place") == "app": continue
        if b["type"] == "url": rows.append([{"text": b["text"], "url": b["value"]}])
        elif b["type"] == "webapp": rows.append([{"text": b["text"], "web_app": {"url": b["value"]}}])
        else: rows.append([{"text": b["text"], "callback_data": f"c:txt:{b['id']}"}])
    sup = str(get_support_username() or "").lstrip("@")
    if sup: rows.append([{"text": "💬 پشتیبانی", "url": f"https://t.me/{sup}"}])
    if admin: rows.append([{"text": "⬅ پنل ادمین", "callback_data": "menu"}])
    return {"inline_keyboard": rows}


async def send_config(chat_id, uid, prefix=""):
    l = LINKS.get(uid)
    if not l: return
    it, tb = item(uid, l, get_host()), _tb()
    exp = "بدون انقضا" if it["days"] is None else f"{it['days']} روز"
    text = (f"{prefix}\n\n" if prefix else "") + f"📦 <b>{tb._h(it['label'])}</b>\nحجم: {tb.fmt_bytes(it['limit']) if it['limit'] else 'نامحدود'} · اعتبار: {exp}\n\n" \
           f"🔗 لینک اتصال:\n<code>{tb._h(it['link'])}</code>\n\n📥 لینک ساب:\n<code>{it['sub']}</code>"
    kb = {"inline_keyboard": [[{"text": "📷 QR", "callback_data": f"c:qr:{uid}"}], [{"text": "⬅ منو", "callback_data": "c:menu"}]]}
    await tb._send(chat_id, text, kb)


async def bot_message(msg: dict):
    frm = msg.get("from") or {}
    if not frm.get("id"): return
    if touch_user(frm): await save()
    if CFG["users"][str(frm["id"])].get("banned"): return
    await _tb()._send(msg["chat"]["id"], CFG["texts"]["welcome"], customer_kb(is_admin(frm["id"])))


async def bot_callback(cb: dict):
    tb, frm, cid, mid, d = _tb(), cb["from"], cb["message"]["chat"]["id"], cb["message"]["message_id"], cb["data"]
    if CFG["users"].get(str(frm["id"]), {}).get("banned"): return
    back = {"inline_keyboard": [[{"text": "⬅ منو", "callback_data": "c:menu"}]]}
    if d == "c:menu":
        await tb._edit(cid, mid, CFG["texts"]["welcome"], customer_kb(is_admin(frm["id"])))
    elif d == "c:trial":
        try:
            it = await make_trial(frm)
            await send_config(cid, it["id"], CFG["texts"]["trial_ok"])
        except HTTPException as e:
            await tb._edit(cid, mid, f"⚠️ {e.detail}", back)
    elif d == "c:my":
        its = mine(frm["id"])
        rows = [[{"text": f"{'🟢' if l.get('active', True) else '🔴'} {l.get('label', '?')[:28]}", "callback_data": f"c:cfg:{u}"}] for u, l in its]
        await tb._edit(cid, mid, "📦 سرویس‌های شما:" if its else "هنوز سرویسی نداری. از تست رایگان شروع کن 👇", {"inline_keyboard": rows + back["inline_keyboard"]})
    elif d.startswith("c:cfg:") and d[6:] in dict(mine(frm["id"])):
        await send_config(cid, d[6:])
    elif d.startswith("c:qr:") and d[5:] in dict(mine(frm["id"])):
        await tb._send_photo(cid, tb._qr_url(item(d[5:], LINKS[d[5:]], get_host())["link"]), "📷 QR کانفیگ")
    elif d.startswith("c:txt:"):
        b = next((b for b in CFG["buttons"] if b["id"] == d[6:]), None)
        if b: await tb._send(cid, b["value"], back)


# سازگاری با telegram_bot قدیمی (پرداخت حذف شده)
async def bot_precheckout(q: dict):
    await _tb()._call("answerPreCheckoutQuery", pre_checkout_query_id=q["id"], ok=False, error_message="خرید غیرفعال است")


async def bot_paid(msg: dict):
    return


# ── صفحات و API ──
def _page(name):
    return HTMLResponse((HERE / name).read_text("utf-8"), headers={"Cache-Control": "no-store"})


@app.get("/tma/shop", response_class=HTMLResponse, include_in_schema=False)
async def page_shop(): return _page("tma_customer.html")


@app.get("/tma/admin", response_class=HTMLResponse, include_in_schema=False)
async def page_admin(): return _page("tma_admin.html")


@app.get("/api/shop/theme", include_in_schema=False)
async def shop_theme(): return CFG["theme"]


@app.post("/api/shop/{action}", include_in_schema=False)
async def shop_api(action: str, request: Request):
    u = tg_user(request)
    try: b = await request.json()
    except Exception: b = {}
    if touch_user(u): await save()
    adm = is_admin(u["id"])
    if CFG["users"][str(u["id"])].get("banned"): raise HTTPException(403, "دسترسی شما مسدود است")
    if CFG["maintenance"] and not adm: raise HTTPException(503, "سرویس موقتاً در حال بروزرسانی است")
    if action == "trial": return {"ok": True, "item": await make_trial(u)}
    if action != "me": raise HTTPException(404, "unknown")
    host, t = get_host(request), CFG["trial"]
    return {"brand": CFG["brand"], "theme": CFG["theme"], "support": get_support_username(),
            "announce": CFG["texts"]["announce"] if CFG["announce_on"] else "", "guide": CFG["texts"]["guide"],
            "user": {"id": u["id"], "name": u.get("first_name", ""), "username": u.get("username", ""), "admin": adm},
            "trial": {"enabled": t["enabled"], "gb": t["gb"], "days": t["days"], "channel": t["channel"], "used": trial_used(u["id"])},
            "buttons": [x for x in sorted(CFG["buttons"], key=lambda x: x.get("order", 0))
                        if x.get("enabled", True) and x["type"] != "text" and x.get("place") != "bot"],
            "mine": [item(i, l, host) for i, l in mine(u["id"])]}


def _num(v, cast=float, lo=0):
    return max(lo, cast(v or 0))


HEX = re.compile(r"^#[0-9a-fA-F]{6}$")


@app.post("/api/shop/admin/{action}", include_in_schema=False)
async def shop_admin_api(action: str, request: Request):
    u = tg_user(request)
    if not is_admin(u["id"]): raise HTTPException(403, "شما ادمین ربات نیستید")
    try: b = await request.json()
    except Exception: b = {}
    host = get_host(request)
    try:
        if action == "state":
            users, today = CFG["users"], datetime.now().date().isoformat()
            used = sum(int(l.get("used_bytes") or 0) for _, l in LINKS.items() if str(l.get("note", "")).startswith("tg:"))
            return {"brand": CFG["brand"], "theme": CFG["theme"], "trial": CFG["trial"], "texts": CFG["texts"],
                    "announce_on": CFG["announce_on"], "maintenance": CFG["maintenance"], "me": u["id"], "owner": is_owner(u["id"]),
                    "buttons": sorted(CFG["buttons"], key=lambda x: x.get("order", 0)),
                    "admins": [{"id": i, "owner": True} for i in sorted(_tb().ADMIN_IDS)] + [{"id": int(i), "owner": False} for i in CFG["admins"]],
                    "stats": {"users": len(users), "banned": sum(1 for x in users.values() if x.get("banned")),
                              "new_today": sum(1 for x in users.values() if str(x.get("first", "")).startswith(today)),
                              "trials": sum(len(x) for x in CFG["trials"].values()),
                              "configs": sum(1 for _ in LINKS.items() if True),
                              "active": sum(1 for _, l in LINKS.items() if l.get("active", True) and str(l.get("note", "")).startswith("tg:")),
                              "used": used}}
        if action == "trial":
            t = CFG["trial"]
            t.update(enabled=bool(b.get("enabled")), gb=_num(b.get("gb")) or 1, days=_num(b.get("days")) or 1,
                     speed_mbps=_num(b.get("speed_mbps")), ip_limit=_num(b.get("ip_limit"), int), per_user=_num(b.get("per_user"), int),
                     channel=str(b.get("channel") or "").strip()[:64])
        elif action == "theme":
            th = CFG["theme"]
            for k in ("accent", "accent2"):
                if HEX.match(str(b.get(k, ""))): th[k] = b[k]
            if b.get("mode") in ("dark", "light", "amoled"): th["mode"] = b["mode"]
            th["glass"] = min(100, max(0, int(b.get("glass", th["glass"]))))
            th["radius"] = min(30, max(8, int(b.get("radius", th["radius"]))))
        elif action == "flags":
            if "maintenance" in b: CFG["maintenance"] = bool(b["maintenance"])
            if "announce_on" in b: CFG["announce_on"] = bool(b["announce_on"])
        elif action == "btn_save":
            typ, val = str(b.get("type")), str(b.get("value") or "").strip()[:1000]
            if typ not in ("url", "webapp", "text") or not str(b.get("text") or "").strip() or not val: raise ValueError("text")
            if typ != "text" and not val.startswith("https://"): raise ValueError("url")
            bid = str(b.get("id") or "") or secrets.token_hex(3)
            place = b.get("place") if b.get("place") in ("both", "bot", "app") else "both"
            row = {"id": bid, "text": str(b["text"]).strip()[:40], "type": typ, "value": val, "place": place,
                   "enabled": bool(b.get("enabled", True)),
                   "order": int(b.get("order") or 0) or (max([x.get("order", 0) for x in CFG["buttons"]] or [0]) + 1)}
            CFG["buttons"] = [x for x in CFG["buttons"] if x["id"] != bid] + [row]
        elif action == "btn_del":
            CFG["buttons"] = [x for x in CFG["buttons"] if x["id"] != str(b.get("id"))]
        elif action == "btn_move":
            bs = sorted(CFG["buttons"], key=lambda x: x.get("order", 0))
            i = next((k for k, x in enumerate(bs) if x["id"] == str(b.get("id"))), None)
            j = None if i is None else i + (-1 if b.get("dir") == "up" else 1)
            if j is not None and 0 <= j < len(bs): bs[i], bs[j] = bs[j], bs[i]
            for k, x in enumerate(bs): x["order"] = k + 1
        elif action == "texts":
            CFG["brand"] = str(b.get("brand") or "VodiWalker")[:30]
            for k in ("welcome", "trial_ok", "announce", "guide"):
                if k in b: CFG["texts"][k] = str(b[k])[:2000]
        elif action == "users":
            q, flt = str(b.get("q") or "").lower().lstrip("@"), str(b.get("f") or "")
            rows = [{"id": k, **v, "trials": len(CFG["trials"].get(k, [])), "configs": len(mine(k)), "bonus": CFG["bonus"].get(k, 0),
                     "admin": is_admin(k)} for k, v in CFG["users"].items()
                    if (not q or q in k or q in str(v.get("username", "")).lower() or q in str(v.get("name", "")).lower())
                    and (flt != "banned" or v.get("banned")) and (flt != "trial" or CFG["trials"].get(k))]
            return {"users": sorted(rows, key=lambda x: x.get("last", ""), reverse=True)[:80]}
        elif action == "user":
            k = str(b.get("id"))
            if k not in CFG["users"]: raise HTTPException(404, "کاربر پیدا نشد")
            return {"user": {"id": k, **CFG["users"][k], "bonus": CFG["bonus"].get(k, 0), "trials": len(CFG["trials"].get(k, [])),
                             "admin": is_admin(k)}, "configs": [item(i, l, host) for i, l in mine(k)]}
        elif action == "ban":
            k = str(b.get("id"))
            r = CFG["users"].get(k)
            if not r: raise HTTPException(404, "کاربر پیدا نشد")
            if is_admin(k): raise HTTPException(400, "ادمین را نمی‌توان مسدود کرد")
            r["banned"] = not r.get("banned")
        elif action == "bonus":   # افزایش سقف تست یک کاربر (+n) یا ریست کامل
            k = str(b.get("id"))
            if k not in CFG["users"]: raise HTTPException(404, "کاربر پیدا نشد")
            if b.get("reset"): CFG["trials"].pop(k, None); CFG["bonus"].pop(k, None)
            else: CFG["bonus"][k] = max(0, int(CFG["bonus"].get(k, 0)) + int(b.get("n", 1)))
        elif action == "grant":   # ساخت تست دستی برای کاربر با حجم/مدت دلخواه
            k = str(b.get("id"))
            r = CFG["users"].get(k)
            if not r: raise HTTPException(404, "کاربر پیدا نشد")
            it = await make_trial({"id": int(k), "username": r.get("username", "")}, True, _num(b.get("gb")) or None, _num(b.get("days")) or None)
            try: await _tb()._send(int(k), "🎁 ادمین یک سرویس تست برای شما فعال کرد. داخل مینی‌اپ ببینیدش.")
            except Exception: pass
            return {"ok": True, "item": it}
        elif action in ("cfg_add", "cfg_toggle", "cfg_del"):
            uid = str(b.get("uid"))
            if uid not in LINKS: raise HTTPException(404, "کانفیگ پیدا نشد")
            if action == "cfg_del":
                await remove_link(uid)
            else:
                async with LINKS_LOCK:
                    l = LINKS[uid]
                    if action == "cfg_toggle": l["active"] = not l.get("active", True)
                    else:
                        gb, days = float(b.get("gb") or 0), float(b.get("days") or 0)
                        if gb and l.get("limit_bytes"): l["limit_bytes"] = max(0, int(l["limit_bytes"]) + int(parse_size_to_bytes(gb, "GB")))
                        elif gb: l["limit_bytes"] = int(parse_size_to_bytes(gb, "GB"))
                        if days:
                            try: base = max(datetime.fromisoformat(str(l.get("expires_at"))), datetime.now()) if l.get("expires_at") else datetime.now()
                            except Exception: base = datetime.now()
                            l["expires_at"] = (base + timedelta(days=days)).isoformat()
                        if b.get("reset_usage"): l["used_bytes"] = 0
                        l["active"] = True
                await save_state()
        elif action == "msg":
            text = str(b.get("text") or "").strip()
            if not text: raise ValueError("text")
            await _tb()._send(int(b["id"]), text)
        elif action == "admin_add":
            if not is_owner(u["id"]): raise HTTPException(403, "فقط ادمین اصلی")
            n = int(str(b.get("id")).strip())
            if n not in CFG["admins"]: CFG["admins"].append(n)
        elif action == "admin_del":
            if not is_owner(u["id"]): raise HTTPException(403, "فقط ادمین اصلی")
            CFG["admins"] = [x for x in CFG["admins"] if int(x) != int(b.get("id"))]
        elif action == "broadcast":
            text, ids = str(b.get("text") or "").strip(), [k for k, v in CFG["users"].items() if not v.get("banned")]
            if not text: raise ValueError("text")
            async def _run():
                for k in ids:
                    await _tb()._send(int(k), text); await asyncio.sleep(0.05)
            asyncio.create_task(_run())
            return {"ok": True, "queued": len(ids)}
        else:
            raise HTTPException(404, "unknown")
    except (KeyError, ValueError, TypeError):
        raise HTTPException(400, "ورودی نامعتبر است")
    await save()
    return {"ok": True}
