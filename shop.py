# VodiWalker Shop — مینی‌اپ مشتری + مینی‌اپ ادمین، تست رایگان، پرداخت Stars، دکمه‌های سفارشی
import asyncio, hashlib, hmac, json, secrets, time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qsl

from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse

from main import (app, DATA_DIR, LINKS, make_link, get_host, get_scheme, get_public_base, vless_link_for_link,
                  parse_size_to_bytes, save_state, bump_daily_stat, get_support_username,
                  DEFAULT_PROTOCOL, LIVE_PROTOCOLS, logger)
import sales

HERE = Path(__file__).parent
FILE = DATA_DIR / "vodiwalker_shop.json"
LOCK = asyncio.Lock()
DEFAULTS = {
    "brand": "VodiWalker",
    "trial": {"enabled": True, "gb": 1, "days": 1, "speed_mbps": 0, "ip_limit": 1, "per_user": 1, "channel": ""},
    "texts": {"welcome": "👋 به VodiWalker خوش اومدی!\nاینترنت سریع و پایدار با تحویل آنی.",
              "trial_ok": "🎁 تست رایگان شما فعال شد!", "paid_ok": "✅ پرداخت موفق بود. سرویس شما آماده است:"},
    "buttons": [], "users": {}, "trials": {},
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


# ── احراز هویت initData (برای همه‌ی کاربران؛ ادمین از روی TELEGRAM_ADMIN_IDS) ──
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


def is_admin(uid) -> bool:
    return int(uid) in _tb().ADMIN_IDS


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


# ── تست رایگان ──
async def make_trial(u: dict) -> dict:
    t, tid = CFG["trial"], u["id"]
    if not t["enabled"]: raise HTTPException(400, "تست رایگان فعلاً غیرفعال است")
    if CFG["users"].get(str(tid), {}).get("banned"): raise HTTPException(403, "دسترسی شما مسدود است")
    if t["per_user"] and len(CFG["trials"].get(str(tid), [])) >= int(t["per_user"]):
        raise HTTPException(400, "شما قبلاً از تست رایگان استفاده کرده‌اید")
    ch = str(t.get("channel") or "").strip()
    if ch:
        r = await _tb()._call("getChatMember", chat_id=ch if ch.startswith("@") else "@" + ch, user_id=tid)
        if ((r or {}).get("result") or {}).get("status") not in ("member", "administrator", "creator"):
            raise HTTPException(400, f"ابتدا در کانال {ch} عضو شوید و دوباره تلاش کنید")
    uid, link = await make_link(
        label=f"Test-{u.get('username') or tid}",
        limit_bytes=int(parse_size_to_bytes(float(t["gb"]), "GB")),
        expires_at=(datetime.now() + timedelta(days=float(t["days"]))).isoformat(),
        protocol="vless-ws" if "vless-ws" in LIVE_PROTOCOLS else DEFAULT_PROTOCOL,
        ip_limit=int(t["ip_limit"]), speed_limit_bytes=int(float(t["speed_mbps"]) * 1024 * 1024 / 8),
        note=f"tg:{tid}:trial")
    CFG["trials"].setdefault(str(tid), []).append(uid); await save(); bump_daily_stat("new_links")
    return item(uid, link, get_host())


# ── پرداخت Stars ──
def _invoice_args(plan, tid):
    return dict(title=f"{plan['name']} · {plan['days']} روز",
                description=f"{plan['volume_gb']:g}GB · {plan['speed_mbps']}Mbps · {plan['ip_limit']} کاربر",
                payload=sales.create_payload(plan["id"], tid), currency="XTR",
                prices=[{"label": plan["name"], "amount": int(plan["stars"])}])


async def invoice_link(u: dict, plan_id: str) -> str:
    p = sales.get_plan(plan_id)
    if not p: raise HTTPException(404, "پلن پیدا نشد")
    link = ((await _tb()._call("createInvoiceLink", **_invoice_args(p, u["id"]))) or {}).get("result")
    if not link: raise HTTPException(502, "ساخت فاکتور ناموفق بود")
    return link


async def bot_precheckout(q: dict):
    ok = False
    try:
        _, plan_id, tid, _r = q["invoice_payload"].split("|")
        ok = bool(sales.get_plan(plan_id)) and int(tid) == q["from"]["id"] and not CFG["users"].get(tid, {}).get("banned")
    except Exception:
        pass
    await _tb()._call("answerPreCheckoutQuery", pre_checkout_query_id=q["id"], ok=ok,
                      **({} if ok else {"error_message": "پلن نامعتبر است"}))


async def bot_paid(msg: dict):
    sp, frm = msg["successful_payment"], msg["from"]
    cid = sp.get("telegram_payment_charge_id", "")
    try:
        _, plan_id, _t, _r = sp["invoice_payload"].split("|")
        if any(o.get("telegram_charge_id") == cid for o in sales.SALES["orders"]): return
        _o, _l, uid, _s = await sales.fulfill_payment(frm, plan_id, cid)
        LINKS[uid]["note"] = f"tg:{frm['id']}"; await save_state()
        await send_config(frm["id"], uid, CFG["texts"]["paid_ok"])
    except Exception as e:
        logger.warning(f"shop payment error: {e}")
        await _tb()._send(frm["id"], f"⚠️ پرداخت ثبت شد ولی ساخت سرویس خطا داد. با پشتیبانی تماس بگیر.\nکد: <code>{cid}</code>")
        for a in _tb().ADMIN_IDS:
            await _tb()._send(a, f"⚠️ خطای تحویل سفارش کاربر {frm['id']}\nکد: <code>{cid}</code>\n{e}")


# ── ربات: منوی مشتری ──
def _base(path):
    b = get_public_base()
    return f"{b}{path}" if b and b.startswith("https://") else None


def admin_rows():
    a, s = _base("/tma/admin"), _base("/tma/shop")
    return ([[{"text": "🛠 مینی‌اپ ادمین", "web_app": {"url": a}}, {"text": "🛍 مینی‌اپ مشتری", "web_app": {"url": s}}]] if a else []) + \
           [[{"text": "👤 نمای مشتری", "callback_data": "c:menu"}]]


def customer_kb(admin=False):
    rows, s = [], _base("/tma/shop")
    if s: rows.append([{"text": "🚀 مینی‌اپ ما", "web_app": {"url": s}}])
    r = [{"text": "🛒 خرید اشتراک", "callback_data": "c:plans"}]
    if CFG["trial"]["enabled"]: r.insert(0, {"text": "🎁 تست رایگان", "callback_data": "c:trial"})
    rows += [r, [{"text": "📦 سرویس‌های من", "callback_data": "c:my"}]]
    for b in sorted(CFG["buttons"], key=lambda b: b.get("order", 0)):
        if not b.get("enabled", True): continue
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
    elif d == "c:plans":
        rows = [[{"text": f"⭐{p['stars']} · {p['name']} · {p['days']} روز · {p['volume_gb']:g}GB", "callback_data": f"c:buy:{p['id']}"}] for p in sales.list_plans()]
        await tb._edit(cid, mid, "🛒 <b>پلن مورد نظرت رو انتخاب کن</b>\nپرداخت با ⭐ Stars تلگرام، تحویل آنی.", {"inline_keyboard": rows + back["inline_keyboard"]})
    elif d.startswith("c:buy:"):
        p = sales.get_plan(d[6:])
        if p: await tb._call("sendInvoice", chat_id=cid, **_invoice_args(p, frm["id"]))
    elif d == "c:my":
        its = mine(frm["id"])
        rows = [[{"text": f"{'🟢' if l.get('active', True) else '🔴'} {l.get('label', '?')[:28]}", "callback_data": f"c:cfg:{u}"}] for u, l in its]
        await tb._edit(cid, mid, "📦 سرویس‌های شما:" if its else "هنوز سرویسی نداری. از تست رایگان یا خرید شروع کن 👇", {"inline_keyboard": rows + back["inline_keyboard"]})
    elif d.startswith("c:cfg:") and d[6:] in dict(mine(frm["id"])):
        await send_config(cid, d[6:])
    elif d.startswith("c:qr:") and d[5:] in dict(mine(frm["id"])):
        await tb._send_photo(cid, tb._qr_url(item(d[5:], LINKS[d[5:]], get_host())["link"]), "📷 QR کانفیگ")
    elif d.startswith("c:txt:"):
        b = next((b for b in CFG["buttons"] if b["id"] == d[6:]), None)
        if b: await tb._send(cid, b["value"], back)


# ── صفحات و API ──
def _page(name):
    return HTMLResponse((HERE / name).read_text("utf-8"), headers={"Cache-Control": "no-store"})


@app.get("/tma/shop", response_class=HTMLResponse, include_in_schema=False)
async def page_shop(): return _page("tma_customer.html")


@app.get("/tma/admin", response_class=HTMLResponse, include_in_schema=False)
async def page_admin(): return _page("tma_admin.html")


@app.post("/api/shop/{action}", include_in_schema=False)
async def shop_api(action: str, request: Request):
    u = tg_user(request)
    try: b = await request.json()
    except Exception: b = {}
    if touch_user(u): await save()
    if CFG["users"][str(u["id"])].get("banned"): raise HTTPException(403, "دسترسی شما مسدود است")
    if action == "trial": return {"ok": True, "item": await make_trial(u)}
    if action == "invoice": return {"ok": True, "link": await invoice_link(u, str(b.get("plan")))}
    if action != "me": raise HTTPException(404, "unknown")
    host, t = get_host(request), CFG["trial"]
    return {"brand": CFG["brand"], "welcome": CFG["texts"]["welcome"], "support": get_support_username(),
            "user": {"id": u["id"], "name": u.get("first_name", ""), "admin": is_admin(u["id"])},
            "trial": {"enabled": t["enabled"], "gb": t["gb"], "days": t["days"], "channel": t["channel"],
                      "used": bool(t["per_user"]) and len(CFG["trials"].get(str(u["id"]), [])) >= int(t["per_user"])},
            "plans": sales.list_plans(),
            "buttons": [x for x in sorted(CFG["buttons"], key=lambda x: x.get("order", 0)) if x.get("enabled", True) and x["type"] != "text"],
            "mine": [item(i, l, host) for i, l in mine(u["id"])]}


def _num(v, cast=float, lo=0):
    return max(lo, cast(v or 0))


@app.post("/api/shop/admin/{action}", include_in_schema=False)
async def shop_admin_api(action: str, request: Request):
    u = tg_user(request)
    if not is_admin(u["id"]): raise HTTPException(403, "شما ادمین ربات نیستید")
    try: b = await request.json()
    except Exception: b = {}
    try:
        if action == "state":
            ss, users = sales.sales_stats(), CFG["users"]
            return {"brand": CFG["brand"], "trial": CFG["trial"], "texts": CFG["texts"], "plans": sales.list_plans(),
                    "buttons": sorted(CFG["buttons"], key=lambda x: x.get("order", 0)),
                    "stats": {**ss, "users": len(users), "banned": sum(1 for x in users.values() if x.get("banned")),
                              "trials": sum(len(x) for x in CFG["trials"].values())},
                    "orders": [{k: o.get(k) for k in ("username", "user_id", "plan_id", "amount_stars", "created_at")} for o in sales.SALES["orders"][:40]]}
        if action == "trial":
            t = CFG["trial"]
            t.update(enabled=bool(b.get("enabled")), gb=_num(b.get("gb")) or 1, days=_num(b.get("days")) or 1,
                     speed_mbps=_num(b.get("speed_mbps")), ip_limit=_num(b.get("ip_limit"), int), per_user=_num(b.get("per_user"), int),
                     channel=str(b.get("channel") or "").strip()[:64])
        elif action == "plan_save":
            pid = str(b.get("id") or "").strip().lower() or "p" + secrets.token_hex(3)
            old = sales.PLANS.get(pid, {})
            await sales.upsert_plan(pid, {"id": pid, "name": str(b["name"])[:30], "days": max(1, int(b["days"])), "volume_gb": _num(b["volume_gb"]),
                                          "speed_mbps": _num(b.get("speed_mbps"), int), "ip_limit": _num(b.get("ip_limit"), int), "stars": max(1, int(b["stars"])),
                                          "badge": str(b.get("badge") or "")[:24], "featured": bool(b.get("featured")),
                                          "order": int(b.get("order") or old.get("order") or len(sales.PLANS) + 1)})
        elif action == "plan_del":
            await sales.delete_plan(str(b.get("id")))
        elif action == "btn_save":
            typ, val = str(b.get("type")), str(b.get("value") or "").strip()[:1000]
            if typ not in ("url", "webapp", "text") or not str(b.get("text") or "").strip() or not val: raise ValueError("text")
            if typ != "text" and not val.startswith("https://"): raise ValueError("url")
            bid = str(b.get("id") or "") or secrets.token_hex(3)
            row = {"id": bid, "text": str(b["text"]).strip()[:40], "type": typ, "value": val, "enabled": bool(b.get("enabled", True)),
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
            for k in ("welcome", "trial_ok", "paid_ok"):
                if b.get(k): CFG["texts"][k] = str(b[k])[:2000]
        elif action == "users":
            q = str(b.get("q") or "").lower()
            rows = [{"id": k, **v, "trials": len(CFG["trials"].get(k, [])), "configs": len(mine(k))} for k, v in CFG["users"].items()
                    if not q or q in k or q in str(v.get("username", "")).lower() or q in str(v.get("name", "")).lower()]
            return {"users": sorted(rows, key=lambda x: x.get("last", ""), reverse=True)[:80]}
        elif action == "ban":
            r = CFG["users"].get(str(b.get("id")))
            if not r: raise HTTPException(404, "کاربر پیدا نشد")
            r["banned"] = not r.get("banned")
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
