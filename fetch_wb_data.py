#!/usr/bin/env python3
"""
Забирает данные из Wildberries Statistics API и готовит данные для дашборда.

Работает ИНКРЕМЕНТАЛЬНО: помнит, докуда дочитал (wb_cache.json), и в каждом
запуске просит у WB только изменения с прошлого раза.

Особенность лимитов WB: «global limiter» кабинета часто пропускает один запрос,
а следующий блокирует на часы (лимит делят все сервисы аналитики). Поэтому за
запуск мы гарантированно делаем ОДИН запрос — к тому источнику, что сильнее
устарел (заказы или продажи), — а второй пробуем только если WB не против.
Два запуска в день по расписанию закрывают оба источника.

Использование:
    WB_TOKEN="..." python3 fetch_wb_data.py --inject index.html
    WB_TOKEN="..." python3 fetch_wb_data.py --full --days 25   # перечитать всё заново
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://statistics-api.wildberries.ru/api/v1/supplier"
MP_BASE = "https://marketplace-api.wildberries.ru/api/v3"
MSK = timezone(timedelta(hours=3))
CACHE = "wb_cache.json"
MAX_WAIT_SEC = 3 * 3600      # первый запрос за запуск готовы ждать до 3 часов
SKIP_WAIT_SEC = 900          # последующие: если WB просит больше 15 мин — оставим на следующий запуск
KEEP_DAYS = 90               # глубина хранения в кэше
_WAITED = 0
_FIRST = True


def api_get(path: str, token: str, date_from: str):
    """Все страницы с lastChangeDate >= date_from. None — если лимит не дождались."""
    global _WAITED, _FIRST
    rows, cursor = [], date_from
    while True:
        url = f"{BASE}/{path}?" + urllib.parse.urlencode({"dateFrom": cursor, "flag": 0})
        req = urllib.request.Request(url, headers={"Authorization": token})
        try:
            with urllib.request.urlopen(req, timeout=120) as r:
                batch = json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry = int(e.headers.get("X-Ratelimit-Retry") or 65)
                if not _FIRST and retry > SKIP_WAIT_SEC:
                    print(f"  {path}: WB просит {retry}с — это уже не первый запрос, оставляем на следующий запуск", flush=True)
                    return rows or None
                if _WAITED + retry > MAX_WAIT_SEC:
                    print(f"  {path}: лимит кабинета занят ещё {retry}с (уже ждали {_WAITED}с) — "
                          f"пропускаем до следующего запуска", flush=True)
                    return rows or None
                print(f"  {path}: лимит кабинета занят другими сервисами, WB просит {retry}с — ждём...", flush=True)
                time.sleep(retry + 5)
                _WAITED += retry
                continue
            print(f"Ошибка API {e.code}: {e.read().decode()[:300]}", file=sys.stderr)
            sys.exit(1)
        _FIRST = False
        if not batch:
            break
        rows.extend(batch)
        print(f"  {path}: получено {len(rows)} строк", flush=True)
        if len(batch) < 80000:
            break
        cursor = max(x["lastChangeDate"] for x in batch)
        time.sleep(61)
    return rows


def mp_get(path: str, token: str, params: dict):
    """GET к Marketplace API (лимиты у него свои, щадящие). None при 401/403."""
    url = f"{MP_BASE}/{path}?" + urllib.parse.urlencode(params)
    for attempt in range(8):
        req = urllib.request.Request(url, headers={"Authorization": token})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                return None
            if e.code == 429:
                retry = min(int(e.headers.get("X-Ratelimit-Retry") or 6), 120)
                time.sleep(retry + 1)
                continue
            print(f"Marketplace API {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
            return None
    return None


def fetch_stages(cache: dict, token: str, start_dt: datetime) -> bool:
    """Этапы: заказ→сдача поставки (closedAt) и сдача→первый скан на СЦ (scanDt).
    Источник — Marketplace API (нужна категория токена «Маркетплейс»)."""
    # 1) поставки: id -> (closedAt, scanDt)
    supplies, next_ = {}, 0
    while True:
        data = mp_get("supplies", token, {"limit": 1000, "next": next_})
        if data is None:
            print("  этапы: Marketplace API недоступен (нужна категория токена «Маркетплейс») — пропускаем", flush=True)
            return False
        batch = data.get("supplies") or []
        for sp in batch:
            supplies[sp.get("id")] = (sp.get("closedAt"), sp.get("scanDt"))
        if len(batch) < 1000:
            break
        next_ = data.get("next") or 0
        time.sleep(0.4)
    print(f"  этапы: поставок получено {len(supplies)}", flush=True)

    # 2) сборочные задания: rid -> createdAt, supplyId
    matched, next_ = 0, 0
    date_from = int(start_dt.timestamp())
    while True:
        data = mp_get("orders", token, {"limit": 1000, "next": next_, "dateFrom": date_from})
        if data is None:
            return False
        batch = data.get("orders") or []
        for o in batch:
            rid = o.get("rid")
            rec = cache["orders"].get(rid)
            if rec is None:
                continue
            sup = supplies.get(o.get("supplyId"))
            if not sup:
                continue
            closed, scan = sup
            try:
                t0 = parse_dt(o["createdAt"])
                if closed:
                    h1 = (parse_dt(closed) - t0).total_seconds() / 3600
                    if 0 < h1 < 24 * 30:
                        rec["hnd"] = round(h1, 1)
                if closed and scan:
                    h2 = (parse_dt(scan) - parse_dt(closed)).total_seconds() / 3600
                    if 0 < h2 < 24 * 30:
                        rec["srt"] = round(h2, 1)
                matched += 1
            except (KeyError, ValueError, TypeError):
                pass
        if len(batch) < 1000:
            break
        next_ = data.get("next") or 0
        time.sleep(0.4)
    print(f"  этапы: сопоставлено заказов {matched}", flush=True)
    return True


def parse_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s.rstrip("Z"))
    return dt if dt.tzinfo else dt.replace(tzinfo=MSK)


def load_cache() -> dict:
    if os.path.exists(CACHE):
        with open(CACHE, encoding="utf-8") as f:
            return json.load(f)
    return {"orders": {}, "sales": {}, "cursor_orders": None, "cursor_sales": None}


def save_cache(c: dict) -> None:
    cutoff = (datetime.now(MSK) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    c["orders"] = {k: v for k, v in c["orders"].items() if v["date"][:10] >= cutoff}
    keep = set(c["orders"])
    c["sales"] = {k: v for k, v in c["sales"].items() if k in keep}
    with open(CACHE, "w", encoding="utf-8") as f:
        json.dump(c, f, ensure_ascii=False, separators=(",", ":"))


def build_dataset(cache: dict) -> dict:
    warehouses, districts, w_idx, d_idx, recs = [], [], {}, {}, []
    for srid, o in cache["orders"].items():
        w, d = o["wh"] or "—", o["okrug"] or "Не определён"
        if w not in w_idx:
            w_idx[w] = len(warehouses); warehouses.append(w)
        if d not in d_idx:
            d_idx[d] = len(districts); districts.append(d)
        t_order = parse_dt(o["date"])
        s = cache["sales"].get(srid)
        hours = None
        if s and s.get("sale"):
            h = (parse_dt(s["sale"]) - t_order).total_seconds() / 3600
            if 0 < h < 24 * 60:
                hours = round(h, 1)
        flags = (1 if o["cancel"] else 0) | (2 if s and s.get("ret") else 0)
        recs.append([t_order.strftime("%Y-%m-%d"), hours, w_idx[w], d_idx[d], flags,
                     o.get("hnd"), o.get("srt")])
    recs.sort(key=lambda r: r[0])
    return {
        "generatedAt": datetime.now(MSK).strftime("%Y-%m-%d %H:%M МСК"),
        "warehouses": warehouses, "districts": districts, "orders": recs,
    }


def inject(html_path: str, data: dict) -> None:
    with open(html_path, encoding="utf-8") as f:
        html = f.read()
    blob = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    html = re.sub(r"(/\*DATA_START\*/)(.*?)(/\*DATA_END\*/)",
                  lambda m: m.group(1) + blob + m.group(3), html, flags=re.S)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Данные вшиты в {html_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=25, help="глубина при полной перечитке")
    ap.add_argument("--full", action="store_true", help="игнорировать кэш, перечитать всё")
    ap.add_argument("--token", default=os.environ.get("WB_TOKEN"))
    ap.add_argument("--inject", metavar="INDEX_HTML")
    ap.add_argument("--out", default="data.json")
    ap.add_argument("--all", action="store_true", help="включить и FBO (по умолчанию только FBS)")
    args = ap.parse_args()
    if not args.token:
        print("Нужен токен: WB_TOKEN=...", file=sys.stderr); sys.exit(1)

    cache = load_cache()
    if args.full:
        cache = {"orders": {}, "sales": {}, "cursor_orders": None, "cursor_sales": None}
    start = (datetime.now(MSK) - timedelta(days=min(args.days, 90))).strftime("%Y-%m-%dT00:00:00")
    updated = 0

    def fetch_orders() -> bool:
        nonlocal updated
        o_from = cache["cursor_orders"] or start
        print(f"Заказы: изменения с {o_from}", flush=True)
        orders = api_get("orders", args.token, o_from)
        if orders is None:
            return False
        for o in orders:
            if not args.all and o.get("warehouseType") != "Склад продавца":
                continue
            cache["orders"][o["srid"]] = {
                "date": o["date"], "wh": o.get("warehouseName") or "",
                "okrug": o.get("oblastOkrugName") or "", "cancel": bool(o.get("isCancel")),
            }
        if orders:
            cache["cursor_orders"] = max(x["lastChangeDate"] for x in orders)
        updated += 1
        print(f"  FBS-заказов в кэше: {len(cache['orders'])}", flush=True)
        return True

    def fetch_sales() -> bool:
        nonlocal updated
        s_from = cache["cursor_sales"] or start
        print(f"Продажи: изменения с {s_from}", flush=True)
        sales = api_get("sales", args.token, s_from)
        if sales is None:
            return False
        for s in sales:
            srid = s.get("srid")
            if not srid:
                continue
            rec = cache["sales"].setdefault(srid, {})
            if str(s.get("saleID", "")).startswith("R"):
                rec["ret"] = True
            else:
                rec["sale"] = s["date"]
        if sales:
            cache["cursor_sales"] = max(x["lastChangeDate"] for x in sales)
        updated += 1
        return True

    # сначала то, что сильнее устарело (None = никогда не качали)
    steps = [fetch_orders, fetch_sales]
    if (cache["cursor_sales"] or "") < (cache["cursor_orders"] or ""):
        steps.reverse()
    for i, step in enumerate(steps):
        ok = step()
        if i == 0 and ok:
            time.sleep(61)

    # этапы пути (Marketplace API) — отдельные щадящие лимиты, не мешают статистике
    try:
        fetch_stages(cache, args.token, datetime.now(MSK) - timedelta(days=min(args.days, 90)))
    except Exception as e:
        print(f"  этапы: ошибка {e} — пропускаем", flush=True)

    save_cache(cache)
    if updated == 0 and not cache["orders"]:
        print("Ничего не получили и кэш пуст — выходим без изменений", file=sys.stderr)
        sys.exit(2)

    data = build_dataset(cache)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Готово: {len(data['orders'])} FBS-заказов, обновлено источников за запуск: {updated}/2 "
          f"(заказы до {cache['cursor_orders']}, продажи до {cache['cursor_sales']})")
    if args.inject:
        inject(args.inject, data)


if __name__ == "__main__":
    main()
