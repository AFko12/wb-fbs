#!/usr/bin/env python3
"""
Забирает данные из Wildberries Statistics API и готовит данные для дашборда.
Работает ИНКРЕМЕНТАЛЬНО: помнит, докуда дочитал (wb_cache.json), и в каждом
запуске просит у WB только изменения с прошлого раза — 2 маленьких запроса.

Использование:
    WB_TOKEN="..." python3 fetch_wb_data.py --inject index.html
    WB_TOKEN="..." python3 fetch_wb_data.py --full --days 40   # перечитать всё заново

Эндпоинты (лимит: 1 запрос в минуту на аккаунт + общий «global limiter» кабинета):
    GET https://statistics-api.wildberries.ru/api/v1/supplier/orders
    GET https://statistics-api.wildberries.ru/api/v1/supplier/sales
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
MSK = timezone(timedelta(hours=3))
CACHE = "wb_cache.json"
MAX_WAIT_SEC = 3 * 3600      # сколько суммарно готовы ждать лимит WB за один запуск
KEEP_DAYS = 90               # глубина хранения в кэше
_WAITED = 0


def api_get(path: str, token: str, date_from: str) -> list | None:
    """Все страницы с lastChangeDate >= date_from. None — если лимит не дождались."""
    global _WAITED
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
                if _WAITED + retry > MAX_WAIT_SEC:
                    print(f"  {path}: лимит кабинета занят ещё {retry}с (уже ждали {_WAITED}с) — "
                          f"пропускаем до следующего запуска", flush=True)
                    return None
                print(f"  {path}: лимит кабинета занят другими сервисами, WB просит {retry}с — ждём...", flush=True)
                time.sleep(retry + 5)
                _WAITED += retry
                continue
            print(f"Ошибка API {e.code}: {e.read().decode()[:300]}", file=sys.stderr)
            sys.exit(1)
        if not batch:
            break
        rows.extend(batch)
        print(f"  {path}: получено {len(rows)} строк", flush=True)
        if len(batch) < 80000:
            break
        cursor = max(x["lastChangeDate"] for x in batch)
        time.sleep(61)
    return rows


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
        recs.append([t_order.strftime("%Y-%m-%d"), hours, w_idx[w], d_idx[d], flags])
    recs.sort()
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
    ap.add_argument("--days", type=int, default=40, help="глубина при полной перечитке")
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

    # --- заказы: только изменения с прошлого раза ---
    o_from = cache["cursor_orders"] or start
    print(f"Заказы: изменения с {o_from}", flush=True)
    orders = api_get("orders", args.token, o_from)
    if orders is not None:
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
        time.sleep(61)

    # --- продажи: только изменения с прошлого раза ---
    s_from = cache["cursor_sales"] or start
    print(f"Продажи: изменения с {s_from}", flush=True)
    sales = api_get("sales", args.token, s_from)
    if sales is not None:
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

    save_cache(cache)
    if updated == 0 and not cache["orders"]:
        print("Ничего не получили и кэш пуст — выходим без изменений", file=sys.stderr)
        sys.exit(2)

    data = build_dataset(cache)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Готово: {len(data['orders'])} FBS-заказов, обновлено источников: {updated}/2")
    if args.inject:
        inject(args.inject, data)


if __name__ == "__main__":
    main()
