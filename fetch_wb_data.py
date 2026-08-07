#!/usr/bin/env python3
"""
Забирает данные из Wildberries Statistics API и готовит data.json для дашборда.

Использование:
    WB_TOKEN="ваш_токен" python3 fetch_wb_data.py            # данные за 90 дней
    WB_TOKEN="ваш_токен" python3 fetch_wb_data.py --days 30  # за 30 дней
    WB_TOKEN="ваш_токен" python3 fetch_wb_data.py --inject index.html
        # дополнительно вшивает данные прямо в index.html (для хостинга одним файлом)

Эндпоинты (лимит: 1 запрос в минуту на аккаунт):
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


def api_get(path: str, token: str, date_from: str) -> list:
    """Один вызов API с пагинацией по lastChangeDate (flag=0)."""
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
                if retry > 1800:
                    print(f"Лимит WB занят надолго ({retry}с) — выходим, попробуем в следующий запуск", file=sys.stderr)
                    sys.exit(2)
                print(f"  лимит запросов, WB просит подождать {retry} сек...")
                time.sleep(retry + 3)
                continue
            print(f"Ошибка API {e.code}: {e.read().decode()[:300]}", file=sys.stderr)
            sys.exit(1)
        if not batch:
            break
        rows.extend(batch)
        print(f"  {path}: получено {len(rows)} строк")
        if len(batch) < 80000:
            break
        cursor = max(x["lastChangeDate"] for x in batch)
        time.sleep(61)  # лимит: 1 запрос/мин
    return rows


def parse_dt(s: str) -> datetime:
    s = s.rstrip("Z")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MSK)
    return dt


def build_dataset(orders: list, sales: list) -> dict:
    # продажи по srid: дата продажи; возвраты (saleID начинается с R)
    sale_by_srid, returns = {}, set()
    for s in sales:
        srid = s.get("srid")
        if not srid:
            continue
        sid = str(s.get("saleID", ""))
        if sid.startswith("R"):
            returns.add(srid)
        else:
            sale_by_srid[srid] = parse_dt(s["date"])

    warehouses, districts = [], []
    w_idx, d_idx = {}, {}
    recs = []
    for o in orders:
        srid = o.get("srid")
        w = o.get("warehouseName") or "—"
        d = o.get("oblastOkrugName") or "Не определён"
        if w not in w_idx:
            w_idx[w] = len(warehouses)
            warehouses.append(w)
        if d not in d_idx:
            d_idx[d] = len(districts)
            districts.append(d)
        t_order = parse_dt(o["date"])
        t_sale = sale_by_srid.get(srid)
        hours = None
        if t_sale is not None:
            h = (t_sale - t_order).total_seconds() / 3600
            if 0 < h < 24 * 60:  # отсекаем мусор
                hours = round(h, 1)
        flags = (1 if o.get("isCancel") else 0) | (2 if srid in returns else 0)
        recs.append([
            t_order.strftime("%Y-%m-%d"),
            hours,
            w_idx[w],
            d_idx[d],
            flags,
        ])

    return {
        "generatedAt": datetime.now(MSK).strftime("%Y-%m-%d %H:%M"),
        "warehouses": warehouses,
        "districts": districts,
        # запись: [дата заказа, часы до вручения|null, склад, округ, флаги(1=отмена,2=возврат)]
        "orders": recs,
    }


def inject(html_path: str, data: dict) -> None:
    with open(html_path, encoding="utf-8") as f:
        html = f.read()
    blob = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    html = re.sub(
        r"(/\*DATA_START\*/)(.*?)(/\*DATA_END\*/)",
        lambda m: m.group(1) + blob + m.group(3),
        html,
        flags=re.S,
    )
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Данные вшиты в {html_path}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90, help="глубина выборки (макс 90)")
    ap.add_argument("--token", default=os.environ.get("WB_TOKEN"), help="токен WB (или переменная WB_TOKEN)")
    ap.add_argument("--inject", metavar="INDEX_HTML", help="вшить данные в HTML-файл дашборда")
    ap.add_argument("--out", default="data.json")
    args = ap.parse_args()

    if not args.token:
        print("Нужен токен: WB_TOKEN=... python3 fetch_wb_data.py", file=sys.stderr)
        sys.exit(1)

    date_from = (datetime.now(MSK) - timedelta(days=min(args.days, 90))).strftime("%Y-%m-%dT00:00:00")
    print(f"Забираю заказы с {date_from}...")
    orders = api_get("orders", args.token, date_from)
    print("Пауза 61 сек (лимит API)...")
    time.sleep(61)
    print("Забираю продажи...")
    sales = api_get("sales", args.token, date_from)

    data = build_dataset(orders, sales)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    print(f"Готово: {args.out} — {len(data['orders'])} заказов, "
          f"{len(data['warehouses'])} складов, {len(data['districts'])} округов")

    if args.inject:
        inject(args.inject, data)


if __name__ == "__main__":
    main()
