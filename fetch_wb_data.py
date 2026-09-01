#!/usr/bin/env python3
"""
Забирает данные из Wildberries Statistics API и Marketplace API,
готовит data.json и вшивает его в index.html.

Работает ИНКРЕМЕНТАЛЬНО: помнит, докуда дочитал (wb_cache.json), и в каждом
запуске просит у WB только изменения с прошлого раза.

Особенность лимитов WB: «global limiter» кабинета часто пропускает один запрос,
а следующий блокирует на часы (лимит делят все сервисы аналитики). Поэтому за
запуск мы гарантированно делаем ОДИН запрос к статистике — к тому источнику, что
сильнее устарел, — а второй пробуем только если WB не против.

КОНТРОЛЬНЫЕ ТОЧКИ ПУТИ (что откуда берём):
    T0  заказ создан            createdAt        /api/v3/orders        точно
    T1  скан ШК ТТН на СЦ       scanDt поставки  /api/v3/supplies      точно
    T2  сортировка товара       статус sorted    poll_status.py        ±5 минут
    T3  прибытие в ПВЗ          ready_for_pickup poll_status.py        ±5 минут
    T4  выдача клиенту          date продажи     /supplier/sales       точно

Метрики: приёмка T0→T1, обработка T1→T2, доставка до ПВЗ T2→T3,
весь путь до ПВЗ T0→T3. Выдача (T3→T4) считается отдельно и в оценку
работы СЦ не входит — клиент может забирать заказ неделями.

Использование:
    WB_TOKEN="..." python3 fetch_wb_data.py --inject index.html
    WB_TOKEN="..." python3 fetch_wb_data.py --full --days 25   # перечитать всё заново
    python3 fetch_wb_data.py --rebuild --inject index.html     # пересобрать из кэша, без API
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
# сколько ждать общий лимит статистики; WB_MAX_WAIT=120 — быстрый режим:
# берём только то, что отдаётся сразу (этапы и СЦ из Marketplace), остальное — в следующий запуск
MAX_WAIT_SEC = int(os.environ.get("WB_MAX_WAIT") or 3 * 3600)
SKIP_WAIT_SEC = 900          # последующие: если WB просит больше 15 мин — оставим на следующий запуск
KEEP_DAYS = 90               # глубина хранения в кэше
_WAITED = 0
_FIRST = True

# СЦ, которые WB называет по-разному в статистике и в маркетплейсе.
# Слева — как приходит из statistics API, справа — как показываем.
ALIAS = {
    "СЦ Софьино": "Москва",
    "Софьино": "Москва",
}
ANON = "Склад WB РФ"          # обезличенное имя, которое WB отдаёт с 14.08.2026
UNKNOWN = "СЦ не определён"


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
    for _ in range(8):
        req = urllib.request.Request(url, headers={"Authorization": token})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                return None
            if e.code == 429:
                time.sleep(min(int(e.headers.get("X-Ratelimit-Retry") or 6), 120) + 1)
                continue
            print(f"Marketplace API {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
            return None
    return None


def fetch_stages(cache: dict, token: str, start_dt: datetime) -> bool:
    """Точка T1 (скан ШК ТТН = scanDt поставки), СЦ приёмки и id задания для опроса статусов.
    Источник — Marketplace API (нужна категория токена «Маркетплейс»)."""
    # 1) поставки: id -> scanDt (момент скана ШК ТТН на СЦ)
    supplies, next_ = {}, 0
    while True:
        data = mp_get("supplies", token, {"limit": 1000, "next": next_})
        if data is None:
            print("  этапы: Marketplace API недоступен (нужна категория токена «Маркетплейс») — пропускаем", flush=True)
            return False
        batch = data.get("supplies") or []
        for sp in batch:
            supplies[sp.get("id")] = sp.get("scanDt")
        if len(batch) < 1000:
            break
        next_ = data.get("next") or 0
        time.sleep(0.4)
    print(f"  этапы: поставок получено {len(supplies)}", flush=True)

    # 2) сборочные задания: rid -> createdAt, supplyId, offices, id
    matched, offices_found, ids_found, next_ = 0, 0, 0, 0
    date_from = int(start_dt.timestamp())
    while True:
        data = mp_get("orders", token, {"limit": 1000, "next": next_, "dateFrom": date_from})
        if data is None:
            return False
        batch = data.get("orders") or []
        for o in batch:
            rec = cache["orders"].get(o.get("rid"))
            if rec is None:
                continue
            # с 14.08.2026 statistics API обезличивает склад («Склад WB РФ»),
            # но Marketplace API по-прежнему отдаёт конкретный СЦ приёмки
            off = (o.get("offices") or [None])[0]
            if off:
                if "office" not in rec:
                    offices_found += 1
                rec["office"] = off
            # id сборочного задания нужен опросу статусов (poll_status.py)
            if o.get("id") and "mid" not in rec:
                rec["mid"] = o["id"]
                ids_found += 1
            scan = supplies.get(o.get("supplyId"))
            if not scan:
                continue
            try:
                rec["t0"] = parse_dt(o["createdAt"]).isoformat()
                rec["t1"] = parse_dt(scan).isoformat()
                matched += 1
            except (KeyError, ValueError, TypeError):
                pass
        if len(batch) < 1000:
            break
        next_ = data.get("next") or 0
        time.sleep(0.4)
    print(f"  этапы: сопоставлено заказов {matched}, СЦ приёмки у {offices_found} новых, "
          f"id заданий у {ids_found} новых", flush=True)
    return True


def parse_dt(s: str) -> datetime:
    dt = datetime.fromisoformat(s.rstrip("Z"))
    return dt if dt.tzinfo else dt.replace(tzinfo=MSK)


def hours(a, b):
    """Часы между двумя ISO-метками, None если что-то не так."""
    if not a or not b:
        return None
    try:
        h = (parse_dt(b) - parse_dt(a)).total_seconds() / 3600
    except (ValueError, TypeError):
        return None
    return round(h, 2) if 0 <= h < 24 * 45 else None


def migrate(c: dict) -> dict:
    """Старый кэш (hnd = заказ→закрытие поставки, srt = закрытие→скан) переводим
    в новую точку T1: заказ→скан ШК ТТН = hnd + srt."""
    n = 0
    for rec in c.get("orders", {}).values():
        if "hnd" in rec or "srt" in rec:
            h, s = rec.pop("hnd", None), rec.pop("srt", None)
            if h is not None and s is not None and "acc" not in rec and "t1" not in rec:
                rec["acc"] = round(h + s, 2)
                n += 1
    if n:
        print(f"  кэш: перенесено старых замеров приёмки: {n}", flush=True)
    return c


MARKS = ("t0", "t1", "t2", "t3", "acc", "mid", "office", "fin", "miss")


def merge_marks(base: dict, mine: dict) -> dict:
    """Вливает метки этапов из `mine` в кэш `base` (скачанный с сервера).
    Метки только добавляются; если метка есть с обеих сторон, берём более
    раннюю — она ближе к моменту, когда статус на самом деле сменился.
    Нужно, когда сборщик и опрос статусов пишут в репозиторий одновременно."""
    for srid, rec in mine.get("orders", {}).items():
        dst = base["orders"].get(srid)
        if dst is None:
            base["orders"][srid] = rec
            continue
        for k in MARKS:
            if k not in rec:
                continue
            if k not in dst:
                dst[k] = rec[k]
            elif k in ("t2", "t3") and str(rec[k]) < str(dst[k]):
                dst[k] = rec[k]
    for srid, s in mine.get("sales", {}).items():
        base["sales"].setdefault(srid, s)
    for k in ("cursor_orders", "cursor_sales"):
        if (mine.get(k) or "") > (base.get(k) or ""):
            base[k] = mine[k]
    return base


def load_cache() -> dict:
    if os.path.exists(CACHE):
        with open(CACHE, encoding="utf-8") as f:
            return migrate(json.load(f))
    return {"orders": {}, "sales": {}, "cursor_orders": None, "cursor_sales": None}


def save_cache(c: dict) -> None:
    cutoff = (datetime.now(MSK) - timedelta(days=KEEP_DAYS)).strftime("%Y-%m-%d")
    c["orders"] = {k: v for k, v in c["orders"].items() if v["date"][:10] >= cutoff}
    keep = set(c["orders"])
    c["sales"] = {k: v for k, v in c["sales"].items() if k in keep}
    tmp = CACHE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(c, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, CACHE)


def build_dataset(cache: dict) -> dict:
    # реальные названия СЦ, которые отдал маркетплейс, — к ним приводим остальные
    offices = {o["office"] for o in cache["orders"].values() if o.get("office")}

    def norm(name: str) -> str:
        if not name or name == ANON:
            return UNKNOWN
        if name in offices:
            return name
        if name in ALIAS:
            return ALIAS[name]
        low = name.lower()
        for off in offices:                     # «СЦ Иваново Окружная» → «Иваново»
            if off.lower() in low:
                return off
        for k, v in ALIAS.items():
            if k.lower() in low:
                return v
        return name

    warehouses, districts, subjects, articles = [], [], [], []
    w_idx, d_idx, s_idx, a_idx, recs = {}, {}, {}, {}, []
    for srid, o in cache["orders"].items():
        w = norm(o.get("office") or o.get("wh") or "")
        d = o["okrug"] or "Не определён"
        subj = o.get("subj") or "—"
        art = o.get("art") or "—"
        for val, idx, arr in ((w, w_idx, warehouses), (d, d_idx, districts),
                              (subj, s_idx, subjects), (art, a_idx, articles)):
            if val not in idx:
                idx[val] = len(arr)
                arr.append(val)

        t0 = o.get("t0") or o["date"]
        t1, t2, t3 = o.get("t1"), o.get("t2"), o.get("t3")
        s = cache["sales"].get(srid)
        t4 = s.get("sale") if s else None

        acc = hours(t0, t1)
        if acc is None:
            acc = o.get("acc")                  # перенесённый замер из старого кэша
        srt = hours(t1, t2)
        pvz = hours(t2, t3)
        ttl = hours(t0, t3)
        give = hours(t3, t4)                    # лежит в ПВЗ до выдачи
        tot = hours(t0, t4)                     # заказ → выдача (справочно)

        flags = (1 if o["cancel"] else 0) | (2 if s and s.get("ret") else 0)
        recs.append([parse_dt(t0).strftime("%Y-%m-%d"), tot, w_idx[w], d_idx[d], flags,
                     acc, srt, pvz, ttl, give, s_idx[subj], a_idx[art]])
    recs.sort(key=lambda r: r[0])
    return {
        "schema": 2,
        "generatedAt": datetime.now(MSK).strftime("%Y-%m-%d %H:%M МСК"),
        "warehouses": warehouses, "districts": districts,
        "subjects": subjects, "articles": articles, "orders": recs,
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


def write_out(cache: dict, out: str, inject_path: str | None) -> dict:
    data = build_dataset(cache)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
    if inject_path:
        inject(inject_path, data)
    return data


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=25, help="глубина при полной перечитке")
    ap.add_argument("--full", action="store_true", help="игнорировать кэш, перечитать всё")
    ap.add_argument("--refresh-orders", action="store_true",
                    help="перечитать только заказы (продажи и метки этапов сохраняются)")
    ap.add_argument("--rebuild", action="store_true", help="пересобрать data.json из кэша, без обращений к API")
    ap.add_argument("--token", default=os.environ.get("WB_TOKEN"))
    ap.add_argument("--inject", metavar="INDEX_HTML")
    ap.add_argument("--out", default="data.json")
    ap.add_argument("--all", action="store_true", help="включить и FBO (по умолчанию только FBS)")
    args = ap.parse_args()

    cache = load_cache()

    if args.rebuild:
        save_cache(cache)
        data = write_out(cache, args.out, args.inject)
        print(f"Пересобрано из кэша: {len(data['orders'])} заказов")
        return

    if not args.token:
        print("Нужен токен: WB_TOKEN=...", file=sys.stderr); sys.exit(1)
    if args.full:
        cache = {"orders": {}, "sales": {}, "cursor_orders": None, "cursor_sales": None}
    elif args.refresh_orders:
        cache["orders"] = {}
        cache["cursor_orders"] = None
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
            prev = cache["orders"].get(o["srid"]) or {}
            rec = {
                "date": o["date"], "wh": o.get("warehouseName") or "",
                "okrug": o.get("oblastOkrugName") or "", "cancel": bool(o.get("isCancel")),
                "subj": o.get("subject") or "", "art": o.get("supplierArticle") or "",
                "cat": o.get("category") or "",
            }
            # не терять уже собранные метки этапов, СЦ и id задания
            for k in ("t0", "t1", "t2", "t3", "acc", "office", "mid", "fin", "miss"):
                if k in prev:
                    rec[k] = prev[k]
            cache["orders"][o["srid"]] = rec
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

    # этапы и реальный СЦ приёмки (Marketplace API) — первыми: у него свои щадящие
    # лимиты, и он не должен ждать очереди в общем лимите статистики
    stages_from = datetime.now(MSK) - timedelta(days=min(args.days, 90))

    def stages(tag=""):
        try:
            fetch_stages(cache, args.token, stages_from)
        except Exception as e:
            print(f"  этапы{tag}: ошибка {e} — пропускаем", flush=True)

    stages()
    было = len(cache["orders"])

    steps = [fetch_orders, fetch_sales]
    if (cache["cursor_sales"] or "") < (cache["cursor_orders"] or ""):
        steps.reverse()
    for i, step in enumerate(steps):
        ok = step()
        if i == 0 and ok:
            time.sleep(61)

    if len(cache["orders"]) > было:
        print(f"  новых заказов: {len(cache['orders']) - было} — повторный проход по этапам", flush=True)
        stages(" (повтор)")

    save_cache(cache)
    if updated == 0 and not cache["orders"]:
        print("Ничего не получили и кэш пуст — выходим без изменений", file=sys.stderr)
        sys.exit(2)

    data = write_out(cache, args.out, args.inject)
    print(f"Готово: {len(data['orders'])} FBS-заказов, обновлено источников за запуск: {updated}/2 "
          f"(заказы до {cache['cursor_orders']}, продажи до {cache['cursor_sales']})")


if __name__ == "__main__":
    main()
