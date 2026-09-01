#!/usr/bin/env python3
"""
Опрос статусов сборочных заданий FBS каждые 5 минут.

Зачем: WB не отдаёт время, когда заказ был отсортирован или когда он приехал
в ПВЗ — отдаётся только текущий статус. Поэтому мы часто спрашиваем статус и
сами ставим метку времени в момент, когда статус сменился. Точность — шаг
опроса (по умолчанию 5 минут).

Что пишем в wb_cache.json к заказу:
    t2  — момент, когда задание впервые увидено в статусе sorted (сортировка)
    t3  — момент, когда задание впервые увидено в ready_for_pickup (прибыло в ПВЗ)
    fin — заказ закрыт (отмена, брак, отказ) — больше не опрашиваем
    miss— увидели sold, не поймав ПВЗ (опрос не работал) — в этапы не берём

Эндпоинт: POST /api/v3/orders/status, до 1000 заданий за запрос,
лимит Marketplace API — 300 запросов в минуту, наш темп много ниже.

Использование:
    WB_TOKEN="..." python3 poll_status.py --minutes 295 --every 5 --git
    WB_TOKEN="..." python3 poll_status.py --once        # один проход, для проверки
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import fetch_wb_data as fw

MSK = timezone(timedelta(hours=3))
STATUS_URL = f"{fw.MP_BASE}/orders/status"
CHUNK = 1000
POLL_DAYS = 21          # заказы старше — уже неинтересны, статус не изменится
FINAL = {"canceled", "canceled_by_client", "declined_by_client", "defect"}


def now_iso() -> str:
    return datetime.now(MSK).replace(microsecond=0).isoformat()


def post_status(ids: list, token: str):
    body = json.dumps({"orders": ids}).encode()
    req = urllib.request.Request(STATUS_URL, data=body, method="POST", headers={
        "Authorization": token, "Content-Type": "application/json"})
    for _ in range(6):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode()).get("orders") or []
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(min(int(e.headers.get("X-Ratelimit-Retry") or 5), 60) + 1)
                continue
            if e.code in (401, 403):
                print(f"Нет доступа к статусам ({e.code}): нужна категория токена «Маркетплейс»",
                      file=sys.stderr)
                return None
            print(f"orders/status {e.code}: {e.read().decode()[:200]}", file=sys.stderr)
            return []
        except (urllib.error.URLError, TimeoutError) as e:
            print(f"  сеть: {e} — повтор", flush=True)
            time.sleep(5)
    return []


def candidates(cache: dict) -> dict:
    """mid -> srid для заданий, у которых ещё может смениться статус."""
    cutoff = (datetime.now(MSK) - timedelta(days=POLL_DAYS)).strftime("%Y-%m-%d")
    out = {}
    for srid, rec in cache["orders"].items():
        if rec.get("fin") or rec.get("t3") or rec.get("miss"):
            continue
        if not rec.get("mid") or rec["date"][:10] < cutoff:
            continue
        out[rec["mid"]] = srid
    return out


def one_pass(cache: dict, token: str) -> tuple:
    """Один проход по всем открытым заданиям. Возвращает (отмечено сортировок, ПВЗ, ошибок)."""
    pool = candidates(cache)
    ids = list(pool)
    got_sort = got_pvz = 0
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        res = post_status(chunk, token)
        if res is None:
            return -1, -1, len(chunk)
        for st in res:
            srid = pool.get(st.get("id"))
            if not srid:
                continue
            rec = cache["orders"][srid]
            wb = st.get("wbStatus") or ""
            if wb == "sorted" and not rec.get("t2"):
                rec["t2"] = now_iso(); got_sort += 1
            elif wb == "ready_for_pickup":
                if not rec.get("t2"):
                    rec["t2"] = now_iso(); got_sort += 1
                if not rec.get("t3"):
                    rec["t3"] = now_iso(); got_pvz += 1
            elif wb == "sold":
                rec["miss"] = 1          # выдан, а прибытие в ПВЗ мы не застали
            elif wb in FINAL:
                rec["fin"] = 1
        if i + CHUNK < len(ids):
            time.sleep(0.3)
    return got_sort, got_pvz, len(ids)


def run(*a):
    return subprocess.run(a, capture_output=True, text=True)


def sync_and_push(cache: dict, out: str, inject: str) -> None:
    """Забираем свежее состояние репозитория, вливаем в него свои метки и пушим.
    Так параллельный запуск сборщика не конфликтует с опросом: файлы всегда
    пересобираются из объединённого кэша, а не сливаются построчно."""
    br = run("git", "rev-parse", "--abbrev-ref", "HEAD").stdout.strip() or "main"
    for attempt in range(4):
        if run("git", "fetch", "origin", br).returncode == 0:
            run("git", "reset", "--hard", f"origin/{br}")
            merged = fw.merge_marks(fw.load_cache(), cache)
            cache.clear(); cache.update(merged)
        fw.save_cache(cache)
        fw.write_out(cache, out, inject)
        run("git", "add", "wb_cache.json", out, inject)
        if run("git", "diff", "--cached", "--quiet").returncode == 0:
            print("  git: изменений нет", flush=True)
            return
        run("git", "commit", "-m", f"Метки этапов {datetime.now(MSK):%Y-%m-%d %H:%M} МСК")
        if run("git", "push").returncode == 0:
            print("  git: запушено", flush=True)
            return
        print(f"  git: push отклонён, повтор {attempt + 1}/4", flush=True)
        time.sleep(5 + attempt * 10)
    print("  git: запушить не удалось, метки останутся в памяти до следующего цикла", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=int, default=295, help="сколько всего работать")
    ap.add_argument("--every", type=int, default=5, help="шаг опроса, минут")
    ap.add_argument("--save-every", type=int, default=30, help="как часто сохранять и пушить, минут")
    ap.add_argument("--stages-every", type=int, default=60,
                    help="как часто обновлять точки T0/T1 и id заданий из Marketplace API, минут")
    ap.add_argument("--once", action="store_true", help="один проход и выход")
    ap.add_argument("--git", action="store_true", help="коммитить и пушить кэш и данные")
    ap.add_argument("--token", default=os.environ.get("WB_TOKEN"))
    ap.add_argument("--inject", default="index.html")
    ap.add_argument("--out", default="data.json")
    args = ap.parse_args()
    if not args.token:
        print("Нужен токен: WB_TOKEN=...", file=sys.stderr); sys.exit(1)

    deadline = time.time() + (0 if args.once else args.minutes * 60)
    last_save = last_stages = 0.0
    pending = 0
    cache = fw.load_cache()

    def refresh_stages():
        """Точки T0/T1, СЦ приёмки и id заданий — только Marketplace API,
        общий лимит статистики WB не трогаем. Без этого опрашивать нечего."""
        nonlocal last_stages
        try:
            fw.fetch_stages(cache, args.token,
                            datetime.now(MSK) - timedelta(days=POLL_DAYS + 4))
        except Exception as e:
            print(f"  обновление этапов: {e}", flush=True)
        last_stages = time.time()

    def flush():
        nonlocal pending, last_save
        if args.git:
            sync_and_push(cache, args.out, args.inject)
        else:
            fw.save_cache(cache)
            fw.write_out(cache, args.out, args.inject)
        pending = 0
        last_save = time.time()

    while True:
        t = time.time()
        if time.time() - last_stages > args.stages_every * 60:
            refresh_stages()
        s, p, n = one_pass(cache, args.token)
        if s < 0:
            print("Опрос статусов недоступен — выходим", file=sys.stderr)
            if pending:
                flush()
            sys.exit(1)
        pending += s + p
        print(f"{datetime.now(MSK):%H:%M} опрошено {n}: сортировок +{s}, ПВЗ +{p} "
              f"({time.time() - t:.0f}с)", flush=True)
        if args.once:
            flush()
            return
        if pending and time.time() - last_save > args.save_every * 60:
            flush()
        if time.time() >= deadline:
            break
        time.sleep(max(5, args.every * 60 - (time.time() - t)))

    flush()
    print("Цикл завершён", flush=True)


if __name__ == "__main__":
    main()
