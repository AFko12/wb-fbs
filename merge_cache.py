#!/usr/bin/env python3
"""
Сливает наш локальный wb_cache.json с версией из указанной git-ревизии.

Нужен, когда сборщик и опрос статусов пишут в репозиторий одновременно:
вместо построчного слияния огромных JSON мы объединяем кэши по смыслу
(метки этапов только добавляются), а data.json и index.html после этого
пересобираются заново.

    python3 merge_cache.py origin/main
"""
import json
import subprocess
import sys

import fetch_wb_data as fw

ref = sys.argv[1] if len(sys.argv) > 1 else "origin/main"
mine = fw.load_cache()
r = subprocess.run(["git", "show", f"{ref}:{fw.CACHE}"], capture_output=True, text=True)
if r.returncode != 0:
    print(f"В {ref} нет {fw.CACHE} — оставляем свой кэш", file=sys.stderr)
    sys.exit(0)
base = json.loads(r.stdout)
base.setdefault("orders", {}); base.setdefault("sales", {})
merged = fw.merge_marks(base, mine)
fw.save_cache(merged)
print(f"Кэш слит с {ref}: заказов {len(merged['orders'])}")
