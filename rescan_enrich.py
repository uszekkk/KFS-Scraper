#!/usr/bin/env python3
"""
Wzbogaca istniejące wyniki TAK o pełną treść ze stron artykułów i reklasyfikuje.
Pobiera stronę szczegółową każdego artykułu TAK, wyciąga pełny tekst,
i ponownie klasyfikuje z dłuższym snippetem.
"""

import json
import os
import re
import sys
import time
import threading
from datetime import date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

TODAY = date.today().strftime("%d.%m.%Y")

BASE_DIR = Path(__file__).parent
RESULTS_FILE = BASE_DIR / "test_results.json"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0"})

API_KEYS_ENV = os.environ.get("GEMINI_API_KEYS", "")
if not API_KEYS_ENV:
    print("BŁĄD: Brak zmiennej środowiskowej GEMINI_API_KEYS")
    sys.exit(1)
API_KEYS = [k.strip() for k in API_KEYS_ENV.split(",") if k.strip()]

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
MAX_RETRIES = 4
RETRY_WAIT = 10

PROMPT = """Jesteś asystentem analizującym ogłoszenia z powiatowych urzędów pracy w Polsce.
Dzisiejsza data: {today}.

Poniższy tekst to ogłoszenie o naborze wniosków KFS. Wyciągnij z niego TERMIN i KWOTĘ.

KRYTYCZNIE WAŻNE:
- Przeszukaj CAŁY tekst w poszukiwaniu dat i kwot.
- Daty mogą mieć formaty: "26.03.2026 r.", "od dnia 26 marca 2026", "26.03.2026r." itp.
- Jeśli widzisz "w dniach 26.03.2026 r. - 03.04.2026 r." to TERMIN = "26.03.2026 - 03.04.2026"
- KWOTA to ŁĄCZNA PULA środków KFS (np. "1 300 000 zł", "7.000.000 zł").
  Szukaj fraz: "kwota", "środki", "pula", "do rozdysponowania", "limit", "wysokości".
  NIE podawaj kwoty na osobę - szukaj ŁĄCZNEJ puli.
- Jeśli jest kilka kwot, podaj największą (łączna pula).
- ZAWSZE podaj datę i kwotę jeśli SĄ w tekście!

Odpowiedz TYLKO w formacie:
WYNIK: TAK
POWOD: (max 8 słów po polsku)
TERMIN: (data od - data do, np. "30.03.2026 - 03.04.2026", lub "w załączniku", lub "brak")
KWOTA: (kwota KFS np. "546 542 zł", lub "w załączniku", lub "brak")

Tytuł: {title}
Treść: {snippet}"""


KFS_KEYWORDS = re.compile(
    r"KFS|Krajow\w+ Fundusz\w* Szkoleniow|"
    r"kszta[łl]ceni\w+ ustawiczn|"
    r"nab[oó]r wniosk[oó]w",
    re.IGNORECASE,
)


def fetch_detail_content(url):
    """Pobiera pełną treść ze strony szczegółowej artykułu."""
    try:
        resp = SESSION.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all(["script", "style", "nav", "footer"]):
            tag.decompose()
        candidates = []
        selectors = [
            ("div", {"class": re.compile(r"journal-content-article")}),
            ("div", {"class": re.compile(r"asset-content")}),
            ("div", {"class": re.compile(r"asset-full-content")}),
            ("div", {"class": re.compile(r"portlet-body")}),
            ("article", {}),
            ("main", {}),
        ]
        for tag, attrs in selectors:
            found_list = soup.find_all(tag, attrs) if attrs else soup.find_all(tag)
            for found in found_list:
                text = found.get_text(separator="\n", strip=True)
                if len(text) > 100:
                    has_kfs = bool(KFS_KEYWORDS.search(text))
                    candidates.append((text, has_kfs))
        if not candidates:
            return ""
        kfs_candidates = [t for t, k in candidates if k]
        if kfs_candidates:
            best = max(kfs_candidates, key=len)
        else:
            best = max((t for t, _ in candidates), key=len)
        lines = [l.strip() for l in best.split("\n") if l.strip()]
        text = "\n".join(lines)
        if len(text) <= 3000:
            return text
        date_pattern = re.compile(r"\d{2}\.\d{2}\.\d{4}")
        amount_pattern = re.compile(r"\d[\d\s.,]+\s*z[łl]", re.IGNORECASE)
        date_matches = list(date_pattern.finditer(text))
        amount_matches = list(amount_pattern.finditer(text))
        last_important = 0
        if date_matches:
            last_important = max(last_important, date_matches[-1].end())
        if amount_matches:
            last_important = max(last_important, amount_matches[-1].end())
        if last_important > 3000:
            tail_size = min(last_important + 200, len(text)) - max(0, last_important - 1500)
            head_size = 3000 - tail_size - 20
            if head_size < 500:
                head_size = 500
                tail_start = max(0, last_important - 1500)
                return text[:head_size] + "\n[...]\n" + text[tail_start:tail_start + 3000 - head_size - 10]
            tail_start = max(0, last_important - 1500)
            tail_end = min(len(text), last_important + 200)
            return text[:head_size] + "\n[...]\n" + text[tail_start:tail_end]
        return text[:3000]
    except Exception:
        return ""


def classify(api_key, title, snippet):
    prompt = PROMPT.format(title=title, snippet=snippet[:3000], today=TODAY)
    url = f"{GEMINI_URL}?key={api_key}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 429:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_WAIT)
                    continue
                return "", ""
            resp.raise_for_status()
            text = resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
            break
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
                continue
            return "", ""

    termin, kwota = "", ""
    skip = {"brak", "nie dotyczy", "nd", "n/d", "-", ""}
    for line in text.split("\n"):
        line = line.strip()
        upper = line.upper()
        if upper.startswith("TERMIN:"):
            v = line.split(":", 1)[1].strip()
            if v.lower() not in skip:
                termin = v
        elif upper.startswith("KWOTA:"):
            v = line.split(":", 1)[1].strip()
            if v.lower() not in skip:
                kwota = v
    return termin, kwota


progress_lock = threading.Lock()
progress = {"done": 0, "total": 0}


def process_one(r, key_index):
    """Pobiera stronę, reklasyfikuje, zwraca zaktualizowany wynik."""
    url = r["url"]
    title = r["title"]
    old_termin = r.get("termin", "")
    old_kwota = r.get("kwota", "")

    # Pobierz pełną treść ze strony
    detail = fetch_detail_content(url)
    time.sleep(0.2)

    new_snippet = detail if detail and len(detail) > len(r.get("snippet", "")) else r.get("snippet", "")

    # Reklasyfikuj
    api_key = API_KEYS[key_index % len(API_KEYS)]
    new_termin, new_kwota = classify(api_key, title, new_snippet)

    # Użyj nowych wartości jeśli lepsze, zachowaj stare jeśli nowe puste
    final_termin = new_termin if new_termin else old_termin
    final_kwota = new_kwota if new_kwota else old_kwota

    changed = (final_termin != old_termin) or (final_kwota != old_kwota)

    with progress_lock:
        progress["done"] += 1
        done = progress["done"]
        total = progress["total"]

    status = "ZMIANA" if changed else "bez zmian"
    print(f"  [{done}/{total}] {r['urzad']:20s} {status:10s}"
          f"  termin: {old_termin or '(brak)'} -> {final_termin or '(brak)'}"
          f"  kwota: {old_kwota or '(brak)'} -> {final_kwota or '(brak)'}")

    result = dict(r)
    result["termin"] = final_termin
    result["kwota"] = final_kwota
    result["snippet"] = new_snippet[:3000]
    return result


def main():
    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        results = json.load(f)

    tak = [r for r in results if r["wynik"] == "TAK"]
    # Znajdź te z brakującymi danymi
    to_enrich = [r for r in tak if not r.get("termin") or not r.get("kwota")
                 or r.get("termin", "").lower().startswith("w za")]

    print(f"Wynikow TAK: {len(tak)}")
    print(f"  Brak terminu: {len([r for r in tak if not r.get('termin')])}")
    print(f"  Termin 'w załączniku': {len([r for r in tak if r.get('termin','').lower().startswith('w za')])}")
    print(f"  Brak kwoty: {len([r for r in tak if not r.get('kwota')])}")
    print(f"  Do wzbogacenia: {len(to_enrich)}")
    print(f"Startuje...\n")

    progress["total"] = len(to_enrich)
    progress["done"] = 0

    enriched = {}
    with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
        futures = {}
        for i, r in enumerate(to_enrich):
            future = executor.submit(process_one, r, i)
            futures[future] = r["url"]

        for future in as_completed(futures):
            result = future.result()
            enriched[result["url"]] = result

    # Podmień wyniki
    changed_count = 0
    for i, r in enumerate(results):
        if r["url"] in enriched:
            old = r
            new = enriched[r["url"]]
            if old.get("termin") != new.get("termin") or old.get("kwota") != new.get("kwota"):
                changed_count += 1
            results[i] = new

    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # Podsumowanie
    tak_after = [r for r in results if r["wynik"] == "TAK"]
    print(f"\n{'='*60}")
    print(f"PODSUMOWANIE WZBOGACENIA")
    print(f"{'='*60}")
    print(f"  Zmieniono: {changed_count} / {len(to_enrich)}")
    print(f"  Brak terminu: {len([r for r in tak_after if not r.get('termin')])}")
    print(f"  Brak kwoty: {len([r for r in tak_after if not r.get('kwota')])}")
    print(f"  Zapisano: {RESULTS_FILE.name}")


if __name__ == "__main__":
    main()
