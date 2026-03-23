#!/usr/bin/env python3
"""Klasyfikuje artykuły z test_articles.json za pomocą Gemini API (multithreaded)."""

import json
import os
import sys
import time
import threading
from datetime import date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

TODAY = date.today().strftime("%d.%m.%Y")

BASE_DIR = Path(__file__).parent
ARTICLES_FILE = BASE_DIR / "test_articles.json"
RESULTS_FILE = BASE_DIR / "test_results.json"

PROMPT_TEMPLATE = """Jesteś asystentem analizującym ogłoszenia z powiatowych urzędów pracy w Polsce.
Dzisiejsza data: {today}.

Oceń czy poniższy tekst to AKTUALNE OGŁOSZENIE O NABORZE WNIOSKÓW KFS (Krajowy Fundusz Szkoleniowy).

Typowe PRAWDZIWE ogłoszenie o naborze KFS zawiera:
- KONKRETNY TERMIN od-do przyjmowania wniosków (np. "od 30.03.2026 do 03.04.2026")
- KWOTĘ środków KFS do rozdysponowania (np. "546 542,00 zł")
- Informację że urząd ROZPOCZYNA/PROWADZI nabór wniosków KFS

TAK jeśli:
- Tekst zawiera termin od-do + kwotę + ogłasza nabór KFS, I termin jeszcze nie minął (>= {today})
- LUB: tekst wyraźnie ogłasza nabór wniosków KFS ale szczegóły (daty, kwoty) są w załącznikach \
  PDF/dokumentach do pobrania - to NADAL jest prawdziwe ogłoszenie, daj TAK

NIE jeśli:
- termin końcowy naboru już MINĄŁ (przed {today}) -> NIE
- zapowiedzi naboru ("wkrótce ruszy", "przygotuj się", "planowany nabór") bez ogłoszenia startu -> NIE
- ogólne informacje o KFS (priorytety, czym jest KFS, warsztaty, szkolenia o KFS) -> NIE
- przypomnienia o naborze ("przypominamy", "przypomnienie") -> NIE
- instrukcje techniczne (jak złożyć wniosek, założyć konto, wzory formularzy, RODO) -> NIE
- punkty konsultacyjne, dostęp do komputera, dni otwarte -> NIE
- wstrzymanie/zawieszenie/zakończenie naboru -> NIE
- inny nabór niż KFS (staże, szkolenia indywidualne z FP, bony, prace interwencyjne, rezerwa FP \
  BEZ wzmianki o KFS, Fundusz Pracy ogólnie, dotacje na rozpoczęcie działalności) -> NIE
- nabór na potwierdzenie kwalifikacji/egzaminy (nie kształcenie ustawiczne KFS) -> NIE
- cokolwiek innego -> NIE

WAŻNE: Samo słowo "nabór" w tytule NIE WYSTARCZY. Musi być jasne że chodzi o KFS.
"Nabór wniosków na szkolenia" BEZ wzmianki o KFS = NIE (to może być FP).
"Nabór wniosków z Krajowego Funduszu Szkoleniowego" = TAK (jeśli termin OK).

KRYTYCZNIE WAŻNE - TERMIN I KWOTA:
- Przeszukaj CAŁY tekst w poszukiwaniu dat i kwot. Nie pisz "brak" jeśli informacja JEST w tekście!
- Daty mogą mieć formaty: "26.03.2026 r.", "od dnia 26 marca 2026", "26.03.2026r.", "2026-03-26" itp.
- Jeśli widzisz "w dniach 26.03.2026 r. - 03.04.2026 r." to TERMIN = "26.03.2026 - 03.04.2026"
- KWOTA to łączna pula środków KFS (np. "1 300 000 zł", "7.000.000 zł", "546 542,00 zł").
  Szukaj fraz: "kwota", "środki", "pula", "do rozdysponowania", "limit", "wysokości".
  NIE podawaj maksymalnej kwoty NA OSOBĘ - szukaj ŁĄCZNEJ PULI środków.
- Jeśli jest kilka kwot, podaj tę największą (łączna pula).
- ZAWSZE podaj datę i kwotę jeśli SĄ w tekście. "brak" TYLKO gdy naprawdę ich nie ma.

Odpowiedz TYLKO w tym formacie:
WYNIK: TAK lub NIE
POWOD: (max 8 słów po polsku)
TERMIN: (data od - data do, np. "30.03.2026 - 03.04.2026", lub "w załączniku" jeśli daty w PDF, lub "brak")
KWOTA: (kwota KFS np. "546 542 zł", lub "w załączniku", lub "brak")

Tytuł: {title}
Treść: {snippet}"""

API_KEYS_ENV = os.environ.get("GEMINI_API_KEYS", "")
if not API_KEYS_ENV:
    print("BŁĄD: Brak zmiennej środowiskowej GEMINI_API_KEYS")
    sys.exit(1)
API_KEYS = [k.strip() for k in API_KEYS_ENV.split(",") if k.strip()]

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
MAX_RETRIES = 4
RETRY_WAIT = 10

results_lock = threading.Lock()
progress_lock = threading.Lock()
progress = {"done": 0, "total": 0, "tak": 0}


def classify(api_key: str, title: str, snippet: str) -> tuple[str, str, str, str]:
    """Wywołuje Gemini API bezpośrednio przez HTTP (thread-safe)."""
    prompt = PROMPT_TEMPLATE.format(title=title, snippet=snippet[:3000], today=TODAY)
    url = f"{GEMINI_URL}?key={api_key}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 429:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_WAIT)
                    continue
                return "NIE", "Blad API: rate limit 429", "", ""
            resp.raise_for_status()
            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
            break
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
                continue
            return "NIE", f"Blad API: {str(e)[:60]}", "", ""

    wynik = "NIE"
    powod = ""
    termin = ""
    kwota = ""
    skip_vals = {"brak", "nie dotyczy", "nd", "n/d", "-", ""}
    for line in text.split("\n"):
        line = line.strip()
        upper = line.upper()
        if upper.startswith("WYNIK:"):
            val = line.split(":", 1)[1].strip().upper()
            wynik = "TAK" if "TAK" in val else "NIE"
        elif upper.startswith("POWOD:") or upper.startswith("POWÓD:"):
            powod = line.split(":", 1)[1].strip()
        elif upper.startswith("TERMIN:"):
            t = line.split(":", 1)[1].strip()
            if t.lower() not in skip_vals:
                termin = t
        elif upper.startswith("KWOTA:"):
            k = line.split(":", 1)[1].strip()
            if k.lower() not in skip_vals:
                kwota = k

    return wynik, powod, termin, kwota


def process_article(art: dict, key_index: int) -> dict:
    api_key = API_KEYS[key_index % len(API_KEYS)]
    title = art.get("title", "")
    snippet = art.get("snippet", "") or title

    wynik, powod, termin, kwota = classify(api_key, title, snippet)

    with progress_lock:
        progress["done"] += 1
        if wynik == "TAK":
            progress["tak"] += 1
        done = progress["done"]
        total = progress["total"]
        tak = progress["tak"]

    extra = ""
    if termin:
        extra += f"  [{termin}]"
    if kwota:
        extra += f"  {kwota}"
    print(f"  [{done}/{total}] {art.get('urzad',''):20s} {title[:45]:45s} -> {wynik}{extra}  (TAK: {tak})")

    return {
        "url": art.get("url", ""),
        "title": title,
        "urzad": art.get("urzad", ""),
        "date": art.get("date", ""),
        "wynik": wynik,
        "powod": powod,
        "termin": termin,
        "kwota": kwota,
        "snippet": art.get("snippet", "")[:3000],
    }


def main():
    with open(ARTICLES_FILE, "r", encoding="utf-8") as f:
        articles = json.load(f)

    # Resume
    done_urls = set()
    existing_results = []
    if RESULTS_FILE.exists():
        with open(RESULTS_FILE, "r", encoding="utf-8") as f:
            existing_results = json.load(f)
        done_urls = {r["url"] for r in existing_results}

    todo = [a for a in articles if a.get("url", "") not in done_urls]

    print(f"Artykulow lacznie: {len(articles)}")
    print(f"Juz sklasyfikowanych: {len(done_urls)}")
    print(f"Do zrobienia: {len(todo)}")
    print(f"Kluczy API: {len(API_KEYS)}, watkow: {len(API_KEYS)}")
    print(f"Startuje...\n")

    progress["total"] = len(todo)
    progress["done"] = 0
    progress["tak"] = len([r for r in existing_results if r.get("wynik") == "TAK"])

    results = list(existing_results)
    batch_count = 0

    with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
        futures = {}
        for i, art in enumerate(todo):
            future = executor.submit(process_article, art, i)
            futures[future] = art

        for future in as_completed(futures):
            result = future.result()
            with results_lock:
                results.append(result)
                batch_count += 1
                if batch_count % 20 == 0:
                    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
                        json.dump(results, f, ensure_ascii=False, indent=2)

    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # --- Retry: reklasyfikuj wpisy z bledem 429 ---
    failed = [r for r in results if "429" in r.get("powod", "") or "Blad API" in r.get("powod", "")]
    if failed:
        print(f"\n{'='*60}")
        print(f"  RETRY: {len(failed)} artykulow z bledem API")
        print(f"{'='*60}\n")

        progress["done"] = 0
        progress["total"] = len(failed)

        retry_results = {}
        with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
            futures = {}
            for i, r in enumerate(failed):
                art = {"title": r["title"], "snippet": r["snippet"], "urzad": r["urzad"],
                       "url": r["url"], "date": r["date"]}
                future = executor.submit(process_article, art, i)
                futures[future] = r["url"]

            for future in as_completed(futures):
                result = future.result()
                retry_results[result["url"]] = result

        # Podmien wyniki
        for i, r in enumerate(results):
            if r["url"] in retry_results:
                results[i] = retry_results[r["url"]]

        with open(RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        still_failed = len([r for r in results if "429" in r.get("powod", "") or "Blad API" in r.get("powod", "")])
        print(f"\n  Retry done. Nadal z bledem: {still_failed}")

    tak = [r for r in results if r["wynik"] == "TAK"]
    nie = [r for r in results if r["wynik"] == "NIE"]
    errors = [r for r in results if "Blad API" in r.get("powod", "")]

    print(f"\n{'='*60}")
    print(f"  WYNIKI KLASYFIKACJI")
    print(f"{'='*60}")
    print(f"  Lacznie: {len(results)}")
    print(f"  TAK (nabory KFS): {len(tak)}")
    print(f"  NIE: {len(nie)}")
    print(f"  Bledy API: {len(errors)}")

    print(f"\n[+] NABORY KFS ({len(tak)}):")
    for r in tak:
        print(f"  - [{r['urzad']}] {r['title']}")
        print(f"    Powod: {r['powod']}")
        print(f"    Link: {r['url']}")

    print(f"\nSprawdz {RESULTS_FILE.name}")


if __name__ == "__main__":
    main()
