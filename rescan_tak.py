#!/usr/bin/env python3
"""Reskanuje TYLKO wpisy z wynikiem TAK przez zaostrzony prompt + reskanuje rate limit 429."""

import json
import os
import sys
import time
import threading
from datetime import date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE_DIR = Path(__file__).parent
RESULTS_FILE = BASE_DIR / "test_results.json"
TODAY = date.today().strftime("%d.%m.%Y")

RESCAN_PROMPT = """Jesteś asystentem analizującym ogłoszenia z powiatowych urzędów pracy w Polsce.
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
- nabór wniosków ale na potwierdzenie kwalifikacji/egzaminy (nie kształcenie ustawiczne KFS) -> NIE
- cokolwiek innego -> NIE

WAŻNE: Samo słowo "nabór" w tytule NIE WYSTARCZY. Musi być jasne że chodzi o KFS.
"Nabór wniosków na szkolenia" BEZ wzmianki o KFS = NIE (to może być FP).
"Nabór wniosków z Krajowego Funduszu Szkoleniowego" = TAK (jeśli termin OK).

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
MAX_RETRIES = 5
RETRY_WAIT = 15

progress_lock = threading.Lock()
progress = {"done": 0, "total": 0, "tak": 0, "nie": 0}


def call_gemini(api_key, prompt):
    url = f"{GEMINI_URL}?key={api_key}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 429:
                if attempt < MAX_RETRIES:
                    wait = RETRY_WAIT * attempt
                    time.sleep(wait)
                    continue
                return None
            resp.raise_for_status()
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
                continue
            return None
    return None


def parse_response(text):
    """Parsuje odpowiedź Gemini."""
    wynik = "NIE"
    powod = ""
    termin = ""
    kwota = ""
    skip_vals = {"brak", "nie dotyczy", "nd", "n/d", "-", ""}

    if not text:
        return wynik, powod, termin, kwota

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


def rescan_one(art, key_index):
    """Reskanuje jeden artykuł."""
    api_key = API_KEYS[key_index % len(API_KEYS)]
    title = art.get("title", "")
    snippet = art.get("snippet", "") or title

    prompt = RESCAN_PROMPT.format(title=title, snippet=snippet[:3000], today=TODAY)
    text = call_gemini(api_key, prompt)
    wynik, powod, termin, kwota = parse_response(text)

    with progress_lock:
        progress["done"] += 1
        if wynik == "TAK":
            progress["tak"] += 1
        else:
            progress["nie"] += 1
        done = progress["done"]
        total = progress["total"]

    extra = ""
    if termin:
        extra += f" [{termin}]"
    if kwota:
        extra += f" {kwota}"
    changed = " *ZMIANA*" if wynik != art.get("wynik", "") else ""
    print(f"  [{done}/{total}] {art.get('urzad',''):20s} {title[:40]:40s} -> {wynik}{extra}{changed}")

    return {
        "url": art.get("url", ""),
        "title": title,
        "urzad": art.get("urzad", ""),
        "date": art.get("date", ""),
        "wynik": wynik,
        "powod": powod,
        "termin": termin,
        "kwota": kwota,
        "snippet": art.get("snippet", "")[:300],
    }


def main():
    if not RESULTS_FILE.exists():
        print(f"Brak pliku {RESULTS_FILE}")
        sys.exit(1)

    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        results = json.load(f)

    # Znajdź wpisy do reskanowania: TAK + rate limit 429
    tak_entries = [(i, r) for i, r in enumerate(results) if r.get("wynik") == "TAK"]
    rate_limit = [(i, r) for i, r in enumerate(results)
                  if "Blad API" in r.get("powod", "") and r.get("wynik") == "NIE"]
    to_rescan = tak_entries + rate_limit

    print(f"Zaladowano {len(results)} wynikow")
    print(f"  Do reskanu: {len(tak_entries)} TAK + {len(rate_limit)} rate limit = {len(to_rescan)}")
    print(f"  Data dzisiejsza: {TODAY}")
    print()

    if not to_rescan:
        print("Nic do reskanu.")
        return

    progress["total"] = len(to_rescan)
    progress["done"] = 0
    progress["tak"] = 0
    progress["nie"] = 0

    updated = {}  # index -> new result

    with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
        futures = {}
        for idx, (i, r) in enumerate(to_rescan):
            future = executor.submit(rescan_one, r, idx)
            futures[future] = i

        for future in as_completed(futures):
            i = futures[future]
            result = future.result()
            updated[i] = result

    # Podmień wyniki
    changes_to_nie = 0
    changes_to_tak = 0
    for i, new_r in updated.items():
        old_wynik = results[i].get("wynik", "")
        results[i] = new_r
        if old_wynik == "TAK" and new_r["wynik"] == "NIE":
            changes_to_nie += 1
        elif old_wynik == "NIE" and new_r["wynik"] == "TAK":
            changes_to_tak += 1

    # Zapisz
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    final_tak = len([r for r in results if r["wynik"] == "TAK"])
    final_errors = len([r for r in results if "Blad API" in r.get("powod", "")])

    print(f"\n{'='*60}")
    print(f"  PODSUMOWANIE RESKANU")
    print(f"{'='*60}")
    print(f"  Przeskanowano: {len(to_rescan)}")
    print(f"  TAK -> NIE: {changes_to_nie} (odrzucone)")
    print(f"  NIE -> TAK: {changes_to_tak} (z rate limit)")
    print(f"  Finalne TAK: {final_tak}")
    print(f"  Nadal bledy API: {final_errors}")
    print(f"\nZapisano: {RESULTS_FILE.name}")

    # Pokaż odrzucone
    if changes_to_nie > 0:
        print(f"\n  ODRZUCONE (TAK -> NIE):")
        for i, new_r in updated.items():
            if new_r["wynik"] == "NIE" and to_rescan[0][1] != new_r:  # skip rate limits
                # Check if it was originally TAK
                for orig_i, orig_r in tak_entries:
                    if orig_i == i:
                        print(f"    - [{new_r['urzad']}] {new_r['title'][:60]}")
                        print(f"      Powod: {new_r['powod']}")
                        break


if __name__ == "__main__":
    main()
