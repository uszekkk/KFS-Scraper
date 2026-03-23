#!/usr/bin/env python3
"""Reskanuje wpisy z rate limit 429 i deduplikuje TAK per powiat."""

import json
import os
import sys
import time
import threading
from datetime import date
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

TODAY = date.today().strftime("%d.%m.%Y")

BASE_DIR = Path(__file__).parent
RESULTS_FILE = BASE_DIR / "test_results.json"

CLASSIFY_PROMPT = """Jesteś asystentem analizującym ogłoszenia z powiatowych urzędów pracy w Polsce.
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

Odpowiedz TYLKO w tym formacie:
WYNIK: TAK lub NIE
POWOD: (max 8 słów po polsku)
TERMIN: (data od - data do, np. "30.03.2026 - 03.04.2026", lub "w załączniku" jeśli daty w PDF, lub "brak")
KWOTA: (kwota KFS np. "546 542 zł", lub "w załączniku", lub "brak")

Tytuł: {title}
Treść: {snippet}"""

DEDUP_PROMPT = """Jesteś asystentem analizującym ogłoszenia z powiatowego urzędu pracy.
Poniżej lista artykułów z urzędu "{urzad}", które zostały wstępnie sklasyfikowane jako \
ogłoszenia o naborze KFS. Twoim zadaniem jest wybrać TYLKO te, które są FAKTYCZNYM \
OGŁOSZENIEM O NABORZE WNIOSKÓW KFS.

ZACHOWAJ tylko artykuły gdzie urząd OFICJALNIE OGŁASZA rozpoczęcie/prowadzenie naboru wniosków KFS.

ODRZUĆ (oznacz jako NIE):
- Przypomnienia o naborze ("Przypomnienie o naborze...", "Przypominamy...")
- Instrukcje techniczne (jak złożyć wniosek, obsługa systemu, klauzule RODO)
- Ogólne informacje o KFS (priorytety, kwoty bez terminów naboru, przygotuj się do naboru)
- Zapowiedzi bez konkretnego terminu naboru ("wkrótce ruszy nabór", "przygotuj się")
- Informacje o dostępie do komputera/punktach konsultacyjnych
- Nabory NIE-KFS (szkolenia indywidualne z FP, rezerwa FP, staże, bony, prace interwencyjne)
- Aktualizacje/zmiany do już istniejącego ogłoszenia (zostaw ORYGINALNE ogłoszenie)
- Jeśli są duplikaty (ta sama treść, inny tytuł) - zostaw JEDEN (nowszy lub z pełniejszym tytułem)

Artykuły:
{articles}

Odpowiedz TYLKO w tym formacie - dla każdego artykułu podaj numer i decyzję:
{format}"""

API_KEYS_ENV = os.environ.get("GEMINI_API_KEYS", "")
if not API_KEYS_ENV:
    print("BŁĄD: Brak zmiennej środowiskowej GEMINI_API_KEYS")
    sys.exit(1)
API_KEYS = [k.strip() for k in API_KEYS_ENV.split(",") if k.strip()]

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
MAX_RETRIES = 5
RETRY_WAIT = 15

progress_lock = threading.Lock()
progress = {"done": 0, "total": 0}


def call_gemini(api_key: str, prompt: str) -> str:
    """Wywołuje Gemini API i zwraca tekst odpowiedzi."""
    url = f"{GEMINI_URL}?key={api_key}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 429:
                if attempt < MAX_RETRIES:
                    wait = RETRY_WAIT * attempt
                    print(f"    429 rate limit, czekam {wait}s (proba {attempt}/{MAX_RETRIES})...")
                    time.sleep(wait)
                    continue
                return ""
            resp.raise_for_status()
            data = resp.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
                continue
            print(f"    Blad API: {str(e)[:80]}")
            return ""

    return ""


def classify_one(api_key: str, title: str, snippet: str) -> tuple[str, str, str, str]:
    """Klasyfikuje jeden artykuł."""
    prompt = CLASSIFY_PROMPT.format(title=title, snippet=snippet[:3000], today=TODAY)
    text = call_gemini(api_key, prompt)

    if not text:
        return "NIE", "Blad API: rate limit 429", "", ""

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


def rescan_rate_limits(results: list) -> list:
    """Reskanuje wpisy z bledem rate limit 429."""
    failed = [r for r in results if "rate limit 429" in r.get("powod", "") or
              ("Blad API" in r.get("powod", "") and r.get("wynik") == "NIE")]

    if not failed:
        print("Brak wpisow z bledem API do reskanowania.")
        return results

    print(f"\n{'='*60}")
    print(f"  RESKAN: {len(failed)} artykulow z bledem API")
    print(f"{'='*60}\n")

    failed_urls = {r["url"] for r in failed}
    progress["done"] = 0
    progress["total"] = len(failed)

    retry_results = {}

    with ThreadPoolExecutor(max_workers=len(API_KEYS)) as executor:
        futures = {}
        for i, r in enumerate(failed):
            api_key = API_KEYS[i % len(API_KEYS)]
            future = executor.submit(classify_one, api_key, r["title"], r.get("snippet", ""))
            futures[future] = r

        for future in as_completed(futures):
            r = futures[future]
            wynik, powod, termin, kwota = future.result()

            with progress_lock:
                progress["done"] += 1
                done = progress["done"]
                total = progress["total"]

            status = "TAK" if wynik == "TAK" else "NIE"
            extra = ""
            if termin:
                extra += f"  [{termin}]"
            if kwota:
                extra += f"  {kwota}"
            print(f"  [{done}/{total}] {r.get('urzad',''):20s} {r['title'][:45]:45s} -> {status}{extra}")

            retry_results[r["url"]] = {
                "url": r["url"],
                "title": r["title"],
                "urzad": r.get("urzad", ""),
                "date": r.get("date", ""),
                "wynik": wynik,
                "powod": powod,
                "termin": termin,
                "kwota": kwota,
                "snippet": r.get("snippet", "")[:300],
            }

    # Podmien wyniki
    for i, r in enumerate(results):
        if r["url"] in retry_results:
            results[i] = retry_results[r["url"]]

    still_failed = len([r for r in results if "Blad API" in r.get("powod", "")])
    new_tak = len([url for url, r in retry_results.items() if r["wynik"] == "TAK"])
    print(f"\n  Reskan done. Nowe TAK: {new_tak}, nadal z bledem: {still_failed}")

    return results


def dedup_tak_per_powiat(results: list) -> list:
    """Deduplikuje wyniki TAK per powiat - zostawia tylko faktyczne ogłoszenia o naborze."""
    tak_by_urzad = defaultdict(list)
    for i, r in enumerate(results):
        if r.get("wynik") == "TAK":
            tak_by_urzad[r["urzad"]].append((i, r))

    # Pomiń powiaty z 1 wynikiem TAK
    multi = {u: items for u, items in tak_by_urzad.items() if len(items) > 1}

    if not multi:
        print("\nBrak duplikatow TAK do sprawdzenia.")
        return results

    print(f"\n{'='*60}")
    print(f"  DEDUPLIKACJA: {len(multi)} powiatow z >1 TAK")
    print(f"{'='*60}\n")

    key_idx = 0
    changes = 0

    for urzad, items in sorted(multi.items()):
        print(f"\n  [{urzad}] - {len(items)} artykulow TAK:")
        for idx, (i, r) in enumerate(items):
            print(f"    {idx+1}. {r['title'][:70]}")
            print(f"       Data: {r.get('date','')} | AI: {r.get('powod','')}")

        # Przygotuj prompt
        articles_text = ""
        format_text = ""
        for idx, (i, r) in enumerate(items):
            num = idx + 1
            snippet = r.get("snippet", "")[:200]
            articles_text += f"\n{num}. Tytuł: {r['title']}\n   Data: {r.get('date','')}\n   Treść: {snippet}\n"
            format_text += f"{num}: TAK/NIE (powod max 8 slow)\n"

        prompt = DEDUP_PROMPT.format(
            urzad=urzad,
            articles=articles_text,
            format=format_text,
        )

        api_key = API_KEYS[key_idx % len(API_KEYS)]
        key_idx += 1

        text = call_gemini(api_key, prompt)

        if not text:
            print(f"    -> Blad API, pomijam deduplikacje dla {urzad}")
            continue

        # Parsuj odpowiedz
        decisions = {}
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            # Szukaj formatu "1: TAK (powod)" lub "1: NIE (powod)"
            for idx in range(1, len(items) + 1):
                if line.startswith(f"{idx}:") or line.startswith(f"{idx}."):
                    rest = line.split(":", 1)[1].strip() if ":" in line else line.split(".", 1)[1].strip()
                    if "TAK" in rest.upper():
                        decisions[idx] = "TAK"
                    elif "NIE" in rest.upper():
                        # Wyciągnij powód
                        powod = ""
                        for sep in ["(", "-", ","]:
                            if sep in rest:
                                powod = rest.split(sep, 1)[1].rstrip(")").strip()
                                break
                        if not powod:
                            powod = rest.replace("NIE", "").replace("nie", "").strip(" -:()")
                        decisions[idx] = ("NIE", powod)

        # Zastosuj decyzje
        for idx, (i, r) in enumerate(items):
            num = idx + 1
            if num in decisions:
                dec = decisions[num]
                if dec == "TAK":
                    print(f"    {num}. ZACHOWAJ -> TAK")
                else:
                    wynik_nie, powod = dec
                    results[i]["wynik"] = "NIE"
                    results[i]["powod"] = f"Duplikat/nie-ogłoszenie: {powod}" if powod else "Duplikat - nie faktyczne ogłoszenie naboru"
                    changes += 1
                    print(f"    {num}. ODRZUC  -> NIE ({results[i]['powod'][:50]})")
            else:
                print(f"    {num}. BRAK DECYZJI - zachowuje TAK")

    print(f"\n  Deduplikacja done. Zmieniono na NIE: {changes}")
    return results


def main():
    if not RESULTS_FILE.exists():
        print(f"Brak pliku {RESULTS_FILE}")
        sys.exit(1)

    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        results = json.load(f)

    tak_before = len([r for r in results if r["wynik"] == "TAK"])
    errors_before = len([r for r in results if "Blad API" in r.get("powod", "")])
    print(f"Zaladowano {len(results)} wynikow")
    print(f"  TAK: {tak_before}")
    print(f"  Bledy API: {errors_before}")

    # Krok 1: Reskanuj rate limits
    results = rescan_rate_limits(results)

    # Zapisz po reskanie
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print("\nZapisano wyniki po reskanie.")

    # Krok 2: Deduplikacja TAK per powiat
    results = dedup_tak_per_powiat(results)

    # Zapisz finalne wyniki
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    tak_after = len([r for r in results if r["wynik"] == "TAK"])
    errors_after = len([r for r in results if "Blad API" in r.get("powod", "")])

    print(f"\n{'='*60}")
    print(f"  PODSUMOWANIE")
    print(f"{'='*60}")
    print(f"  TAK przed: {tak_before} -> TAK po: {tak_after}")
    print(f"  Bledy API przed: {errors_before} -> po: {errors_after}")
    print(f"\nWyniki zapisane w {RESULTS_FILE.name}")


if __name__ == "__main__":
    main()
