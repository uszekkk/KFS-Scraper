#!/usr/bin/env python3
"""
Pipeline produkcyjny: scrape → cache → classify (Gemini) → raport HTML z mapą.
Uruchamiany co 5h przez GitHub Actions lub ręcznie.
"""

import json
import os
import re
import sys
import time
import threading
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup

# ============================================================
# KONFIGURACJA
# ============================================================
BASE_DIR = Path(__file__).parent
URZEDY_FILE = BASE_DIR / "urzedy.json"
CACHE_FILE = BASE_DIR / "cache.json"
RESULTS_FILE = BASE_DIR / "results.json"
ERRORS_FILE = BASE_DIR / "errors.json"
OUTPUT_FILE = BASE_DIR / "index.html"
GEOJSON_FILE = BASE_DIR / "powiaty.geojson"
MAPPING_FILE = BASE_DIR / "urzad_to_powiat.json"

TODAY = date.today().strftime("%d.%m.%Y")
REQUEST_DELAY = 0.3
MAX_SNIPPET = 10000

# Klucze API Gemini — ze zmiennej środowiskowej GEMINI_API_KEYS (rozdzielone przecinkiem)
API_KEYS_ENV = os.environ.get("GEMINI_API_KEYS", "")
if not API_KEYS_ENV:
    print("BŁĄD: Brak zmiennej środowiskowej GEMINI_API_KEYS")
    print("Ustaw: export GEMINI_API_KEYS='klucz1,klucz2,...'")
    sys.exit(1)
API_KEYS = [k.strip() for k in API_KEYS_ENV.split(",") if k.strip()]

GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
MAX_RETRIES = 5
RETRY_WAIT = 15

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
- LUB: tytuł wyraźnie mówi o "naborze wniosków KFS" a treść jest bardzo krótka/ucięta \
  (np. kilka zdań, "Szczegółowe informacje", brak pełnej treści) - to NADAL jest prawdziwe \
  ogłoszenie o naborze, daj TAK z TERMIN/KWOTA "w załączniku" lub podaj jeśli są w tekście

NIE jeśli:
- termin końcowy naboru już MINĄŁ (przed {today}) -> NIE
- zapowiedzi naboru ("wkrótce ruszy", "przygotuj się", "planowany nabór", \
  "w związku z planowanym naborem") -> NIE
- przygotowania do naboru (zakładanie kont, rejestracja na platformie, instrukcje \
  techniczne przed naborem) -> NIE
- ogólne informacje o KFS (priorytety, czym jest KFS, warsztaty) -> NIE
- przypomnienia o naborze ("przypominamy", "przypomnienie") -> NIE
- instrukcje techniczne (jak złożyć wniosek, założyć konto, wzory, RODO) -> NIE
- spotkania, konsultacje, szkolenia, dni otwarte, punkty konsultacyjne, \
  dostęp do komputera dla przedsiębiorców -> NIE
- wstrzymanie/zawieszenie/zakończenie naboru -> NIE
- inny nabór niż KFS (staże, szkolenia indywidualne z FP, bony, prace interwencyjne, \
  rezerwa FP BEZ wzmianki o KFS, dotacje) -> NIE
- cokolwiek innego -> NIE

WAŻNE: Samo "nabór" lub "KFS" w tytule NIE WYSTARCZY. Tekst musi FAKTYCZNIE ogłaszać nabór wniosków.
WAŻNE: Jeśli treść mówi o "planowanym naborze", przygotowaniach, zakładaniu kont — to NIE jest nabór, to zapowiedź.

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
TERMIN: (data od - data do, np. "30.03.2026 - 03.04.2026", lub "w załączniku", lub "brak")
KWOTA: (kwota KFS np. "546 542 zł", lub "w załączniku", lub "brak")

Tytuł: {title}
Treść: {snippet}"""

# ============================================================
# SCRAPING
# ============================================================
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0"})


def fetch(url):
    for attempt in range(2):
        try:
            resp = SESSION.get(url, timeout=20)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser"), ""
        except requests.RequestException as e:
            if attempt == 0:
                time.sleep(2)
                continue
            return None, str(e)[:100]
    return None, "timeout"


KFS_KEYWORDS = re.compile(
    r"KFS|Krajow\w+ Fundusz\w* Szkoleniow|"
    r"kszta[łl]ceni\w+ ustawiczn|"
    r"nab[oó]r wniosk[oó]w",
    re.IGNORECASE,
)


def fetch_detail_content(url):
    """Pobiera pełną treść ze strony szczegółowej artykułu (z retry)."""
    for attempt in range(2):
        try:
            resp = SESSION.get(url, timeout=20)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup.find_all(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            candidates = []
            selectors = [
                ("div", {"class": re.compile(r"journal-content-article")}),
                ("div", {"class": re.compile(r"asset-content")}),
                ("div", {"class": re.compile(r"asset-full-content")}),
                ("div", {"class": re.compile(r"web-content-article")}),
                ("div", {"class": re.compile(r"portlet-content")}),
                ("div", {"class": re.compile(r"portlet-body")}),
                ("section", {}),
                ("article", {}),
                ("main", {}),
            ]
            for tag, attrs in selectors:
                found_list = soup.find_all(tag, attrs) if attrs else soup.find_all(tag)
                for found in found_list:
                    text = found.get_text(separator="\n", strip=True)
                    if len(text) > 20:
                        has_kfs = bool(KFS_KEYWORDS.search(text))
                        candidates.append((text, has_kfs))
            # Fallback: body text
            if not candidates:
                body = soup.find("body")
                if body:
                    text = body.get_text(separator="\n", strip=True)
                    if len(text) > 50:
                        candidates.append((text, bool(KFS_KEYWORDS.search(text))))
            if not candidates:
                if attempt == 0:
                    time.sleep(2)
                    continue
                return ""
            kfs_candidates = [t for t, k in candidates if k]
            if kfs_candidates:
                best = max(kfs_candidates, key=len)
            else:
                best = max((t for t, _ in candidates), key=len)
            lines = [l.strip() for l in best.split("\n") if l.strip()]
            text = "\n".join(lines)
            if len(text) <= MAX_SNIPPET:
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
            if last_important > MAX_SNIPPET:
                tail_size = min(last_important + 200, len(text)) - max(0, last_important - 1500)
                head_size = MAX_SNIPPET - tail_size - 20
                if head_size < 500:
                    head_size = 500
                    tail_start = max(0, last_important - 1500)
                    return text[:head_size] + "\n[...]\n" + text[tail_start:tail_start + MAX_SNIPPET - head_size - 10]
                tail_start = max(0, last_important - 1500)
                tail_end = min(len(text), last_important + 200)
                return text[:head_size] + "\n[...]\n" + text[tail_start:tail_end]
            return text[:MAX_SNIPPET]
        except Exception:
            if attempt == 0:
                time.sleep(2)
                continue
            return ""
    return ""


def extract_articles(soup, base_url):
    seen = set()
    articles = []
    for h3 in soup.find_all("h3"):
        parent = h3.parent
        if parent and parent.name == "a" and parent.get("href"):
            href, title = parent["href"], h3.get_text(strip=True)
        else:
            link = h3.find("a", href=True)
            if not link:
                continue
            href = link["href"]
            title = link.get_text(strip=True) or h3.get_text(strip=True)

        if not title or not href:
            continue
        if not href.startswith("http"):
            href = urljoin(base_url, href)

        key = title.strip().lower()
        if key in seen:
            continue
        seen.add(key)

        # Date
        pub_date = ""
        container = h3.find_parent("div", class_=re.compile(r"nnk|asset|results-row"))
        if container:
            dm = re.search(r"\d{2}\.\d{2}\.\d{4}", container.get_text())
            if dm:
                pub_date = dm.group()
        if not pub_date:
            prev = h3.find_previous(string=re.compile(r"\d{2}\.\d{2}\.\d{4}"))
            if prev:
                dm = re.search(r"\d{2}\.\d{2}\.\d{4}", prev)
                if dm:
                    pub_date = dm.group()

        # Snippet
        snippet = ""
        anchor = parent if parent and parent.name == "a" else h3
        next_p = anchor.find_next("p")
        if next_p:
            snippet = next_p.get_text(strip=True)[:MAX_SNIPPET]

        articles.append({
            "title": title, "url": href, "snippet": snippet,
            "date": pub_date, "source_type": "Aktualnosci",
        })
    return articles


def extract_kfs(soup, kfs_url):
    for cls in [r"journal-content-article", r"portlet-body"]:
        div = soup.find("div", class_=re.compile(cls))
        if div:
            break
    else:
        div = soup.find("main") or soup
    text = div.get_text(separator="\n", strip=True)
    if not text or len(text) < 20:
        return None
    return {"title": "KFS", "url": kfs_url, "snippet": text[:MAX_SNIPPET], "date": "", "source_type": "KFS"}


def scrape_all(urzedy):
    """Scrapuje wszystkie urzędy, zwraca (articles, errors)."""
    all_articles = []
    errors = []

    for i, urzad in enumerate(urzedy, 1):
        name = urzad["name"]
        homepage = urzad.get("homepage", "")
        kfs_url = urzad.get("kfs_url", "")
        base_url = urzad.get("base_url", homepage.rstrip("/"))

        print(f"  [{i}/{len(urzedy)}] {name}", end="", flush=True)

        # Aktualności
        soup, err = fetch(urzad.get("aktualnosci_url", homepage))
        time.sleep(REQUEST_DELAY)
        news = extract_articles(soup, base_url) if soup else []
        if not news and urzad.get("aktualnosci_url") and urzad["aktualnosci_url"] != homepage:
            soup2, err2 = fetch(urzad["aktualnosci_url"])
            time.sleep(REQUEST_DELAY)
            if soup2:
                news = extract_articles(soup2, base_url)

        for a in news:
            a["urzad"] = name
        # Pobierz pełną treść ze stron szczegółowych
        for a in news:
            detail = fetch_detail_content(a["url"])
            if detail and len(detail) > len(a.get("snippet", "")):
                a["snippet"] = detail
            time.sleep(0.2)
        # Retry dla artykułów z KFS w tytule i krótkim snippetem
        for a in news:
            if len(a.get("snippet", "")) < 200 and KFS_KEYWORDS.search(a.get("title", "")):
                time.sleep(1)
                detail = fetch_detail_content(a["url"])
                if detail and len(detail) > len(a.get("snippet", "")):
                    a["snippet"] = detail
        all_articles.extend(news)

        if not news:
            errors.append({"urzad": name, "url": homepage, "typ": "Aktualnosci",
                          "blad": err or "Brak artykulow"})

        # KFS
        if kfs_url:
            ks, ke = fetch(kfs_url)
            time.sleep(REQUEST_DELAY)
            if ks:
                kfs_art = extract_kfs(ks, kfs_url)
                if kfs_art:
                    kfs_art["urzad"] = name
                    all_articles.append(kfs_art)
            else:
                errors.append({"urzad": name, "url": kfs_url, "typ": "KFS", "blad": ke})

        print(f" -> {len(news)} art.")

    return all_articles, errors


# ============================================================
# KLASYFIKACJA AI (z cache)
# ============================================================
progress_lock = threading.Lock()
progress = {"done": 0, "total": 0, "tak": 0}

# Rate limiter — 5 RPM per key, globalnie max 1 request / 2.5s
RPM_PER_KEY = 5
_rate_lock = threading.Lock()
_key_last_used = {}  # api_key -> timestamp ostatniego użycia


def call_gemini(api_key, prompt):
    # Throttle: odczekaj min 12s od ostatniego użycia tego klucza (60/5=12s)
    min_interval = 60.0 / RPM_PER_KEY
    with _rate_lock:
        now = time.time()
        last = _key_last_used.get(api_key, 0)
        wait = last + min_interval - now
        if wait > 0:
            time.sleep(wait)
        _key_last_used[api_key] = time.time()

    url = f"{GEMINI_URL}?key={api_key}"
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 429:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_WAIT * attempt)
                    # Zaktualizuj timestamp po retry
                    with _rate_lock:
                        _key_last_used[api_key] = time.time()
                    continue
                return None
            resp.raise_for_status()
            return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_WAIT)
                continue
            return None
    return None


def parse_response(text):
    wynik, powod, termin, kwota = "NIE", "", "", ""
    if not text:
        return wynik, powod, termin, kwota
    skip = {"brak", "nie dotyczy", "nd", "n/d", "-", ""}
    for line in text.split("\n"):
        line = line.strip()
        up = line.upper()
        if up.startswith("WYNIK:"):
            wynik = "TAK" if "TAK" in line.split(":", 1)[1].upper() else "NIE"
        elif up.startswith("POWOD:") or up.startswith("POWÓD:"):
            powod = line.split(":", 1)[1].strip()
        elif up.startswith("TERMIN:"):
            v = line.split(":", 1)[1].strip()
            if v.lower() not in skip:
                termin = v
        elif up.startswith("KWOTA:"):
            v = line.split(":", 1)[1].strip()
            if v.lower() not in skip:
                kwota = v
    return wynik, powod, termin, kwota


KFS_NABOR_TITLE = re.compile(
    r"nab[oó]r.*(?:KFS|Krajow\w+ Fundusz\w* Szkoleniow)|"
    r"(?:KFS|Krajow\w+ Fundusz\w* Szkoleniow).*nab[oó]r",
    re.IGNORECASE,
)


def classify_article(art, key_index, cache):
    """Klasyfikuje artykuł — zwraca wynik (z cache lub z AI)."""
    url = art.get("url", "")
    title = art.get("title", "")

    # Sprawdź cache
    if url in cache:
        cached = cache[url]
        # Reklasyfikuj jeśli: cache=NIE ale tytuł/snippet sugeruje nabór KFS
        # (prawdopodobnie wcześniej snippet był za krótki)
        snippet = art.get("snippet", "")
        title_or_snippet = title + " " + snippet[:500]
        if cached.get("wynik") == "NIE" and KFS_NABOR_TITLE.search(title_or_snippet):
            pass  # pomijamy cache, reklasyfikuj
        else:
            return cached, True

    # Nowy artykuł — wyślij do AI
    api_key = API_KEYS[key_index % len(API_KEYS)]
    prompt = CLASSIFY_PROMPT.format(
        title=art.get("title", ""),
        snippet=(art.get("snippet", "") or art.get("title", ""))[:MAX_SNIPPET],
        today=TODAY,
    )
    text = call_gemini(api_key, prompt)
    wynik, powod, termin, kwota = parse_response(text)

    result = {
        "wynik": wynik,
        "powod": powod,
        "termin": termin,
        "kwota": kwota,
        "classified_date": TODAY,
    }

    with progress_lock:
        progress["done"] += 1
        if wynik == "TAK":
            progress["tak"] += 1

    extra = ""
    if termin:
        extra += f" [{termin}]"
    if kwota:
        extra += f" {kwota}"
    print(f"    AI [{progress['done']}/{progress['total']}] "
          f"{art.get('urzad',''):18s} -> {wynik}{extra}")

    return result, False


def classify_all(articles, cache):
    """Klasyfikuje wszystkie artykuły — nowe przez AI, stare z cache."""
    new_articles = [a for a in articles if a.get("url", "") not in cache]
    cached_count = len(articles) - len(new_articles)

    print(f"\n  Artykulow: {len(articles)} (z cache: {cached_count}, nowych: {len(new_articles)})")

    progress["total"] = len(new_articles)
    progress["done"] = 0
    progress["tak"] = 0

    results = []

    # Cache hits — przetwarzaj synchronicznie
    for art in articles:
        url = art.get("url", "")
        if url in cache:
            r = cache[url]
            results.append({
                "url": url,
                "title": art.get("title", ""),
                "urzad": art.get("urzad", ""),
                "date": art.get("date", ""),
                "wynik": r.get("wynik", "NIE"),
                "powod": r.get("powod", ""),
                "termin": r.get("termin", ""),
                "kwota": r.get("kwota", ""),
                "snippet": art.get("snippet", "")[:MAX_SNIPPET],
            })

    # Nowe artykuły — klasyfikuj równolegle
    if new_articles:
        with ThreadPoolExecutor(max_workers=min(len(API_KEYS), 6)) as executor:
            futures = {}
            for i, art in enumerate(new_articles):
                future = executor.submit(classify_article, art, i, cache)
                futures[future] = art

            for future in as_completed(futures):
                art = futures[future]
                result, from_cache = future.result()
                url = art.get("url", "")

                # Zapisz do cache
                cache[url] = result

                results.append({
                    "url": url,
                    "title": art.get("title", ""),
                    "urzad": art.get("urzad", ""),
                    "date": art.get("date", ""),
                    "wynik": result.get("wynik", "NIE"),
                    "powod": result.get("powod", ""),
                    "termin": result.get("termin", ""),
                    "kwota": result.get("kwota", ""),
                    "snippet": art.get("snippet", "")[:MAX_SNIPPET],
                })

    return results


# ============================================================
# WYSYŁKA DO ESPOCRM
# ============================================================
def push_to_crm(results):
    """Wysyła nowe nabory TAK do EspoCRM."""
    crm_url = os.environ.get("ESPOCRM_URL", "").rstrip("/")
    crm_key = os.environ.get("ESPOCRM_API_KEY", "")
    if not crm_url or not crm_key:
        print("  Brak ESPOCRM_URL/ESPOCRM_API_KEY — pomijam CRM")
        return

    tak = [r for r in results if r.get("wynik") == "TAK"]
    print(f"\n  CRM: {len(tak)} naborów TAK do wysłania")

    headers = {
        "X-Api-Key": crm_key,
        "Content-Type": "application/json",
    }

    # Pobierz istniejące URL-e z CRM żeby nie duplikować
    existing = set()
    try:
        resp = requests.get(
            f"{crm_url}/api/v1/NaborKFS",
            headers=headers,
            params={"select": "url", "maxSize": 200},
            timeout=15,
        )
        if resp.ok:
            for rec in resp.json().get("list", []):
                if rec.get("url"):
                    existing.add(rec["url"])
    except Exception as ex:
        print(f"  CRM: Błąd pobierania istniejących: {ex}")

    added = 0
    for r in tak:
        if r.get("url") in existing:
            continue
        # Parsuj datę DD.MM.YYYY → YYYY-MM-DD
        crm_date = ""
        if r.get("date"):
            parts = r["date"].split(".")
            if len(parts) == 3:
                crm_date = f"{parts[2]}-{parts[1]}-{parts[0]}"

        payload = {
            "name": r.get("title", "")[:255],
            "urzad": r.get("urzad", ""),
            "termin": r.get("termin", ""),
            "kwota": r.get("kwota", ""),
            "url": r.get("url", ""),
            "datapublikacji": crm_date,
            "powod": r.get("powod", ""),
            "status": "Nowy",
        }
        try:
            resp = requests.post(
                f"{crm_url}/api/v1/NaborKFS",
                headers=headers,
                json=payload,
                timeout=15,
            )
            if resp.status_code in (200, 201):
                added += 1
            else:
                print(f"  CRM: Błąd {resp.status_code} dla {r.get('urzad','')}")
        except Exception as ex:
            print(f"  CRM: {ex}")

    print(f"  CRM: Dodano {added} nowych naborów (pominięto {len(tak) - added} istniejących)")


# ============================================================
# GENEROWANIE RAPORTU HTML
# ============================================================
def generate_report(results, errors):
    """Generuje raport HTML z mapą powiatów."""
    import html as html_mod

    def e(text):
        return html_mod.escape(str(text)) if text else ""

    # Load geo data
    geojson = {"type": "FeatureCollection", "features": []}
    if GEOJSON_FILE.exists():
        with open(GEOJSON_FILE, "r", encoding="utf-8") as f:
            geojson = json.load(f)

    urzad_to_powiat = {}
    if MAPPING_FILE.exists():
        with open(MAPPING_FILE, "r", encoding="utf-8") as f:
            urzad_to_powiat = json.load(f)

    # Potwierdzone nabory: TAK + konkretny termin (nie "w załączniku"/"brak"/pusty)
    _uncertain = {"w załączniku", "brak", ""}
    confirmed = [r for r in results if r["wynik"] == "TAK"
                 and r.get("termin", "").lower() not in _uncertain]
    # Artykuły KFS: TAK bez szczegółów + NIE wspominające KFS
    tak_no_details = [r for r in results if r["wynik"] == "TAK"
                      and r.get("termin", "").lower() in _uncertain]
    nie_kfs = [r for r in results if r["wynik"] != "TAK" and KFS_KEYWORDS.search(
        r.get("title", "") + " " + r.get("snippet", "")[:500])]
    related = tak_no_details + nie_kfs
    total = len(results)
    count_tak = len(confirmed)
    count_related = len(related)
    tak_powiaty = sorted(set(r["urzad"] for r in confirmed))
    count_tak_powiaty = len(tak_powiaty)
    count_err = len(errors)

    confirmed.sort(key=lambda r: r.get("date", ""), reverse=True)
    related.sort(key=lambda r: r.get("date", ""), reverse=True)
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Enrich GeoJSON
    urzad_data = {}
    for r in results:
        u = r.get("urzad", "")
        if not u:
            continue
        if u not in urzad_data:
            urzad_data[u] = {"tak": [], "total": 0}
        urzad_data[u]["total"] += 1
        if r["wynik"] == "TAK":
            urzad_data[u]["tak"].append({
                "title": r.get("title", ""), "url": r.get("url", ""),
                "date": r.get("date", ""), "powod": r.get("powod", ""),
                "termin": r.get("termin", ""), "kwota": r.get("kwota", ""),
            })

    powiat_status = {}
    for urzad, geo_name in urzad_to_powiat.items():
        if urzad in urzad_data:
            data = urzad_data[urzad]
            ex = powiat_status.get(geo_name)
            if ex:
                ex["tak"] = ex["tak"] or len(data["tak"]) > 0
                ex["count"] += len(data["tak"])
                ex["articles"].extend(data["tak"][:3])
                ex["urzedy"].append(urzad)
            else:
                powiat_status[geo_name] = {
                    "urzedy": [urzad], "tak": len(data["tak"]) > 0,
                    "count": len(data["tak"]), "articles": data["tak"][:5],
                }

    for feat in geojson["features"]:
        nazwa = feat["properties"]["nazwa"]
        st = powiat_status.get(nazwa)
        if st:
            feat["properties"]["has_tak"] = st["tak"]
            feat["properties"]["tak_count"] = st["count"]
            feat["properties"]["urzedy"] = ", ".join(st["urzedy"])
            feat["properties"]["articles"] = json.dumps(st["articles"][:5], ensure_ascii=False)
        else:
            feat["properties"]["has_tak"] = False
            feat["properties"]["tak_count"] = 0
            feat["properties"]["urzedy"] = ""
            feat["properties"]["articles"] = "[]"

    geojson_json = json.dumps(geojson, ensure_ascii=False)

    # Build cards
    def card(r, show_move_btn=False):
        is_tak = r["wynik"] == "TAK"
        badge = '<span class="badge tak">NABOR KFS</span>' if is_tak else '<span class="badge nie">INNE</span>'
        dt = f'<span class="date">{e(r["date"])}</span>' if r.get("date") else ""
        termin = r.get("termin", "")
        kwota = r.get("kwota", "")
        termin_h = f'<span class="termin">{e(termin)}</span>' if termin and is_tak else ""
        kwota_h = f'<span class="kwota">{e(kwota)}</span>' if kwota and is_tak else ""
        snippet = r.get("snippet", "")[:200]
        sd = f'{r.get("urzad","")} {r.get("title","")} {snippet}'.lower()
        url = e(r.get("url", ""))
        move_btn = f'<button class="btn-promote" onclick="promote(this)" title="Przeniес do naborów">&#x2714; Do naborów</button>' if show_move_btn else ""
        return f'''<div class="card" data-search="{e(sd)}" data-url="{url}">
<div class="row1"><span class="urzad">{e(r.get("urzad",""))}</span>{badge}{termin_h}{kwota_h}{move_btn}{dt}</div>
<div class="title"><a href="{url}" target="_blank">{e(r.get("title",""))}</a></div>
<div class="snippet">{e(snippet)}</div>
<div class="ai">AI: {e(r.get("powod",""))}</div></div>'''

    tak_cards = "\n".join(card(r) for r in confirmed)
    related_cards = "\n".join(card(r, show_move_btn=True) for r in related)
    err_cards = "\n".join(
        f'<div class="card err-card"><div class="row1"><span class="urzad">{e(x.get("urzad",""))}</span>'
        f'<span class="err-kfs">{e(x.get("typ",""))}</span></div>'
        f'<div class="err-msg">{e(x.get("blad",""))}</div></div>'
        for x in errors
    )

    err_news = len([x for x in errors if x.get("typ") == "Aktualnosci"])
    err_kfs = len([x for x in errors if x.get("typ") == "KFS"])

    # Write full HTML (using same template as test_report.py but cleaner)
    page = f'''<!DOCTYPE html>
<html lang="pl"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Nabory KFS - Mapa Polski</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",system-ui,sans-serif;background:#f0f2f5;color:#1a1a1a}}
.header{{background:linear-gradient(135deg,#1e3a5f,#2d5a8e);color:#fff;padding:24px 32px}}
.header h1{{font-size:22px;font-weight:600;margin-bottom:4px}}
.header .meta{{font-size:14px;opacity:.85}}
.header .meta2{{font-size:13px;opacity:.7;margin-top:2px}}
.tabs{{display:flex;background:#fff;border-bottom:2px solid #e5e7eb;position:sticky;top:0;z-index:1000;flex-wrap:wrap}}
.tab{{padding:14px 24px;cursor:pointer;font-size:15px;font-weight:500;color:#666;border-bottom:3px solid transparent;user-select:none}}
.tab:hover{{color:#1e3a5f;background:#f8f9fa}}.tab.active{{color:#1e3a5f;border-bottom-color:#1e3a5f}}
.tab.map-tab{{color:#059669}}.tab.map-tab.active{{color:#059669;border-bottom-color:#059669}}
.tab.err-tab{{color:#b91c1c}}.tab.err-tab.active{{color:#b91c1c;border-bottom-color:#b91c1c}}
.panel{{display:none;max-width:900px;margin:0 auto;padding:20px}}.panel.active{{display:block}}
.panel.map-panel{{max-width:100%;padding:0}}
.search{{width:100%;padding:10px 16px;font-size:15px;border:1px solid #d1d5db;border-radius:8px;margin-bottom:16px;outline:none}}
.search:focus{{border-color:#2d5a8e;box-shadow:0 0 0 3px rgba(45,90,142,.15)}}
.card{{background:#fff;border:1px solid #e5e7eb;border-radius:10px;padding:14px 18px;margin-bottom:8px}}
.card:hover{{box-shadow:0 2px 8px rgba(0,0,0,.08)}}.card.hidden{{display:none}}
.err-card{{border-left:4px solid #ef4444}}
.row1{{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:4px}}
.urzad{{font-weight:700;font-size:15px}}
.badge{{padding:2px 10px;border-radius:5px;font-size:12px;font-weight:700;text-transform:uppercase}}
.badge.tak{{background:#dcfce7;color:#166534}}.badge.nie{{background:#f3f4f6;color:#6b7280}}
.termin{{background:#dbeafe;color:#1e40af;padding:2px 8px;border-radius:5px;font-size:12px;font-weight:600;white-space:nowrap}}
.kwota{{background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:5px;font-size:12px;font-weight:600;white-space:nowrap}}
.date{{margin-left:auto;color:#9ca3af;font-size:13px}}
.title a{{color:#1e3a5f;text-decoration:none;font-size:15px;font-weight:500}}.title a:hover{{text-decoration:underline}}
.snippet{{font-size:13px;color:#6b7280;line-height:1.5;margin-bottom:4px}}
.ai{{font-size:12px;color:#9ca3af;font-style:italic}}
.err-msg{{font-size:13px;color:#b91c1c}}.err-kfs{{background:#e0e7ff;color:#3730a3;padding:2px 10px;border-radius:5px;font-size:12px;font-weight:600}}
.btn-promote{{background:#dcfce7;color:#166534;border:1px solid #86efac;border-radius:6px;padding:2px 10px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap}}
.btn-promote:hover{{background:#bbf7d0}}
.btn-demote{{background:#fef3c7;color:#92400e;border:1px solid #fcd34d;border-radius:6px;padding:2px 10px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap}}
.btn-demote:hover{{background:#fde68a}}
.promoted-badge{{background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:5px;font-size:11px;font-weight:600}}
.tab.related-tab{{color:#2563eb}}.tab.related-tab.active{{color:#2563eb;border-bottom-color:#2563eb}}
.empty{{text-align:center;padding:48px;color:#9ca3af;display:none}}
#map{{width:100%;height:calc(100vh - 140px);min-height:500px}}
.map-legend{{background:rgba(255,255,255,.95);padding:14px 18px;border-radius:10px;box-shadow:0 2px 12px rgba(0,0,0,.15);font-size:13px;line-height:2}}
.map-legend h4{{margin:0 0 8px;font-size:14px;color:#1e3a5f}}
.map-legend .legend-item{{display:flex;align-items:center;gap:8px}}
.map-legend .swatch{{width:20px;height:14px;border-radius:3px;display:inline-block;border:1px solid rgba(0,0,0,.2)}}
.map-stats{{position:absolute;top:10px;right:10px;z-index:999;background:rgba(255,255,255,.95);padding:14px 18px;border-radius:10px;box-shadow:0 2px 12px rgba(0,0,0,.15);font-size:13px}}
.map-stats h4{{margin:0 0 6px;font-size:14px;color:#1e3a5f}}.map-stats .stat-num{{font-weight:700;color:#166534}}
.info-hover{{background:rgba(255,255,255,.92);padding:8px 12px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.12);font-size:13px;display:none}}
.info-hover b{{color:#1e3a5f}}.info-hover .info-tak{{color:#166534;font-weight:700}}
.popup-title{{font-weight:700;font-size:14px;color:#1e3a5f;margin-bottom:8px;border-bottom:2px solid #22c55e;padding-bottom:6px}}
.popup-title.no-tak{{border-bottom-color:#d1d5db;color:#6b7280}}
.popup-article{{margin:6px 0;padding:4px 0;border-bottom:1px solid #f0f0f0}}
.popup-article:last-child{{border-bottom:none}}
.popup-article a{{color:#1e3a5f;text-decoration:none;font-size:12px;font-weight:500}}.popup-article a:hover{{text-decoration:underline}}
.popup-termin{{font-size:11px;color:#1e40af;font-weight:600}}.popup-date{{font-size:11px;color:#9ca3af}}
</style></head><body>
<div class="header">
<h1>Nabory KFS - Powiatowe Urzedy Pracy</h1>
<div class="meta">Aktualizacja: {now} | {count_tak} naborow KFS w {count_tak_powiaty} powiatach / {total} artykulow</div>
<div class="meta2">Automatyczna aktualizacja co 5h | Bledy: {count_err}</div>
</div>
<div class="tabs">
<div class="tab map-tab active" data-tab="map">&#x1F5FA; Mapa ({count_tak_powiaty})</div>
<div class="tab" data-tab="nabory">&#x1F7E2; Nabory KFS (<span id="cnt-nabory">{count_tak}</span>)</div>
<div class="tab related-tab" data-tab="related">&#x1F4C4; Artykuly KFS ({count_related})</div>
<div class="tab err-tab" data-tab="errors">&#x26A0; Bledy ({count_err})</div>
</div>
<div class="panel map-panel active" id="p-map"><div style="position:relative">
<div id="map"></div>
<div class="map-stats"><h4>Statystyki</h4>
<div>Nabory KFS: <span class="stat-num">{count_tak}</span></div>
<div>Powiaty z naborem: <span class="stat-num">{count_tak_powiaty}</span></div>
<div>Artykulow: {total}</div></div></div></div>
<div class="panel" id="p-nabory">
<input class="search" placeholder="Szukaj po urzedzie lub tytule..." data-list="l-nabory">
<div id="promoted-section" style="display:none"><h3 style="font-size:14px;color:#92400e;margin:12px 0 8px">Recznie przeniesione</h3><div id="l-promoted"></div></div>
<div id="l-nabory">{tak_cards}</div><div class="empty" id="e-nabory">Brak wynikow.</div></div>
<div class="panel" id="p-related">
<input class="search" placeholder="Szukaj po urzedzie lub tytule..." data-list="l-related">
<div id="l-related">{related_cards}</div><div class="empty" id="e-related">Brak wynikow.</div></div>
<div class="panel" id="p-errors"><div id="l-errors">{err_cards}</div></div>
<script>
document.querySelectorAll(".tab").forEach(function(t){{t.addEventListener("click",function(){{
document.querySelectorAll(".tab").forEach(function(x){{x.classList.remove("active")}});
document.querySelectorAll(".panel").forEach(function(x){{x.classList.remove("active")}});
t.classList.add("active");document.getElementById("p-"+t.dataset.tab).classList.add("active");
if(t.dataset.tab==="map"&&window._map)window._map.invalidateSize()}});}});
document.querySelectorAll(".search").forEach(function(input){{input.addEventListener("input",function(){{
var q=input.value.toLowerCase(),list=document.getElementById(input.dataset.list),
cards=list.querySelectorAll(".card"),id=input.dataset.list.split("-")[1],empty=document.getElementById("e-"+id),n=0;
cards.forEach(function(c){{var ok=c.dataset.search.indexOf(q)!==-1;c.classList.toggle("hidden",!ok);if(ok)n++}});
empty.style.display=n===0?"block":"none"}});}});

var geoData={geojson_json};
var map=L.map("map").setView([52,19.4],6);window._map=map;
L.tileLayer("https://{{s}}.basemaps.cartocdn.com/light_nolabels/{{z}}/{{x}}/{{y}}{{r}}.png",{{
subdomains:"abcd",maxZoom:19,attribution:'OSM/CARTO'}}).addTo(map);
var info=L.control({{position:"topright"}});
info.onAdd=function(){{this._div=L.DomUtil.create("div","info-hover");return this._div}};
info.update=function(p){{if(!p){{this._div.style.display="none";return}}this._div.style.display="block";
var n=p.nazwa.replace("powiat ","");n=n.charAt(0).toUpperCase()+n.slice(1);
this._div.innerHTML=p.has_tak?'<b>'+n+'</b><br><span class="info-tak">Nabor KFS: '+p.tak_count+'</span>'
+(p.urzedy?'<br><small>'+p.urzedy+'</small>':''):'<b>'+n+'</b><br>Brak naboru'}};info.addTo(map);

function style(f){{var t=f.properties.has_tak,c=f.properties.tak_count||0;
return t?{{fillColor:c>=3?"#15803d":c>=2?"#22c55e":"#4ade80",weight:1.5,color:"#fff",fillOpacity:.75}}
:{{fillColor:"#e8e8e8",weight:.8,color:"#ccc",fillOpacity:.5}}}}
var gl;function onEach(f,l){{l.on({{
mouseover:function(e){{e.target.setStyle({{weight:2.5,color:f.properties.has_tak?"#166534":"#666",
fillOpacity:f.properties.has_tak?.9:.7}});e.target.bringToFront();info.update(f.properties)}},
mouseout:function(e){{gl.resetStyle(e.target);info.update()}},
click:function(e){{var p=f.properties,n=p.nazwa.replace("powiat ","");n=n.charAt(0).toUpperCase()+n.slice(1);
var h="";if(p.has_tak){{h+='<div class="popup-title">'+n+(p.urzedy?' <small style="font-weight:400;color:#6b7280">('+p.urzedy+')</small>':'')+'</div>';
var arts=JSON.parse(p.articles||"[]");arts.forEach(function(a){{h+='<div class="popup-article"><a href="'+a.url+'" target="_blank">'+a.title.substring(0,70)+(a.title.length>70?"...":"")+'</a>';
var m=[];if(a.termin)m.push("<b>"+a.termin+"</b>");if(a.kwota)m.push(a.kwota);
if(m.length)h+='<br><span class="popup-termin">'+m.join(" | ")+"</span>";
else if(a.date)h+='<br><span class="popup-date">'+a.date+"</span>";h+="</div>"}})}}
else{{h+='<div class="popup-title no-tak">'+n+'</div><div style="font-size:12px;color:#9ca3af">Brak naboru</div>'}}
L.popup({{maxWidth:350}}).setLatLng(e.latlng).setContent(h).openOn(map)}}}})}};
gl=L.geoJSON(geoData,{{style:style,onEachFeature:onEach}}).addTo(map);
L.tileLayer("https://{{s}}.basemaps.cartocdn.com/light_only_labels/{{z}}/{{x}}/{{y}}{{r}}.png",{{
subdomains:"abcd",maxZoom:19,pane:"overlayPane"}}).addTo(map);
var leg=L.control({{position:"bottomleft"}});
leg.onAdd=function(){{var d=L.DomUtil.create("div","map-legend");
d.innerHTML='<h4>Nabory KFS</h4><div class="legend-item"><span class="swatch" style="background:#4ade80"></span> 1 nabor</div>'
+'<div class="legend-item"><span class="swatch" style="background:#22c55e"></span> 2 nabory</div>'
+'<div class="legend-item"><span class="swatch" style="background:#15803d"></span> 3+</div>'
+'<div class="legend-item"><span class="swatch" style="background:#e8e8e8"></span> Brak</div>';return d}};
leg.addTo(map);map.fitBounds(gl.getBounds().pad(.02));

/* Promote/demote: przenoszenie kart miedzy zakladkami */
var STORE_KEY="kfs_promoted";
function getPromoted(){{try{{return JSON.parse(localStorage.getItem(STORE_KEY)||"[]")}}catch(e){{return[]}}}}
function savePromoted(arr){{localStorage.setItem(STORE_KEY,JSON.stringify(arr))}}
function updateCount(){{var n=document.querySelectorAll("#l-nabory .card").length+document.querySelectorAll("#l-promoted .card").length;document.getElementById("cnt-nabory").textContent=n}}
function promote(btn){{
var card=btn.closest(".card"),url=card.dataset.url,pr=getPromoted();
if(pr.indexOf(url)===-1)pr.push(url);savePromoted(pr);
card.querySelector(".btn-promote").remove();
var db=document.createElement("button");db.className="btn-demote";db.textContent="Cofnij";
db.setAttribute("onclick","demote(this)");card.querySelector(".row1").appendChild(db);
var pb=document.createElement("span");pb.className="promoted-badge";pb.textContent="reczne";card.querySelector(".row1").appendChild(pb);
document.getElementById("l-promoted").appendChild(card);
document.getElementById("promoted-section").style.display="block";updateCount()}}
function demote(btn){{
var card=btn.closest(".card"),url=card.dataset.url,pr=getPromoted();
pr=pr.filter(function(u){{return u!==url}});savePromoted(pr);
btn.remove();var pb=card.querySelector(".promoted-badge");if(pb)pb.remove();
var mb=document.createElement("button");mb.className="btn-promote";mb.textContent="\\u2714 Do naborow";
mb.setAttribute("onclick","promote(this)");card.querySelector(".row1").appendChild(mb);
document.getElementById("l-related").prepend(card);
if(!document.querySelectorAll("#l-promoted .card").length)document.getElementById("promoted-section").style.display="none";updateCount()}}
/* Restore promoted on load */
(function(){{var pr=getPromoted();if(!pr.length)return;
var cards=document.querySelectorAll("#l-related .card");
cards.forEach(function(c){{if(pr.indexOf(c.dataset.url)!==-1){{
var btn=c.querySelector(".btn-promote");if(btn)btn.remove();
var db=document.createElement("button");db.className="btn-demote";db.textContent="Cofnij";
db.setAttribute("onclick","demote(this)");c.querySelector(".row1").appendChild(db);
var pb=document.createElement("span");pb.className="promoted-badge";pb.textContent="reczne";c.querySelector(".row1").appendChild(pb);
document.getElementById("l-promoted").appendChild(c);}}}});
if(document.querySelectorAll("#l-promoted .card").length)document.getElementById("promoted-section").style.display="block";
updateCount()}})();
</script></body></html>'''

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(page)

    return count_tak, count_tak_powiaty


# ============================================================
# MAIN
# ============================================================
def main():
    print(f"{'='*60}")
    print(f"  KFS SCRAPER - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Data dzisiejsza: {TODAY}")
    print(f"  Klucze API: {len(API_KEYS)}")
    print(f"{'='*60}")

    # Load urzedy
    with open(URZEDY_FILE, "r", encoding="utf-8") as f:
        urzedy = json.load(f)
    print(f"\n[1/4] Ladowanie {len(urzedy)} urzedow...")

    # Load cache
    cache = {}
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
    print(f"[2/4] Cache: {len(cache)} wpisow")

    # Scrape
    print(f"\n[3/4] Scrapowanie stron urzedow...")
    articles, errors = scrape_all(urzedy)
    print(f"  Znaleziono {len(articles)} artykulow, {len(errors)} bledow")

    # Odrzuć artykuły z datą publikacji sprzed 2026 roku
    MIN_YEAR = 2026
    before = len(articles)
    filtered = []
    for a in articles:
        d = a.get("date", "")
        if d:
            try:
                year = int(d.split(".")[-1])
                if year < MIN_YEAR:
                    continue
            except ValueError:
                pass
        filtered.append(a)
    articles = filtered
    if before != len(articles):
        print(f"  Odfiltrowano {before - len(articles)} starych artykulow (przed {MIN_YEAR})")

    # Classify
    print(f"\n[4/4] Klasyfikacja AI...")
    results = classify_all(articles, cache)

    # Save cache
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    # Save results
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    with open(ERRORS_FILE, "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    # Push to CRM
    push_to_crm(results)

    # Generate report
    count_tak, count_powiaty = generate_report(results, errors)

    # Summary
    print(f"\n{'='*60}")
    print(f"  GOTOWE!")
    print(f"  Nabory KFS: {count_tak} (w {count_powiaty} powiatach)")
    print(f"  Artykulow: {len(results)}")
    print(f"  Nowych (AI): {progress['done']}")
    print(f"  Z cache: {len(results) - progress['done']}")
    print(f"  Raport: {OUTPUT_FILE.name}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
