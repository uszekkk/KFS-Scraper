#!/usr/bin/env python3
"""
Pipeline produkcyjny: scrape → cache → classify (Gemini) → raport HTML z mapą.
Uruchamiany co 5h przez GitHub Actions lub ręcznie.
"""

import json
import os
import re
import smtplib
import sys
import time
import threading
import unicodedata
from datetime import date, datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from urllib.parse import urljoin, urlparse, parse_qs
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
FORECASTS_FILE = BASE_DIR / "forecasts.json"
ERRORS_FILE = BASE_DIR / "errors.json"
OUTPUT_FILE = BASE_DIR / "index.html"
GEOJSON_FILE = BASE_DIR / "powiaty.geojson"
MAPPING_FILE = BASE_DIR / "urzad_to_powiat.json"
WOJ_FILE = BASE_DIR / "urzad_to_woj.json"

TODAY = date.today().strftime("%d.%m.%Y")
REQUEST_DELAY = 0.3
MAX_SNIPPET = 10000
NEWS_PAGE_LIMIT = int(os.environ.get("NEWS_PAGE_LIMIT", "8"))

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


def _article_fingerprint(url):
    """Canonical key for Liferay articles — (domain, slug).

    Liferay URLs have two formats for the same article:
      1) /page/-/asset_publisher/ID/content/SLUG?p_r_p_assetEntryId=...
      2) /-/SLUG  (friendly URL)
    Both share the same slug. Returns (domain, slug) or None.
    """
    parsed = urlparse(url)
    # Format 1: .../content/SLUG
    m = re.search(r"/content/([^/?]+)", parsed.path)
    if m:
        return (parsed.netloc, m.group(1))
    # Format 2: /-/SLUG (friendly URL)
    m = re.search(r"/-/([^/?]+)", parsed.path)
    if m:
        return (parsed.netloc, m.group(1))
    return None


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


def _clean_url(url):
    """URL without a fragment; enough for visited/dedup keys."""
    return url.split("#", 1)[0]


_ASCII_PL_MAP = str.maketrans("ąćęłńóśźżĄĆĘŁŃÓŚŹŻ", "acelnoszzACELNOSZZ")


def _ascii_lower(text):
    """Lowercase ASCII approximation used for robust Polish text matching."""
    text = str(text or "").translate(_ASCII_PL_MAP)
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii").lower()


def _pagination_links(soup, current_url):
    """Find likely news-list pagination links on the same host."""
    links = []
    current = urlparse(current_url)
    for link in soup.find_all("a", href=True):
        href = _clean_url(urljoin(current_url, link["href"]))
        parsed = urlparse(href)
        if parsed.netloc != current.netloc or href == _clean_url(current_url):
            continue

        text = _ascii_lower(link.get_text(" ", strip=True))
        rel_attr = link.get("rel") or []
        if isinstance(rel_attr, str):
            rel = rel_attr.lower()
        else:
            rel = " ".join(rel_attr).lower()
        qs = parse_qs(parsed.query)
        qkeys = {k.lower() for k in qs}

        has_page_param = any(
            k == "cur" or k.endswith("_cur") or k in {"page", "pagenum"}
            for k in qkeys
        )
        has_pager_text = (
            "next" in rel
            or text in {">", ">>", "»", "next", "nastepna", "nastepne"}
            or bool(re.fullmatch(r"\d{1,3}", text))
        )
        if has_page_param or has_pager_text:
            links.append(href)
    return links


KFS_KEYWORDS = re.compile(
    r"KFS"
    r"|Krajow\w+ Fundusz\w* Szkoleniow"
    r"|fundusz\w* szkoleniow"
    r"|kszta[łl]ceni\w+ ustawiczn\w* .{0,30}(?:środk|wnios|nab[oó]r|dofin)"
    r"|(?:środk|wnios|nab[oó]r|dofin)\w* .{0,30}kszta[łl]ceni\w+ ustawiczn"
    r"|dofinansow\w+ kszta[łl]ceni"
    r"|nab[oó]r .{0,20}(?:KFS|fundusz\w* szkoleniow)"
    r"|(?:KFS|fundusz\w* szkoleniow) .{0,20}nab[oó]r",
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


def _extract_date(element):
    """Szuka daty DD.MM.YYYY w kontenerze nadrzędnym lub poprzedzającym elemencie."""
    container = element.find_parent("div", class_=re.compile(r"nnk|asset|results-row"))
    if container:
        date_nodes = container.find_all(
            class_=re.compile(r"date|data|publish|modified|metadata|subinfo|czas", re.IGNORECASE)
        )
        for node in date_nodes:
            dm = re.search(r"\d{2}\.\d{2}\.\d{4}", node.get_text(" ", strip=True))
            if dm:
                return dm.group()
        dm = re.search(r"\d{2}\.\d{2}\.\d{4}", container.get_text(" ", strip=True)[:250])
        if dm:
            return dm.group()
    prev = element.find_previous(string=re.compile(r"\d{2}\.\d{2}\.\d{4}"))
    if prev:
        dm = re.search(r"\d{2}\.\d{2}\.\d{4}", prev)
        if dm:
            return dm.group()
    return ""


def extract_articles(soup, base_url):
    seen = set()
    articles = []

    # 1) Nagłówki h3/h4 z linkami
    for heading in soup.find_all(["h3", "h4"]):
        parent = heading.parent
        if parent and parent.name == "a" and parent.get("href"):
            href, title = parent["href"], heading.get_text(strip=True)
        else:
            link = heading.find("a", href=True)
            if not link:
                continue
            href = link["href"]
            title = link.get_text(strip=True) or heading.get_text(strip=True)

        if not title or not href:
            continue
        if not href.startswith("http"):
            href = urljoin(base_url, href)

        key = title.strip().lower()
        if key in seen:
            continue
        seen.add(key)

        pub_date = _extract_date(heading)
        snippet = ""
        anchor = parent if parent and parent.name == "a" else heading
        next_p = anchor.find_next("p")
        if next_p:
            snippet = next_p.get_text(strip=True)[:MAX_SNIPPET]

        articles.append({
            "title": title, "url": href, "snippet": snippet,
            "date": pub_date, "source_type": "Aktualnosci",
        })

    # 2) Liferay nnk-title-list — linki w <div class="nnk-title-list-list-item">
    for div in soup.find_all("div", class_=re.compile(r"nnk-title-list-list-item")):
        link = div.find("a", href=True)
        if not link:
            continue
        href = link["href"]
        title = link.get_text(strip=True)
        if not title or not href:
            continue
        if not href.startswith("http"):
            href = urljoin(base_url, href)
        key = title.strip().lower()
        if key in seen:
            continue
        seen.add(key)
        pub_date = _extract_date(div)
        articles.append({
            "title": title, "url": href, "snippet": "",
            "date": pub_date, "source_type": "Aktualnosci",
        })

    return articles


def fetch_news_articles(start_url, base_url, max_pages=NEWS_PAGE_LIMIT):
    """Fetch news list plus a small number of pagination pages."""
    if not start_url:
        return [], "Brak URL aktualnosci"

    queue = [_clean_url(start_url)]
    visited = set()
    articles = []
    seen = set()
    first_err = ""

    while queue and len(visited) < max_pages:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)

        soup, err = fetch(url)
        if not soup:
            first_err = first_err or err
            continue

        for art in extract_articles(soup, base_url):
            clean = _clean_url(art.get("url", ""))
            key = _article_fingerprint(clean) or clean or art.get("title", "").strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            articles.append(art)

        for href in _pagination_links(soup, url):
            if href not in visited and href not in queue and len(visited) + len(queue) < max_pages:
                queue.append(href)

    return articles, first_err


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
    # Szukaj "Data modyfikacji" na stronie — jeśli z 2025 lub wcześniej, odrzuć
    mod_match = re.search(r"[Dd]ata\s+modyfikacji[:\s]*(\d{2})\.(\d{2})\.(\d{4})", soup.get_text())
    if mod_match:
        mod_year = int(mod_match.group(3))
        if mod_year < 2026:
            return None
    return {"title": "KFS", "url": kfs_url, "snippet": text[:MAX_SNIPPET], "date": "", "source_type": "KFS"}


def _scrape_one(i, urzad, total):
    """Scrapuje jeden urząd — zwraca (articles, errors)."""
    name = urzad["name"]
    homepage = urzad.get("homepage", "")
    kfs_url = urzad.get("kfs_url", "")
    base_url = urzad.get("base_url", homepage.rstrip("/"))
    articles = []
    errs = []

    # Aktualności
    news, err = fetch_news_articles(urzad.get("aktualnosci_url", homepage), base_url)
    if not news and urzad.get("aktualnosci_url") and urzad["aktualnosci_url"] != homepage:
        news, err2 = fetch_news_articles(homepage, base_url)
        err = err or err2

    for a in news:
        a["urzad"] = name
    # Pobierz pełną treść TYLKO dla artykułów z KFS keywords w tytule
    for a in news:
        if not KFS_KEYWORDS.search(a.get("title", "")):
            continue
        detail = fetch_detail_content(a["url"])
        if detail and len(detail) > len(a.get("snippet", "")):
            a["snippet"] = detail
    articles.extend(news)

    if not news:
        errs.append({"urzad": name, "url": homepage, "typ": "Aktualnosci",
                      "blad": err or "Brak artykulow"})

    # Homepage — szukaj artykułów KFS na stronie głównej (asset_publisher)
    akt_url = urzad.get("aktualnosci_url", "")
    if homepage and homepage != akt_url:
        hp_soup, _ = fetch(homepage)
        if hp_soup:
            hp_arts = extract_articles(hp_soup, base_url)
            existing_urls = {a["url"] for a in articles}
            existing_fps = {_article_fingerprint(a["url"]) for a in articles} - {None}
            for a in hp_arts:
                if a["url"] in existing_urls:
                    continue
                fp = _article_fingerprint(a["url"])
                if fp and fp in existing_fps:
                    continue
                title_text = a.get("title", "")
                # Pełny match KFS keywords w tytule — od razu bierz
                if KFS_KEYWORDS.search(title_text):
                    a["urzad"] = name
                    detail = fetch_detail_content(a["url"])
                    if detail and len(detail) > len(a.get("snippet", "")):
                        a["snippet"] = detail
                    articles.append(a)
                    continue
                # Luźny match (ucięte tytuły) — sprawdź detail page
                loose = re.search(r"(?:KFS|[Ff]undusz|nab[oó]r\s+wniosk)", title_text)
                if loose:
                    detail = fetch_detail_content(a["url"])
                    if detail and KFS_KEYWORDS.search(detail[:2000]):
                        a["urzad"] = name
                        a["snippet"] = detail
                        articles.append(a)

    # KFS
    if kfs_url:
        ks, ke = fetch(kfs_url)
        if ks:
            kfs_art = extract_kfs(ks, kfs_url)
            if kfs_art:
                kfs_art["urzad"] = name
                articles.append(kfs_art)
        else:
            errs.append({"urzad": name, "url": kfs_url, "typ": "KFS", "blad": ke})

    print(f"  [{i}/{total}] {name} -> {len(articles)} art.")
    return articles, errs


def scrape_all(urzedy):
    """Scrapuje wszystkie urzędy równolegle, zwraca (articles, errors)."""
    all_articles = []
    errors = []
    total = len(urzedy)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {}
        for i, urzad in enumerate(urzedy, 1):
            future = executor.submit(_scrape_one, i, urzad, total)
            futures[future] = urzad

        for future in as_completed(futures):
            arts, errs = future.result()
            all_articles.extend(arts)
            errors.extend(errs)

    # Deduplikacja — ten sam artykuł Liferay pod różnymi ścieżkami
    seen_urls = set()
    seen_fps = set()
    deduped = []
    for art in all_articles:
        url = art.get("url", "")
        if url in seen_urls:
            continue
        seen_urls.add(url)
        fp = _article_fingerprint(url)
        if fp:
            if fp in seen_fps:
                continue
            seen_fps.add(fp)
        deduped.append(art)
    if len(deduped) < len(all_articles):
        print(f"  Deduplikacja: usunięto {len(all_articles) - len(deduped)} duplikatów")
    return deduped, errors


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


DATE_DMY_RE = re.compile(r"\b([0-3]?\d)[.\-/]([01]?\d)[.\-/](20\d{2})\b")
DATE_ISO_RE = re.compile(r"\b(20\d{2})-([01]\d)-([0-3]\d)\b")
AMOUNT_RE = re.compile(
    r"\b\d{1,3}(?:[\s.]\d{3})+(?:,\d{1,2})?\s*(?:z[l\u0142]|PLN)\b"
    r"|\b\d{4,12}(?:,\d{1,2})?\s*(?:z[l\u0142]|PLN)\b",
    re.IGNORECASE,
)


def _date_from_parts(day, month, year):
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


def _format_date_pl(d):
    return d.strftime("%d.%m.%Y")


def _format_date_range(date_range):
    return f"{_format_date_pl(date_range[0])} - {_format_date_pl(date_range[1])}"


def _extract_date_ranges(text):
    """Extract explicit date ranges while preserving text order."""
    text = str(text or "")
    matches = []
    for m in DATE_DMY_RE.finditer(text):
        d = _date_from_parts(m.group(1), m.group(2), m.group(3))
        if d:
            matches.append({"start": m.start(), "end": m.end(), "date": d})
    for m in DATE_ISO_RE.finditer(text):
        d = _date_from_parts(m.group(3), m.group(2), m.group(1))
        if d:
            matches.append({"start": m.start(), "end": m.end(), "date": d})
    matches.sort(key=lambda x: x["start"])

    ranges = []
    used = set()
    for i in range(len(matches) - 1):
        if i in used or i + 1 in used:
            continue
        gap = text[matches[i]["end"]:matches[i + 1]["start"]]
        gap_norm = _ascii_lower(gap)
        if len(gap) <= 90 and (
            "-" in gap or "do" in gap_norm or "od" in gap_norm or "w dniach" in gap_norm
        ):
            start = matches[i]["date"]
            end = matches[i + 1]["date"]
            if end >= start:
                ranges.append((start, end))
                used.add(i)
                used.add(i + 1)

    unique = []
    seen = set()
    for item in ranges:
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique


def _extract_amount_strings(text):
    return [entry["raw"] for entry in _extract_amount_entries(text)]


def _extract_amount_entries(text):
    text = str(text or "")
    amounts = []
    seen = set()
    for m in AMOUNT_RE.finditer(str(text or "")):
        raw = re.sub(r"\s+", " ", m.group(0)).strip()
        number = _parse_kwota_number(raw)
        if not number or number < 1000:
            continue
        key = re.sub(r"[^\d]", "", raw)
        if key not in seen:
            seen.add(key)
            before = _ascii_lower(text[max(0, m.start() - 140):m.start()])
            after = _ascii_lower(text[m.end():m.end() + 100])
            context = before + " " + after
            score = 0
            if any(x in before for x in ("obecnego naboru", "pozostalo", "w ramach obecnego")):
                score += 30
            if any(x in context for x in ("limit srodkow", "kwota srodkow", "do rozdysponowania")):
                score += 15
            if any(x in after for x in ("obecnego naboru", "pozostalo", "w ramach obecnego")):
                score += 5
            if any(x in context for x in ("na jednego", "przecietnego wynagrodzenia", "maksymalna")):
                score -= 50
            amounts.append({"raw": raw, "number": number, "start": m.start(), "score": score})
    return amounts


def _article_text(art):
    return f"{art.get('title', '')}\n{art.get('snippet', '')}"


def _is_strong_kfs_nabor(art):
    """Conservative deterministic recognizer for obvious KFS calls."""
    raw = _article_text(art)
    norm = _ascii_lower(raw)
    title_norm = _ascii_lower(art.get("title", ""))
    head_norm = norm[:1200]
    has_kfs = "kfs" in norm or "krajowy fundusz szkoleniowy" in norm
    has_nabor = "nabor" in norm and ("wniosk" in norm or "pracodawc" in norm)
    has_action = any(x in norm for x in ("oglasza", "ogloszenie", "rozpocz", "prowadzi", "przyjm"))
    if not has_kfs or not (has_nabor or has_action):
        return False

    hard_reject = (
        any(x in title_norm for x in ("wstrzym", "zawiesz", "zakoncz", "rozstrzyg", "wynik"))
        or any(x in head_norm for x in ("wstrzym", "zawiesz", "zakoncz", "rozstrzyg"))
        or bool(re.search(r"(wynik.{0,40}nabor|nabor.{0,40}wynik)", head_norm))
        or bool(re.search(r"(planowan.{0,50}nabor|nabor.{0,50}planowan)", head_norm))
        or bool(re.search(r"(przygotuj.{0,50}nabor|nabor.{0,50}przygotuj)", head_norm))
    )
    if hard_reject:
        return False
    soft_reject = any(x in norm for x in (
        "spotkan", "warsztat", "konsultac", "konto", "instrukcj",
    ))
    if soft_reject and not has_action:
        return False
    return True


def rule_based_classify_kfs(art):
    """Return a TAK/NIE result for obvious KFS calls without spending Gemini."""
    if not _is_strong_kfs_nabor(art):
        return None

    text = _article_text(art)
    norm = _ascii_lower(text)
    ranges = _extract_date_ranges(text)
    if ranges:
        active_or_future = [r for r in ranges if r[1] >= date.today()]
        if not active_or_future:
            return {
                "wynik": "NIE",
                "powod": "Termin minal",
                "termin": " oraz ".join(_format_date_range(r) for r in ranges),
                "kwota": "",
                "classified_date": TODAY,
            }
        ranges = active_or_future

    amount_entries = _extract_amount_entries(text)
    if len(ranges) == 1 and len(amount_entries) > 1:
        best = max(amount_entries, key=lambda x: (x["score"], x["start"]))
        amounts = [best["raw"]] if best["score"] > 0 else [amount_entries[0]["raw"]]
    else:
        amounts = [entry["raw"] for entry in amount_entries]
    amount_limit = len(ranges) if ranges and len(amounts) >= len(ranges) else len(amounts)
    kwota = " oraz ".join(amounts[:amount_limit])
    termin = " oraz ".join(_format_date_range(r) for r in ranges)

    attachment_hint = any(x in norm for x in ("zalacznik", "szczegolowe informacje", "dokument"))
    if not termin and attachment_hint:
        termin = "w zalaczniku"
    if not kwota and attachment_hint:
        kwota = "w zalaczniku"

    return {
        "wynik": "TAK",
        "powod": "Regula: nabor KFS",
        "termin": termin,
        "kwota": kwota,
        "classified_date": TODAY,
    }


def _cache_lookup(art, cache):
    url = art.get("url", "")
    if url in cache:
        return cache[url]
    fp = _article_fingerprint(url)
    if fp and fp in _cache_fp_index:
        cached = _cache_fp_index[fp]
        cache[url] = cached
        return cached
    return None


def _cache_needs_refresh(art, cached):
    if not cached:
        return True
    reason = _ascii_lower(cached.get("powod", ""))
    if any(x in reason for x in ("blad api", "rate limit", "brak odpowiedzi gemini")):
        return True
    if _is_strong_kfs_nabor(art):
        wynik = cached.get("wynik", "NIE")
        if wynik != "TAK" and (not reason or "brak slow kluczowych kfs" in reason):
            return True
        if wynik == "TAK" and not (cached.get("termin") or cached.get("kwota")):
            return True
    return False


def classify_article(art, key_index, cache):
    """Klasyfikuje artykuł — zwraca wynik (z cache lub z AI)."""
    url = art.get("url", "")
    title = art.get("title", "")

    snippet = art.get("snippet", "")
    text_to_check = title + " " + snippet[:1000]
    cached = _cache_lookup(art, cache)
    if cached and not _cache_needs_refresh(art, cached):
        return cached, True, True

    # Pre-filtr: brak słów kluczowych KFS → automatycznie NIE (bez AI)
    if not KFS_KEYWORDS.search(text_to_check) and not _is_strong_kfs_nabor(art):
        result = {"wynik": "NIE", "powod": "Brak słów kluczowych KFS",
                  "termin": "", "kwota": "", "classified_date": TODAY}
        with progress_lock:
            progress["done"] += 1
        return result, False, True

    # Sprawdź cache (po URL lub po fingerprint — ten sam artykuł pod inną ścieżką)
    # Nowy artykuł z KFS keywords — wyślij do AI
    rule_result = rule_based_classify_kfs(art)
    if rule_result:
        with progress_lock:
            progress["done"] += 1
            if rule_result.get("wynik") == "TAK":
                progress["tak"] += 1
            done = progress["done"]
            total = progress["total"]
        extra = ""
        if rule_result.get("termin"):
            extra += f" [{rule_result['termin']}]"
        if rule_result.get("kwota"):
            extra += f" {rule_result['kwota']}"
        print(f"    RULE [{done}/{total}] {art.get('urzad',''):18s} -> {rule_result.get('wynik')}{extra}")
        return rule_result, False, True

    api_key = API_KEYS[key_index % len(API_KEYS)]
    prompt = CLASSIFY_PROMPT.format(
        title=art.get("title", ""),
        snippet=(art.get("snippet", "") or art.get("title", ""))[:MAX_SNIPPET],
        today=TODAY,
    )
    text = call_gemini(api_key, prompt)
    if not text:
        result = {"wynik": "NIE", "powod": "Blad API Gemini",
                  "termin": "", "kwota": "", "classified_date": TODAY}
        with progress_lock:
            progress["done"] += 1
        print(f"    API [{progress['done']}/{progress['total']}] "
              f"{art.get('urzad',''):18s} -> NIE (bez cache)")
        return result, False, False

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

    return result, False, True


_cache_fp_index = {}  # fingerprint -> cache value (budowany w classify_all)


def classify_all(articles, cache):
    """Klasyfikuje wszystkie artykuły — nowe przez AI, stare z cache."""
    # Buduj indeks fingerprint → cache value (szybki lookup po slug+entryId)
    global _cache_fp_index
    _cache_fp_index = {}
    for url, val in cache.items():
        fp = _article_fingerprint(url)
        if fp:
            _cache_fp_index[fp] = val

    def _cache_usable(art):
        cached = _cache_lookup(art, cache)
        return bool(cached and not _cache_needs_refresh(art, cached))

    new_articles = [a for a in articles if not _cache_usable(a)]
    # Ile z nowych ma KFS keywords (pójdzie do AI), ile bez (auto-NIE)
    kfs_new = [a for a in new_articles if KFS_KEYWORDS.search(
        a.get("title", "") + " " + a.get("snippet", "")[:1000]) or _is_strong_kfs_nabor(a)]
    skip_count = len(new_articles) - len(kfs_new)
    cached_count = len(articles) - len(new_articles)

    print(f"\n  Artykulow: {len(articles)} (z cache: {cached_count}, nowych: {len(new_articles)}, do AI: {len(kfs_new)}, auto-NIE: {skip_count})")

    progress["total"] = len(new_articles)
    progress["done"] = 0
    progress["tak"] = 0

    results = []
    new_tak_urls = set()

    # Cache hits — przetwarzaj synchronicznie
    for art in articles:
        url = art.get("url", "")
        r = _cache_lookup(art, cache)
        if r and not _cache_needs_refresh(art, r):
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
                "source_type": art.get("source_type", ""),
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
                result, from_cache, cacheable = future.result()
                url = art.get("url", "")

                # Zapisz do cache
                if cacheable:
                    cache[url] = result

                wynik = result.get("wynik", "NIE")
                results.append({
                    "url": url,
                    "title": art.get("title", ""),
                    "urzad": art.get("urzad", ""),
                    "date": art.get("date", ""),
                    "wynik": wynik,
                    "powod": result.get("powod", ""),
                    "termin": result.get("termin", ""),
                    "kwota": result.get("kwota", ""),
                    "snippet": art.get("snippet", "")[:MAX_SNIPPET],
                    "source_type": art.get("source_type", ""),
                })
                if wynik == "TAK":
                    new_tak_urls.add(url)

    return results, new_tak_urls


def expand_multi_nabory(results):
    """Split one TAK result with multiple date ranges into separate calls."""
    expanded = []
    split_count = 0
    for r in results:
        if r.get("wynik") != "TAK":
            expanded.append(r)
            continue

        ranges = _extract_date_ranges(r.get("termin", ""))
        if len(ranges) <= 1:
            expanded.append(r)
            continue

        amounts = _extract_amount_strings(r.get("kwota", ""))
        for idx, date_range in enumerate(ranges):
            item = dict(r)
            item["termin"] = _format_date_range(date_range)
            if len(amounts) == len(ranges):
                item["kwota"] = amounts[idx]
            elif len(amounts) > 1 and idx < len(amounts):
                item["kwota"] = amounts[idx]
            elif len(amounts) == 1:
                item["kwota"] = amounts[0]
            item["multiIndex"] = idx + 1
            item["multiTotal"] = len(ranges)
            powod = item.get("powod", "")
            item["powod"] = f"{powod}; wielotermin {idx + 1}/{len(ranges)}".strip("; ")
            expanded.append(item)
            split_count += 1

    if split_count:
        print(f"  Rozbito wieloterminowe nabory: {split_count} rekordow wynikowych")
    return expanded


# ============================================================
# WYSYŁKA DO ESPOCRM
# ============================================================

def _parse_termin_dates(termin_raw):
    """Parsuje termin '20.04.2026 - 24.04.2026' → (date_start, date_end) lub (None, None)."""
    termin_dates = re.findall(r"(\d{2})\.(\d{2})\.(\d{4})", termin_raw)
    if not termin_dates:
        return None, None
    try:
        start = date(int(termin_dates[0][2]), int(termin_dates[0][1]), int(termin_dates[0][0]))
        end = date(int(termin_dates[-1][2]), int(termin_dates[-1][1]), int(termin_dates[-1][0]))
        return start, end
    except ValueError:
        return None, None


def _compute_status_kfs(start, end):
    """Oblicza status NaboryKfs na podstawie terminu."""
    if not start or not end:
        return "Nowy"
    today = date.today()
    if today > end:
        return "Nieaktualny"
    elif today >= start:
        return "W trakcie"
    return "Nowy"


def _crm_paginate(crm_url, entity, headers, select_fields):
    """Pobiera wszystkie rekordy encji z CRM z paginacją."""
    records = []
    offset = 0
    page_size = 200
    try:
        while True:
            resp = requests.get(
                f"{crm_url}/api/v1/{entity}",
                headers=headers,
                params={"select": select_fields, "maxSize": page_size, "offset": offset},
                timeout=15,
            )
            if not resp.ok:
                print(f"  CRM: Błąd pobierania {entity}: HTTP {resp.status_code}")
                break
            page = resp.json().get("list", [])
            records.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
    except Exception as ex:
        print(f"  CRM: Błąd pobierania {entity}: {ex}")
    return records


_PL_MAP = str.maketrans("ąćęłńóśźżĄĆĘŁŃÓŚŹŻ", "acelnoszzACELNOSZZ")


def _stem_city_word(word):
    if len(word) <= 4:
        return word
    for suffix in ("ach", "owie", "iego", "skiej", "ckiej"):
        if word.endswith(suffix):
            word = word[:-len(suffix)]
            break
    else:
        for suffix in ("ie", "iu", "u"):
            if len(word) > 5 and word.endswith(suffix):
                word = word[:-len(suffix)]
                break
        else:
            if len(word) > 5 and word[-1] in "aeiy":
                word = word[:-1]
    return word


def _normalize_miasto(name):
    """Normalizuje nazwę miasta do porównań: usuwa polskie znaki, PUP/MUP/WUP, skróty."""
    if not name:
        return ""
    s = name.lower().strip().translate(_PL_MAP)
    s = re.sub(r"\b(pup|mup|wup|gup|up)\b", "", s)
    s = re.sub(
        r"\b(powiatowy|powiatowego|miejski|miejskiego|grodzki|grodzkiego|wojewodzki|"
        r"wojewodzkiego|urzad|pracy|powiatu|dla|w|z|siedziba|siedziba w)\b",
        " ",
        s,
    )
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = s.replace("wlkp", "wielkopolski")
    return " ".join(_stem_city_word(part) for part in s.split())


def _parse_kwota_number(kwota_str):
    """Parsuje '1 500 000,00 zł' → 1500000 (int) lub None.
    Odcina grosze (po przecinku), ignoruje 'w załączniku' / 'brak' / 'tys'."""
    if not kwota_str:
        return None
    if re.search(r"(za[lł][aą]cznik|brak|tys)", kwota_str, re.IGNORECASE):
        return None
    # Usuń 'zł', 'PLN', 'złotych'
    s = re.sub(r"(z[łl](otych)?|pln)\s*$", "", kwota_str.strip(), flags=re.IGNORECASE).strip()
    # Odetnij grosze po przecinku (np. ',00' ',46')
    s = re.sub(r",\d{1,2}$", "", s)
    # Zostaje np. '900.000' '1 500 000' '531 000' — wyciągnij cyfry
    digits = re.sub(r"[^\d]", "", s)
    if digits and 3 <= len(digits) <= 12:
        return int(digits)
    return None


def _format_money_pln(value):
    if value in (None, ""):
        return ""
    try:
        return f"{int(value):,}".replace(",", " ") + " zl"
    except (TypeError, ValueError):
        return ""


def _dates_overlap(a_start, a_end, b_start, b_end):
    if not all((a_start, a_end, b_start, b_end)):
        return False
    return a_start <= b_end and b_start <= a_end


def _forecast_level(probability):
    if probability >= 75:
        return "HOT", "Wysokie"
    if probability >= 50:
        return "WARM", "Srednie"
    if probability >= 25:
        return "WATCH", "Obserwuj"
    return "COLD", "Niskie"


LIMIT_CONTEXT_RE = re.compile(
    r"(dostepn\w*.{0,80}(?:rok|2026|kwot|srodk)|"
    r"przyznan\w*.{0,80}(?:limit|kwot|srodk)|"
    r"limit\w*.{0,80}(?:kfs|srodk|rok|2026)|"
    r"kwot\w*.{0,80}(?:na\s+rok|2026|dostepn|srodkow\s+kfs)|"
    r"srodk\w*.{0,80}(?:na\s+rok|2026|dostepn))"
)

NABOR_CONTEXT_RE = re.compile(
    r"(obecnego\s+naboru|w\s+ramach\s+naboru|prowadzon\w*\s+nabor|"
    r"naboru\s+dostepn|do\s+rozdysponowania|pozostal\w*|"
    r"wniosk\w*\s+mozna\s+skladac)"
)

EXHAUSTED_RE = re.compile(
    r"(wyczerpan\w*.{0,80}(?:srodk|limit|kfs)|"
    r"(?:srodki|limit).{0,80}wyczerpan\w*|"
    r"brak\s+srodkow|"
    r"rozdysponowan[ey]\w*.{0,80}(?:wszystk|cal\w*)|"
    r"rozdysponowano.{0,80}(?:wszystk|cal\w*)|"
    r"(?:wszystk|cal\w*).{0,80}rozdysponowan[ey]\w*|"
    r"wykorzystan[ey]\w*.{0,80}(?:wszystk|cal\w*)|"
    r"wykorzystano.{0,80}(?:wszystk|cal\w*))"
)

RESERVE_RE = re.compile(
    r"(rezerw\w*\s+kfs|zmniejszen\w*\s+limitu|niewykorzystan\w*\s+srodk|"
    r"zwrot\w*\s+srodk|oddan\w*\s+srodk)"
)

PLANNED_RE = re.compile(
    r"(planowan\w*.{0,60}nabor|kolejn\w*.{0,60}nabor|"
    r"dodatkow\w*.{0,60}nabor|nabor.{0,60}(?:w\s+krotce|wkrotce|planowan|kolejn|dodatkow))"
)

PROMOTION_RE = re.compile(
    r"(promocj\w*|badan\w*|analiz\w*|art\.?\s*125\s*ust\.?\s*7)"
)


def _has_exhausted_signal(norm_text):
    for match in EXHAUSTED_RE.finditer(norm_text):
        context = norm_text[max(0, match.start() - 80):match.end() + 80]
        if (
            "do wyczerpania" in context
            or "do czasu wyczerpania" in context
            or "powtarzany do wyczerpania" in context
            or "az do wyczerpania" in context
        ):
            continue
        return True
    return False


def _result_text(result):
    return f"{result.get('title', '')}\n{result.get('snippet', '')}"


def _kfs_text(result):
    text = _result_text(result)
    return bool(KFS_KEYWORDS.search(text))


def _extract_limit_candidates(result):
    text = _result_text(result)
    if not _kfs_text(result):
        return []

    candidates = []
    for entry in _extract_amount_entries(text):
        start = entry.get("start", 0)
        context = _ascii_lower(text[max(0, start - 260):start + 260])
        score = entry.get("score", 0)
        if LIMIT_CONTEXT_RE.search(context):
            score += 55
        if "2026" in context:
            score += 10
        if result.get("source_type") == "KFS" or _ascii_lower(result.get("title", "")).strip() == "kfs":
            score += 15
        if result.get("wynik") == "TAK":
            score -= 10
        if NABOR_CONTEXT_RE.search(context):
            score -= 25
        if re.search(r"(na\s+jedn\w*|przecietn\w*\s+wynagrodz|maksymaln\w*)", context):
            score -= 70
        if entry["number"] < 20000:
            score -= 20
        if score > 15:
            candidates.append({
                "amount": entry["number"],
                "raw": entry["raw"],
                "score": score,
                "title": result.get("title", ""),
                "url": result.get("url", ""),
            })
    return candidates


def _extract_promotion_candidates(result):
    text = _result_text(result)
    norm = _ascii_lower(text)
    if not (_kfs_text(result) and PROMOTION_RE.search(norm)):
        return []
    items = []
    for entry in _extract_amount_entries(text):
        start = entry.get("start", 0)
        context = _ascii_lower(text[max(0, start - 220):start + 220])
        if PROMOTION_RE.search(context):
            items.append({
                "amount": entry["number"],
                "raw": entry["raw"],
                "title": result.get("title", ""),
                "url": result.get("url", ""),
            })
    return items


def _unique_nabor_entries(results):
    entries = []
    for r in results:
        if r.get("wynik") != "TAK":
            continue
        amount = _parse_kwota_number(r.get("kwota", ""))
        if not amount:
            continue
        start, end = _parse_termin_dates(r.get("termin", ""))
        fp = _article_fingerprint(r.get("url", "")) or r.get("url", "")

        duplicate = False
        for existing in entries:
            if existing["amount"] != amount:
                continue
            same_source = existing["fingerprint"] and existing["fingerprint"] == fp
            same_end = existing["end"] and end and existing["end"] == end
            overlap = _dates_overlap(existing["start"], existing["end"], start, end)
            no_dates_same_amount = not any((existing["start"], existing["end"], start, end))
            if same_source or same_end or overlap or no_dates_same_amount:
                duplicate = True
                break
        if duplicate:
            continue

        entries.append({
            "amount": amount,
            "raw": r.get("kwota", ""),
            "start": start,
            "end": end,
            "fingerprint": fp,
            "title": r.get("title", ""),
            "url": r.get("url", ""),
        })
    return entries


def build_budget_forecasts(results, urzad_to_woj=None):
    """Build per-PUP budget and next-call probability from scraped public text."""
    urzad_to_woj = urzad_to_woj or {}
    by_office = {}
    for r in results:
        office = r.get("urzad", "")
        if not office:
            continue
        bucket = by_office.setdefault(office, {
            "results": [],
            "kfs_count": 0,
            "exhausted": [],
            "reserve": [],
            "planned": [],
            "promotion": [],
            "limits": [],
        })
        bucket["results"].append(r)
        text_norm = _ascii_lower(_result_text(r))
        if _kfs_text(r):
            bucket["kfs_count"] += 1
        if _has_exhausted_signal(text_norm):
            bucket["exhausted"].append(r)
        if RESERVE_RE.search(text_norm):
            bucket["reserve"].append(r)
        if PLANNED_RE.search(text_norm):
            bucket["planned"].append(r)
        bucket["limits"].extend(_extract_limit_candidates(r))
        bucket["promotion"].extend(_extract_promotion_candidates(r))

    today = date.today()
    forecasts = []
    for office, bucket in by_office.items():
        office_results = bucket["results"]
        nabor_entries = _unique_nabor_entries(office_results)
        allocated = sum(x["amount"] for x in nabor_entries)
        active_entries = [x for x in nabor_entries if x["end"] and x["end"] >= today]
        ended_entries = [x for x in nabor_entries if x["end"] and x["end"] < today]

        limits = sorted(bucket["limits"], key=lambda x: (x["score"], x["amount"]), reverse=True)
        best_limit = limits[0] if limits else None
        annual_limit = best_limit["amount"] if best_limit else None
        remaining = None
        if annual_limit and allocated:
            remaining = annual_limit - allocated
            if remaining < 0 and abs(remaining) <= max(50000, annual_limit * 0.08):
                remaining = 0

        signals = []
        probability = 15
        ask_public_info = False

        if bucket["exhausted"]:
            probability = 5
            signals.append("Komunikat wyglada na wyczerpanie/rozdysponowanie srodkow.")
        elif bucket["reserve"]:
            probability = 20
            signals.append("Sa sygnaly rezerwy albo zmniejszenia/niewykorzystania limitu.")
        elif remaining is not None:
            if remaining > max(25000, annual_limit * 0.03):
                ratio = remaining / annual_limit if annual_limit else 0
                probability = 62 + min(28, int(ratio * 45))
                signals.append("Limit roczny jest wyzszy niz suma wykrytych pul naborow.")
                ask_public_info = True
            else:
                probability = 18
                signals.append("Wykryte pule naborow prawie domykaja limit roczny.")
        elif bucket["planned"]:
            probability = 72
            signals.append("W tekstach pojawia sie planowany/kolejny/dodatkowy nabor.")
        elif ended_entries and not active_entries:
            probability = 44
            signals.append("Byl nabor, termin minal, brak jednoznacznego komunikatu o wyczerpaniu.")
            ask_public_info = True
        elif active_entries:
            probability = 30
            signals.append("Jest aktywny lub przyszly nabor; kolejny zalezy od rozstrzygniecia.")
        elif bucket["kfs_count"]:
            probability = 38
            signals.append("Sa artykuly KFS, ale scraper nie widzi aktywnego naboru ani rozliczenia.")
            ask_public_info = True
        else:
            signals.append("Brak wystarczajacych danych KFS dla urzedu.")

        if (
            bucket["planned"]
            and not bucket["exhausted"]
            and not bucket["reserve"]
            and (
                remaining is None
                or remaining > max(25000, (annual_limit or 0) * 0.03)
                or allocated == 0
            )
        ):
            probability = max(probability, 70)
        if active_entries and not bucket["planned"]:
            probability = min(probability, 45)
        if remaining is not None and remaining > 0 and bucket["promotion"]:
            signals.append("Znaleziono wzmianki o promocji/badaniach/analizach KFS przy kwotach.")
            ask_public_info = True
        if annual_limit and allocated == 0 and bucket["kfs_count"] and not bucket["exhausted"]:
            probability = max(probability, 55)
            signals.append("Limit roczny jest wykryty, ale nie znaleziono puli naboru.")

        probability = max(0, min(95, probability))
        level, label = _forecast_level(probability)
        sources = []
        for source in (limits[:2] + bucket["planned"][:2] + bucket["exhausted"][:1]):
            title = source.get("title", "")
            url = source.get("url", "")
            if url and not any(s["url"] == url for s in sources):
                sources.append({"title": title, "url": url})

        forecasts.append({
            "urzad": office,
            "woj": urzad_to_woj.get(office, ""),
            "probability": probability,
            "level": level,
            "label": label,
            "limit_roczny": annual_limit,
            "limit_roczny_text": _format_money_pln(annual_limit),
            "limit_source_url": best_limit.get("url", "") if best_limit else "",
            "limit_confidence": best_limit.get("score", 0) if best_limit else 0,
            "suma_naborow": allocated or None,
            "suma_naborow_text": _format_money_pln(allocated) if allocated else "",
            "szacowane_pozostalo": remaining if remaining is not None and remaining >= 0 else None,
            "szacowane_pozostalo_text": _format_money_pln(remaining) if remaining is not None and remaining >= 0 else "",
            "nabor_count": len(nabor_entries),
            "active_count": len(active_entries),
            "ended_count": len(ended_entries),
            "kfs_articles": bucket["kfs_count"],
            "has_exhausted_signal": bool(bucket["exhausted"]),
            "has_reserve_signal": bool(bucket["reserve"]),
            "has_planned_signal": bool(bucket["planned"]),
            "promotion_candidates": bucket["promotion"][:5],
            "ask_public_info": ask_public_info,
            "signals": signals[:5],
            "sources": sources[:5],
        })

    forecasts.sort(
        key=lambda x: (
            x["probability"],
            x.get("szacowane_pozostalo") or 0,
            x.get("limit_roczny") or 0,
        ),
        reverse=True,
    )
    return forecasts


def _date_field(value):
    if not value:
        return ""
    return str(value)[:10]


def _coerce_kwota_int(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    return _parse_kwota_number(str(value))


def make_nabory_key(miasto, od_str, do_str, kwota_int):
    return (_normalize_miasto(miasto), od_str or "", do_str or "", kwota_int or 0)


def push_to_crm(results):
    """Wysyła nowe nabory TAK do EspoCRM (NaboryKfs + Nabory). Zwraca listę nowo dodanych."""
    crm_url = os.environ.get("ESPOCRM_URL", "").rstrip("/")
    crm_key = os.environ.get("ESPOCRM_API_KEY", "")
    if not crm_url or not crm_key:
        print("  Brak ESPOCRM_URL/ESPOCRM_API_KEY — pomijam CRM")
        return []

    # Wczytaj mapowania urzad → powiat i urzad → województwo
    urzad_to_powiat = {}
    urzad_to_woj = {}
    try:
        if MAPPING_FILE.exists():
            with open(MAPPING_FILE, "r", encoding="utf-8") as f:
                urzad_to_powiat = json.load(f)
        if WOJ_FILE.exists():
            with open(WOJ_FILE, "r", encoding="utf-8") as f:
                urzad_to_woj = json.load(f)
    except Exception as ex:
        print(f"  CRM: Błąd wczytywania mapowań: {ex}")

    tak = [r for r in results if r.get("wynik") == "TAK"]
    print(f"\n  CRM: {len(tak)} naborów TAK do wysłania")

    headers = {
        "X-Api-Key": crm_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # --- NaboryKfs: pobierz istniejące ---
    kfs_existing = set()       # url / name — szybka dedup po URL
    kfs_dedup_keys = set()     # (urzad, termin, kwota) — dedup treściowa
    kfs_records = []
    for rec in _crm_paginate(crm_url, "NaboryKfs", headers, "name,url,status,termin,urzad,kwota"):
        if rec.get("url"):
            kfs_existing.add(rec["url"])
        if rec.get("name"):
            kfs_existing.add(rec["name"])
        # Klucz treściowy: ten sam urząd + termin + kwota = ten sam nabór
        u = (rec.get("urzad") or "").strip().lower()
        t = (rec.get("termin") or "").strip().lower()
        k = (rec.get("kwota") or "").strip().lower()
        if u and (t or k):
            kfs_dedup_keys.add((u, t, k))
        kfs_records.append(rec)
    print(f"  CRM NaboryKfs: {len(kfs_records)} istniejących rekordów, {len(kfs_dedup_keys)} kluczy dedup")

    # Aktualizuj statusy istniejących NaboryKfs (Nowy→W trakcie→Nieaktualny)
    updated = 0
    for rec in kfs_records:
        start, end = _parse_termin_dates(rec.get("termin", ""))
        if not start:
            continue
        new_status = _compute_status_kfs(start, end)
        if new_status != rec.get("status"):
            try:
                resp = requests.put(
                    f"{crm_url}/api/v1/NaboryKfs/{rec['id']}",
                    headers=headers,
                    json={"status": new_status},
                    timeout=15,
                )
                if resp.ok:
                    updated += 1
            except Exception:
                pass
    if updated:
        print(f"  CRM NaboryKfs: Zaktualizowano status {updated} rekordów")

    # --- Dodawanie nowych rekordów ---
    added_kfs = 0
    skipped_kfs = 0
    errors_count = 0
    newly_added = []

    for r in tak:
        title = r.get("title", "")[:255]
        urzad = r.get("urzad", "")
        url = r.get("url", "")
        termin_raw = r.get("termin", "")
        kwota_str = r.get("kwota", "")
        start, end = _parse_termin_dates(termin_raw)

        # --- NaboryKfs ---
        # Dedup 1: po URL/tytule
        kfs_is_new = url not in kfs_existing and title not in kfs_existing
        # Dedup 2: po treści (urzad+termin+kwota) — ten sam nabór w innym artykule
        dedup_key = (urzad.strip().lower(), termin_raw.strip().lower(), kwota_str.strip().lower())
        has_content_key = bool(urzad and (termin_raw or kwota_str))
        if has_content_key:
            kfs_is_new = dedup_key not in kfs_dedup_keys
        elif dedup_key in kfs_dedup_keys:
            kfs_is_new = False
        if kfs_is_new:
            crm_date = ""
            if r.get("date"):
                parts = r["date"].split(".")
                if len(parts) == 3:
                    crm_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
            status = _compute_status_kfs(start, end)
            payload_kfs = {
                "name": title,
                "urzad": urzad,
                "termin": termin_raw,
                "kwota": kwota_str,
                "url": url,
                "datapublikacji": crm_date,
                "powod": r.get("powod", ""),
                "status": status,
            }
            try:
                resp = requests.post(
                    f"{crm_url}/api/v1/NaboryKfs",
                    headers=headers,
                    json=payload_kfs,
                    timeout=15,
                )
                if resp.status_code in (200, 201):
                    added_kfs += 1
                    kfs_existing.add(url)
                    kfs_existing.add(title)
                    if has_content_key:
                        kfs_dedup_keys.add(dedup_key)
                else:
                    errors_count += 1
                    detail = resp.text[:200] if resp.text else ""
                    print(f"  CRM NaboryKfs: Błąd {resp.status_code} dla {urzad} — {detail}")
            except Exception as ex:
                errors_count += 1
                print(f"  CRM NaboryKfs: {ex}")
        else:
            skipped_kfs += 1

        if kfs_is_new:
            newly_added.append(r)

    print(f"  CRM NaboryKfs: Dodano {added_kfs}, pominięto {skipped_kfs}")

    # --- Nabory: uzupełnij brakujące na podstawie NaboryKfs ---
    # Pobierz istniejące miasta z Nabory 2026
    nabory_existing_keys = set()
    nabory_by_link = {}
    nabory_records = _crm_paginate(
        crm_url,
        "Nabory",
        headers,
        "id,name,miasto,od,do,kwota,kwotaConverted,link",
    )
    for rec in nabory_records:
        od_raw = _date_field(rec.get("od", ""))
        if od_raw and not od_raw.startswith("2026"):
            continue
        do_raw = _date_field(rec.get("do", ""))
        kwota_existing = _coerce_kwota_int(rec.get("kwota") or rec.get("kwotaConverted"))
        norm = _normalize_miasto(rec.get("miasto", ""))
        if norm:
            nabory_existing_keys.add(make_nabory_key(rec.get("miasto", ""), od_raw, do_raw, kwota_existing))
        if rec.get("link"):
            nabory_by_link.setdefault(_clean_url(rec["link"]), []).append(rec)
    print(f"  CRM Nabory: {len(nabory_existing_keys)} istniejacych kluczy (miasto+termin+kwota) w 2026")

    added_nabory = 0
    updated_nabory = 0
    skipped_nabory = 0
    for r in tak:
        urzad = r.get("urzad", "").strip()
        termin_raw = r.get("termin", "").strip()
        kwota_str = r.get("kwota", "").strip()
        url = r.get("url", "")
        start, end = _parse_termin_dates(termin_raw)

        norm = _normalize_miasto(urzad)
        if not norm:
            continue

        # Sprawdź czy miasto (znormalizowane) już jest w Nabory
        # Potrzebujemy przynajmniej daty 'od'
        od_str = start.isoformat() if start else ""
        do_str = end.isoformat() if end else ""
        if not od_str:
            skipped_nabory += 1
            continue

        kwota_int = _parse_kwota_number(kwota_str)
        nabory_key = make_nabory_key(urzad, od_str, do_str, kwota_int)
        update_rec = None
        already = nabory_key in nabory_existing_keys
        for rec in nabory_by_link.get(_clean_url(url), []):
            rec_norm = _normalize_miasto(rec.get("miasto", ""))
            rec_od = _date_field(rec.get("od", ""))
            rec_do = _date_field(rec.get("do", ""))
            rec_kwota = _coerce_kwota_int(rec.get("kwota") or rec.get("kwotaConverted"))
            rec_key = make_nabory_key(rec.get("miasto", ""), rec_od, rec_do, rec_kwota)
            same_city = rec_norm and (norm == rec_norm or norm in rec_norm or rec_norm in norm)
            if same_city and rec_key == nabory_key:
                already = True
                break
            if same_city and rec_od == od_str and rec_do == do_str and not rec_kwota:
                update_rec = rec
            if same_city and not rec_od and not rec_do and not rec_kwota:
                update_rec = rec
        if already:
            skipped_nabory += 1
            continue

        woj = urzad_to_woj.get(urzad, "")
        powiat_raw = urzad_to_powiat.get(urzad, "")
        powiat_name = ""
        if powiat_raw:
            powiat_name = re.sub(r"^powiat\s+", "", powiat_raw, flags=re.IGNORECASE).strip()
            if powiat_name:
                powiat_name = powiat_name[0].upper() + powiat_name[1:]

        crm_date = ""
        if r.get("date"):
            parts = r["date"].split(".")
            if len(parts) == 3:
                crm_date = f"{parts[2]}-{parts[1]}-{parts[0]}"

        payload_nabory = {
            "name": f"I limit PUP {urzad}",
            "powiat": [powiat_name] if powiat_name else [],
            "od": od_str,
            "do": do_str,
            "kwota": kwota_int,
            "kwotaCurrency": "PLN" if kwota_int else None,
            "miasto": urzad,
            "link": url,
            "wojewodztwo": woj,
            "status": "Do weryfikacji",
            "priorytetyZ": "limit",
            "dataPublikacji": crm_date,
        }
        # Usuń None wartości
        payload_nabory = {k: v for k, v in payload_nabory.items() if v is not None}

        try:
            if update_rec:
                resp = requests.put(
                    f"{crm_url}/api/v1/Nabory/{update_rec['id']}",
                    headers=headers,
                    json=payload_nabory,
                    timeout=15,
                )
            else:
                resp = requests.post(
                    f"{crm_url}/api/v1/Nabory",
                    headers=headers,
                    json=payload_nabory,
                    timeout=15,
                )
            if resp.status_code in (200, 201):
                if update_rec:
                    updated_nabory += 1
                else:
                    added_nabory += 1
                nabory_existing_keys.add(nabory_key)
                if url:
                    nabory_by_link.setdefault(_clean_url(url), []).append({
                        "id": update_rec.get("id") if update_rec else "",
                        "miasto": urzad,
                        "od": od_str,
                        "do": do_str,
                        "kwota": kwota_int,
                        "link": url,
                    })
            else:
                errors_count += 1
                detail = resp.text[:200] if resp.text else ""
                print(f"  CRM Nabory: Błąd {resp.status_code} dla {urzad} — {detail}")
        except Exception as ex:
            errors_count += 1
            print(f"  CRM Nabory: {ex}")

    print(f"  CRM Nabory:    Dodano {added_nabory}, zaktualizowano {updated_nabory}, pominięto {skipped_nabory}")
    if errors_count:
        print(f"  CRM: Błędów łącznie: {errors_count}")

    return newly_added


# ============================================================
# POWIADOMIENIA EMAIL
# ============================================================
def send_email_notification(new_tak):
    """Wysyła email z nowymi naborami TAK."""
    smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASSWORD", "")
    email_to = os.environ.get("EMAIL_TO", "")

    if not smtp_user or not smtp_pass or not email_to:
        print("  Brak SMTP_USER/SMTP_PASSWORD/EMAIL_TO — pomijam email")
        return

    if not new_tak:
        print("  Brak nowych naborów — pomijam email")
        return

    recipients = [x.strip() for x in email_to.split(",") if x.strip()]
    today_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    subject = f"KFS: {len(new_tak)} nowych naborów ({today_str})"

    rows = ""
    for r in new_tak:
        rows += (
            f'<tr><td style="padding:6px 10px;border:1px solid #ddd">{r.get("urzad","")}</td>'
            f'<td style="padding:6px 10px;border:1px solid #ddd"><a href="{r.get("url","")}">{r.get("title","")[:80]}</a></td>'
            f'<td style="padding:6px 10px;border:1px solid #ddd">{r.get("termin","")}</td>'
            f'<td style="padding:6px 10px;border:1px solid #ddd">{r.get("kwota","")}</td></tr>'
        )

    body = f"""<html><body style="font-family:sans-serif">
<h2 style="color:#1e3a5f">Nowe nabory KFS — {len(new_tak)}</h2>
<p>Znaleziono {len(new_tak)} nowych naborów KFS ({today_str}):</p>
<table style="border-collapse:collapse;width:100%;font-size:14px">
<tr style="background:#1e3a5f;color:#fff">
<th style="padding:8px 10px;text-align:left">Urząd</th>
<th style="padding:8px 10px;text-align:left">Tytuł</th>
<th style="padding:8px 10px;text-align:left">Termin</th>
<th style="padding:8px 10px;text-align:left">Kwota</th></tr>
{rows}</table>
<p style="margin-top:16px"><a href="https://uszekkk.github.io/KFS-Scraper/">Pełny raport na mapie</a></p>
</body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(body, "html", "utf-8"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipients, msg.as_string())
        print(f"  Email: Wysłano do {', '.join(recipients)} ({len(new_tak)} naborów)")
    except Exception as ex:
        print(f"  Email: Błąd wysyłki — {ex}")


# ============================================================
# GENEROWANIE RAPORTU HTML
# ============================================================
def generate_report(results, errors, forecasts=None):
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

    WOJ_FILE = BASE_DIR / "urzad_to_woj.json"
    urzad_to_woj = {}
    if WOJ_FILE.exists():
        with open(WOJ_FILE, "r", encoding="utf-8") as f:
            urzad_to_woj = json.load(f)
    all_woj = sorted(set(urzad_to_woj.values()))
    if forecasts is None:
        forecasts = build_budget_forecasts(results, urzad_to_woj)

    # Potwierdzone nabory: wszystkie TAK
    confirmed = [r for r in results if r["wynik"] == "TAK"]
    # Artykuły KFS: NIE ale wspominające KFS
    nie_kfs = [r for r in results if r["wynik"] != "TAK" and KFS_KEYWORDS.search(
        r.get("title", "") + " " + r.get("snippet", "")[:500])]
    related = nie_kfs
    total = len(results)
    count_tak = len(confirmed)
    count_related = len(related)
    tak_powiaty = sorted(set(r["urzad"] for r in confirmed))
    count_tak_powiaty = len(tak_powiaty)
    count_err = len(errors)
    count_forecast_hot = len([f for f in forecasts if f.get("probability", 0) >= 75])
    count_forecast_watch = len([f for f in forecasts if f.get("probability", 0) >= 50])

    confirmed.sort(key=lambda r: r.get("date", ""), reverse=True)
    related.sort(key=lambda r: r.get("date", ""), reverse=True)
    forecasts.sort(key=lambda f: (f.get("probability", 0), f.get("szacowane_pozostalo") or 0), reverse=True)
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
        termin_h = f'<span class="termin" data-termin="{e(termin)}">{e(termin)}</span>' if termin and is_tak else ""
        kwota_h = f'<span class="kwota">{e(kwota)}</span>' if kwota and is_tak else ""
        snippet = r.get("snippet", "")[:200]
        sd = f'{r.get("urzad","")} {r.get("title","")} {snippet}'.lower()
        url = e(r.get("url", ""))
        woj = urzad_to_woj.get(r.get("urzad", ""), "")
        move_btn = f'<button class="btn-promote" onclick="promote(this)" title="Przeniес do naborów">&#x2714; Do naborów</button>' if show_move_btn else ""
        return f'''<div class="card" data-search="{e(sd)}" data-url="{url}" data-woj="{e(woj)}">
<div class="row1"><span class="urzad">{e(r.get("urzad",""))}</span>{badge}{termin_h}{kwota_h}{move_btn}{dt}</div>
<div class="title"><a href="{url}" target="_blank">{e(r.get("title",""))}</a></div>
<div class="snippet">{e(snippet)}</div>
<div class="ai">AI: {e(r.get("powod",""))}</div></div>'''

    def forecast_card(f):
        prob = int(f.get("probability", 0))
        level = f.get("level", "COLD").lower()
        woj = f.get("woj", "")
        sd = " ".join([
            f.get("urzad", ""),
            woj,
            " ".join(f.get("signals", [])),
            f.get("limit_roczny_text", ""),
            f.get("szacowane_pozostalo_text", ""),
        ]).lower()
        limit = f.get("limit_roczny_text") or "brak"
        allocated = f.get("suma_naborow_text") or "brak"
        remaining = f.get("szacowane_pozostalo_text") or "nieznane"
        info_badge = '<span class="ask-info">wniosek info</span>' if f.get("ask_public_info") else ""
        signals = "".join(f'<li>{e(x)}</li>' for x in f.get("signals", [])[:4])
        sources = "".join(
            f'<a href="{e(s.get("url",""))}" target="_blank" rel="noopener">{e((s.get("title") or "zrodlo")[:70])}</a>'
            for s in f.get("sources", [])[:3]
        )
        if sources:
            sources = f'<div class="forecast-sources">{sources}</div>'
        promo = ""
        if f.get("promotion_candidates"):
            promo = '<div class="forecast-warn">Wykryto kwoty przy promocji/badaniach/analizach KFS - warto zapytac o rozbicie.</div>'
        return f'''<div class="card forecast-card forecast-{level}" data-search="{e(sd)}" data-woj="{e(woj)}" data-prob="{prob}">
<div class="row1"><span class="urzad">{e(f.get("urzad",""))}</span><span class="prob prob-{level}">{prob}% {e(f.get("label",""))}</span>{info_badge}</div>
<div class="money-grid">
  <div><b>Limit roczny</b><span>{e(limit)}</span></div>
  <div><b>Suma naborow</b><span>{e(allocated)}</span></div>
  <div><b>Szac. zostalo</b><span>{e(remaining)}</span></div>
  <div><b>Nabory</b><span>{f.get("nabor_count", 0)} / aktywne {f.get("active_count", 0)}</span></div>
</div>
<ul class="forecast-signals">{signals}</ul>
{promo}
{sources}
</div>'''

    tak_cards = "\n".join(card(r) for r in confirmed)
    related_cards = "\n".join(card(r, show_move_btn=True) for r in related)
    forecast_cards = "\n".join(forecast_card(f) for f in forecasts)
    err_cards = "\n".join(
        f'<div class="card err-card"><div class="row1"><span class="urzad">{e(x.get("urzad",""))}</span>'
        f'<span class="err-kfs">{e(x.get("typ",""))}</span></div>'
        f'<div class="err-msg">{e(x.get("blad",""))}</div></div>'
        for x in errors
    )

    err_news = len([x for x in errors if x.get("typ") == "Aktualnosci"])
    err_kfs = len([x for x in errors if x.get("typ") == "KFS"])

    woj_options = '<option value="">Wszystkie wojewodztwa</option>'
    woj_options += "".join(f'<option value="{e(w)}">{e(w)}</option>' for w in all_woj)

    js_date_re = r"(\d{2})\.(\d{2})\.(\d{4})"

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
.termin.termin-red{{background:#fee2e2;color:#991b1b}}.termin.termin-yellow{{background:#fef3c7;color:#92400e}}
.termin.termin-green{{background:#dcfce7;color:#166534}}.termin.termin-expired{{background:#f3f4f6;color:#9ca3af;text-decoration:line-through}}
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
.tab.forecast-tab{{color:#7c3aed}}.tab.forecast-tab.active{{color:#7c3aed;border-bottom-color:#7c3aed}}
.forecast-card{{border-left:4px solid #9ca3af}}
.forecast-hot{{border-left-color:#dc2626}}.forecast-warm{{border-left-color:#f59e0b}}
.forecast-watch{{border-left-color:#2563eb}}.forecast-cold{{border-left-color:#9ca3af}}
.prob{{padding:2px 10px;border-radius:5px;font-size:12px;font-weight:700;text-transform:uppercase}}
.prob-hot{{background:#fee2e2;color:#991b1b}}.prob-warm{{background:#fef3c7;color:#92400e}}
.prob-watch{{background:#dbeafe;color:#1e40af}}.prob-cold{{background:#f3f4f6;color:#6b7280}}
.ask-info{{background:#ede9fe;color:#5b21b6;padding:2px 8px;border-radius:5px;font-size:12px;font-weight:700}}
.money-grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px;margin:8px 0 6px}}
.money-grid div{{background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:8px 10px}}
.money-grid b{{display:block;font-size:11px;color:#64748b;text-transform:uppercase;margin-bottom:2px}}
.money-grid span{{font-size:14px;font-weight:700;color:#1f2937}}
.forecast-signals{{margin:8px 0 0 18px;color:#4b5563;font-size:13px;line-height:1.45}}
.forecast-warn{{font-size:12px;color:#92400e;background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:8px 10px;margin-top:8px}}
.forecast-sources{{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}}
.forecast-sources a{{font-size:12px;color:#1e3a5f;background:#eff6ff;border-radius:6px;padding:4px 8px;text-decoration:none}}
.forecast-sources a:hover{{text-decoration:underline}}
.woj-filter{{width:100%;padding:10px 16px;font-size:15px;border:1px solid #d1d5db;border-radius:8px;margin-bottom:8px;outline:none;background:#fff}}
.woj-filter:focus{{border-color:#2d5a8e;box-shadow:0 0 0 3px rgba(45,90,142,.15)}}
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
@media(max-width:760px){{.money-grid{{grid-template-columns:repeat(2,minmax(0,1fr))}}}}
</style></head><body>
<div class="header">
<h1>Nabory KFS - Powiatowe Urzedy Pracy</h1>
<div class="meta">Aktualizacja: {now} | {count_tak} naborow KFS w {count_tak_powiaty} powiatach / {total} artykulow</div>
<div class="meta2">Automatyczna aktualizacja co 5h | Prognozy wysokie: {count_forecast_hot} | Do obserwacji: {count_forecast_watch} | Bledy: {count_err}</div>
</div>
<div class="tabs">
<div class="tab map-tab active" data-tab="map">&#x1F5FA; Mapa ({count_tak_powiaty})</div>
<div class="tab" data-tab="nabory">&#x1F7E2; Nabory KFS (<span id="cnt-nabory">{count_tak}</span>)</div>
<div class="tab forecast-tab" data-tab="forecast">&#x1F52E; Prognoza ({count_forecast_hot})</div>
<div class="tab related-tab" data-tab="related">&#x1F4C4; Artykuly KFS ({count_related})</div>
<div class="tab err-tab" data-tab="errors">&#x26A0; Bledy ({count_err})</div>
</div>
<div class="panel map-panel active" id="p-map"><div style="position:relative">
<div id="map"></div>
<div class="map-stats"><h4>Statystyki</h4>
<div>Nabory KFS: <span class="stat-num">{count_tak}</span></div>
<div>Powiaty z naborem: <span class="stat-num">{count_tak_powiaty}</span></div>
<div>Prognozy wysokie: <span class="stat-num">{count_forecast_hot}</span></div>
<div>Artykulow: {total}</div></div></div></div>
<div class="panel" id="p-nabory">
<select class="woj-filter" data-list="l-nabory">{woj_options}</select>
<input class="search" placeholder="Szukaj po urzedzie lub tytule..." data-list="l-nabory">
<div id="promoted-section" style="display:none"><h3 style="font-size:14px;color:#92400e;margin:12px 0 8px">Recznie przeniesione</h3><div id="l-promoted"></div></div>
<div id="l-nabory">{tak_cards}</div><div class="empty" id="e-nabory">Brak wynikow.</div></div>
<div class="panel" id="p-forecast">
<select class="woj-filter" data-list="l-forecast">{woj_options}</select>
<input class="search" placeholder="Szukaj w prognozach..." data-list="l-forecast">
<div id="l-forecast">{forecast_cards}</div><div class="empty" id="e-forecast">Brak wynikow.</div></div>
<div class="panel" id="p-related">
<select class="woj-filter" data-list="l-related">{woj_options}</select>
<input class="search" placeholder="Szukaj po urzedzie lub tytule..." data-list="l-related">
<div id="l-related">{related_cards}</div><div class="empty" id="e-related">Brak wynikow.</div></div>
<div class="panel" id="p-errors"><div id="l-errors">{err_cards}</div></div>
<script>
document.querySelectorAll(".tab").forEach(function(t){{t.addEventListener("click",function(){{
document.querySelectorAll(".tab").forEach(function(x){{x.classList.remove("active")}});
document.querySelectorAll(".panel").forEach(function(x){{x.classList.remove("active")}});
t.classList.add("active");document.getElementById("p-"+t.dataset.tab).classList.add("active");
if(t.dataset.tab==="map"&&window._map)window._map.invalidateSize()}});}});
function filterCards(panel){{
var list=panel.querySelector("[id^='l-']");if(!list)return;
var search=panel.querySelector(".search"),woj=panel.querySelector(".woj-filter");
var q=search?search.value.toLowerCase():"",w=woj?woj.value:"";
var cards=list.querySelectorAll(".card"),id=list.id.split("-")[1],empty=document.getElementById("e-"+id),n=0;
cards.forEach(function(c){{var okS=!q||c.dataset.search.indexOf(q)!==-1;var okW=!w||c.dataset.woj===w;
var vis=okS&&okW;c.classList.toggle("hidden",!vis);if(vis)n++}});
if(empty)empty.style.display=n===0?"block":"none"}}
document.querySelectorAll(".search,.woj-filter").forEach(function(el){{
el.addEventListener(el.tagName==="SELECT"?"change":"input",function(){{filterCards(el.closest(".panel"))}});}});

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
var m=[];if(a.termin)m.push(terminBadge(a.termin));if(a.kwota)m.push(a.kwota);
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
/* Countdown terminow */
function parseTerminDates(raw){{var re=new RegExp("{js_date_re}","g"),dates=[],m;
while((m=re.exec(raw))!==null)dates.push(new Date(parseInt(m[3]),parseInt(m[2])-1,parseInt(m[1])));
return dates}}
function terminDiff(raw){{var dates=parseTerminDates(raw);if(!dates.length)return null;
var end=dates[dates.length-1],today=new Date();today.setHours(0,0,0,0);
return Math.ceil((end-today)/86400000)}}
function terminBadge(termin){{var diff=terminDiff(termin);if(diff===null)return'<b>'+termin+'</b>';
var cls=diff<0?'termin-expired':diff<=3?'termin-red':diff<=7?'termin-yellow':'termin-green';
return'<b class="'+cls+'" style="padding:1px 5px;border-radius:4px">'+termin+'</b>'}}
(function(){{document.querySelectorAll(".termin[data-termin]").forEach(function(el){{
var raw=el.dataset.termin;if(!raw)return;var diff=terminDiff(raw);if(diff===null)return;
var suffix="";if(diff<0){{suffix=" (zakonczone)";el.classList.add("termin-expired")}}
else if(diff===0){{suffix=" (ostatni dzien!)";el.classList.add("termin-red")}}
else if(diff<=3){{suffix=" (jeszcze "+diff+" dn.)";el.classList.add("termin-red")}}
else if(diff<=7){{suffix=" (jeszcze "+diff+" dn.)";el.classList.add("termin-yellow")}}
else{{suffix=" (jeszcze "+diff+" dn.)";el.classList.add("termin-green")}}
el.textContent=el.textContent+suffix}});}})();
</script></body></html>'''

    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="\n") as f:
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
    results, new_tak_urls = classify_all(articles, cache)
    results = expand_multi_nabory(results)
    urzad_to_woj = {}
    if WOJ_FILE.exists():
        with open(WOJ_FILE, "r", encoding="utf-8") as f:
            urzad_to_woj = json.load(f)
    forecasts = build_budget_forecasts(results, urzad_to_woj)

    # Save cache
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)

    # Save results
    with open(RESULTS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    with open(FORECASTS_FILE, "w", encoding="utf-8", newline="\n") as f:
        json.dump(forecasts, f, ensure_ascii=False, indent=2)
    with open(ERRORS_FILE, "w", encoding="utf-8") as f:
        json.dump(errors, f, ensure_ascii=False, indent=2)

    # Push to CRM (zwraca listę nowo dodanych)
    newly_added = push_to_crm(results)

    # Email notification — tylko nowo dodane nabory
    send_email_notification(newly_added or [])

    # Generate report
    count_tak, count_powiaty = generate_report(results, errors, forecasts)

    # Summary
    print(f"\n{'='*60}")
    print(f"  GOTOWE!")
    print(f"  Nabory KFS: {count_tak} (w {count_powiaty} powiatach)")
    print(f"  Prognozy wysokie: {len([f for f in forecasts if f.get('probability', 0) >= 75])}")
    print(f"  Artykulow: {len(results)}")
    print(f"  Nowych (AI): {progress['done']}")
    print(f"  Z cache: {len(results) - progress['done']}")
    print(f"  Raport: {OUTPUT_FILE.name}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
