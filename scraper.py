#!/usr/bin/env python3
"""Scraper ogłoszeń z powiatowych urzędów pracy."""

import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import google.generativeai as genai

CACHE_FILE = Path(__file__).parent / "cache.json"
ARTICLES_FILE = Path(__file__).parent / "articles.json"
URZEDY_FILE = Path(__file__).parent / "urzedy.json"
REQUEST_DELAY = 0.5

GEMINI_PROMPT = """Jesteś asystentem analizującym ogłoszenia z powiatowych urzędów pracy w Polsce.
Oceń czy poniższy tekst dotyczy aktywnego NABORU WNIOSKÓW - czyli czy urząd ogłasza przyjmowanie wniosków o dofinansowanie (np. KFS, staże, prace interwencyjne, dotacje, refundacje).

NIE klasyfikuj jako naboru: ogólne informacje o programach, statystyki, targi pracy, porady, zmiany przepisów.
TAK klasyfikuj: ogłoszenia z datami przyjmowania wniosków, kwotami do rozdysponowania, warunkami ubiegania się o środki.

Odpowiedz TYLKO jedną linią w formacie:
WYNIK: TAK lub NIE
POWOD: max 10 słów po polsku

Tytuł: {title}
Treść: {content}"""

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
})


def load_cache() -> dict:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def fetch_page(url: str) -> BeautifulSoup | None:
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.RequestException as e:
        print(f"  [BŁĄD] Nie udało się pobrać {url}: {e}")
        return None


def extract_articles_from_homepage(soup: BeautifulSoup, base_url: str) -> list[dict]:
    """Wyciąga artykuły z sekcji aktualności strony Liferay CMS."""
    articles = []

    # Strategia 1: szukaj w divach journal-content-article
    containers = soup.find_all("div", class_=re.compile(r"journal-content-article"))

    # Strategia 2: szukaj asset-abstract (typowa klasa Liferay dla list artykułów)
    if not containers:
        containers = soup.find_all("div", class_=re.compile(r"asset-abstract"))

    # Strategia 3: szukaj sekcji z h3 tagami linkującymi do artykułów
    if not containers:
        containers = [soup]

    for container in containers:
        h3_tags = container.find_all("h3")
        for h3 in h3_tags:
            link = h3.find("a", href=True)
            if not link:
                continue

            title = h3.get_text(strip=True)
            if not title:
                continue

            href = link["href"]
            if not href.startswith("http"):
                href = urljoin(base_url, href)

            # Szukaj zajawki - pierwszy paragraf po h3
            snippet = ""
            next_p = h3.find_next("p")
            if next_p:
                snippet = next_p.get_text(strip=True)

            # Szukaj daty publikacji - tekst z datą DD.MM.YYYY przed h3
            pub_date = ""
            # Szukaj w elementach rodzeństwa i rodzicach
            prev = h3.find_previous(string=re.compile(r"\d{2}\.\d{2}\.\d{4}"))
            if prev:
                date_match = re.search(r"\d{2}\.\d{2}\.\d{4}", prev)
                if date_match:
                    pub_date = date_match.group()

            # Sprawdź też klasy typowe dla dat w Liferay
            if not pub_date:
                date_el = h3.find_previous(class_=re.compile(r"(date|data|czas|time|modified)"))
                if date_el:
                    date_match = re.search(r"\d{2}\.\d{2}\.\d{4}", date_el.get_text())
                    if date_match:
                        pub_date = date_match.group()

            articles.append({
                "title": title,
                "url": href,
                "snippet": snippet[:500],
                "date": pub_date,
            })

    return articles


def extract_kfs_content(soup: BeautifulSoup) -> str:
    """Wyciąga główną treść ze strony KFS."""
    # Szukaj głównego kontentu
    content_div = soup.find("div", class_=re.compile(r"journal-content-article"))
    if not content_div:
        content_div = soup.find("div", class_=re.compile(r"journal-content"))
    if not content_div:
        content_div = soup.find("div", id=re.compile(r"content|main"))
    if not content_div:
        content_div = soup.find("main")
    if not content_div:
        content_div = soup

    text = content_div.get_text(separator="\n", strip=True)
    # Ogranicz długość
    return text[:3000]


def classify_with_gemini(model, title: str, content: str) -> dict:
    """Klasyfikuje artykuł za pomocą Gemini API."""
    prompt = GEMINI_PROMPT.format(title=title, content=content[:2000])

    try:
        response = model.generate_content(prompt)
        text = response.text.strip()

        # Parsuj odpowiedź
        wynik = "NIE"
        powod = ""

        for line in text.split("\n"):
            line = line.strip()
            if line.upper().startswith("WYNIK:"):
                val = line.split(":", 1)[1].strip().upper()
                if "TAK" in val:
                    wynik = "TAK"
                else:
                    wynik = "NIE"
            elif line.upper().startswith("POWOD:") or line.upper().startswith("POWÓD:"):
                powod = line.split(":", 1)[1].strip()

        return {"wynik": wynik, "powod": powod}
    except Exception as e:
        print(f"  [BŁĄD] Gemini API: {e}")
        return {"wynik": "NIE", "powod": f"Błąd API: {e}"}


def main():
    # Sprawdź klucz API
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("BŁĄD: Brak zmiennej środowiskowej GEMINI_API_KEY")
        sys.exit(1)

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.0-flash")

    # Wczytaj urzędy
    with open(URZEDY_FILE, "r", encoding="utf-8") as f:
        urzedy = json.load(f)

    print(f"Załadowano {len(urzedy)} urzędów z {URZEDY_FILE.name}")

    cache = load_cache()

    # Statystyki
    total = 0
    from_cache = 0
    from_gemini = 0
    count_tak = 0
    count_nie = 0

    results_tak = []
    all_saved_articles = []

    for i, urzad in enumerate(urzedy, 1):
        name = urzad["name"]
        homepage = urzad.get("homepage", "")
        kfs_url = urzad.get("kfs_url", "")

        print(f"\n[{i}/{len(urzedy)}] {name}")

        all_articles = []

        # --- Scrapuj stronę główną / aktualności ---
        scrape_url = urzad.get("aktualnosci_url", homepage)
        print(f"  Pobieram aktualności: {scrape_url}")
        soup = fetch_page(scrape_url)
        time.sleep(REQUEST_DELAY)

        if soup:
            articles = extract_articles_from_homepage(soup, scrape_url)
            for a in articles:
                a["source_type"] = "Aktualności"
            print(f"  Znaleziono {len(articles)} artykułów")
            all_articles.extend(articles)
        else:
            print("  Brak artykułów (błąd pobierania)")

        # --- Scrapuj stronę KFS ---
        if kfs_url:
            print(f"  Pobieram KFS: {kfs_url}")
            kfs_soup = fetch_page(kfs_url)
            time.sleep(REQUEST_DELAY)

            if kfs_soup:
                kfs_text = extract_kfs_content(kfs_soup)
                all_articles.append({
                    "title": f"KFS - {name}",
                    "url": kfs_url,
                    "snippet": kfs_text,
                    "date": "",
                    "source_type": "KFS",
                })
            else:
                print("  Brak treści KFS (błąd pobierania)")

        # --- Klasyfikuj artykuły ---
        for art in all_articles:
            total += 1
            url = art["url"]
            art["urzad"] = name
            content = art["snippet"] or art["title"]

            if url in cache:
                result = cache[url]
                from_cache += 1
                label = result["wynik"]
            else:
                result = classify_with_gemini(model, art["title"], content)
                from_gemini += 1
                cache[url] = result
                save_cache(cache)
                label = result["wynik"]

            all_saved_articles.append(art)

            if label == "TAK":
                count_tak += 1
                results_tak.append({
                    "urzad": name,
                    "title": art["title"],
                    "url": url,
                    "date": art.get("date", ""),
                    "powod": result.get("powod", ""),
                })
                print(f"  ✓ TAK: {art['title'][:60]}")
            else:
                count_nie += 1

    # Zapisz wszystkie artykuły
    with open(ARTICLES_FILE, "w", encoding="utf-8") as f:
        json.dump(all_saved_articles, f, ensure_ascii=False, indent=2)
    print(f"\nZapisano {len(all_saved_articles)} artykułów do {ARTICLES_FILE.name}")

    # --- Podsumowanie ---
    print("\n" + "=" * 60)
    print("PODSUMOWANIE")
    print("=" * 60)
    print(f"Artykułów łącznie:       {total}")
    print(f"Nowych (wysłanych do AI): {from_gemini}")
    print(f"Z cache:                  {from_cache}")
    print(f"Sklasyfikowane TAK:       {count_tak}")
    print(f"Sklasyfikowane NIE:       {count_nie}")

    if results_tak:
        print(f"\n{'=' * 60}")
        print("AKTYWNE NABORY:")
        print("=" * 60)
        for r in results_tak:
            date_str = f" ({r['date']})" if r["date"] else ""
            print(f"  [{r['urzad']}]{date_str} {r['title']}")
            print(f"    {r['url']}")
            if r["powod"]:
                print(f"    Powód: {r['powod']}")


if __name__ == "__main__":
    main()
